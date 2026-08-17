//! Чтение базы знаний и поддержание поискового индекса.
//!
//! Заметки правят люди в чужом репозитории
//! ([ADR-0015](../../../docs/adr/0015-knowledge-repository.md)), поэтому
//! каталог перечитывается на ходу: между правкой заметки и её появлением в
//! поиске не должно стоять развёртывание.
//!
//! Индекс собирается заново целиком. На сотнях заметок это дешевле, чем
//! выяснять, что именно изменилось, и надёжнее: расходиться с диском ему
//! попросту негде ([ADR-0008](../../../docs/adr/0008-full-text-knowledge-search.md)).

use std::sync::Arc;

use sre_store::{Memory, Store};

use crate::config::Knowledge;
use crate::metrics::Metrics;

/// Заводит перечитывание базы знаний.
pub fn keep(store: &Store, metrics: &Arc<Metrics>, settings: &Knowledge) {
    let (store, metrics, settings) = (store.clone(), Arc::clone(metrics), settings.clone());
    tokio::spawn(async move {
        let mut known = None;
        let mut ticker =
            tokio::time::interval(settings.refresh.max(std::time::Duration::from_secs(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let touched = sre_knowledge::touched(&settings.notes).ok();
            if touched.is_some() && touched == known {
                continue;
            }
            known = touched;
            learn(&store, &metrics, &settings).await;
        }
    });
}

/// Перечитывает каталог заметок и пересобирает индекс.
pub async fn learn(store: &Store, metrics: &Arc<Metrics>, settings: &Knowledge) {
    let notes = match sre_knowledge::read(&settings.notes) {
        Ok(notes) => notes,
        Err(failure) => {
            tracing::warn!(
                path = %settings.notes.display(),
                %failure,
                "база знаний не прочитана: расследования пойдут без неё"
            );
            return;
        }
    };
    let learned = notes
        .iter()
        .map(|note| Memory {
            name: note.front.name.clone(),
            title: note.front.title.clone(),
            tags: note.words(),
            marks: note.marks(),
            body: note.body.clone(),
        })
        .collect();
    match store.remember(learned).await {
        Ok(count) => {
            metrics.notes(count);
            tracing::info!(notes = count, "база знаний прочитана");
        }
        Err(failure) => tracing::error!(%failure, "индекс базы знаний не собран"),
    }
}
