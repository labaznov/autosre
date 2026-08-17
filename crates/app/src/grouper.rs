//! Группировка отклонений в инциденты.
//!
//! Порядок шагов важен: сперва образцы и сигнатура, потом ключ, и только потом
//! всё остальное. Пилот делал наоборот — ходил в модель первым делом, а
//! дедупликацию проверял по полю, которое сама модель и заполняла; строки не
//! совпадали никогда, и подавление не сработало ни разу.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sre_domain::signature::groups;
use sre_domain::{Deviation, Minute, Service, Signature, Span};
use sre_source::Source;
use sre_store::Store;

use crate::config::Incidents;
use crate::metrics::Metrics;

/// Заводит группировку: разбирает свежие отклонения и закрывает затихшее.
pub fn group(
    sources: &[Arc<dyn Source>],
    store: &Store,
    metrics: &Arc<Metrics>,
    settings: &Incidents,
) {
    let sources: HashMap<String, Arc<dyn Source>> = sources
        .iter()
        .map(|source| (source.name().to_owned(), Arc::clone(source)))
        .collect();
    let store = store.clone();
    let metrics = Arc::clone(metrics);
    let settings = settings.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_mins(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            sort(&sources, &store, &metrics, &settings).await;
            quiet(&store, &settings).await;
        }
    });
}

/// Разбирает отклонения, у которых ещё нет инцидента.
async fn sort(
    sources: &HashMap<String, Arc<dyn Source>>,
    store: &Store,
    metrics: &Metrics,
    settings: &Incidents,
) {
    let loose = match store.loose(settings.batch).await {
        Ok(loose) => loose,
        Err(failure) => {
            metrics.failure();
            tracing::error!(%failure, "отклонения не прочитаны");
            return;
        }
    };
    for (id, deviation) in loose {
        let Some(source) = sources.get(&deviation.source) else {
            tracing::warn!(
                source = deviation.source,
                "источник неизвестен, отклонение пропущено"
            );
            continue;
        };
        let signature = signature(source.as_ref(), &deviation, settings).await;
        let service = Service::of(&deviation.stream, &settings.service_labels);
        match store.attach(id, &service, &signature, &deviation).await {
            Ok((incident, fresh)) if fresh => {
                let apart = i64::try_from(settings.link.as_secs()).unwrap_or(300);
                let near = store.link(incident, apart).await.unwrap_or_default();
                metrics.incident();
                tracing::info!(
                    incident,
                    service = service.as_str(),
                    signature = signature.as_str(),
                    value = deviation.value,
                    related = near,
                    "инцидент заведён"
                );
            }
            Ok((incident, _)) => {
                tracing::debug!(incident, "отклонение подтвердило инцидент");
            }
            Err(failure) => {
                metrics.failure();
                tracing::error!(%failure, "отклонение не привязано");
            }
        }
    }
}

/// Сигнатура самой частой ошибки окна.
///
/// Образцов может не быть вовсе: у метрик их нет по природе, а логи могли
/// уехать по retention. Тогда сигнатурой становится горизонт с потоком — беда
/// всё равно должна дойти до дежурного, пусть и без узнаваемого имени.
async fn signature(source: &dyn Source, deviation: &Deviation, settings: &Incidents) -> Signature {
    let width = i64::try_from(settings.sample_window.as_secs() / 60).unwrap_or(15);
    let span = Span::new(deviation.at.back(width), deviation.at, width + 1)
        .unwrap_or_else(|_| Span::single(deviation.at));
    let messages = source
        .samples(&deviation.stream, span, settings.samples)
        .await
        .unwrap_or_else(|failure| {
            tracing::warn!(%failure, "образцы не получены");
            Vec::new()
        });
    groups(&messages, 1).first().map_or_else(
        || Signature::of(&format!("без образцов: {}", deviation.horizon)),
        |group| group.signature.clone(),
    )
}

/// Закрывает инциденты, о которых давно нет вестей.
async fn quiet(store: &Store, settings: &Incidents) {
    let silence = i64::try_from(settings.silence.as_secs() / 60).unwrap_or(30);
    let before = Minute::of(Utc::now()).back(silence);
    match store.hush(before).await {
        Ok(closed) if closed > 0 => tracing::info!(closed, "инциденты закрыты тишиной"),
        Ok(_) => {}
        Err(failure) => tracing::error!(%failure, "инциденты не закрыты"),
    }
}
