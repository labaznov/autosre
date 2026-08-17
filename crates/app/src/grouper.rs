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

use sre_model::{Model, prompt};

use crate::config::Incidents;
use crate::metrics::Metrics;

/// Заводит группировку: разбирает свежие отклонения и закрывает затихшее.
pub fn group(
    sources: &[Arc<dyn Source>],
    store: &Store,
    metrics: &Arc<Metrics>,
    model: &Arc<Model>,
    settings: &Incidents,
) {
    let sources: HashMap<String, Arc<dyn Source>> = sources
        .iter()
        .map(|source| (source.name().to_owned(), Arc::clone(source)))
        .collect();
    let store = store.clone();
    let metrics = Arc::clone(metrics);
    let model = Arc::clone(model);
    let settings = settings.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_mins(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            sort(&sources, &store, &metrics, &model, &settings).await;
            quiet(&store, &settings).await;
        }
    });
}

/// Разбирает отклонения, у которых ещё нет инцидента.
async fn sort(
    sources: &HashMap<String, Arc<dyn Source>>,
    store: &Store,
    metrics: &Metrics,
    model: &Model,
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
        let groups = groups(
            &messages(source.as_ref(), &deviation, settings).await,
            settings.groups,
        );
        let signature = pick(&groups, &deviation);
        let service = Service::of(&deviation.stream, &settings.service_labels);

        // Отсев: стоит ли этим заниматься. Модель молчит — заводим инцидент
        // всё равно: находка обязана дойти до дежурного даже без объяснения.
        let because = match model
            .triage(&prompt::about(&deviation, service.as_str(), &groups))
            .await
        {
            Ok(triage) if !triage.worth => {
                metrics.sifted();
                tracing::info!(
                    service = service.as_str(),
                    because = triage.because,
                    "отклонение отсеяно как привычный шум"
                );
                if let Err(failure) = store.sift(id, &triage.because).await {
                    tracing::error!(%failure, "отсев не записан");
                }
                continue;
            }
            Ok(triage) => triage.because,
            Err(failure) => {
                metrics.failure();
                tracing::warn!(%failure, "отсев не удался, инцидент заводится без него");
                "разбор отсева не состоялся: модель недоступна".to_owned()
            }
        };

        match store
            .attach(id, &service, &signature, &deviation, &because)
            .await
        {
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

/// Живые записи потока за окно отклонения.
async fn messages(source: &dyn Source, deviation: &Deviation, settings: &Incidents) -> Vec<String> {
    let width = i64::try_from(settings.sample_window.as_secs() / 60).unwrap_or(15);
    let span = Span::new(deviation.at.back(width), deviation.at, width + 1)
        .unwrap_or_else(|_| Span::single(deviation.at));
    source
        .samples(&deviation.stream, span, settings.samples)
        .await
        .unwrap_or_else(|failure| {
            tracing::warn!(%failure, "образцы не получены");
            Vec::new()
        })
}

/// Сигнатура самой частой ошибки окна.
///
/// Образцов может не быть вовсе: у метрик их нет по природе, а логи могли
/// уехать по retention. Тогда сигнатурой становится сама серия или горизонт —
/// беда должна дойти до дежурного, пусть и без узнаваемого имени.
fn pick(groups: &[sre_domain::Group], deviation: &Deviation) -> Signature {
    groups.first().map_or_else(
        || {
            Signature::of(&format!(
                "{} на горизонте {}",
                deviation.source, deviation.horizon
            ))
        },
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
