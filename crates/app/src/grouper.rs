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

        // Приглушение проверяется до отсева: платить модели за то, что человек
        // уже велел не показывать, незачем
        // ([ADR-0019](../../../docs/adr/0019-muting-instead-of-per-service-thresholds.md)).
        if let Ok(Some(mute)) = store
            .muted(
                &service,
                &signature,
                &deviation.stream,
                Minute::of(Utc::now()),
            )
            .await
        {
            if let Err(failure) = store.hush_deviation(id, mute).await {
                tracing::error!(%failure, "приглушённое отклонение не помечено");
            }
            metrics.hushed();
            tracing::debug!(
                service = service.as_str(),
                signature = signature.as_str(),
                mute,
                "отклонение приглушено человеком и копится тихо"
            );
            continue;
        }

        // Отсев: стоит ли этим заниматься. Модель молчит — заводим инцидент
        // всё равно: находка обязана дойти до дежурного даже без объяснения.
        // Отсев спрашивают до того, как инцидент существует, поэтому заход
        // придерживается и записывается ниже, когда номер станет известен:
        // урок без инцидента нечем разметить, а метка отсева — самая ценная.
        let (answer, told) = crate::scribe::watch(model.triage(&prompt::about(
            &deviation,
            service.as_str(),
            &groups,
        )))
        .await;
        let because = match answer {
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
                // Отсеянное тоже урок, и метка у него есть: модель сказала
                // «шум», и дальше видно, была ли она права.
                crate::scribe::keep(store, metrics, told, (None, None)).await;
                continue;
            }
            Ok(triage) => triage.because,
            Err(failure) => {
                metrics.failure();
                tracing::warn!(%failure, "отсев не удался, инцидент заводится без него");
                "разбор отсева не состоялся: модель недоступна".to_owned()
            }
        };

        let attached = store
            .attach(id, &service, &signature, &deviation, &because)
            .await;
        if let Ok((incident, _)) = attached {
            crate::scribe::keep(store, metrics, told, (Some(incident), None)).await;
        }
        match attached {
            Ok((incident, fresh)) if fresh => {
                opened(
                    store,
                    metrics,
                    settings,
                    (incident, &service, &signature),
                    &deviation,
                )
                .await;
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

/// Заведённый инцидент: связи, числа и подозрительная тишина рядом.
async fn opened(
    store: &Store,
    metrics: &Metrics,
    settings: &Incidents,
    about: (i64, &Service, &Signature),
    deviation: &Deviation,
) {
    let (incident, service, signature) = about;
    let apart = i64::try_from(settings.link.as_secs()).unwrap_or(300);
    let near = store.link(incident, apart).await.unwrap_or_default();
    metrics.incident();
    metrics.detected(Utc::now().timestamp() - deviation.at.stamp());
    // Инцидент по сервису, который дежурный уже просил помолчать, но с другой
    // сигнатурой: приглушение обошли стороной. Пока это число мало, точного
    // совпадения пары достаточно.
    if store
        .quieted(service, Minute::of(Utc::now()))
        .await
        .unwrap_or(false)
    {
        metrics.dodged();
        tracing::info!(
            incident,
            service = service.as_str(),
            signature = signature.as_str(),
            "инцидент мимо действующего приглушения того же сервиса"
        );
    }
    tracing::info!(
        incident,
        service = service.as_str(),
        signature = signature.as_str(),
        value = deviation.value,
        related = near,
        "инцидент заведён"
    );
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
/// уехать по retention. Тогда сигнатурой становится имя серии — оно и есть то,
/// что дежурный назовёт вслух.
///
/// Собирается напрямую, а не через маскирование: там числа заменяются
/// заглушками, и «15m» превращается в «<n>m» — имя, которого никто не поймёт.
fn pick(groups: &[sre_domain::Group], deviation: &Deviation) -> Signature {
    if let Some(group) = groups.first() {
        return group.signature.clone();
    }
    let series = Service::of(&deviation.stream, &["__series__".to_owned()]);
    Signature::stored(format!("{series} на горизонте {}", deviation.horizon))
}

/// Закрывает инциденты, о которых давно нет вестей.
async fn quiet(store: &Store, settings: &Incidents) {
    let silence = i64::try_from(settings.silence.as_secs() / 60).unwrap_or(30);
    let now = Minute::of(Utc::now());
    match store.hush(now.back(silence), now).await {
        Ok(closed) if closed > 0 => tracing::info!(closed, "инциденты закрыты тишиной"),
        Ok(_) => {}
        Err(failure) => tracing::error!(%failure, "инциденты не закрыты"),
    }
}
