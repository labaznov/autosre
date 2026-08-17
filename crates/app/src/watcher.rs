//! Оценка горизонтов: считает окна поверх накопленного ряда и ищет отклонения.
//!
//! Задержку обнаружения задаёт **период оценки**, а не ширина окна: горизонт в
//! пятнадцать минут, пересчитываемый каждую минуту, замечает беду за те же две
//! минуты, что и пятиминутный ([ADR-0014](../../../docs/adr/0014-horizons-are-configurable.md)).
//!
//! Одно и то же отклонение будет замечено несколько раз подряд, пока держится:
//! окно скользит, а беда стоит. Дубли гасит не детектор, а группировка в
//! инцидент.
//!
//! Окна, которые агент не снимал, в счёт не идут. Иначе пустая история читается
//! как «ошибок не было», медиана выходит нулевой, и после каждого запуска агент
//! находит отклонение в любом живом сервисе.

use std::sync::Arc;

use chrono::Utc;
use sre_domain::{Detector, Deviation, Minute, Thresholds};
use sre_source::Source;
use sre_store::Store;

use crate::config::Horizon;
use crate::metrics::Metrics;

/// Заводит оценку по всем включённым горизонтам.
pub fn watch(
    sources: &[Arc<dyn Source>],
    store: &Store,
    metrics: &Arc<Metrics>,
    horizons: &[&Horizon],
) {
    for horizon in horizons {
        let names: Vec<String> = sources
            .iter()
            .map(|source| source.name().to_owned())
            .collect();
        let store = store.clone();
        let metrics = Arc::clone(metrics);
        let horizon = Watch::of(horizon);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(horizon.period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                for name in &names {
                    look(name, &store, &metrics, &horizon).await;
                }
            }
        });
    }
}

/// Горизонт в том виде, в каком он нужен оценке.
#[derive(Debug, Clone)]
struct Watch {
    name: String,
    period: std::time::Duration,
    /// Ширина окна в минутах.
    width: usize,
    /// Сколько прошлых окон составляют базовую линию.
    history: usize,
    detector: Detector,
}

impl Watch {
    fn of(horizon: &Horizon) -> Self {
        Self {
            name: horizon.name.clone(),
            period: horizon.period,
            width: usize::try_from(horizon.width.as_secs() / 60)
                .unwrap_or(15)
                .max(1),
            history: horizon.history,
            detector: Detector::new(Thresholds {
                minimum: horizon.minimum,
                score: horizon.score,
                ratio: horizon.ratio,
                drift: horizon.drift,
            }),
        }
    }
}

/// Какая доля минут окна должна быть снята, чтобы окну можно было верить.
const ENOUGH: usize = 80;

/// Какая доля окон базовой линии должна быть известна, чтобы судить.
const READY: usize = 50;

/// Один проход оценки: последнее закрытое окно против его истории.
async fn look(source: &str, store: &Store, metrics: &Metrics, horizon: &Watch) {
    let end = Minute::of(Utc::now());
    let series = match store
        .windows(source, end, horizon.width, horizon.history + 1)
        .await
    {
        Ok(series) => series,
        Err(failure) => {
            metrics.failure();
            tracing::error!(source, horizon = horizon.name, %failure, "окна не собраны");
            return;
        }
    };

    let known = match store
        .covered(source, end, horizon.width, horizon.history + 1)
        .await
    {
        Ok(known) => known,
        Err(failure) => {
            metrics.failure();
            tracing::error!(source, horizon = horizon.name, %failure, "покрытие не собрано");
            return;
        }
    };
    let whole: Vec<usize> = known
        .iter()
        .enumerate()
        .filter(|(_, minutes)| **minutes * 100 >= horizon.width * ENOUGH)
        .map(|(index, _)| index)
        .collect();
    if !whole.contains(&0) || whole.len() * 100 < (horizon.history + 1) * READY {
        tracing::debug!(
            source,
            horizon = horizon.name,
            known = whole.len(),
            of = horizon.history + 1,
            "базовой линии ещё нет, горизонт пропущен"
        );
        return;
    }

    let mut found = 0;
    for (stream, (kind, windows)) in &series {
        let Some(value) = windows.first() else {
            continue;
        };
        let history: Vec<f64> = whole
            .iter()
            .filter(|index| **index > 0)
            .filter_map(|index| windows.get(*index).copied())
            .collect();
        let verdict = horizon.detector.verdict(&history, *value, *kind);
        if !verdict.deviates {
            continue;
        }
        let deviation = Deviation::new(source, stream, &horizon.name, end, verdict);
        match store.spot(&deviation, end).await {
            Ok(true) => {
                found += 1;
                tracing::info!(
                    source,
                    horizon = horizon.name,
                    stream = stream.as_str(),
                    value = verdict.value,
                    baseline = verdict.baseline,
                    score = verdict.score,
                    weight = verdict.weight,
                    "отклонение"
                );
            }
            Ok(false) => {}
            Err(failure) => {
                metrics.failure();
                tracing::error!(source, %failure, "отклонение не записано");
            }
        }
    }
    metrics.deviations(found);
    tracing::debug!(
        source,
        horizon = horizon.name,
        streams = series.len(),
        found,
        "горизонт оценён"
    );
}
