//! Съём наблюдений: раз в минуту у каждого источника.
//!
//! Снимается **последняя закрытая** минута: в текущей счётчик заведомо неполон,
//! а неполная минута в ряду хуже отсутствующей — её никто не считает дырой.
//!
//! Отказ источника не отмечает минуту снятой. Так пропуск остаётся дырой, и
//! дозапрос при старте её закроет.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sre_domain::{Bucket, Hour, Minute, Span};
use sre_source::Source;
use sre_store::Store;

use crate::config::{Collector, Retention};
use crate::metrics::Metrics;

/// Заводит съём наблюдений по всем источникам.
pub fn collect(
    sources: Vec<Arc<dyn Source>>,
    store: &Store,
    metrics: &Arc<Metrics>,
    settings: &Collector,
    depth: Duration,
) {
    for source in sources {
        let store = store.clone();
        let metrics = Arc::clone(metrics);
        let chunk = settings.chunk;
        tokio::spawn(async move {
            heal(source.as_ref(), &store, &metrics, depth, chunk).await;
            let mut ticker = tokio::time::interval(Duration::from_mins(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                minute(source.as_ref(), &store, &metrics).await;
            }
        });
    }
}

/// Заводит уборку: свёртку минут в часы и забвение просроченного.
pub fn tidy(sources: &[Arc<dyn Source>], store: &Store, settings: &Retention) {
    let names: Vec<String> = sources
        .iter()
        .map(|source| source.name().to_owned())
        .collect();
    let store = store.clone();
    let minutes = settings.minute_buckets;
    let hours = settings.hour_buckets;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_hours(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            for name in &names {
                sweep(name, &store, minutes, hours).await;
            }
        }
    });
}

/// Закрывает дыры в ряду, оставшиеся от простоя.
///
/// Глубина ограничена настройкой: после недельного простоя восстанавливать
/// неделю поминутно незачем — источник столько и не отдаст, а базовая линия
/// соберётся заново за пару часов.
async fn heal(
    source: &dyn Source,
    store: &Store,
    metrics: &Metrics,
    depth: Duration,
    chunk: Duration,
) {
    let now = Minute::of(Utc::now());
    let deep = i64::try_from(depth.as_secs() / 60).unwrap_or(120).max(1);
    let Ok(window) = Span::new(now.back(deep), now, deep + 1) else {
        return;
    };
    let holes = match store.gaps(source.name(), window).await {
        Ok(holes) => holes,
        Err(failure) => {
            tracing::error!(source = source.name(), %failure, "дыры в ряду не найдены");
            return;
        }
    };
    if holes.is_empty() {
        return;
    }
    let limit = usize::try_from(chunk.as_secs() / 60).unwrap_or(60).max(1);
    let runs = Span::runs(&holes, limit);
    tracing::info!(
        source = source.name(),
        minutes = holes.len(),
        requests = runs.len(),
        "дозапрашиваю пропущенное"
    );
    for run in runs {
        snap(source, store, metrics, run).await;
    }
}

/// Снимает последнюю закрытую минуту.
async fn minute(source: &dyn Source, store: &Store, metrics: &Metrics) {
    let closed = Minute::of(Utc::now()).previous();
    snap(source, store, metrics, Span::single(closed)).await;
}

/// Снимает промежуток и сохраняет его.
async fn snap(source: &dyn Source, store: &Store, metrics: &Metrics, span: Span) {
    let buckets = match source.buckets(span).await {
        Ok(buckets) => keep(buckets, span, source.name()),
        Err(failure) => {
            metrics.failure();
            tracing::warn!(
                source = source.name(),
                from = span.from().stamp(),
                minutes = span.len(),
                %failure,
                "промежуток не снят и остался дырой"
            );
            return;
        }
    };
    match store.save(source.name(), span, buckets).await {
        Ok(saved) => {
            metrics.bucket(span.to().start().into());
            tracing::info!(
                source = source.name(),
                from = span.from().stamp(),
                minutes = span.len(),
                buckets = saved,
                "промежуток снят"
            );
        }
        Err(failure) => {
            metrics.failure();
            tracing::error!(source = source.name(), %failure, "промежуток не сохранён");
        }
    }
}

/// Сворачивает просроченные минуты в часы и забывает то, чей срок вышел.
async fn sweep(source: &str, store: &Store, minutes: Duration, hours: Duration) {
    let now = Minute::of(Utc::now());
    let edge = now.back(i64::try_from(minutes.as_secs() / 60).unwrap_or(10_080));
    loop {
        match store.roll(source, edge).await {
            Ok(rolled) if rolled.more => {
                tracing::info!(source, streams = rolled.streams, "час свёрнут");
                // Уступаем очередь: свёртка идёт по часу за раз именно затем,
                // чтобы съём минут не ждал её окончания.
                tokio::task::yield_now().await;
            }
            Ok(_) => break,
            Err(failure) => {
                tracing::error!(source, %failure, "свёртка не удалась");
                return;
            }
        }
    }
    let old = Hour::of(now.back(i64::try_from(hours.as_secs() / 60).unwrap_or(568_800)));
    match store.forget(source, edge, old).await {
        Ok(gone) if gone > 0 => tracing::info!(source, rows = gone, "просроченное забыто"),
        Ok(_) => {}
        Err(failure) => tracing::error!(source, %failure, "забыть просроченное не удалось"),
    }
}

/// Отбрасывает бакеты, попавшие мимо запрошенного промежутка.
///
/// Источник с разъехавшимися часами вернёт минуту, которую агент не запрашивал.
/// Такой бакет лёг бы в минуту, не отмеченную снятой: значение есть, а по
/// отметкам это дыра — ряд перестаёт сходиться сам с собой.
fn keep(buckets: Vec<Bucket>, span: Span, source: &str) -> Vec<Bucket> {
    let (inside, outside): (Vec<_>, Vec<_>) = buckets
        .into_iter()
        .partition(|bucket| span.contains(bucket.minute));
    if !outside.is_empty() {
        tracing::warn!(
            source,
            dropped = outside.len(),
            "источник вернул минуты вне запрошенного промежутка: разошлись часы"
        );
    }
    inside
}
