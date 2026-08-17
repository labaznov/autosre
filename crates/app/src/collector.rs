//! Съём наблюдений: раз в минуту у каждого источника.
//!
//! Снимается **последняя закрытая** минута: в текущей счётчик заведомо неполон,
//! а неполная минута в ряду хуже отсутствующей — её никто не считает дырой.
//!
//! Отказ источника не отмечает минуту снятой. Так пропуск остаётся дырой, и
//! дозапрос при следующем удобном случае её закроет.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sre_domain::{Minute, Span};
use sre_source::Source;
use sre_store::Store;

use crate::metrics::Metrics;

/// Заводит съём наблюдений по всем источникам.
pub fn collect(sources: Vec<Arc<dyn Source>>, store: &Store, metrics: &Arc<Metrics>) {
    for source in sources {
        let store = store.clone();
        let metrics = Arc::clone(metrics);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_mins(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                minute(source.as_ref(), &store, &metrics).await;
            }
        });
    }
}

/// Отбрасывает бакеты, попавшие мимо запрошенного промежутка.
///
/// Источник с разъехавшимися часами вернёт минуту, которую агент не запрашивал.
/// Такой бакет лёг бы в минуту, не отмеченную снятой: значение есть, а по
/// отметкам это дыра — ряд перестаёт сходиться сам с собой.
fn keep(buckets: Vec<sre_domain::Bucket>, span: Span, source: &str) -> Vec<sre_domain::Bucket> {
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

/// Снимает последнюю закрытую минуту.
async fn minute(source: &dyn Source, store: &Store, metrics: &Metrics) {
    let closed = Minute::of(Utc::now()).previous();
    let span = Span::single(closed);
    let buckets = match source.buckets(span).await {
        Ok(buckets) => keep(buckets, span, source.name()),
        Err(failure) => {
            metrics.failure();
            tracing::warn!(
                source = source.name(),
                minute = closed.stamp(),
                %failure,
                "минута не снята и осталась дырой"
            );
            return;
        }
    };
    match store.save(source.name(), span, buckets).await {
        Ok(saved) => {
            metrics.bucket(closed.end().into());
            tracing::info!(
                source = source.name(),
                minute = closed.stamp(),
                buckets = saved,
                "минута снята"
            );
        }
        Err(failure) => {
            metrics.failure();
            tracing::error!(source = source.name(), %failure, "минута не сохранена");
        }
    }
}
