//! Съём наблюдений: раз в минуту у каждого источника.
//!
//! Снимается **последняя закрытая** минута: в текущей счётчик заведомо неполон,
//! а неполная минута в ряду хуже отсутствующей — её никто не считает дырой.
//!
//! Отказ источника не отмечает минуту снятой. Так пропуск остаётся дырой, и
//! дозапрос при старте её закроет.

use std::sync::Arc;
use std::time::Duration;

use autosre_domain::{Bucket, Hour, Minute, Span};
use autosre_source::Source;
use autosre_store::{Stale, Store};
use chrono::Utc;

use crate::config::{Collecting, Retention};
use crate::metrics::Metrics;

/// Каждый сколький круг съёма закрывает дыры.
///
/// Не каждый: когда источник лежит, дозапрос — это столько же напрасных
/// обращений, сколько дыр в глубине. Раз в пять минут их немного, а слепота
/// после часа простоя кончается через пять минут, а не после перезапуска.
pub const HEAL_EVERY: u64 = 5;

/// Съём одного источника: минута за минутой и дозапрос дыр.
#[derive(Clone)]
pub struct Collector {
    source: Arc<dyn Source>,
    store: Store,
    metrics: Arc<Metrics>,
    /// Насколько глубоко закрывать дыры.
    depth: Duration,
    /// Сколько минут просить у источника за один запрос.
    chunk: Duration,
}

impl Collector {
    #[must_use]
    pub fn new(
        source: Arc<dyn Source>,
        store: &Store,
        metrics: &Arc<Metrics>,
        depth: Duration,
        chunk: Duration,
    ) -> Self {
        Self {
            source,
            store: store.clone(),
            metrics: Arc::clone(metrics),
            depth,
            chunk,
        }
    }

    /// Один круг: дозапрос дыр, если его черёд, и съём последней закрытой минуты.
    ///
    /// Нулевой круг — старт: дыры от простоя закрываются до первой минуты.
    pub async fn round(&self, tick: u64) {
        if tick.is_multiple_of(HEAL_EVERY) {
            heal(
                self.source.as_ref(),
                &self.store,
                &self.metrics,
                self.depth,
                self.chunk,
            )
            .await;
        }
        minute(self.source.as_ref(), &self.store, &self.metrics).await;
    }
}

/// Заводит съём наблюдений по всем источникам.
pub fn collect(
    sources: Vec<Arc<dyn Source>>,
    store: &Store,
    metrics: &Arc<Metrics>,
    settings: &Collecting,
    depth: Duration,
) {
    for source in sources {
        let collector = Collector::new(source, store, metrics, depth, settings.chunk);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_mins(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut tick = 0;
            loop {
                ticker.tick().await;
                collector.round(tick).await;
                tick += 1;
            }
        });
    }
}

/// Сколько приглушение хранится после срока: чтобы можно было спросить, кто
/// и почему заглушил ([ADR-0022](../../../docs/adr/0022-retention.md)).
const MUTES_AFTER: Duration = Duration::from_hours(90 * 24);

/// Заводит уборку: свёртку минут в часы и забвение просроченного.
pub fn tidy(sources: &[Arc<dyn Source>], store: &Store, settings: &Retention) {
    let names: Vec<String> = sources
        .iter()
        .map(|source| source.name().to_owned())
        .collect();
    let store = store.clone();
    let minutes = settings.minute_buckets;
    let hours = settings.hour_buckets;
    let investigations = settings.investigations;
    let incidents = settings.incidents;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_hours(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            for name in &names {
                sweep(name, &store, minutes, hours).await;
            }
            let now = Minute::of(Utc::now());
            // Отклонения без инцидента живут столько же, сколько расследования:
            // это тот же род данных — что видел агент и что решил.
            for (stale, after) in [
                (Stale::Investigations, investigations),
                (Stale::Deviations, investigations),
                (Stale::Incidents, incidents),
                (Stale::Mutes, MUTES_AFTER),
            ] {
                prune(&store, stale, now.back(minutes_of(after))).await;
                tokio::task::yield_now().await;
            }
        }
    });
}

/// Забывает просроченное одного рода и пишет об этом в журнал.
async fn prune(store: &Store, stale: Stale, edge: Minute) {
    match store.prune(stale, edge).await {
        Ok(gone) if gone > 0 => tracing::info!(?stale, rows = gone, "просроченное забыто"),
        Ok(_) => {}
        Err(failure) => tracing::error!(?stale, %failure, "забыть просроченное не удалось"),
    }
}

/// Длительность в минутах: сроки хранения меряются днями и в `i64` влезают.
fn minutes_of(duration: Duration) -> i64 {
    i64::try_from(duration.as_secs() / 60).unwrap_or(i64::MAX)
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
