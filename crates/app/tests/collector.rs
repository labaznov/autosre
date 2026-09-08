use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use autosre_app::collector::{Collector, HEAL_EVERY};
use autosre_app::metrics::Metrics;
use autosre_domain::{Bucket, Minute, Span, Stream};
use autosre_source::{Source, SourceError};
use autosre_store::Store;
use tempfile::TempDir;

/// Источник, отказывающий первые `fails` раз, потом отдающий по бакету на минуту.
struct Shaky {
    fails: AtomicUsize,
    asked: AtomicUsize,
}

#[async_trait]
impl Source for Shaky {
    fn name(&self) -> &'static str {
        "logs"
    }

    async fn buckets(&self, span: Span) -> Result<Vec<Bucket>, SourceError> {
        self.asked.fetch_add(1, Ordering::Relaxed);
        if self
            .fails
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return Err(SourceError::Transport("лежит".to_owned()));
        }
        Ok(span
            .minutes()
            .into_iter()
            .map(|minute| Bucket::counted(Stream::new("{a=\"b\"}"), minute, 1.0))
            .collect())
    }
}

struct Stand {
    store: Store,
    source: Arc<Shaky>,
    collector: Collector,
    _directory: TempDir,
}

impl Stand {
    fn with(fails: usize) -> Self {
        let directory = TempDir::new().expect("временный каталог не создан");
        let store = Store::open(&directory.path().join("autosre.db")).expect("база не открыта");
        let source = Arc::new(Shaky {
            fails: AtomicUsize::new(fails),
            asked: AtomicUsize::new(0),
        });
        let collector = Collector::new(
            Arc::clone(&source) as Arc<dyn Source>,
            &store,
            &Arc::new(Metrics::new("тест")),
            Duration::from_mins(5),
            Duration::from_hours(1),
        );
        Self {
            store,
            source,
            collector,
            _directory: directory,
        }
    }

    /// Дыры за последние пять минут.
    async fn holes(&self) -> Vec<Minute> {
        let now = Minute::of(chrono::Utc::now());
        let window = Span::new(now.back(5), now, 6).unwrap();
        self.store.gaps("logs", window).await.unwrap()
    }
}

#[tokio::test]
async fn leaves_a_hole_when_the_source_is_down() {
    let stand = Stand::with(1);
    stand.collector.round(1).await;
    assert!(
        stand
            .holes()
            .await
            .contains(&Minute::of(chrono::Utc::now()).previous())
    );
}

#[tokio::test]
async fn closes_the_holes_on_a_healing_round() {
    let stand = Stand::with(1);
    stand.collector.round(1).await;
    stand.collector.round(HEAL_EVERY).await;
    assert!(stand.holes().await.is_empty());
}

#[tokio::test]
async fn heals_on_the_first_round_after_start() {
    let stand = Stand::with(0);
    stand.collector.round(0).await;
    assert!(stand.holes().await.is_empty());
}

#[tokio::test]
async fn does_not_bother_the_source_when_there_are_no_holes() {
    let stand = Stand::with(0);
    stand.collector.round(0).await;
    let asked = stand.source.asked.load(Ordering::Relaxed);
    stand.collector.round(HEAL_EVERY).await;
    assert_eq!(stand.source.asked.load(Ordering::Relaxed), asked + 1);
}
