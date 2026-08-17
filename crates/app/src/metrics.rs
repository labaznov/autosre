//! Собственные метрики агента.
//!
//! Их собирает `VictoriaMetrics`, а алерты живут в существующем мониторинге
//! ([ADR-0024](../../../docs/adr/0024-agent-under-watch.md)). Главный
//! показатель — не время работы процесса, а момент последнего снятого бакета:
//! агент бывает жив и слеп, и это то же самое, что лежать.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Показатели агента, доступные снаружи.
#[derive(Debug)]
pub struct Metrics {
    version: &'static str,
    started: u64,
    last_bucket: AtomicU64,
    failures: AtomicU64,
    deviations: AtomicU64,
}

impl Metrics {
    #[must_use]
    pub fn new(version: &'static str) -> Self {
        Self {
            version,
            started: stamp(SystemTime::now()),
            last_bucket: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            deviations: AtomicU64::new(0),
        }
    }

    /// Отмечает, что бакет снят.
    pub fn bucket(&self, at: SystemTime) {
        self.last_bucket.store(stamp(at), Ordering::Relaxed);
    }

    /// Отмечает найденные отклонения.
    pub fn deviations(&self, found: usize) {
        self.deviations
            .fetch_add(found.try_into().unwrap_or(0), Ordering::Relaxed);
    }

    /// Отмечает отказ источника или модели.
    pub fn failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Выкладка в формате, который понимает Prometheus.
    ///
    /// Момента последнего бакета может не быть вовсе — до первого снятия его
    /// не выдумываем. Возраст считает тот, кто настраивает алерт: `time()`
    /// минус этот момент.
    #[must_use]
    pub fn expose(&self) -> String {
        let mut out = String::new();
        gauge(&mut out, "sre_up", "Агент отвечает", "1");
        gauge(
            &mut out,
            "sre_started_timestamp_seconds",
            "Момент запуска агента",
            &self.started.to_string(),
        );
        let _ = writeln!(
            out,
            "# HELP sre_build_info Версия агента\n# TYPE sre_build_info gauge\nsre_build_info{{version=\"{}\"}} 1",
            self.version
        );
        let last = self.last_bucket.load(Ordering::Relaxed);
        if last > 0 {
            gauge(
                &mut out,
                "sre_last_bucket_timestamp_seconds",
                "Момент последнего снятого бакета",
                &last.to_string(),
            );
        }
        counter(
            &mut out,
            "sre_deviations_total",
            "Найденные отклонения",
            &self.deviations.load(Ordering::Relaxed).to_string(),
        );
        counter(
            &mut out,
            "sre_source_failures_total",
            "Отказы источников и модели",
            &self.failures.load(Ordering::Relaxed).to_string(),
        );
        out
    }
}

fn gauge(out: &mut String, name: &str, help: &str, value: &str) {
    let _ = writeln!(
        out,
        "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}"
    );
}

fn counter(out: &mut String, name: &str, help: &str, value: &str) {
    let _ = writeln!(
        out,
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}"
    );
}

fn stamp(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default()
}
