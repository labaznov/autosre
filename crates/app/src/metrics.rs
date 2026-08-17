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
    incidents: AtomicU64,
    sifted: AtomicU64,
    concluded: AtomicU64,
    skipped: AtomicU64,
    notes: AtomicU64,
    reports: AtomicU64,
    hushed: AtomicU64,
    drafts: AtomicU64,
    inquiries: AtomicU64,
    answered: AtomicU64,
    detection: Spread,
    conclusion: Spread,
}

/// Разброс времени по корзинам — гистограмма в понимании Prometheus.
///
/// Процентиль агент не считает: это работа мониторинга, у которого есть все
/// экземпляры и все окна ([ADR-0024](../../../docs/adr/0024-agent-under-watch.md)).
/// Своё среднее арифметическое было бы числом, которым удобно отчитываться и
/// нельзя пользоваться.
#[derive(Debug)]
struct Spread {
    name: &'static str,
    about: &'static str,
    /// Границы корзин в секундах, по возрастанию.
    bounds: &'static [u64],
    counts: Vec<AtomicU64>,
    sum: AtomicU64,
    total: AtomicU64,
}

impl Spread {
    fn new(name: &'static str, about: &'static str, bounds: &'static [u64]) -> Self {
        Self {
            name,
            about,
            bounds,
            counts: (0..bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum: AtomicU64::new(0),
            total: AtomicU64::new(0),
        }
    }

    /// Отмечает одно измерение. Отрицательное время — разошедшиеся часы, а не
    /// мгновенный ответ: такое измерение не учитывается вовсе.
    fn see(&self, seconds: i64) {
        let Ok(seconds) = u64::try_from(seconds) else {
            tracing::warn!(
                self.name,
                seconds,
                "измерение времени отброшено: часы разошлись"
            );
            return;
        };
        for (index, edge) in self.bounds.iter().enumerate() {
            if seconds <= *edge {
                self.counts[index].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.sum.fetch_add(seconds, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    fn expose(&self, out: &mut String) {
        let total = self.total.load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "# HELP {} {}\n# TYPE {} histogram",
            self.name, self.about, self.name
        );
        for (index, edge) in self.bounds.iter().enumerate() {
            let _ = writeln!(
                out,
                "{}_bucket{{le=\"{edge}\"}} {}",
                self.name,
                self.counts[index].load(Ordering::Relaxed)
            );
        }
        let _ = writeln!(out, "{}_bucket{{le=\"+Inf\"}} {total}", self.name);
        let _ = writeln!(
            out,
            "{}_sum {}\n{}_count {total}",
            self.name,
            self.sum.load(Ordering::Relaxed),
            self.name
        );
    }
}

/// Корзины времени до обнаружения: цель — пять минут по 90-му процентилю
/// ([SPEC §12](../../../docs/SPEC.md)), поэтому вокруг неё их гуще.
const DETECTION: &[u64] = &[60, 120, 300, 600, 900, 1800, 3600];

/// Корзины времени до вывода: цель — пятнадцать минут по 90-му процентилю.
const CONCLUSION: &[u64] = &[300, 600, 900, 1800, 3600, 7200];

impl Metrics {
    #[must_use]
    pub fn new(version: &'static str) -> Self {
        Self {
            version,
            started: stamp(SystemTime::now()),
            last_bucket: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            deviations: AtomicU64::new(0),
            incidents: AtomicU64::new(0),
            sifted: AtomicU64::new(0),
            concluded: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            notes: AtomicU64::new(0),
            reports: AtomicU64::new(0),
            hushed: AtomicU64::new(0),
            drafts: AtomicU64::new(0),
            inquiries: AtomicU64::new(0),
            answered: AtomicU64::new(0),
            detection: Spread::new(
                "sre_detection_seconds",
                "Время от наблюдения с отклонением до заведения инцидента",
                DETECTION,
            ),
            conclusion: Spread::new(
                "sre_conclusion_seconds",
                "Время от первого наблюдения инцидента до готового вывода",
                CONCLUSION,
            ),
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

    /// Отмечает заведённый инцидент.
    pub fn incident(&self) {
        self.incidents.fetch_add(1, Ordering::Relaxed);
    }

    /// Отмечает отклонение, отсеянное как привычный шум.
    pub fn sifted(&self) {
        self.sifted.fetch_add(1, Ordering::Relaxed);
    }

    /// Отмечает готовый вывод расследования.
    pub fn concluded(&self) {
        self.concluded.fetch_add(1, Ordering::Relaxed);
    }

    /// Отмечает инцидент, дошедший до дежурного без вывода.
    pub fn skipped(&self) {
        self.skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Запоминает, сколько заметок в поисковом индексе.
    pub fn notes(&self, count: usize) {
        self.notes
            .store(count.try_into().unwrap_or(0), Ordering::Relaxed);
    }

    /// Отмечает собранный отчёт.
    pub fn reported(&self) {
        self.reports.fetch_add(1, Ordering::Relaxed);
    }

    /// Отмечает, за сколько секунд отклонение стало инцидентом.
    ///
    /// Начало отсчёта — момент наблюдения, а не момент, когда агент до него
    /// добрался: дежурному важно, сколько беда прожила незамеченной, а не
    /// сколько агент думал ([ADR-0010](../../../docs/adr/0010-incident-aggregate.md)).
    pub fn detected(&self, seconds: i64) {
        self.detection.see(seconds);
    }

    /// Отмечает, за сколько секунд инцидент дошёл до вывода.
    pub fn explained(&self, seconds: i64) {
        self.conclusion.see(seconds);
    }

    /// Отмечает отклонение, приглушённое человеком.
    pub fn hushed(&self) {
        self.hushed.fetch_add(1, Ordering::Relaxed);
    }

    /// Отмечает написанный черновик заметки.
    pub fn drafted(&self) {
        self.drafts.fetch_add(1, Ordering::Relaxed);
    }

    /// Отмечает оставленную дежурному заявку.
    pub fn inquiry(&self) {
        self.inquiries.fetch_add(1, Ordering::Relaxed);
    }

    /// Отмечает заявку, на которую дежурный ответил.
    pub fn answered(&self) {
        self.answered.fetch_add(1, Ordering::Relaxed);
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
            "sre_incidents_total",
            "Заведённые инциденты",
            &self.incidents.load(Ordering::Relaxed).to_string(),
        );
        counter(
            &mut out,
            "sre_sifted_total",
            "Отклонения, отсеянные как привычный шум",
            &self.sifted.load(Ordering::Relaxed).to_string(),
        );
        counter(
            &mut out,
            "sre_conclusions_total",
            "Готовые выводы расследований",
            &self.concluded.load(Ordering::Relaxed).to_string(),
        );
        counter(
            &mut out,
            "sre_skipped_total",
            "Инциденты, дошедшие до дежурного без вывода",
            &self.skipped.load(Ordering::Relaxed).to_string(),
        );
        gauge(
            &mut out,
            "sre_notes",
            "Заметки базы знаний в поисковом индексе",
            &self.notes.load(Ordering::Relaxed).to_string(),
        );
        counter(
            &mut out,
            "sre_reports_total",
            "Собранные отчёты",
            &self.reports.load(Ordering::Relaxed).to_string(),
        );
        counter(
            &mut out,
            "sre_hushed_total",
            "Отклонения, приглушённые человеком",
            &self.hushed.load(Ordering::Relaxed).to_string(),
        );
        counter(
            &mut out,
            "sre_drafts_total",
            "Написанные черновики заметок",
            &self.drafts.load(Ordering::Relaxed).to_string(),
        );
        counter(
            &mut out,
            "sre_inquiries_total",
            "Заявки, оставленные дежурному",
            &self.inquiries.load(Ordering::Relaxed).to_string(),
        );
        counter(
            &mut out,
            "sre_inquiries_answered_total",
            "Заявки, на которые дежурный ответил",
            &self.answered.load(Ordering::Relaxed).to_string(),
        );
        self.detection.expose(&mut out);
        self.conclusion.expose(&mut out);
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
