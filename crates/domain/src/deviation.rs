//! Отклонение — окно, вышедшее за пороги детектора.
//!
//! Решение числовое и воспроизводимое: те же данные и те же пороги дают тот же
//! ответ. Всё, что дальше — отсев и расследование, — работает моделью, и вот
//! там воспроизводимости уже нет.

use serde::Serialize;

use crate::bucket::Stream;
use crate::detector::Verdict;
use crate::minute::Minute;

/// Найденное отклонение вместе с числами, которые к нему привели.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Deviation {
    /// Имя источника: `logs`, `metrics` и дальше.
    pub source: String,
    pub stream: Stream,
    /// Имя горизонта, на котором нашлось.
    pub horizon: String,
    /// Конец окна: момент, к которому отклонение относится.
    pub at: Minute,
    pub value: f64,
    pub baseline: f64,
    pub score: f64,
    pub weight: f64,
}

impl Deviation {
    #[must_use]
    pub fn new(source: &str, stream: &Stream, horizon: &str, at: Minute, verdict: Verdict) -> Self {
        Self {
            source: source.to_owned(),
            stream: stream.clone(),
            horizon: horizon.to_owned(),
            at,
            value: verdict.value,
            baseline: verdict.baseline,
            score: verdict.score,
            weight: verdict.weight,
        }
    }

    /// Во сколько раз окно превысило обычное. Бесконечность, если обычным был ноль.
    #[must_use]
    pub fn times(&self) -> f64 {
        if self.baseline > 0.0 {
            self.value / self.baseline
        } else {
            f64::INFINITY
        }
    }
}
