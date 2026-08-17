//! Поток и бакет — то, из чего складывается ряд.

use serde::Serialize;

use crate::minute::Minute;

/// Селектор источника: то, что источник считает отдельной струёй записей.
///
/// Для логов это `{container="orders-api",host="node-01"}`, для метрик — имя серии
/// с метками. Агент не разбирает селектор и не додумывает его: строка приходит
/// из источника и уходит обратно в запрос как есть.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Stream(String);

impl Stream {
    #[must_use]
    pub fn new(selector: impl Into<String>) -> Self {
        Self(selector.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Stream {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str(&self.0)
    }
}

/// Числовой факт за одну минуту.
///
/// У логов это счётчик событий, у метрик — среднее значение либо приращение
/// ([ADR-0016](../../../docs/adr/0016-observed-metrics.md)). Тип один, потому
/// что дальше по конвейеру разницы нет: детектор считает по ряду чисел.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Bucket {
    pub stream: Stream,
    pub minute: Minute,
    pub value: f64,
}

impl Bucket {
    #[must_use]
    pub fn new(stream: Stream, minute: Minute, value: f64) -> Self {
        Self {
            stream,
            minute,
            value,
        }
    }
}

impl Serialize for Minute {
    fn serialize<S: serde::Serializer>(&self, out: S) -> Result<S::Ok, S::Error> {
        out.serialize_i64(self.stamp())
    }
}
