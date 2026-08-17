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

/// Как числа минут складываются в час при свёртке.
///
/// Счётчик ошибок за час — сумма минутных. Занятая память за час — среднее:
/// сумма здесь бессмысленна. Знает об этом источник, а не хранилище, поэтому
/// признак едет вместе с бакетом.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Kind {
    /// Складывается: события, запросы, ошибки.
    Sum,
    /// Усредняется: уровни — память, место на диске, задержка.
    Mean,
}

impl Kind {
    #[must_use]
    pub fn code(self) -> i64 {
        match self {
            Self::Sum => 0,
            Self::Mean => 1,
        }
    }

    #[must_use]
    pub fn of(code: i64) -> Self {
        match code {
            1 => Self::Mean,
            _ => Self::Sum,
        }
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
    pub kind: Kind,
}

impl Bucket {
    /// Бакет-счётчик: складывается при свёртке.
    #[must_use]
    pub fn counted(stream: Stream, minute: Minute, value: f64) -> Self {
        Self {
            stream,
            minute,
            value,
            kind: Kind::Sum,
        }
    }

    /// Бакет-уровень: усредняется при свёртке.
    #[must_use]
    pub fn level(stream: Stream, minute: Minute, value: f64) -> Self {
        Self {
            stream,
            minute,
            value,
            kind: Kind::Mean,
        }
    }
}

impl Serialize for Minute {
    fn serialize<S: serde::Serializer>(&self, out: S) -> Result<S::Ok, S::Error> {
        out.serialize_i64(self.stamp())
    }
}
