//! Минута — единица наблюдения.
//!
//! Агент снимает счётчики минутными бакетами и складывает их у себя, а горизонт
//! считается поверх накопленного ряда как сумма последних N минут
//! ([ADR-0013](../../../docs/adr/0013-minute-buckets.md)). Поэтому минута —
//! не деталь реализации, а тип: границы выровнены, арифметика в одном месте.

use chrono::{DateTime, TimeDelta, Utc};

/// Минута, выровненная по абсолютной границе.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Minute(i64);

/// Секунд в минуте.
const SECONDS: i64 = 60;

impl Minute {
    /// Минута, в которую попадает момент времени.
    #[must_use]
    pub fn of(at: DateTime<Utc>) -> Self {
        let stamp = at.timestamp();
        Self(stamp - stamp.rem_euclid(SECONDS))
    }

    /// Минута по числу секунд от эпохи; значение выравнивается вниз.
    #[must_use]
    pub fn at(stamp: i64) -> Self {
        Self(stamp - stamp.rem_euclid(SECONDS))
    }

    #[must_use]
    pub fn stamp(self) -> i64 {
        self.0
    }

    #[must_use]
    pub fn start(self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.0, 0).unwrap_or_default()
    }

    #[must_use]
    pub fn end(self) -> DateTime<Utc> {
        self.next().start()
    }

    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + SECONDS)
    }

    #[must_use]
    pub fn previous(self) -> Self {
        Self(self.0 - SECONDS)
    }

    /// Минута, отстоящая назад на указанное число минут.
    #[must_use]
    pub fn back(self, count: i64) -> Self {
        Self(self.0 - count * SECONDS)
    }
}

/// Отказы построения промежутка.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SpanError {
    #[error("промежуток пуст: конец не позже начала")]
    Empty,
    #[error("промежуток из {0} минут превышает предел {1}")]
    Oversized(i64, i64),
}

/// Промежуток минут `[from, to)` — то, что снимается одним запросом.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    from: Minute,
    to: Minute,
}

impl Span {
    /// Промежуток от одной минуты до другой, не включая последнюю.
    ///
    /// # Errors
    /// [`SpanError::Empty`], если конец не позже начала, [`SpanError::Oversized`],
    /// если минут больше предела.
    pub fn new(from: Minute, to: Minute, limit: i64) -> Result<Self, SpanError> {
        if to <= from {
            return Err(SpanError::Empty);
        }
        let length = (to.stamp() - from.stamp()) / SECONDS;
        if length > limit {
            return Err(SpanError::Oversized(length, limit));
        }
        Ok(Self { from, to })
    }

    /// Промежуток из одной минуты.
    #[must_use]
    pub fn single(minute: Minute) -> Self {
        Self {
            from: minute,
            to: minute.next(),
        }
    }

    #[must_use]
    pub fn from(self) -> Minute {
        self.from
    }

    #[must_use]
    pub fn to(self) -> Minute {
        self.to
    }

    #[must_use]
    pub fn len(self) -> usize {
        usize::try_from((self.to.stamp() - self.from.stamp()) / SECONDS).unwrap_or(0)
    }

    #[must_use]
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn width(self) -> TimeDelta {
        self.to.start() - self.from.start()
    }

    #[must_use]
    pub fn contains(self, minute: Minute) -> bool {
        minute >= self.from && minute < self.to
    }

    /// Минуты промежутка по порядку.
    #[must_use]
    pub fn minutes(self) -> Vec<Minute> {
        let mut walk = self.from;
        let mut all = Vec::with_capacity(self.len());
        while walk < self.to {
            all.push(walk);
            walk = walk.next();
        }
        all
    }
}
