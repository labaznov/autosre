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

    /// Секунды от эпохи. В таком виде минута хранится в базе и уходит в запрос
    /// к источнику.
    #[must_use]
    pub fn stamp(self) -> i64 {
        self.0
    }

    /// Начало минуты — момент, который в неё включён.
    #[must_use]
    pub fn start(self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.0, 0).unwrap_or_default()
    }

    /// Конец минуты — момент, который в неё **не** включён.
    ///
    /// Совпадает с началом следующей: промежутки полуоткрыты, `[start, end)`.
    /// Иначе соседние минуты делили бы одну секунду, и она считалась бы дважды.
    #[must_use]
    pub fn end(self) -> DateTime<Utc> {
        self.next().start()
    }

    /// Следующая минута. Это шаг по ряду, а не конец текущей минуты: конец —
    /// [`Minute::end`], и он момент времени, а не минута.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + SECONDS)
    }

    /// Предыдущая минута.
    #[must_use]
    pub fn previous(self) -> Self {
        Self(self.0 - SECONDS)
    }

    /// Минута, отстоящая назад на указанное число минут.
    ///
    /// Отрицательное число двигает вперёд: `back(-5)` — это пять минут спустя.
    /// Так пишется обход окна в одну сторону без второго метода и без знака,
    /// разбросанного по вызовам.
    #[must_use]
    pub fn back(self, count: i64) -> Self {
        Self(self.0 - count * SECONDS)
    }
}

/// Час, выровненный по абсолютной границе: единица свёрнутого ряда.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hour(i64);

/// Секунд в часе.
const HOURLY: i64 = 3600;

impl Hour {
    /// Час, в который попадает минута.
    #[must_use]
    pub fn of(minute: Minute) -> Self {
        Self(minute.stamp() - minute.stamp().rem_euclid(HOURLY))
    }

    /// Час по числу секунд от эпохи; значение выравнивается вниз.
    #[must_use]
    pub fn at(stamp: i64) -> Self {
        Self(stamp - stamp.rem_euclid(HOURLY))
    }

    /// Секунды от эпохи: в таком виде час лежит в свёрнутом ряду.
    #[must_use]
    pub fn stamp(self) -> i64 {
        self.0
    }

    /// Следующий час.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + HOURLY)
    }

    /// Минуты часа: промежуток от его начала до начала следующего.
    #[must_use]
    pub fn span(self) -> Span {
        Span {
            from: Minute::at(self.0),
            to: Minute::at(self.next().0),
        }
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

    /// Первая минута промежутка. Она в него входит.
    #[must_use]
    pub fn from(self) -> Minute {
        self.from
    }

    /// Минута **за** промежутком: она в него уже не входит.
    ///
    /// Так граница одного промежутка совпадает с началом следующего, и при
    /// склейке соседних минута не считается дважды.
    #[must_use]
    pub fn to(self) -> Minute {
        self.to
    }

    /// Сколько минут в промежутке.
    #[must_use]
    pub fn len(self) -> usize {
        usize::try_from((self.to.stamp() - self.from.stamp()) / SECONDS).unwrap_or(0)
    }

    /// Пуст ли промежуток. Собранный [`Span::new`] пустым не бывает — он для
    /// этого и возвращает ошибку.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Ширина промежутка временем, а не числом минут: в таком виде её ждут
    /// запросы к источникам.
    #[must_use]
    pub fn width(self) -> TimeDelta {
        self.to.start() - self.from.start()
    }

    /// Входит ли минута в промежуток. Последняя минута — та, что перед
    /// [`Span::to`], сама `to` уже снаружи.
    #[must_use]
    pub fn contains(self, minute: Minute) -> bool {
        minute >= self.from && minute < self.to
    }

    /// Собирает подряд идущие минуты в промежутки не длиннее предела.
    ///
    /// Дыры в ряду редко идут одной полосой: агент мог падать несколько раз.
    /// Просить каждую минуту отдельным запросом расточительно, а одним
    /// запросом на всё — значит просить и то, что уже снято.
    #[must_use]
    pub fn runs(minutes: &[Minute], limit: usize) -> Vec<Self> {
        let mut runs: Vec<Self> = Vec::new();
        for minute in minutes {
            match runs.last_mut() {
                Some(run) if run.to == *minute && run.len() < limit => run.to = minute.next(),
                _ => runs.push(Self::single(*minute)),
            }
        }
        runs
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
