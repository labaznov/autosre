//! Инцидент — то, к чему липнет всё остальное.
//!
//! Ключ узкий: сервис плюс сигнатура ([ADR-0017](../../../docs/adr/0017-narrow-grouping.md)).
//! Широкая группировка склеила бы ночную выгрузку с чужим выкатом, узкая при
//! общей причине заведёт десяток карточек — второе лечится подсказкой о связи,
//! первое не лечится ничем.

use serde::Serialize;

use crate::bucket::Stream;
use crate::minute::Minute;
use crate::signature::Signature;

/// Имя сервиса, вытащенное из селектора потока.
///
/// Селектор источник отдаёт как есть — `{host="node-01",service="orders-api"}`, — а
/// человеку и ключу инцидента нужно имя. Какая метка его несёт, знает только
/// площадка, поэтому список меток — настройка.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Service(String);

impl Service {
    /// Достаёт имя сервиса из селектора по первой подошедшей метке.
    ///
    /// Не нашлось ни одной — сервисом становится сам селектор: потерять поток
    /// хуже, чем показать дежурному длинное имя.
    #[must_use]
    pub fn of(stream: &Stream, labels: &[String]) -> Self {
        let text = stream.as_str();
        for label in labels {
            if let Some(value) = value(text, label) {
                return Self(value);
            }
        }
        Self(text.to_owned())
    }

    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Service {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str(&self.0)
    }
}

/// Значение метки в селекторе вида `{a="b",c="d"}`.
fn value(selector: &str, label: &str) -> Option<String> {
    let head = format!("{label}=\"");
    let start = selector.find(&head)? + head.len();
    let rest = selector.get(start..)?;
    let end = rest.find('"')?;
    Some(rest.get(..end)?.to_owned())
}

/// Состояние инцидента.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    /// Подтверждения приходят.
    Open,
    /// Закрыт тишиной или человеком.
    Closed,
    /// Дежурный не знал, что с ним делать: закрыт без разбора.
    Abandoned,
    /// Влит в другой инцидент: дежурный решил, что это одно и то же.
    Merged,
}

impl State {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
            Self::Abandoned => "abandoned",
            Self::Merged => "merged",
        }
    }

    #[must_use]
    pub fn of(text: &str) -> Self {
        match text {
            "closed" => Self::Closed,
            "abandoned" => Self::Abandoned,
            "merged" => Self::Merged,
            _ => Self::Open,
        }
    }
}

/// Важность: насколько всё плохо.
///
/// Ставит её модель в выводе — в отличие от веса, который считает агент
/// ([SPEC §3](../../../docs/SPEC.md)). Вес — про очередь, про то, кого
/// разбирать первым; важность — про человека, про то, насколько всё плохо.
/// Их легко перепутать, поэтому они разные типы и разные слова.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum Severity {
    /// Работает, но хуже обычного.
    Low,
    /// Часть работы теряется или замедлена заметно для людей.
    Medium,
    /// Отказ: работа не делается.
    High,
}

impl Severity {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }

    /// Важность из строки; неизвестное слово — средняя.
    ///
    /// Не низкая: модель, ответившая мимо словаря, — не повод считать, что
    /// всё хорошо.
    #[must_use]
    pub fn of(text: &str) -> Self {
        match text {
            "low" => Self::Low,
            "high" => Self::High,
            _ => Self::Medium,
        }
    }
}

/// Инцидент: сгруппированные отклонения об одной беде.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Incident {
    pub id: i64,
    pub service: Service,
    pub signature: Signature,
    pub stream: Stream,
    pub source: String,
    pub state: State,
    /// Момент первого наблюдения: от него считается время до обнаружения.
    pub began: Minute,
    /// Момент последнего подтверждения.
    pub last: Minute,
    /// Сколько отклонений к нему прилипло.
    pub seen: u64,
    /// Наибольшее значение за всё время инцидента.
    pub peak: f64,
    /// Наибольший вес: по нему инцидент попадает в очередь.
    pub weight: f64,
    /// Оценка дежурного: по делу или ложный.
    pub verdict: Option<bool>,
    /// Почему отсев счёл, что этим стоит заняться. Пусто, если модель молчала.
    pub because: Option<String>,
    /// Важность: пусто, пока не было вывода.
    pub severity: Option<Severity>,
}

/// Счёт инцидентов и оценок — основание метрик приёмки.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Tally {
    pub total: u64,
    pub useful: u64,
    pub useless: u64,
    pub open: u64,
}

impl Tally {
    /// Доля ложных среди оценённых; `None`, пока никто не оценивал.
    ///
    /// Считается по оценённым, а не по всем: неоценённый инцидент не говорит
    /// ни за, ни против, и включать его в знаменатель значит выдавать
    /// нерасторопность дежурного за качество агента.
    #[must_use]
    pub fn wrong(&self) -> Option<f64> {
        let judged = self.useful + self.useless;
        (judged > 0).then(|| ratio(self.useless) / ratio(judged))
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "число инцидентов далеко ниже предела точности f64"
)]
fn ratio(count: u64) -> f64 {
    count as f64
}
