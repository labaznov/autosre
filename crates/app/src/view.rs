//! Подготовка инцидентов к показу.
//!
//! Форматирование живёт здесь, а не в шаблонах: в шаблон приходят готовые
//! строки, и вёрстка не знает ни про типы предметной области, ни про
//! округления, ни про бесконечности.

use chrono::{TimeZone, Utc};
use sre_domain::{Incident, Minute, State};

/// Инцидент в том виде, в каком его читает человек.
pub struct Card {
    pub id: i64,
    pub service: String,
    pub signature: String,
    pub stream: String,
    pub source: String,
    pub state: &'static str,
    pub open: bool,
    pub began: String,
    pub last: String,
    pub alive: String,
    pub seen: u64,
    pub peak: String,
    pub weight: String,
    pub verdict: Option<bool>,
    pub related: Vec<i64>,
}

impl Card {
    #[must_use]
    pub fn of(incident: Incident, related: Vec<i64>) -> Self {
        Self {
            id: incident.id,
            service: incident.service.to_string(),
            signature: incident.signature.to_string(),
            stream: incident.stream.to_string(),
            source: incident.source,
            state: match incident.state {
                State::Open => "идёт",
                State::Closed => "закрыт",
                State::Abandoned => "без разбора",
            },
            open: incident.state == State::Open,
            began: moment(incident.began),
            last: moment(incident.last),
            alive: lasting(incident.began, incident.last),
            seen: incident.seen,
            peak: number(incident.peak),
            weight: number(incident.weight),
            verdict: incident.verdict,
            related,
        }
    }
}

/// Момент в местном для читателя виде.
fn moment(minute: Minute) -> String {
    Utc.timestamp_opt(minute.stamp(), 0)
        .single()
        .map_or_else(|| "—".to_owned(), |at| at.format("%d.%m %H:%M").to_string())
}

/// Сколько инцидент длится.
fn lasting(began: Minute, last: Minute) -> String {
    let minutes = (last.stamp() - began.stamp()) / 60;
    if minutes < 60 {
        format!("{minutes} мин")
    } else {
        format!("{} ч {} мин", minutes / 60, minutes % 60)
    }
}

/// Число без хвоста из нулей.
fn number(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.0}")
    } else {
        "∞".to_owned()
    }
}
