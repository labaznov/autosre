//! Подготовка инцидентов к показу.
//!
//! Форматирование живёт здесь, а не в шаблонах: в шаблон приходят готовые
//! строки, и вёрстка не знает ни про типы предметной области, ни про
//! округления, ни про бесконечности.

use chrono::{TimeZone, Utc};
use sre_domain::{Incident, Minute, State};
use sre_store::{Asked, Finding, Written};

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
    pub because: Option<String>,
    pub related: Vec<i64>,
    /// Вывод расследования, если он уже есть.
    pub cause: Option<String>,
    pub advice: Option<String>,
    pub confidence: Option<String>,
    pub skill: Option<String>,
    /// Разбор не состоялся: очередь не дошла или модель отказала.
    pub missing: Option<&'static str>,
    /// Заявки к дежурному по этому инциденту.
    pub inquiries: Vec<Question>,
    /// Заметка, на которую опёрся вывод.
    pub note: Option<String>,
    /// По каким словам и что нашлось в базе знаний.
    pub reading: Option<String>,
    /// Черновики заметок, написанные по этому инциденту.
    pub drafts: Vec<Paper>,
}

/// Черновик в том виде, в каком его читает дежурный.
pub struct Paper {
    pub id: i64,
    pub incident: i64,
    pub name: String,
    pub title: String,
    pub written: String,
    pub open: bool,
    pub state: &'static str,
    pub who: Option<String>,
}

impl Paper {
    #[must_use]
    pub fn of(written: &Written) -> Self {
        Self {
            id: written.id,
            incident: written.incident,
            name: written.name.clone(),
            title: written.title.clone(),
            written: moment(written.written),
            open: written.state == "open",
            state: match written.state.as_str() {
                "open" => "ждёт приёмки",
                "accepted" => "принят в базу знаний",
                _ => "отклонён",
            },
            who: written.who.clone(),
        }
    }
}

/// Заявка в том виде, в каком её читает дежурный.
pub struct Question {
    pub id: i64,
    pub incident: i64,
    pub service: String,
    pub host: String,
    pub command: String,
    pub reason: String,
    pub asked: String,
    /// Сколько заявка уже ждёт: в списке это главное число.
    pub waited: String,
    pub open: bool,
    pub who: Option<String>,
    pub answer: Option<String>,
    pub state: &'static str,
}

impl Question {
    #[must_use]
    pub fn of(asked: &Asked) -> Self {
        Self {
            id: asked.id,
            incident: asked.incident,
            service: asked.service.clone(),
            waited: lasting(asked.asked, Minute::of(Utc::now())),
            host: asked.host.clone(),
            command: asked.command.clone(),
            reason: asked.reason.clone(),
            asked: moment(asked.asked),
            open: asked.state == "open",
            who: asked.who.clone(),
            answer: asked.answer.clone(),
            state: match asked.state.as_str() {
                "open" => "ждёт ответа",
                "answered" => "отвечена",
                "dropped" => "снята дежурным",
                _ => "погасла без ответа",
            },
        }
    }
}

impl Card {
    #[must_use]
    pub fn of(incident: Incident, related: Vec<i64>, finding: Option<&Finding>) -> Self {
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
            because: incident.because,
            related,
            cause: finding.and_then(|it| it.cause.clone()),
            advice: finding.and_then(|it| it.advice.clone()),
            confidence: finding
                .and_then(|it| it.confidence)
                .map(|it| format!("{:.0}%", it * 100.0)),
            skill: finding.map(|it| it.skill.clone()),
            missing: finding.and_then(|it| match it.state.as_str() {
                "skipped" => Some("вывод пропущен: очередь не дошла вовремя"),
                "failed" => Some("разбор не удался: модель не ответила"),
                "stale" => Some("разбор устарел и был закрыт"),
                "running" => Some("разбор идёт"),
                "unskilled" => Some("разбирать нечем: подходящего скилла нет"),
                "waiting" => Some("агент ждёт ответа на заявку"),
                "answered" => Some("ответ получен, разбор пойдёт заново"),
                "dropped" => Some("заявка снята, разбирать нечем"),
                "unanswered" => Some("заявка погасла без ответа"),
                _ => None,
            }),
            inquiries: Vec::new(),
            note: finding.and_then(|it| it.note.clone()),
            reading: None,
            drafts: Vec::new(),
        }
    }

    /// Та же карточка с тем, что агент читал в базе знаний.
    ///
    /// Попадание объяснимо: видно, по каким словам нашлась заметка. Это важно
    /// ровно тогда, когда дежурный агенту не верит и проверяет его
    /// ([ADR-0008](../../../docs/adr/0008-full-text-knowledge-search.md)).
    #[must_use]
    pub fn reading(self, steps: &[(String, String, String)]) -> Self {
        Self {
            reading: steps
                .iter()
                .find(|(tool, _, _)| tool == "knowledge")
                .map(|(_, about, _)| about.clone()),
            ..self
        }
    }

    /// Та же карточка с черновиками заметок.
    #[must_use]
    pub fn drafting(self, drafts: &[Written]) -> Self {
        Self {
            drafts: drafts.iter().map(Paper::of).collect(),
            ..self
        }
    }

    /// Та же карточка с заявками: они нужны только на своей странице, лента
    /// обходится счётчиком.
    #[must_use]
    pub fn asking(self, inquiries: &[Asked]) -> Self {
        Self {
            inquiries: inquiries.iter().map(Question::of).collect(),
            ..self
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

/// Число, разбитое на разряды.
///
/// Единиц измерения агент не знает: у одной серии это байты, у другой запросы.
/// Придумывать «МБ» значит однажды написать «751 МБ» там, где на самом деле
/// секунды. Разряды же читаются всегда.
fn number(value: f64) -> String {
    if !value.is_finite() {
        return "∞".to_owned();
    }
    let whole = format!("{:.0}", value.abs());
    let mut out = String::new();
    for (index, digit) in whole.chars().enumerate() {
        if index > 0 && (whole.len() - index).is_multiple_of(3) {
            out.push('\u{202f}');
        }
        out.push(digit);
    }
    if value < 0.0 {
        format!("−{out}")
    } else {
        out
    }
}
