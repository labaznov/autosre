//! Подготовка инцидентов к показу.
//!
//! Форматирование живёт здесь, а не в шаблонах: в шаблон приходят готовые
//! строки, и вёрстка не знает ни про типы предметной области, ни про
//! округления, ни про бесконечности.

use autosre_domain::{Incident, Minute, State};
use autosre_store::{Asked, Finding, Muted, Written};
use chrono::{TimeZone, Utc};

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
    /// Насколько всё плохо — по мнению модели. Пусто, пока не было вывода.
    pub severity: Option<&'static str>,
    pub loud: bool,
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
    /// Номера инцидентов, влитых в этот.
    pub merged: Vec<i64>,
    /// Потоки инцидента со счётчиками: по ним его и разделяют.
    pub parts: Vec<Part>,
}

/// Поток инцидента в том виде, в каком его читает дежурный.
pub struct Part {
    pub stream: String,
    pub seen: u64,
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

/// Приглушение в том виде, в каком его читает дежурный.
pub struct Silence {
    pub id: i64,
    pub service: String,
    pub signature: String,
    pub until: String,
    pub author: String,
    pub reason: String,
    pub live: bool,
    pub seen: u64,
}

impl Silence {
    #[must_use]
    pub fn of(muted: &Muted) -> Self {
        Self {
            id: muted.id,
            service: muted.service.clone(),
            signature: muted.signature.clone(),
            until: moment(muted.until),
            author: muted.author.clone(),
            reason: muted.reason.clone(),
            live: muted.live,
            seen: muted.seen,
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
                State::Merged => "влит в другой",
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
            severity: incident.severity.map(|it| match it {
                autosre_domain::Severity::Low => "хуже обычного",
                autosre_domain::Severity::Medium => "теряем часть работы",
                autosre_domain::Severity::High => "работа не делается",
            }),
            loud: incident.severity == Some(autosre_domain::Severity::High),
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
            merged: Vec::new(),
            parts: Vec::new(),
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

    /// Та же карточка с историей группировки: из чего собрана и на что делится.
    #[must_use]
    pub fn built(self, merged: Vec<i64>, parts: &[(autosre_domain::Stream, u64)]) -> Self {
        Self {
            merged,
            parts: parts
                .iter()
                .map(|(stream, seen)| Part {
                    stream: stream.to_string(),
                    seen: *seen,
                })
                .collect(),
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

/// Отчёт в том виде, в каком его читает человек.
pub struct Filed {
    pub kind: String,
    pub name: String,
    pub title: String,
    pub made: String,
    /// Как называется вид отчёта по-русски.
    pub about: &'static str,
}

impl Filed {
    #[must_use]
    pub fn of(filed: &autosre_store::Filed) -> Self {
        Self {
            kind: filed.kind.clone(),
            name: filed.name.clone(),
            title: filed.title.clone(),
            made: moment(filed.made),
            about: match filed.kind.as_str() {
                "daily" => "сутки",
                "weekly" => "неделя",
                _ => "инцидент",
            },
        }
    }
}

/// Наблюдаемый поток в том виде, в каком его читает человек.
pub struct Line {
    pub source: String,
    pub stream: String,
    pub last: String,
    pub buckets: u64,
    pub kind: &'static str,
}

impl Line {
    #[must_use]
    pub fn of(watched: &autosre_store::Watched) -> Self {
        Self {
            source: watched.source.clone(),
            stream: watched.stream.to_string(),
            last: moment(watched.last),
            buckets: watched.buckets,
            kind: match watched.kind {
                autosre_domain::Kind::Mean => "уровень",
                autosre_domain::Kind::Sum => "счётчик",
            },
        }
    }
}

/// Отсеянное отклонение в том виде, в каком его читает дежурный.
pub struct Dropped {
    pub id: i64,
    pub service: String,
    pub stream: String,
    pub source: String,
    pub horizon: String,
    pub at: String,
    pub value: String,
    pub baseline: String,
    pub because: String,
    /// Уже оценено: `Some(true)` — отсеяли правильно.
    pub verdict: Option<bool>,
    pub judge: Option<String>,
}

impl Dropped {
    #[must_use]
    pub fn of(sifted: &autosre_store::Sifted, labels: &[String]) -> Self {
        Self {
            id: sifted.id,
            service: autosre_domain::Service::of(&sifted.stream, labels).to_string(),
            stream: sifted.stream.to_string(),
            source: sifted.source.clone(),
            horizon: sifted.horizon.clone(),
            at: moment(sifted.at),
            value: number(sifted.value),
            baseline: number(sifted.baseline),
            because: sifted.because.clone(),
            verdict: sifted.verdict,
            judge: sifted.judge.clone(),
        }
    }
}

/// Момент в виде, годном для показа: наружу из этого модуля.
#[must_use]
pub fn when(minute: Minute) -> String {
    moment(minute)
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
