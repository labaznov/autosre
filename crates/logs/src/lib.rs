//! Коннектор `VictoriaLogs`.
//!
//! Счётчики снимаются одним запросом на промежуток: `stats by (_stream, _time:1m)`
//! возвращает и все потоки, и все минуты разом. Пилот вместо этого перебирал
//! восемь самых шумных потоков и на каждый гонял два десятка запросов — около
//! двухсот обращений за скан, при этом тихие сервисы не проверялись никогда.

pub mod query;

use std::time::Duration;

use async_trait::async_trait;
use autosre_domain::{Bucket, Minute, Span, Stream};
use autosre_source::{Source, SourceError};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use url::Url;

pub use query::{Filter, FilterError};

/// Имя источника в базе, журнале и метриках.
const NAME: &str = "logs";

/// Путь выборки в API `VictoriaLogs`.
const ENDPOINT: &str = "select/logsql/query";

/// Настройки соединения.
#[derive(Debug, Clone)]
pub struct Settings {
    pub url: Url,
    pub username: String,
    pub password: String,
    pub timeout: Duration,
    /// Предел строк в ответе на один запрос.
    pub rows: usize,
}

/// Клиент логов: знает адрес, доступ и фильтр ошибок.
#[derive(Debug, Clone)]
pub struct Logs {
    http: reqwest::Client,
    endpoint: Url,
    username: String,
    password: String,
    filter: Filter,
    rows: usize,
    /// Паузы между попытками при проходящем отказе; пусто — одна попытка.
    pauses: Vec<Duration>,
}

impl Logs {
    /// Собирает коннектор под заданные настройки и фильтр ошибок.
    ///
    /// # Errors
    /// [`SourceError::Transport`] на неразбираемом адресе или несобравшемся
    /// HTTP-клиенте.
    pub fn new(settings: &Settings, filter: Filter) -> Result<Self, SourceError> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(settings.timeout)
                .build()
                .map_err(|cause| transport(&cause))?,
            endpoint: settings
                .url
                .join(ENDPOINT)
                .map_err(|cause| transport(&cause))?,
            username: settings.username.clone(),
            password: settings.password.clone(),
            filter,
            rows: settings.rows,
            pauses: Vec::new(),
        })
    }

    /// Тот же коннектор, повторяющий запрос после обрыва связи или занятого
    /// источника. Таймаут и отказ разобрать запрос не повторяются.
    #[must_use]
    pub fn retrying(self, pauses: &[Duration]) -> Self {
        Self {
            pauses: pauses.to_vec(),
            ..self
        }
    }
}

#[async_trait]
impl Source for Logs {
    fn name(&self) -> &str {
        NAME
    }

    async fn buckets(&self, span: Span) -> Result<Vec<Bucket>, SourceError> {
        let body = self.ask(&self.filter.buckets(span), self.rows).await?;
        let buckets = rows(&body)?
            .iter()
            .map(bucket)
            .collect::<Result<Vec<_>, _>>()?;
        tracing::info!(
            buckets = buckets.len(),
            minutes = span.len(),
            "счётчики ошибок сняты"
        );
        Ok(buckets)
    }

    async fn samples(
        &self,
        stream: &Stream,
        span: Span,
        limit: usize,
    ) -> Result<Vec<String>, SourceError> {
        let body = self
            .ask(&self.filter.samples(stream.as_str(), span), limit)
            .await?;
        Ok(rows(&body)?
            .iter()
            .filter_map(|row| row.get("_msg").and_then(Value::as_str))
            .map(ToOwned::to_owned)
            .collect())
    }

    async fn query(
        &self,
        text: &str,
        _span: Span,
        limit: usize,
    ) -> Result<Vec<String>, SourceError> {
        let body = self.ask(text, limit).await?;
        Ok(rows(&body)?.iter().map(line).collect())
    }
}

impl Logs {
    /// Один запрос к источнику.
    async fn ask(&self, query: &str, limit: usize) -> Result<String, SourceError> {
        tracing::debug!(query, limit, "запрос к логам");
        let got = autosre_source::again(
            &self.pauses,
            |got: &Result<(reqwest::StatusCode, String), reqwest::Error>| match got {
                Err(cause) => !cause.is_timeout(),
                Ok((status, _)) => autosre_source::busy(status.as_u16()),
            },
            || {
                let mut request = self
                    .http
                    .get(self.endpoint.clone())
                    .query(&[("query", query), ("limit", &limit.to_string())]);
                if !self.username.is_empty() {
                    request = request.basic_auth(&self.username, Some(&self.password));
                }
                async move {
                    let response = request.send().await?;
                    let status = response.status();
                    Ok((status, response.text().await?))
                }
            },
        )
        .await;
        let (status, body) = got.map_err(|cause| transport(&cause))?;
        if status.is_success() {
            Ok(body)
        } else {
            Err(SourceError::Status {
                status: status.as_u16(),
                body: clip(&body),
            })
        }
    }
}

/// Разбирает ответ, идущий потоком JSON-строк.
fn rows(body: &str) -> Result<Vec<Map<String, Value>>, SourceError> {
    body.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| match serde_json::from_str(line) {
            Ok(Value::Object(row)) => Ok(row),
            _ => Err(SourceError::Shape(clip(line))),
        })
        .collect()
}

/// Запись ответа одной строкой для модели.
///
/// Живая запись — это её текст, остальное в ней служебное. Результат `stats` —
/// поля и числа, и там важны все. Различаются они по составу полей, а не по
/// запросу: агент запроса не понимает.
fn line(row: &Map<String, Value>) -> String {
    const OWN: &[&str] = &["_msg", "_stream", "_stream_id", "_time"];
    if let Some(message) = row.get("_msg").and_then(Value::as_str)
        && row.keys().all(|key| OWN.contains(&key.as_str()))
    {
        return message.to_owned();
    }
    row.iter()
        .filter(|(key, _)| *key != "_stream_id")
        .map(|(key, value)| match value {
            Value::String(text) => format!("{key}={text}"),
            other => format!("{key}={other}"),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn bucket(row: &Map<String, Value>) -> Result<Bucket, SourceError> {
    Ok(Bucket::counted(
        Stream::new(text(row, "_stream")?),
        Minute::of(moment(row, "_time")?),
        count(row, "total")?,
    ))
}

fn text<'a>(row: &'a Map<String, Value>, field: &str) -> Result<&'a str, SourceError> {
    row.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| missing(row, field))
}

/// Счётчик: `VictoriaLogs` отдаёт результат `count()` строкой, а не числом.
fn count(row: &Map<String, Value>, field: &str) -> Result<f64, SourceError> {
    let value = row.get(field).ok_or_else(|| missing(row, field))?;
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .ok_or_else(|| missing(row, field))
}

fn moment(row: &Map<String, Value>, field: &str) -> Result<DateTime<Utc>, SourceError> {
    DateTime::parse_from_rfc3339(text(row, field)?)
        .map(|at| at.with_timezone(&Utc))
        .map_err(|_| missing(row, field))
}

fn missing(row: &Map<String, Value>, field: &str) -> SourceError {
    SourceError::Shape(format!(
        "нет поля {field}: {}",
        clip(&Value::Object(row.clone()).to_string())
    ))
}

fn transport(cause: &dyn std::fmt::Display) -> SourceError {
    SourceError::Transport(cause.to_string())
}

/// Обрезает текст для сообщения об ошибке: тела ответов в журнал не попадают.
fn clip(text: &str) -> String {
    text.chars().take(300).collect()
}
