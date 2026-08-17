//! Коннектор `VictoriaLogs`.
//!
//! Счётчики снимаются одним запросом на промежуток: `stats by (_stream, _time:1m)`
//! возвращает и все потоки, и все минуты разом. Пилот вместо этого перебирал
//! восемь самых шумных потоков и на каждый гонял два десятка запросов — около
//! двухсот обращений за скан, при этом тихие сервисы не проверялись никогда.

pub mod query;

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use sre_domain::{Bucket, Minute, Span, Stream};
use sre_source::{Source, SourceError};
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
        })
    }
}

#[async_trait]
impl Source for Logs {
    fn name(&self) -> &str {
        NAME
    }

    async fn buckets(&self, span: Span) -> Result<Vec<Bucket>, SourceError> {
        let query = self.filter.buckets(span);
        tracing::debug!(query, minutes = span.len(), "запрос к логам");
        let mut request = self
            .http
            .get(self.endpoint.clone())
            .query(&[("query", query.as_str()), ("limit", &self.rows.to_string())]);
        if !self.username.is_empty() {
            request = request.basic_auth(&self.username, Some(&self.password));
        }
        let response = request.send().await.map_err(|cause| transport(&cause))?;
        let status = response.status();
        let body = response.text().await.map_err(|cause| transport(&cause))?;
        if !status.is_success() {
            return Err(SourceError::Status {
                status: status.as_u16(),
                body: clip(&body),
            });
        }
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
