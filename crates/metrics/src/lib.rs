//! Коннектор `VictoriaMetrics`.
//!
//! Наблюдаемые серии отбираются правилами из настроек
//! ([ADR-0016](../../../docs/adr/0016-observed-metrics.md)): в базе тысячи
//! серий, и большая их часть к здоровью отношения не имеет.
//!
//! Бакет метрики — не сумма, а **среднее за минуту** для уровней и приращение
//! для счётчиков. Сумма занятой памяти за час бессмысленна, а сумма запросов —
//! ровно то, что нужно.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use autosre_domain::{Bucket, Minute, Span, Stream};
use autosre_source::{Source, SourceError};
use serde_json::Value;
use url::Url;

/// Имя источника в базе, журнале и метриках.
const NAME: &str = "metrics";

/// Путь выборки диапазона в API `VictoriaMetrics`.
const ENDPOINT: &str = "api/v1/query_range";

/// Путь мгновенного запроса: так исполняются запросы скиллов.
const INSTANT: &str = "api/v1/query";

/// Метка, под которой имя серии лежит в селекторе потока.
///
/// Своя, а не `__name__`: селектор потока агент собирает сам, и в нём имя
/// серии — такая же метка, как остальные. В запрос к источнику она переводится
/// обратно.
const SERIES: &str = "__series__";

/// Настройки соединения.
#[derive(Debug, Clone)]
pub struct Settings {
    pub url: Url,
    pub timeout: Duration,
    /// Правила отбора: имена серий, за которыми смотрим.
    pub select: Vec<String>,
    /// Метки, по которым серия сводится к сервису.
    pub labels: Vec<String>,
}

/// Клиент метрик.
#[derive(Debug, Clone)]
pub struct Metrics {
    http: reqwest::Client,
    endpoint: Url,
    instant: Url,
    select: Vec<String>,
    labels: Vec<String>,
}

impl Metrics {
    /// Собирает коннектор под заданные настройки.
    ///
    /// # Errors
    /// [`SourceError::Transport`] на неразбираемом адресе или несобравшемся
    /// HTTP-клиенте.
    pub fn new(settings: &Settings) -> Result<Self, SourceError> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(settings.timeout)
                .build()
                .map_err(|cause| transport(&cause))?,
            endpoint: settings
                .url
                .join(ENDPOINT)
                .map_err(|cause| transport(&cause))?,
            instant: settings
                .url
                .join(INSTANT)
                .map_err(|cause| transport(&cause))?,
            select: settings.select.clone(),
            labels: settings.labels.clone(),
        })
    }

    /// Запрос одной серии за промежуток с шагом в минуту.
    ///
    /// Счётчики берутся приращением (`increase`), уровни — как есть: что перед
    /// нами, видно по имени серии, и это соглашение Prometheus, а не наша
    /// выдумка.
    fn query(&self, series: &str) -> String {
        let by = self.labels.join(",");
        if series.ends_with("_total") || series.ends_with("_count") {
            format!("sum by ({by}) (increase({series}[1m]))")
        } else {
            format!("avg by ({by}) ({series})")
        }
    }
}

#[async_trait]
impl Source for Metrics {
    fn name(&self) -> &str {
        NAME
    }

    async fn buckets(&self, span: Span) -> Result<Vec<Bucket>, SourceError> {
        let mut all = Vec::new();
        for series in &self.select {
            let query = self.query(series);
            tracing::debug!(query, minutes = span.len(), "запрос к метрикам");
            let response = self
                .http
                .get(self.endpoint.clone())
                .query(&[
                    ("query", query.as_str()),
                    ("start", &span.from().stamp().to_string()),
                    ("end", &span.to().stamp().to_string()),
                    ("step", "60"),
                ])
                .send()
                .await
                .map_err(|cause| transport(&cause))?;
            let status = response.status();
            let body = response.text().await.map_err(|cause| transport(&cause))?;
            if !status.is_success() {
                return Err(SourceError::Status {
                    status: status.as_u16(),
                    body: clip(&body),
                });
            }
            all.extend(read(&body, series, series.ends_with("_total"))?);
        }
        tracing::info!(
            buckets = all.len(),
            series = self.select.len(),
            minutes = span.len(),
            "значения метрик сняты"
        );
        Ok(all)
    }

    /// Запрос исполняется мгновенным на конец промежутка: отрезок автор
    /// скилла задаёт сам, диапазоном внутри запроса — `[7d]`, `[1h]`.
    async fn query(
        &self,
        text: &str,
        span: Span,
        limit: usize,
    ) -> Result<Vec<String>, SourceError> {
        let query = text.replace(&format!("{SERIES}="), "__name__=");
        tracing::debug!(query, "запрос скилла к метрикам");
        let response = self
            .http
            .get(self.instant.clone())
            .query(&[
                ("query", query.as_str()),
                ("time", &span.to().stamp().to_string()),
            ])
            .send()
            .await
            .map_err(|cause| transport(&cause))?;
        let status = response.status();
        let body = response.text().await.map_err(|cause| transport(&cause))?;
        if !status.is_success() {
            return Err(SourceError::Status {
                status: status.as_u16(),
                body: clip(&body),
            });
        }
        let mut lines = lines(&body)?;
        lines.truncate(limit);
        Ok(lines)
    }
}

/// Ответ мгновенного запроса строками: селектор и значение, у отрезка — по
/// строке на точку.
fn lines(body: &str) -> Result<Vec<String>, SourceError> {
    let answer: Value =
        serde_json::from_str(body).map_err(|cause| SourceError::Shape(cause.to_string()))?;
    let data = &answer["data"];
    if data["resultType"] == "scalar" || data["resultType"] == "string" {
        return Ok(vec![point(&data["result"], "")]);
    }
    let rows = data["result"]
        .as_array()
        .ok_or_else(|| SourceError::Shape(clip(body)))?;
    let mut lines = Vec::new();
    for row in rows {
        let labels = selector(&row["metric"]);
        if let Some(points) = row["values"].as_array() {
            for pair in points {
                lines.push(point(pair, &labels));
            }
        } else {
            lines.push(format!("{labels} {}", reading_text(&row["value"][1])));
        }
    }
    Ok(lines)
}

/// Точка «время, значение» одной строкой.
fn point(pair: &Value, labels: &str) -> String {
    let at = pair[0]
        .as_f64()
        .map(|it| format!("{it:.0}"))
        .unwrap_or_default();
    let value = reading_text(&pair[1]);
    if labels.is_empty() {
        format!("{at} {value}")
    } else {
        format!("{labels} {at} {value}")
    }
}

/// Значение как текст: строкой оно и приходит.
fn reading_text(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToOwned::to_owned)
}

/// Селектор из меток ответа, в порядке имён.
fn selector(labels: &Value) -> String {
    let inside = labels
        .as_object()
        .map(|map| {
            map.iter()
                .filter_map(|(key, value)| value.as_str().map(|it| format!("{key}=\"{it}\"")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    format!("{{{inside}}}")
}

/// Разбирает ответ `query_range`.
fn read(body: &str, series: &str, counted: bool) -> Result<Vec<Bucket>, SourceError> {
    let answer: Value =
        serde_json::from_str(body).map_err(|cause| SourceError::Shape(cause.to_string()))?;
    let rows = answer["data"]["result"]
        .as_array()
        .ok_or_else(|| SourceError::Shape(clip(body)))?;
    let mut buckets = Vec::new();
    for row in rows {
        let stream = name(series, &row["metric"]);
        let values = row["values"]
            .as_array()
            .ok_or_else(|| SourceError::Shape("в ответе нет значений".to_owned()))?;
        for pair in values {
            let (Some(at), Some(value)) = (pair[0].as_f64(), reading(&pair[1])) else {
                continue;
            };
            #[expect(
                clippy::cast_possible_truncation,
                reason = "метки времени Prometheus далеко ниже предела i64"
            )]
            let minute = Minute::at(at as i64);
            buckets.push(if counted {
                Bucket::counted(stream.clone(), minute, value)
            } else {
                Bucket::level(stream.clone(), minute, value)
            });
        }
    }
    Ok(buckets)
}

/// Имя потока: серия и её метки в том же виде, что у логов.
///
/// Селектор собирается одинаково с логами не для красоты: инцидент группируется
/// по сервису, а сервис вытаскивается из селектора одним и тем же кодом.
fn name(series: &str, labels: &Value) -> Stream {
    let mut pairs: BTreeMap<String, String> = BTreeMap::new();
    if let Some(map) = labels.as_object() {
        for (key, value) in map {
            if let Some(value) = value.as_str() {
                pairs.insert(key.clone(), value.to_owned());
            }
        }
    }
    pairs.insert(SERIES.to_owned(), series.to_owned());
    let inside = pairs
        .iter()
        .map(|(key, value)| format!("{key}=\"{value}\""))
        .collect::<Vec<_>>()
        .join(",");
    Stream::new(format!("{{{inside}}}"))
}

/// Значение из пары «время, значение»: `VictoriaMetrics` отдаёт его строкой.
fn reading(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .filter(|number: &f64| number.is_finite())
}

fn transport(cause: &dyn std::fmt::Display) -> SourceError {
    SourceError::Transport(cause.to_string())
}

fn clip(text: &str) -> String {
    text.chars().take(300).collect()
}
