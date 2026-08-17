//! Настройки: файл TOML плюс секреты из окружения.
//!
//! Разделение жёсткое ([ADR-0025](../../../docs/adr/0025-config-file-and-secrets.md)):
//! в файле лежит всё, что описывает поведение, и ничего, что нельзя показать.
//! Ключи приходят только из окружения, и без них агент не стартует.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

/// Откуда берутся секреты. Подмена в тестах избавляет их от общего окружения.
pub trait Env {
    fn var(&self, key: &str) -> Option<String>;
}

/// Настоящее окружение процесса.
pub struct Process;

impl Env for Process {
    fn var(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

/// Отказы разбора настроек.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("файл настроек {path} не прочитан: {cause}")]
    Read {
        path: PathBuf,
        cause: std::io::Error,
    },
    #[error("файл настроек не разобран: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("переменная окружения {0} не задана")]
    Missing(&'static str),
    #[error("{0}")]
    Invalid(String),
}

/// Настройки целиком: файл плюс секреты.
#[derive(Debug)]
pub struct Config {
    pub file: File,
    pub secrets: Secrets,
    /// Поля, которых агент не знает: старее или новее его самого.
    pub unknown: Vec<String>,
}

/// Ключи доступа. Значений по умолчанию у них нет.
#[derive(Debug)]
pub struct Secrets {
    pub model: String,
    pub session: String,
    pub logs_password: String,
}

impl Secrets {
    /// Читает ключи из окружения.
    ///
    /// # Errors
    /// [`ConfigError::Missing`] на первом же отсутствующем ключе.
    pub fn read(env: &dyn Env) -> Result<Self, ConfigError> {
        Ok(Self {
            model: required(env, "SREAGENT_MODEL_KEY")?,
            session: required(env, "SREAGENT_SESSION_KEY")?,
            logs_password: env.var("SREAGENT_LOGS_PASSWORD").unwrap_or_default(),
        })
    }
}

/// Обязательный ключ. Пустое значение — то же самое, что отсутствующее:
/// проверка живёт здесь, а не в реализации окружения, иначе её обходит любая
/// другая реализация.
fn required(env: &dyn Env, key: &'static str) -> Result<String, ConfigError> {
    env.var(key)
        .filter(|value| !value.trim().is_empty())
        .ok_or(ConfigError::Missing(key))
}

/// Содержимое файла настроек.
#[derive(Debug, Deserialize)]
pub struct File {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
    #[serde(default = "default_database")]
    pub database: PathBuf,
    pub logs: Logs,
    pub metrics: Metrics,
    pub model: Model,
    #[serde(default, rename = "horizon")]
    pub horizons: Vec<Horizon>,
    #[serde(default)]
    pub collector: Collector,
    /// Учётные записи веб-морды.
    #[serde(default, rename = "account")]
    pub accounts: Vec<Account>,
    #[serde(default)]
    pub incidents: Incidents,
    #[serde(default)]
    pub digging: Digging,
    #[serde(default)]
    pub queue: Queue,
    #[serde(default)]
    pub retention: Retention,
    #[serde(flatten)]
    rest: BTreeMap<String, toml::Value>,
}

/// Источник логов.
#[derive(Debug, Deserialize)]
pub struct Logs {
    pub url: String,
    #[serde(default)]
    pub username: String,
    #[serde(default = "default_error_pattern")]
    pub error_pattern: String,
    /// Селекторы собственных потоков агента: без них он находит сам себя.
    #[serde(default)]
    pub self_streams: Vec<String>,
    #[serde(default = "default_source_timeout", with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(flatten)]
    rest: BTreeMap<String, toml::Value>,
}

/// Источник метрик.
#[derive(Debug, Deserialize)]
pub struct Metrics {
    pub url: String,
    /// Правила отбора наблюдаемых серий.
    #[serde(default)]
    pub select: Vec<String>,
    #[serde(default = "default_source_timeout", with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(flatten)]
    rest: BTreeMap<String, toml::Value>,
}

/// Модель за OpenAI-совместимым API.
#[derive(Debug, Deserialize)]
pub struct Model {
    pub url: String,
    pub name: String,
    #[serde(default = "default_model_timeout", with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    #[serde(flatten)]
    rest: BTreeMap<String, toml::Value>,
}

/// Горизонт: ширина окна, период оценки и пороги детектора.
#[derive(Debug, Deserialize)]
pub struct Horizon {
    pub name: String,
    #[serde(with = "humantime_serde")]
    pub width: Duration,
    #[serde(with = "humantime_serde")]
    pub period: Duration,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Сколько прошлых окон составляют базовую линию.
    #[serde(default = "default_history")]
    pub history: usize,
    #[serde(default = "default_minimum")]
    pub minimum: f64,
    #[serde(default = "default_score")]
    pub score: f64,
    #[serde(default = "default_ratio")]
    pub ratio: f64,
    /// На какую долю должен сдвинуться уровень метрики.
    #[serde(default = "default_drift")]
    pub drift: f64,
    #[serde(flatten)]
    rest: BTreeMap<String, toml::Value>,
}

/// Съём наблюдений.
#[derive(Debug, Deserialize)]
pub struct Collector {
    /// Насколько глубоко закрывать дыры при старте.
    #[serde(default = "default_backfill", with = "humantime_serde")]
    pub backfill: Duration,
    /// Сколько минут просить у источника за один запрос.
    #[serde(default = "default_chunk", with = "humantime_serde")]
    pub chunk: Duration,
    #[serde(flatten)]
    rest: BTreeMap<String, toml::Value>,
}

/// Учётная запись: имя и хеш пароля.
#[derive(Debug, Clone, Deserialize)]
pub struct Account {
    pub login: String,
    /// Хеш argon2. Пароль в открытом виде сюда не попадает никогда.
    pub password: String,
}

/// Инциденты: группировка, связи, тишина.
#[derive(Debug, Clone, Deserialize)]
pub struct Incidents {
    /// Метки селектора, из которых берётся имя сервиса, в порядке предпочтения.
    #[serde(default = "default_labels")]
    pub service_labels: Vec<String>,
    /// Сколько отклонений разбирать за проход.
    #[serde(default = "default_batch")]
    pub batch: usize,
    /// Сколько строк тянуть на сигнатуру.
    #[serde(default = "default_samples")]
    pub samples: usize,
    /// Сколько групп сигнатур показывать модели.
    #[serde(default = "default_groups")]
    pub groups: usize,
    /// За какое окно брать образцы.
    #[serde(default = "default_sample_window", with = "humantime_serde")]
    pub sample_window: Duration,
    /// Через сколько тишины инцидент закрывается.
    #[serde(default = "default_silence", with = "humantime_serde")]
    pub silence: Duration,
    /// Насколько близко во времени инциденты считаются связанными.
    #[serde(default = "default_link", with = "humantime_serde")]
    pub link: Duration,
    #[serde(flatten)]
    rest: BTreeMap<String, toml::Value>,
}

/// Разбор: очередь, глубина, каталог скиллов.
#[derive(Debug, Clone, Deserialize)]
pub struct Digging {
    /// Каталог скиллов в рабочей копии репозитория знаний.
    #[serde(default = "default_skills")]
    pub skills: PathBuf,
    /// Сколько расследований идёт одновременно.
    #[serde(default = "default_parallel")]
    pub parallel: usize,
    /// Сколько инцидентов брать за проход очереди.
    #[serde(default = "default_batch")]
    pub batch: usize,
    /// Сколько раз модель может попросить добрать данные.
    #[serde(default = "default_steps")]
    pub steps: usize,
    /// За какое окно добирать данные по просьбе модели.
    #[serde(default = "default_wider", with = "humantime_serde")]
    pub wider: Duration,
    /// Сколько инцидент ждёт разбора, прежде чем дойти до дежурного без вывода.
    #[serde(default = "default_patience", with = "humantime_serde")]
    pub patience: Duration,
    #[serde(flatten)]
    rest: BTreeMap<String, toml::Value>,
}

/// Очередь расследований.
#[derive(Debug, Deserialize)]
pub struct Queue {
    #[serde(default = "default_parallel")]
    pub parallel: usize,
    /// Сколько подозрение ждёт разбора, прежде чем дойти до дежурного без вывода.
    #[serde(default = "default_patience", with = "humantime_serde")]
    pub patience: Duration,
    #[serde(flatten)]
    rest: BTreeMap<String, toml::Value>,
}

/// Сроки хранения.
#[derive(Debug, Deserialize)]
pub struct Retention {
    #[serde(default = "default_minutes", with = "humantime_serde")]
    pub minute_buckets: Duration,
    #[serde(default = "default_hours", with = "humantime_serde")]
    pub hour_buckets: Duration,
    #[serde(default = "default_investigations", with = "humantime_serde")]
    pub investigations: Duration,
    #[serde(default = "default_incidents", with = "humantime_serde")]
    pub incidents: Duration,
    #[serde(flatten)]
    rest: BTreeMap<String, toml::Value>,
}

impl Config {
    /// Читает файл настроек и секреты окружения.
    ///
    /// # Errors
    /// [`ConfigError::Read`] на недоступном файле, [`ConfigError::Parse`] на
    /// неразбираемом, [`ConfigError::Invalid`] на бессмысленных значениях,
    /// [`ConfigError::Missing`] на отсутствующем ключе доступа.
    pub fn read(path: &Path, env: &dyn Env) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|cause| ConfigError::Read {
            path: path.to_owned(),
            cause,
        })?;
        let file: File = toml::from_str(&text)?;
        file.check()?;
        Ok(Self {
            unknown: file.unknown(),
            secrets: Secrets::read(env)?,
            file,
        })
    }
}

impl File {
    /// Проверяет то, что схема выразить не может.
    fn check(&self) -> Result<(), ConfigError> {
        if self.horizons.is_empty() {
            return Err(ConfigError::Invalid(
                "не задан ни один горизонт: агенту нечего считать".to_owned(),
            ));
        }
        for horizon in &self.horizons {
            if horizon.period.is_zero() || horizon.width.is_zero() {
                return Err(ConfigError::Invalid(format!(
                    "горизонт {}: ширина и период оценки должны быть положительны",
                    horizon.name
                )));
            }
            if horizon.history == 0 {
                return Err(ConfigError::Invalid(format!(
                    "горизонт {}: базовая линия из нуля окон — сравнивать не с чем",
                    horizon.name
                )));
            }
            if horizon.period > horizon.width {
                return Err(ConfigError::Invalid(format!(
                    "горизонт {}: период оценки {:?} больше ширины окна {:?} — окна разойдутся",
                    horizon.name, horizon.period, horizon.width
                )));
            }
        }
        if self.collector.chunk.is_zero() {
            return Err(ConfigError::Invalid(
                "размер куска дозапроса равен нулю: просить будет нечего".to_owned(),
            ));
        }
        if self.digging.parallel == 0 || self.queue.parallel == 0 {
            return Err(ConfigError::Invalid(
                "потолок одновременных расследований равен нулю: разбирать будет некому".to_owned(),
            ));
        }
        Ok(())
    }

    /// Поля, которых агент не знает, — с путём до каждого.
    fn unknown(&self) -> Vec<String> {
        let mut found = Vec::new();
        collect("", &self.rest, &mut found);
        collect("logs", &self.logs.rest, &mut found);
        collect("metrics", &self.metrics.rest, &mut found);
        collect("model", &self.model.rest, &mut found);
        collect("collector", &self.collector.rest, &mut found);
        collect("incidents", &self.incidents.rest, &mut found);
        collect("digging", &self.digging.rest, &mut found);
        collect("queue", &self.queue.rest, &mut found);
        collect("retention", &self.retention.rest, &mut found);
        for horizon in &self.horizons {
            collect(
                &format!("horizon.{}", horizon.name),
                &horizon.rest,
                &mut found,
            );
        }
        found
    }

    /// Горизонты, которые включены.
    #[must_use]
    pub fn enabled(&self) -> Vec<&Horizon> {
        self.horizons.iter().filter(|it| it.enabled).collect()
    }
}

fn collect(section: &str, rest: &BTreeMap<String, toml::Value>, found: &mut Vec<String>) {
    for key in rest.keys() {
        found.push(if section.is_empty() {
            key.clone()
        } else {
            format!("{section}.{key}")
        });
    }
}

impl Default for Collector {
    fn default() -> Self {
        Self {
            backfill: default_backfill(),
            chunk: default_chunk(),
            rest: BTreeMap::new(),
        }
    }
}

impl Default for Incidents {
    fn default() -> Self {
        Self {
            service_labels: default_labels(),
            batch: default_batch(),
            samples: default_samples(),
            groups: default_groups(),
            sample_window: default_sample_window(),
            silence: default_silence(),
            link: default_link(),
            rest: BTreeMap::new(),
        }
    }
}

impl Digging {
    /// Те же настройки, но с другим терпением: очередь проверяется на обоих
    /// краях — успели и не успели.
    #[must_use]
    pub fn patient(self, patience: Duration) -> Self {
        Self { patience, ..self }
    }
}

impl Default for Digging {
    fn default() -> Self {
        Self {
            skills: default_skills(),
            parallel: default_parallel(),
            batch: default_batch(),
            steps: default_steps(),
            wider: default_wider(),
            patience: default_patience(),
            rest: BTreeMap::new(),
        }
    }
}

impl Default for Queue {
    fn default() -> Self {
        Self {
            parallel: default_parallel(),
            patience: default_patience(),
            rest: BTreeMap::new(),
        }
    }
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            minute_buckets: default_minutes(),
            hour_buckets: default_hours(),
            investigations: default_investigations(),
            incidents: default_incidents(),
            rest: BTreeMap::new(),
        }
    }
}

/// Сутки как длительность.
///
/// `Duration::from_days` пока нестабилен, а сроки хранения измеряются днями —
/// одно место с оговоркой лучше россыпи одинаковых умножений.
const fn days(count: u64) -> Duration {
    Duration::from_secs(count * 24 * 60 * 60)
}

fn default_bind() -> SocketAddr {
    "0.0.0.0:8096"
        .parse()
        .expect("адрес по умолчанию корректен")
}
fn default_database() -> PathBuf {
    PathBuf::from("/opt/data/sreagent/sre.db")
}
fn default_error_pattern() -> String {
    "i(error*) OR i(exception*) OR i(panic*) OR i(fatal*) OR i(traceback*)".to_owned()
}
fn default_source_timeout() -> Duration {
    Duration::from_mins(1)
}
fn default_model_timeout() -> Duration {
    Duration::from_mins(3)
}
fn default_max_tokens() -> u32 {
    900
}
fn default_temperature() -> f32 {
    0.2
}
fn default_enabled() -> bool {
    true
}
fn default_history() -> usize {
    24
}
fn default_minimum() -> f64 {
    20.0
}
fn default_score() -> f64 {
    3.5
}
fn default_ratio() -> f64 {
    2.0
}
fn default_labels() -> Vec<String> {
    // `__series__` последним: у метрики без метки сервиса именем становится сама
    // серия, и это читается несравнимо лучше, чем весь селектор.
    ["service", "container", "job", "app", "__series__"]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect()
}
fn default_batch() -> usize {
    50
}
fn default_samples() -> usize {
    200
}
fn default_groups() -> usize {
    7
}
fn default_sample_window() -> Duration {
    Duration::from_mins(15)
}
fn default_silence() -> Duration {
    Duration::from_mins(30)
}
fn default_link() -> Duration {
    Duration::from_mins(5)
}
fn default_backfill() -> Duration {
    Duration::from_hours(2)
}
fn default_chunk() -> Duration {
    Duration::from_hours(1)
}
fn default_drift() -> f64 {
    0.15
}
fn default_skills() -> PathBuf {
    PathBuf::from("/opt/data/sreagent/knowledge/skills")
}
fn default_steps() -> usize {
    3
}
fn default_wider() -> Duration {
    Duration::from_hours(1)
}
fn default_parallel() -> usize {
    4
}
fn default_patience() -> Duration {
    Duration::from_mins(15)
}
fn default_minutes() -> Duration {
    days(7)
}
fn default_hours() -> Duration {
    days(395)
}
fn default_investigations() -> Duration {
    days(90)
}
fn default_incidents() -> Duration {
    days(365)
}
