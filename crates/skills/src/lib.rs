//! Скиллы анализа: markdown с фронтматтером.
//!
//! Пишет их SRE, а не разработчик агента — в этом вся затея
//! ([ADR-0003](../../../docs/adr/0003-skills-as-markdown.md)). Отсюда два
//! правила, которые важнее удобства кода:
//!
//! - битый скилл не роняет агента: он не применяется, а причина уходит в журнал;
//! - незнакомое поле — предупреждение, а не отказ: скиллы живут дольше версий.

use std::collections::BTreeMap;
use std::path::Path;

use autosre_domain::{Deviation, Stream};
use regex::Regex;
use serde::Deserialize;

/// Отказы разбора скилла.
#[derive(Debug, thiserror::Error)]
pub enum SkillError {
    #[error("каталог скиллов не прочитан: {0}")]
    Directory(#[from] std::io::Error),
    #[error("нет фронтматтера: файл должен начинаться с строки ---")]
    Frontless,
    #[error("фронтматтер не разобран: {0}")]
    Shape(#[from] serde_yaml_ng::Error),
    #[error("условие применения не разобрано: {0}")]
    Condition(String),
}

/// Условие применения скилла.
#[derive(Debug, Clone, Deserialize)]
pub struct When {
    /// По какому сигналу пришло отклонение: `errors` или `metric`.
    pub signal: String,
    /// Необязательное сужение по селектору потока.
    #[serde(default)]
    pub stream: Option<String>,
    /// Необязательное сужение по имени метрики.
    #[serde(default)]
    pub metric: Option<String>,
}

/// Запрос первого досье.
#[derive(Debug, Clone, Deserialize)]
pub struct Collect {
    /// Имя, под которым результат придёт в промпт.
    pub id: String,
    /// Запрос на языке источника логов.
    #[serde(default)]
    pub vl: Option<String>,
    /// Запрос на языке источника метрик.
    #[serde(default)]
    pub vm: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Машинная часть скилла.
#[derive(Debug, Clone, Deserialize)]
pub struct Front {
    pub name: String,
    pub title: String,
    pub horizon: String,
    pub when: When,
    #[serde(default)]
    pub collect: Vec<Collect>,
    #[serde(default)]
    pub steps: Option<usize>,
    /// Поля, которых агент не знает: скилл новее его самого.
    #[serde(flatten)]
    rest: BTreeMap<String, serde_yaml_ng::Value>,
}

/// Скилл целиком: машинная часть и наставление модели.
#[derive(Debug, Clone)]
pub struct Skill {
    pub front: Front,
    /// Тело: что смотреть и как отличать. Уходит в промпт как есть.
    pub body: String,
    stream: Option<Regex>,
    metric: Option<Regex>,
}

impl Skill {
    /// Разбирает скилл из текста файла.
    ///
    /// # Errors
    /// [`SkillError::Frontless`] без фронтматтера, [`SkillError::Shape`] на
    /// неразбираемом фронтматтере, [`SkillError::Condition`] на негодном
    /// регулярном выражении в условии.
    pub fn parse(text: &str) -> Result<Self, SkillError> {
        let rest = text.strip_prefix("---").ok_or(SkillError::Frontless)?;
        let (front, body) = rest.split_once("\n---").ok_or(SkillError::Frontless)?;
        let front: Front = serde_yaml_ng::from_str(front)?;
        let stream = compile(front.when.stream.as_deref())?;
        let metric = compile(front.when.metric.as_deref())?;
        Ok(Self {
            front,
            body: body.trim().to_owned(),
            stream,
            metric,
        })
    }

    /// Подходит ли скилл этому отклонению.
    #[must_use]
    pub fn fits(&self, deviation: &Deviation) -> bool {
        if self.front.when.signal != signal(&deviation.source) && self.front.when.signal != "any" {
            return false;
        }
        if self.front.horizon != deviation.horizon {
            return false;
        }
        let selector = deviation.stream.as_str();
        self.stream.as_ref().is_none_or(|it| it.is_match(selector))
            && self.metric.as_ref().is_none_or(|it| it.is_match(selector))
    }

    /// Незнакомые поля фронтматтера — их надо назвать, но не падать из-за них.
    #[must_use]
    pub fn unknown(&self) -> Vec<String> {
        self.front.rest.keys().cloned().collect()
    }

    /// Знает ли агент сигнал, на который написан скилл.
    ///
    /// Неизвестное слово в `when.signal` — это скилл, который не применится
    /// никогда и никому об этом не скажет. Молчаливое несовпадение хуже отказа:
    /// горизонт выглядит разобранным, а разбирать его нечем.
    #[must_use]
    pub fn heard(&self) -> bool {
        SIGNALS.contains(&self.front.when.signal.as_str())
    }
}

/// Сигналы, на которые пишут скиллы.
///
/// Единственное число, как и всё в [`NAMES.md`](../../../docs/NAMES.md):
/// метрика одна, а ошибок много — отсюда `errors` рядом с `metric`.
const SIGNALS: &[&str] = &["errors", "metric", "any"];

/// Как источник наблюдения зовётся в скиллах.
///
/// Имена источников — внутреннее дело агента, имена сигналов — интерфейс с
/// людьми ([`KNOWLEDGE.md`](../../../docs/KNOWLEDGE.md) §2). Отображение живёт
/// в одном месте, чтобы они больше не разъезжались.
fn signal(source: &str) -> &'static str {
    match source {
        "logs" => "errors",
        _ => "metric",
    }
}

/// Читает все скиллы каталога.
///
/// Битый скилл пропускается со строкой в журнале: один неверный файл не должен
/// лишать агента остальных.
///
/// # Errors
/// [`SkillError::Directory`], если каталог недоступен целиком.
pub fn read(directory: &Path) -> Result<Vec<Skill>, SkillError> {
    let mut skills = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        // Скрытые файлы — не скиллы: архиватор macOS оставляет рядом с каждым
        // файлом свой `._имя.md`, и ругаться на него нечего.
        if path.extension().is_none_or(|it| it != "md")
            || path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with('.'))
        {
            continue;
        }
        match std::fs::read_to_string(&path)
            .map_err(SkillError::from)
            .and_then(|text| Skill::parse(&text))
        {
            Ok(skill) => {
                if !skill.heard() {
                    tracing::error!(
                        skill = skill.front.name,
                        signal = skill.front.when.signal,
                        known = SIGNALS.join(", "),
                        "сигнал скилла неизвестен агенту: скилл не применится ни разу"
                    );
                }
                for field in skill.unknown() {
                    tracing::warn!(
                        skill = skill.front.name,
                        field,
                        "поле скилла неизвестно агенту и пропущено"
                    );
                }
                skills.push(skill);
            }
            Err(failure) => {
                tracing::error!(path = %path.display(), %failure, "скилл не применяется");
            }
        }
    }
    skills.sort_by(|left, right| left.front.name.cmp(&right.front.name));
    Ok(skills)
}

/// Окно, подставляемое в запрос: границы в RFC 3339.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    /// Начало окна горизонта.
    pub start: String,
    /// Конец окна горизонта.
    pub end: String,
    /// Начало такого же окна перед этим: для сравнения «до и во время».
    pub before: String,
}

/// Подставляет в запрос данные отклонения.
///
/// Единственный путь данным наблюдения попасть в запрос. Собирается в одном
/// месте, чтобы кавычка в имени контейнера не превращалась в чужой запрос.
#[must_use]
pub fn fill(query: &str, deviation: &Deviation, service: &str, window: &Window) -> String {
    query
        .replace("{start}", &window.start)
        .replace("{end}", &window.end)
        .replace("{before}", &window.before)
        .replace("{horizon}", &deviation.horizon)
        .replace("{stream}", &safe(&deviation.stream))
        .replace("{metric}", &safe(&Stream::new(series(&deviation.stream))))
        .replace("{service}", &safe(&Stream::new(service)))
}

/// Имя серии из селектора потока метрик; у логов его нет, и это пусто.
fn series(stream: &Stream) -> String {
    let text = stream.as_str();
    let Some(start) = text.find("__series__=\"") else {
        return String::new();
    };
    let rest = &text[start + "__series__=\"".len()..];
    rest.split('"').next().unwrap_or_default().to_owned()
}

/// Убирает из значения то, чем можно закрыть строку и приписать своё.
fn safe(value: &Stream) -> String {
    value.as_str().replace(['\\', '\n'], "").replace('|', "")
}

fn compile(pattern: Option<&str>) -> Result<Option<Regex>, SkillError> {
    pattern
        .map(|it| Regex::new(it).map_err(|cause| SkillError::Condition(cause.to_string())))
        .transpose()
}
