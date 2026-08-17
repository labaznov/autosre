//! База знаний: заметки, которые пишут и читают люди.
//!
//! Живёт в отдельном репозитории и правится людьми
//! ([ADR-0015](../../../docs/adr/0015-knowledge-repository.md)), формат описан
//! в [`docs/KNOWLEDGE.md`](../../../docs/KNOWLEDGE.md). Отсюда те же два
//! правила, что и у скиллов: битая заметка не роняет агента, а незнакомое поле
//! не отменяет заметку — файлы переживут не одну версию агента.
//!
//! Крейт только читает каталог и разбирает файлы. Поиск живёт в хранилище: он
//! требует индекса, а индекс — базы.

pub mod draft;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

pub use draft::Draft;

/// Отказы чтения базы знаний.
#[derive(Debug, thiserror::Error)]
pub enum KnowledgeError {
    #[error("каталог знаний не прочитан: {0}")]
    Directory(#[from] std::io::Error),
    #[error("нет фронтматтера: файл должен начинаться с строки ---")]
    Frontless,
    #[error("фронтматтер не разобран: {0}")]
    Shape(#[from] serde_yaml_ng::Error),
}

/// Машинная часть заметки.
#[derive(Debug, Clone, Deserialize)]
pub struct Front {
    pub name: String,
    pub title: String,
    /// `error`, `incident` или `method`.
    pub kind: String,
    /// Слова для поиска, включая русские синонимы: без них «диск кончился» не
    /// найдёт `no space left`.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Точные куски сообщений — по ним заметка находится вернее всего.
    #[serde(default)]
    pub signatures: Vec<String>,
    #[serde(default)]
    pub services: Vec<String>,
    #[serde(default)]
    pub related: Vec<String>,
    /// Поля, которых агент не знает: заметка новее его самого.
    #[serde(flatten)]
    rest: BTreeMap<String, serde_yaml_ng::Value>,
}

/// Заметка целиком.
#[derive(Debug, Clone)]
pub struct Note {
    pub front: Front,
    /// Тело: то, что читает человек, и то, по чему идёт поиск.
    pub body: String,
    /// Откуда прочитана — по ней заметку правят и её же видно в карточке.
    pub path: PathBuf,
}

impl Note {
    /// Разбирает заметку из текста файла.
    ///
    /// # Errors
    /// [`KnowledgeError::Frontless`] без фронтматтера,
    /// [`KnowledgeError::Shape`] на неразбираемом фронтматтере.
    pub fn parse(text: &str, path: &Path) -> Result<Self, KnowledgeError> {
        let rest = text.strip_prefix("---").ok_or(KnowledgeError::Frontless)?;
        let (front, body) = rest.split_once("\n---").ok_or(KnowledgeError::Frontless)?;
        Ok(Self {
            front: serde_yaml_ng::from_str(front)?,
            body: body.trim().to_owned(),
            path: path.to_owned(),
        })
    }

    /// Незнакомые поля фронтматтера.
    #[must_use]
    pub fn unknown(&self) -> Vec<String> {
        self.front.rest.keys().cloned().collect()
    }

    /// Теги и сигнатуры одной строкой — в таком виде их принимает индекс.
    #[must_use]
    pub fn words(&self) -> String {
        let mut words = self.front.tags.clone();
        words.extend(self.front.services.clone());
        words.join(" ")
    }

    /// Сигнатуры одной строкой.
    #[must_use]
    pub fn marks(&self) -> String {
        self.front.signatures.join(" ")
    }
}

/// Читает все заметки каталога.
///
/// Битая заметка пропускается со строкой в журнале: один неверный файл не
/// должен лишать агента остальных.
///
/// # Errors
/// [`KnowledgeError::Directory`], если каталог недоступен целиком.
pub fn read(directory: &Path) -> Result<Vec<Note>, KnowledgeError> {
    let mut notes = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().is_none_or(|it| it != "md") {
            continue;
        }
        match std::fs::read_to_string(&path)
            .map_err(KnowledgeError::from)
            .and_then(|text| Note::parse(&text, &path))
        {
            Ok(note) => {
                for field in note.unknown() {
                    tracing::warn!(
                        note = note.front.name,
                        field,
                        "поле заметки неизвестно агенту и пропущено"
                    );
                }
                notes.push(note);
            }
            Err(failure) => {
                tracing::error!(path = %path.display(), %failure, "заметка не прочитана");
            }
        }
    }
    notes.sort_by(|left, right| left.front.name.cmp(&right.front.name));
    Ok(notes)
}

/// Момент последней правки в каталоге и число файлов.
///
/// По этой паре видно, что репозиторий знаний обновился и индекс пора
/// пересобрать. Читать заново весь каталог ради этого не нужно.
///
/// # Errors
/// [`KnowledgeError::Directory`], если каталог недоступен.
pub fn touched(directory: &Path) -> Result<(u64, usize), KnowledgeError> {
    let mut newest = 0;
    let mut count = 0;
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.path().extension().is_none_or(|it| it != "md") {
            continue;
        }
        count += 1;
        if let Ok(stamp) = entry.metadata()?.modified()
            && let Ok(since) = stamp.duration_since(std::time::UNIX_EPOCH)
        {
            newest = newest.max(since.as_secs());
        }
    }
    Ok((newest, count))
}
