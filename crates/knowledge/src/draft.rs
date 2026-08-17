//! Черновик заметки: что агент написал сам и что ещё не проверил человек.
//!
//! Агент пишет только в `drafts/`, в базу черновик переводит дежурный
//! ([ADR-0009](../../../docs/adr/0009-drafts-before-knowledge.md)). Иначе
//! ошибочный вывод становится источником для будущих расследований — петля, в
//! которой неверное знание закрепляется и выглядит всё убедительнее.
//!
//! Приёмка — это перенос файла и коммит. Правит текст дежурный сам, в своём
//! редакторе: агент не умеет писать за него и не должен делать вид.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::KnowledgeError;

/// Черновик, готовый лечь файлом.
#[derive(Debug, Clone, PartialEq)]
pub struct Draft {
    /// Имя файла без расширения, латиницей.
    pub name: String,
    pub title: String,
    pub kind: String,
    pub tags: Vec<String>,
    pub signatures: Vec<String>,
    pub services: Vec<String>,
    /// Из какого инцидента родился.
    pub incident: i64,
    /// Насколько агент себе верит.
    pub confidence: f64,
    pub body: String,
}

impl Draft {
    /// Текст файла целиком: фронтматтер плюс тело.
    #[must_use]
    pub fn page(&self) -> String {
        let signatures = self
            .signatures
            .iter()
            .map(|it| format!("  - '{}'", it.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "---\nname: {}\ntitle: {}\nkind: {}\ntags: [{}]\nsignatures:\n{}\nservices: [{}]\nauthor: agent\nincident: {}\nconfidence: {:.1}\n---\n\n> Черновик. Написан агентом, дежурным не проверен.\n\n{}\n",
            self.name,
            self.title,
            self.kind,
            self.tags.join(", "),
            signatures,
            self.services
                .iter()
                .map(|it| format!("'{it}'"))
                .collect::<Vec<_>>()
                .join(", "),
            self.incident,
            self.confidence,
            self.body.trim(),
        )
    }
}

/// Кладёт черновик в каталог и отвечает путём к нему.
///
/// # Errors
/// [`KnowledgeError::Directory`], если каталог недоступен или файл не записан.
pub fn write(drafts: &Path, draft: &Draft) -> Result<PathBuf, KnowledgeError> {
    std::fs::create_dir_all(drafts)?;
    let path = drafts.join(format!("{}.md", draft.name));
    std::fs::write(&path, draft.page())?;
    Ok(path)
}

/// Переводит черновик в базу знаний и отвечает новым путём.
///
/// Три поля агента — `author`, `incident`, `confidence` — и врезка «черновик»
/// снимаются: в `notes/` лежит знание, а не история его происхождения. История
/// остаётся в git.
///
/// # Errors
/// [`KnowledgeError::Directory`], если файл не читается или не переносится.
pub fn accept(draft: &Path, notes: &Path, who: &str) -> Result<PathBuf, KnowledgeError> {
    let text = std::fs::read_to_string(draft)?;
    std::fs::create_dir_all(notes)?;
    let name = draft
        .file_name()
        .map_or_else(|| PathBuf::from("note.md"), PathBuf::from);
    let path = notes.join(name);
    std::fs::write(&path, grown(&text))?;
    std::fs::remove_file(draft)?;
    commit(
        &path,
        draft,
        &format!("Заметка принята: {}", stem(draft)),
        who,
    );
    Ok(path)
}

/// Помечает черновик отклонённым, оставляя его на месте.
///
/// Не удаляет: отклонённый черновик — это запись о том, что агент подумал и
/// ошибся, и она стоит дороже освобождённого килобайта.
///
/// # Errors
/// [`KnowledgeError::Directory`], если файл не читается или не пишется.
pub fn reject(draft: &Path, who: &str) -> Result<(), KnowledgeError> {
    let text = std::fs::read_to_string(draft)?;
    std::fs::write(draft, marked(&text, who))?;
    Ok(())
}

/// Заметка без следов черновика.
fn grown(text: &str) -> String {
    text.lines()
        .filter(|line| {
            !line.starts_with("author:")
                && !line.starts_with("incident:")
                && !line.starts_with("confidence:")
                && !line.starts_with("> Черновик")
        })
        .collect::<Vec<_>>()
        .join("\n")
        .replace("\n\n\n", "\n\n")
}

/// Черновик с пометкой отклонившего.
fn marked(text: &str, who: &str) -> String {
    if text.contains("rejected_by:") {
        return text.to_owned();
    }
    text.replacen(
        "author: agent",
        &format!("author: agent\nrejected_by: {who}"),
        1,
    )
}

/// Имя файла без расширения.
fn stem(path: &Path) -> String {
    path.file_stem()
        .map(|it| it.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Коммитит перенос, если репозиторий знаний под git.
///
/// Отказ git — предупреждение, а не отказ приёмки: заметка уже в `notes/`, и
/// откатывать её из-за несделанного коммита значит терять работу дежурного.
fn commit(added: &Path, removed: &Path, message: &str, who: &str) {
    let Some(repo) = added.parent().and_then(Path::parent) else {
        return;
    };
    let git = |args: &[&str]| {
        Command::new("git")
            .current_dir(repo)
            .args(args)
            .output()
            .ok()
            .filter(|done| done.status.success())
    };
    if git(&["rev-parse", "--git-dir"]).is_none() {
        tracing::info!(repo = %repo.display(), "репозиторий знаний не под git, коммита не будет");
        return;
    }
    let added = added.to_string_lossy().into_owned();
    let removed = removed.to_string_lossy().into_owned();
    // Черновик мог и не попасть в git: агент его создал, и никто не коммитил.
    // Тогда «удалить» нечего, и это не отказ.
    git(&["add", "-A", "--", &removed]);
    let done = git(&["add", "--", &added]).and_then(|_| {
        git(&[
            "-c",
            "user.name=Auto SRE",
            "-c",
            "user.email=sreagent@localhost",
            "commit",
            "-m",
            &format!("{message}\n\nПринял: {who}"),
        ])
    });
    if done.is_none() {
        tracing::warn!(%added, "коммит в репозиторий знаний не сделан");
    }
}
