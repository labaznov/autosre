use std::path::Path;
use std::process::Command;

use sre_knowledge::draft::{accept, reject, write};
use sre_knowledge::{Draft, Note};
use tempfile::TempDir;

fn draft() -> Draft {
    Draft {
        name: "2026-08-17-orders-api-1043".to_owned(),
        title: "Апстрим orders-api перестал отвечать".to_owned(),
        kind: "incident".to_owned(),
        tags: vec!["таймаут".to_owned(), "orders-api".to_owned()],
        signatures: vec!["upstream timed out".to_owned()],
        services: vec!["orders-api".to_owned()],
        incident: 1043,
        confidence: 0.7,
        body: "## Что видно\n\nАпстрим молчит тридцать секунд.".to_owned(),
    }
}

/// Репозиторий знаний во временном каталоге.
struct Repo {
    directory: TempDir,
}

impl Repo {
    fn new() -> Self {
        let directory = TempDir::new().expect("временный каталог не создан");
        std::fs::create_dir_all(directory.path().join("drafts")).expect("каталог не создан");
        std::fs::create_dir_all(directory.path().join("notes")).expect("каталог не создан");
        Self { directory }
    }

    fn drafts(&self) -> std::path::PathBuf {
        self.directory.path().join("drafts")
    }

    fn notes(&self) -> std::path::PathBuf {
        self.directory.path().join("notes")
    }

    /// Тот же репозиторий, но под git.
    fn versioned(self) -> Self {
        let git = |args: &[&str]| {
            Command::new("git")
                .current_dir(self.directory.path())
                .args(args)
                .output()
                .expect("git не запустился")
        };
        git(&["init", "-q"]);
        git(&["config", "user.email", "тест@localhost"]);
        git(&["config", "user.name", "тест"]);
        std::fs::write(self.directory.path().join("README.md"), "знания").expect("файл не записан");
        git(&["add", "."]);
        git(&["commit", "-qm", "начало"]);
        self
    }

    fn log(&self) -> String {
        String::from_utf8_lossy(
            &Command::new("git")
                .current_dir(self.directory.path())
                .args(["log", "--format=%B"])
                .output()
                .expect("git не запустился")
                .stdout,
        )
        .into_owned()
    }
}

#[test]
fn writes_a_draft_where_the_duty_engineer_looks() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    assert!(path.ends_with("2026-08-17-orders-api-1043.md"));
}

#[test]
fn marks_the_draft_as_written_by_the_agent() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    let text = std::fs::read_to_string(path).expect("черновик не прочитан");
    assert!(text.contains("author: agent"));
}

#[test]
fn writes_a_draft_a_reader_can_parse() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    let text = std::fs::read_to_string(&path).expect("черновик не прочитан");
    assert_eq!(
        Note::parse(&text, &path)
            .expect("черновик не разобран")
            .front
            .name,
        "2026-08-17-orders-api-1043"
    );
}

#[test]
fn says_out_loud_that_nobody_checked_it() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    let text = std::fs::read_to_string(path).expect("черновик не прочитан");
    assert!(text.contains("дежурным не проверен"));
}

#[test]
fn moves_an_accepted_draft_into_the_base() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    let note = accept(&path, &repo.notes(), "букин").expect("черновик не принят");
    assert!(note.starts_with(repo.notes()));
}

#[test]
fn takes_an_accepted_draft_out_of_the_drafts() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    accept(&path, &repo.notes(), "букин").expect("черновик не принят");
    assert!(!Path::new(&path).exists());
}

#[test]
fn drops_the_traces_of_a_draft_from_the_note() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    let note = accept(&path, &repo.notes(), "букин").expect("черновик не принят");
    let text = std::fs::read_to_string(note).expect("заметка не прочитана");
    assert!(!text.contains("author: agent"));
}

#[test]
fn keeps_the_knowledge_of_an_accepted_draft() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    let note = accept(&path, &repo.notes(), "букин").expect("черновик не принят");
    let text = std::fs::read_to_string(note).expect("заметка не прочитана");
    assert!(text.contains("Апстрим молчит тридцать секунд"));
}

#[test]
fn signs_the_commit_with_the_name_of_the_one_who_accepted() {
    let repo = Repo::new().versioned();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    accept(&path, &repo.notes(), "букин").expect("черновик не принят");
    assert!(repo.log().contains("Принял: букин"));
}

#[test]
fn accepts_a_draft_outside_of_any_repository() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    assert!(accept(&path, &repo.notes(), "букин").is_ok());
}

#[test]
fn keeps_a_rejected_draft_where_it_was() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    reject(&path, "букин").expect("черновик не отклонён");
    assert!(Path::new(&path).exists());
}

#[test]
fn names_the_one_who_rejected_a_draft() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    reject(&path, "букин").expect("черновик не отклонён");
    let text = std::fs::read_to_string(path).expect("черновик не прочитан");
    assert!(text.contains("rejected_by: букин"));
}

#[test]
fn rejects_a_draft_once() {
    let repo = Repo::new();
    let path = write(&repo.drafts(), &draft()).expect("черновик не записан");
    reject(&path, "букин").expect("черновик не отклонён");
    reject(&path, "другой").expect("черновик не отклонён");
    let text = std::fs::read_to_string(path).expect("черновик не прочитан");
    assert_eq!(text.matches("rejected_by").count(), 1);
}
