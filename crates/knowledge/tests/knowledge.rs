use std::path::Path;

use autosre_knowledge::{Note, read};
use tempfile::TempDir;

/// Заметка, годная во всём: от неё отталкиваются проверки.
const PLAIN: &str = r"---
name: vl-no-space-left
title: Кончилось место на диске
kind: error
tags: [диск, место, no-space-left]
signatures:
  - 'no space left on device'
services: ['victorialogs']
---

## Что значит

Процессу отказано в записи.
";

fn note(text: &str) -> Note {
    Note::parse(text, Path::new("notes/x.md")).expect("заметка не разобрана")
}

#[test]
fn reads_the_name_of_a_note() {
    assert_eq!(note(PLAIN).front.name, "vl-no-space-left");
}

#[test]
fn keeps_the_body_as_the_author_wrote_it() {
    assert!(note(PLAIN).body.starts_with("## Что значит"));
}

#[test]
fn reads_the_signatures_a_note_is_found_by() {
    assert_eq!(
        note(PLAIN).front.signatures,
        vec!["no space left on device"]
    );
}

#[test]
fn refuses_a_file_without_frontmatter() {
    assert!(Note::parse("просто текст", Path::new("x.md")).is_err());
}

#[test]
fn refuses_a_note_without_a_kind() {
    let broken = "---\nname: x\ntitle: без вида\n---\nтело\n";
    assert!(Note::parse(broken, Path::new("x.md")).is_err());
}

#[test]
fn names_an_unknown_field_instead_of_choking_on_it() {
    let newer = PLAIN.replace("kind: error", "kind: error\nseverity: высокая");
    assert_eq!(note(&newer).unknown(), vec!["severity"]);
}

#[test]
fn gathers_the_words_a_note_is_searched_by() {
    assert!(note(PLAIN).words().contains("диск"));
}

#[test]
fn counts_the_service_among_the_words() {
    assert!(note(PLAIN).words().contains("victorialogs"));
}

#[test]
fn reads_every_note_of_a_directory() {
    let directory = TempDir::new().expect("временный каталог не создан");
    for name in ["one", "two"] {
        std::fs::write(
            directory.path().join(format!("{name}.md")),
            PLAIN.replace("vl-no-space-left", name),
        )
        .expect("заметка не записана");
    }
    assert_eq!(read(directory.path()).unwrap().len(), 2);
}

#[test]
fn skips_a_broken_note_and_keeps_the_rest() {
    let directory = TempDir::new().expect("временный каталог не создан");
    std::fs::write(directory.path().join("good.md"), PLAIN).expect("заметка не записана");
    std::fs::write(directory.path().join("broken.md"), "мусор").expect("файл не записан");
    assert_eq!(read(directory.path()).unwrap().len(), 1);
}

#[test]
fn ignores_files_that_are_not_notes() {
    let directory = TempDir::new().expect("временный каталог не создан");
    std::fs::write(directory.path().join("good.md"), PLAIN).expect("заметка не записана");
    std::fs::write(directory.path().join("README.txt"), "не заметка").expect("файл не записан");
    assert_eq!(read(directory.path()).unwrap().len(), 1);
}

#[test]
fn sees_a_new_file_in_the_directory() {
    let directory = TempDir::new().expect("временный каталог не создан");
    std::fs::write(directory.path().join("one.md"), PLAIN).expect("заметка не записана");
    let before = autosre_knowledge::touched(directory.path()).unwrap();
    std::fs::write(directory.path().join("two.md"), PLAIN).expect("заметка не записана");
    assert_ne!(
        autosre_knowledge::touched(directory.path()).unwrap(),
        before
    );
}

#[test]
fn reads_the_shipped_notes() {
    let path =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/knowledge/notes");
    assert_eq!(read(&path).unwrap().len(), 2);
}
