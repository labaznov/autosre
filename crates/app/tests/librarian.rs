use std::sync::Arc;
use std::time::Duration;

use sre_app::config::Knowledge;
use sre_app::librarian::{keep, learn};
use sre_app::metrics::Metrics;
use sre_store::Store;
use tempfile::TempDir;

/// Заметка, годная во всём.
const NOTE: &str = r"---
name: vl-no-space-left
title: Кончилось место на диске
kind: error
tags: [диск, место, no-space-left]
signatures:
  - 'no space left on device'
---

Процессу отказано в записи.
";

/// Стенд: база и каталог заметок, который можно править на ходу.
struct Shelf {
    store: Store,
    metrics: Arc<Metrics>,
    settings: Knowledge,
    notes: TempDir,
    _directory: TempDir,
}

impl Shelf {
    fn new() -> Self {
        let directory = TempDir::new().expect("временный каталог не создан");
        let notes = TempDir::new().expect("каталог заметок не создан");
        let store = Store::open(&directory.path().join("sre.db")).expect("база не открыта");
        let settings =
            Knowledge::default().shelf(notes.path().to_path_buf(), Duration::from_millis(100));
        Self {
            store,
            metrics: Arc::new(Metrics::new("тест")),
            settings,
            notes,
            _directory: directory,
        }
    }

    fn write(&self, name: &str) {
        std::fs::write(
            self.notes.path().join(format!("{name}.md")),
            NOTE.replace("vl-no-space-left", name),
        )
        .expect("заметка не записана");
    }

    async fn wait(&self, count: u64) -> bool {
        for _ in 0..50 {
            if self.store.notes().await.unwrap_or(0) == count {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }
}

#[tokio::test]
async fn indexes_the_notes_it_found() {
    let shelf = Shelf::new();
    shelf.write("vl-no-space-left");
    learn(&shelf.store, &shelf.metrics, &shelf.settings).await;
    assert_eq!(shelf.store.notes().await.unwrap(), 1);
}

#[tokio::test]
async fn finds_an_indexed_note_by_its_signature() {
    let shelf = Shelf::new();
    shelf.write("vl-no-space-left");
    learn(&shelf.store, &shelf.metrics, &shelf.settings).await;
    let found = shelf
        .store
        .recall("no space left on device", 3)
        .await
        .unwrap();
    assert_eq!(found[0].name, "vl-no-space-left");
}

#[tokio::test]
async fn shows_the_number_of_notes_in_the_metrics() {
    let shelf = Shelf::new();
    shelf.write("vl-no-space-left");
    learn(&shelf.store, &shelf.metrics, &shelf.settings).await;
    assert!(shelf.metrics.expose().contains("sre_notes 1"));
}

#[tokio::test]
async fn picks_up_a_note_written_while_it_runs() {
    let shelf = Shelf::new();
    keep(&shelf.store, &shelf.metrics, &shelf.settings);
    shelf.wait(0).await;
    shelf.write("disk-fill-rate");
    assert!(shelf.wait(1).await);
}

#[tokio::test]
async fn lives_without_a_knowledge_directory() {
    let shelf = Shelf::new();
    let settings = shelf.settings.clone().shelf(
        shelf.notes.path().join("нет-такого"),
        Duration::from_secs(1),
    );
    learn(&shelf.store, &shelf.metrics, &settings).await;
    assert_eq!(shelf.store.notes().await.unwrap(), 0);
}
