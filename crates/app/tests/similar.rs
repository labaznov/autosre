use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::extract::State;
use axum::routing::post;
use sre_app::config::{Digging, Incidents, Knowledge};
use sre_app::digger::{Digger, dig};
use sre_app::metrics::Metrics;
use sre_domain::{
    Bucket, Detector, Deviation, Kind, Minute, Service, Signature, Span, Stream, Thresholds,
};
use sre_model::{Model, Settings};
use sre_source::{Source, SourceError};
use sre_store::{Memory, Store};
use tempfile::TempDir;

const SKILL: &str = r"---
name: error-burst
title: Всплеск ошибок
horizon: 15m
when:
  signal: errors
collect:
  - id: signatures
    vl: '_time:[{start}, {end}) {stream} | fields _msg'
---

Сравни сигнатуры окна с прошлым.
";

struct Talker;

#[async_trait]
impl Source for Talker {
    fn name(&self) -> &'static str {
        "logs"
    }

    async fn buckets(&self, _span: Span) -> Result<Vec<Bucket>, SourceError> {
        Ok(Vec::new())
    }

    async fn samples(
        &self,
        _stream: &Stream,
        _span: Span,
        _limit: usize,
    ) -> Result<Vec<String>, SourceError> {
        Ok(vec!["no space left on device".to_owned()])
    }
}

/// Поддельная модель: запоминает досье и ссылается на заданную заметку.
#[derive(Clone)]
struct Talk {
    seen: Arc<std::sync::Mutex<String>>,
    asked: Arc<AtomicUsize>,
    note: &'static str,
}

async fn answer(State(talk): State<Talk>, body: String) -> String {
    talk.asked.fetch_add(1, Ordering::Relaxed);
    *talk.seen.lock().expect("замок цел") = body;
    format!(
        r#"{{"choices":[{{"message":{{"content":"{{\"cause\":\"кончилось место\",\"confidence\":0.8,\"advice\":\"чистить диск\",\"need\":\"nothing\",\"severity\":\"high\",\"note\":\"{}\"}}"}}}}]}}"#,
        talk.note
    )
}

/// Клиент модели, складывающий каждый заход в корпус: так поднимает его агент
/// со включённым сбором живых данных.
fn recording(model: Model, store: &Store) -> Model {
    model.recording(Arc::new(sre_app::scribe::Scribe::new(
        store,
        &Arc::new(Metrics::new("тест")),
    )))
}

struct Stand {
    store: Store,
    seen: Arc<std::sync::Mutex<String>>,
    drafts: TempDir,
    _skills: TempDir,
    _directory: TempDir,
}

impl Stand {
    async fn start(note: &'static str) -> Self {
        let directory = TempDir::new().expect("временный каталог не создан");
        let store = Store::open(&directory.path().join("sre.db")).expect("база не открыта");
        let skills = TempDir::new().expect("каталог скиллов не создан");
        let drafts = TempDir::new().expect("каталог черновиков не создан");
        std::fs::write(skills.path().join("error-burst.md"), SKILL).expect("скилл не записан");
        store
            .remember(vec![Memory {
                name: "vl-no-space-left".to_owned(),
                title: "Кончилось место на диске".to_owned(),
                tags: "диск место orders-api".to_owned(),
                marks: "no space left on device".to_owned(),
                body: "Процессу отказано в записи на разделе".to_owned(),
            }])
            .await
            .expect("индекс не собран");

        let seen = Arc::new(std::sync::Mutex::new(String::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("порт не занят");
        let address: SocketAddr = listener.local_addr().expect("адрес не получен");
        let router = Router::new()
            .route("/v1/chat/completions", post(answer))
            .with_state(Talk {
                seen: Arc::clone(&seen),
                asked: Arc::new(AtomicUsize::new(0)),
                note,
            });
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("сервер упал");
        });

        let model = Arc::new(recording(
            Model::new(Settings {
                url: format!("http://{address}/")
                    .parse()
                    .expect("адрес некорректен"),
                key: "sk-lab".to_owned(),
                name: "поддельная".to_owned(),
                temperature: 0.2,
                tokens: 300,
                timeout: Duration::from_secs(5),
            })
            .expect("клиент не собрался"),
            &store,
        ));
        let sources: Vec<Arc<dyn Source>> = vec![Arc::new(Talker)];
        dig(Digger::new(
            &sources,
            &store,
            &Arc::new(Metrics::new("тест")),
            &model,
            sre_skills::read(skills.path()).expect("скиллы не прочитаны"),
            &sre_app::digger::Recipe {
                digging: &Digging::default()
                    .patient(Duration::from_hours(1))
                    .quick(Duration::from_millis(200)),
                incidents: &Incidents::default(),
                knowledge: &Knowledge::default().drafting(drafts.path().to_path_buf()),
            },
        ));
        Self {
            store,
            seen,
            drafts,
            _skills: skills,
            _directory: directory,
        }
    }

    async fn incident(&self) -> i64 {
        let at = Minute::of(chrono::Utc::now()).back(1);
        let stream = Stream::new("{host=\"node-01\",service=\"orders-api\"}");
        let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0], 91.0, Kind::Sum);
        let deviation = Deviation::new("logs", &stream, "15m", at, verdict);
        self.store.spot(&deviation, at).await.unwrap();
        let id = self.store.loose(10).await.unwrap()[0].0;
        self.store
            .attach(
                id,
                &Service::new("orders-api"),
                &Signature::of("no space left on device"),
                &deviation,
                "такого раньше не было",
            )
            .await
            .unwrap()
            .0
    }

    async fn done(&self, incident: i64) -> sre_store::Finding {
        for _ in 0..100 {
            if let Ok(Some(found)) = self.store.conclusion(incident).await
                && found.state == "done"
            {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("вывода нет");
    }
}

#[tokio::test]
async fn puts_a_similar_case_into_the_dossier() {
    let stand = Stand::start("vl-no-space-left").await;
    let incident = stand.incident().await;
    stand.done(incident).await;
    let seen = stand.seen.lock().expect("замок цел").clone();
    assert!(seen.contains("Процессу отказано в записи"));
}

#[tokio::test]
async fn keeps_the_note_the_conclusion_leaned_on() {
    let stand = Stand::start("vl-no-space-left").await;
    let incident = stand.incident().await;
    assert_eq!(
        stand.done(incident).await.note.as_deref(),
        Some("vl-no-space-left")
    );
}

#[tokio::test]
async fn drops_a_reference_to_a_note_that_does_not_exist() {
    let stand = Stand::start("выдуманная-заметка").await;
    let incident = stand.incident().await;
    assert_eq!(stand.done(incident).await.note, None);
}

#[tokio::test]
async fn writes_down_the_words_the_note_was_found_by() {
    let stand = Stand::start("vl-no-space-left").await;
    let incident = stand.incident().await;
    let found = stand.done(incident).await;
    let steps = stand.store.steps(found.id).await.unwrap();
    assert!(
        steps
            .iter()
            .any(|(tool, about, _)| tool == "knowledge" && about.contains("vl-no-space-left"))
    );
}

#[tokio::test]
async fn writes_a_draft_of_what_it_learned() {
    let stand = Stand::start("выдуманная-заметка").await;
    let incident = stand.incident().await;
    stand.done(incident).await;
    assert_eq!(stand.store.unsettled(10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn puts_the_draft_where_the_duty_engineer_looks() {
    let stand = Stand::start("выдуманная-заметка").await;
    let incident = stand.incident().await;
    stand.done(incident).await;
    let written = std::fs::read_dir(stand.drafts.path())
        .expect("каталог не прочитан")
        .count();
    assert_eq!(written, 1);
}

#[tokio::test]
async fn spares_a_draft_when_the_answer_was_already_in_the_base() {
    let stand = Stand::start("vl-no-space-left").await;
    let incident = stand.incident().await;
    stand.done(incident).await;
    assert!(stand.store.unsettled(10).await.unwrap().is_empty());
}

#[tokio::test]
async fn keeps_how_bad_it_is_by_the_word_of_the_model() {
    let stand = Stand::start("vl-no-space-left").await;
    let incident = stand.incident().await;
    stand.done(incident).await;
    assert_eq!(
        stand.store.one(incident).await.unwrap().unwrap().severity,
        Some(sre_domain::Severity::High)
    );
}

#[tokio::test]
async fn keeps_what_it_asked_the_model_when_collecting() {
    let stand = Stand::start("vl-no-space-left").await;
    let incident = stand.incident().await;
    stand.done(incident).await;
    for _ in 0..20 {
        if stand.store.learned().await.unwrap() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(stand.store.learned().await.unwrap() > 0);
}

#[tokio::test]
async fn ties_a_lesson_to_the_incident_it_came_from() {
    let stand = Stand::start("vl-no-space-left").await;
    let incident = stand.incident().await;
    stand.done(incident).await;
    for _ in 0..20 {
        if stand.store.learned().await.unwrap() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let lessons = stand.store.lessons(0, 10).await.unwrap();
    assert_eq!(lessons[0].incident, Some(incident));
}
