use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use autosre_app::config::{Digging, Incidents};
use autosre_app::digger::{Digger, dig};
use autosre_app::metrics::Metrics;
use autosre_domain::{
    Bucket, Detector, Deviation, Kind, Minute, Service, Signature, Span, Stream, Thresholds,
};
use autosre_model::{Model, Settings};
use autosre_source::{Source, SourceError};
use autosre_store::Store;
use axum::Router;
use axum::extract::State;
use axum::routing::post;
use tempfile::TempDir;

/// Скилл, годный для всплеска ошибок на пятнадцати минутах.
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

/// Источник, который всегда отдаёт одни и те же строки.
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
        Ok(vec![
            "upstream 192.0.2.19 timed out after 30s".to_owned(),
            "upstream 192.0.2.22 timed out after 45s".to_owned(),
        ])
    }
}

/// Поддельная модель: первым ответом просит добор, вторым заканчивает.
#[derive(Clone)]
struct Talk(Arc<AtomicUsize>);

async fn answer(State(asked): State<Talk>, _body: String) -> String {
    let first = asked.0.fetch_add(1, Ordering::Relaxed) == 0;
    let need = if first { "logs" } else { "nothing" };
    let confidence = if first { 0.3 } else { 0.8 };
    format!(
        r#"{{"choices":[{{"message":{{"content":"{{\"cause\":\"апстрим молчит\",\"confidence\":{confidence},\"advice\":\"смотреть соседей\",\"need\":\"{need}\"}}"}}}}]}}"#
    )
}

async fn model() -> (SocketAddr, Arc<AtomicUsize>) {
    let asked = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("порт не занят");
    let address = listener.local_addr().expect("адрес не получен");
    let router = Router::new()
        .route("/v1/chat/completions", post(answer))
        .with_state(Talk(Arc::clone(&asked)));
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("сервер упал");
    });
    (address, asked)
}

/// Стенд: база, скиллы, поддельная модель, источник.
struct Stand {
    store: Store,
    digger: Digger,
    asked: Arc<AtomicUsize>,
    _skills: TempDir,
    _directory: TempDir,
}

impl Stand {
    /// Стенд с работающей очередью.
    async fn start(patience: Duration) -> Self {
        let stand = Self::still(patience).await;
        dig(stand.digger.clone());
        stand
    }

    /// Стенд без очереди: для проверок, которым нужен покой.
    async fn still(patience: Duration) -> Self {
        let directory = TempDir::new().expect("временный каталог не создан");
        let store = Store::open(&directory.path().join("autosre.db")).expect("база не открыта");
        let skills = TempDir::new().expect("каталог скиллов не создан");
        std::fs::write(skills.path().join("error-burst.md"), SKILL).expect("скилл не записан");

        let (address, asked) = model().await;
        let model = Arc::new(
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
        );

        let sources: Vec<Arc<dyn Source>> = vec![Arc::new(Talker)];
        let digger = Digger::new(
            &sources,
            &store,
            &Arc::new(Metrics::new("тест")),
            &model,
            autosre_skills::read(skills.path()).expect("скиллы не прочитаны"),
            &autosre_app::digger::Recipe {
                digging: &Digging::default().patient(patience),
                incidents: &Incidents::default(),
                knowledge: &autosre_app::config::Knowledge::default()
                    .drafting(directory.path().join("drafts")),
            },
        );
        Self {
            store,
            digger,
            asked,
            _skills: skills,
            _directory: directory,
        }
    }

    /// Заводит инцидент, начавшийся указанное число минут назад.
    async fn incident(&self, ago: i64) -> i64 {
        let at = Minute::of(chrono::Utc::now()).back(ago);
        let stream = Stream::new("{host=\"node-01\",service=\"orders-api\"}");
        let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0], 91.0, Kind::Sum);
        let deviation = Deviation::new("logs", &stream, "15m", at, verdict);
        self.store.spot(&deviation, at).await.unwrap();
        let id = self.store.loose(10).await.unwrap()[0].0;
        self.store
            .attach(
                id,
                &Service::new("orders-api"),
                &Signature::of("upstream timed out"),
                &deviation,
                "такого раньше не было",
            )
            .await
            .unwrap()
            .0
    }

    /// Ждёт, пока у инцидента появится расследование в нужном состоянии.
    async fn wait(&self, incident: i64, state: &str) -> Option<autosre_store::Finding> {
        for _ in 0..100 {
            if let Ok(Some(found)) = self.store.conclusion(incident).await
                && found.state == state
            {
                return Some(found);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }
}

#[tokio::test]
async fn digs_a_fresh_incident() {
    let stand = Stand::start(Duration::from_hours(1)).await;
    let incident = stand.incident(1).await;
    assert!(stand.wait(incident, "done").await.is_some());
}

#[tokio::test]
async fn writes_down_the_cause() {
    let stand = Stand::start(Duration::from_hours(1)).await;
    let incident = stand.incident(1).await;
    let found = stand.wait(incident, "done").await.expect("вывода нет");
    assert_eq!(found.cause.as_deref(), Some("апстрим молчит"));
}

#[tokio::test]
async fn names_the_skill_that_worked() {
    let stand = Stand::start(Duration::from_hours(1)).await;
    let incident = stand.incident(1).await;
    let found = stand.wait(incident, "done").await.expect("вывода нет");
    assert_eq!(found.skill, "error-burst");
}

#[tokio::test]
async fn asks_again_when_the_model_wants_more() {
    let stand = Stand::start(Duration::from_hours(1)).await;
    let incident = stand.incident(1).await;
    stand.wait(incident, "done").await.expect("вывода нет");
    assert_eq!(stand.asked.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn keeps_the_confidence_of_the_second_answer() {
    let stand = Stand::start(Duration::from_hours(1)).await;
    let incident = stand.incident(1).await;
    let found = stand.wait(incident, "done").await.expect("вывода нет");
    assert_eq!(found.confidence, Some(0.8));
}

#[tokio::test]
async fn writes_every_step_as_it_goes() {
    let stand = Stand::start(Duration::from_hours(1)).await;
    let incident = stand.incident(1).await;
    let found = stand.wait(incident, "done").await.expect("вывода нет");
    assert_eq!(stand.store.steps(found.id).await.unwrap().len(), 2);
}

#[tokio::test]
async fn lets_a_stale_incident_through_without_a_conclusion() {
    let stand = Stand::start(Duration::from_mins(15)).await;
    let incident = stand.incident(120).await;
    assert!(stand.wait(incident, "skipped").await.is_some());
}

#[tokio::test]
async fn spares_the_model_on_a_stale_incident() {
    let stand = Stand::start(Duration::from_mins(15)).await;
    let incident = stand.incident(120).await;
    stand.wait(incident, "skipped").await.expect("пометки нет");
    assert_eq!(stand.asked.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn closes_an_investigation_torn_by_a_restart() {
    let stand = Stand::still(Duration::from_hours(1)).await;
    let incident = stand.incident(1).await;
    let torn = stand
        .store
        .dig(incident, "error-burst", Minute::of(chrono::Utc::now()))
        .await
        .unwrap();
    // Так выглядит агент, убитый на середине: расследование есть, вывода нет.
    let _ = torn;
    assert_eq!(stand.store.unfinished().await.unwrap().len(), 1);
}

#[tokio::test]
async fn tells_a_stale_investigation_from_a_fresh_one() {
    let stand = Stand::still(Duration::from_hours(1)).await;
    let incident = stand.incident(1).await;
    let old = Minute::of(chrono::Utc::now()).back(48 * 60);
    stand.store.dig(incident, "error-burst", old).await.unwrap();
    let (_, _, _, started) = stand.store.unfinished().await.unwrap()[0];
    assert_eq!(started, old);
}

#[tokio::test]
async fn takes_a_torn_investigation_out_of_the_way() {
    let stand = Stand::still(Duration::from_hours(1)).await;
    let incident = stand.incident(1).await;
    stand
        .store
        .dig(incident, "error-burst", Minute::of(chrono::Utc::now()))
        .await
        .unwrap();
    autosre_app::digger::resume(&stand.digger).await;
    assert!(stand.store.unfinished().await.unwrap().is_empty());
}

#[tokio::test]
async fn marks_a_day_old_investigation_as_stale() {
    let stand = Stand::still(Duration::from_hours(1)).await;
    let incident = stand.incident(1).await;
    let old = Minute::of(chrono::Utc::now()).back(48 * 60);
    stand.store.dig(incident, "error-burst", old).await.unwrap();
    autosre_app::digger::resume(&stand.digger).await;
    assert_eq!(
        stand
            .store
            .conclusion(incident)
            .await
            .unwrap()
            .unwrap()
            .state,
        "stale"
    );
}

#[tokio::test]
async fn marks_a_fresh_torn_investigation_as_failed() {
    let stand = Stand::still(Duration::from_hours(1)).await;
    let incident = stand.incident(1).await;
    stand
        .store
        .dig(incident, "error-burst", Minute::of(chrono::Utc::now()))
        .await
        .unwrap();
    autosre_app::digger::resume(&stand.digger).await;
    assert_eq!(
        stand
            .store
            .conclusion(incident)
            .await
            .unwrap()
            .unwrap()
            .state,
        "failed"
    );
}
