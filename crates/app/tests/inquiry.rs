use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::extract::State;
use axum::routing::post;
use sre_app::config::{Digging, Incidents};
use sre_app::digger::{Digger, dig};
use sre_app::metrics::Metrics;
use sre_domain::{
    Bucket, Detector, Deviation, Kind, Minute, Service, Signature, Span, Stream, Thresholds,
};
use sre_model::{Model, Settings};
use sre_source::{Source, SourceError};
use sre_store::Store;
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
        Ok(vec!["no space left on device".to_owned()])
    }
}

/// Поддельная модель: первым ответом просит команду, вторым заканчивает.
#[derive(Clone)]
struct Talk {
    asked: Arc<AtomicUsize>,
    command: &'static str,
}

async fn answer(State(talk): State<Talk>, _body: String) -> String {
    let first = talk.asked.fetch_add(1, Ordering::Relaxed) == 0;
    let tail = if first {
        format!(
            r#"\"need\":\"ask\",\"host\":\"node-01\",\"command\":\"{}\""#,
            talk.command
        )
    } else {
        r#"\"need\":\"nothing\""#.to_owned()
    };
    let confidence = if first { 0.3 } else { 0.9 };
    format!(
        r#"{{"choices":[{{"message":{{"content":"{{\"cause\":\"кончилось место\",\"confidence\":{confidence},\"advice\":\"посмотреть диск\",{tail}}}"}}}}]}}"#
    )
}

async fn model(command: &'static str) -> (SocketAddr, Arc<AtomicUsize>) {
    let asked = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("порт не занят");
    let address = listener.local_addr().expect("адрес не получен");
    let router = Router::new()
        .route("/v1/chat/completions", post(answer))
        .with_state(Talk {
            asked: Arc::clone(&asked),
            command,
        });
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("сервер упал");
    });
    (address, asked)
}

/// Стенд с моделью, которая просит выполнить заданную команду.
struct Stand {
    store: Store,
    _skills: TempDir,
    _directory: TempDir,
}

impl Stand {
    async fn start(command: &'static str) -> Self {
        let directory = TempDir::new().expect("временный каталог не создан");
        let store = Store::open(&directory.path().join("sre.db")).expect("база не открыта");
        let skills = TempDir::new().expect("каталог скиллов не создан");
        std::fs::write(skills.path().join("error-burst.md"), SKILL).expect("скилл не записан");

        let (address, _) = model(command).await;
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
                knowledge: &sre_app::config::Knowledge::default(),
            },
        ));
        Self {
            store,
            _skills: skills,
            _directory: directory,
        }
    }

    /// Заводит инцидент, начавшийся минуту назад.
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
                &Signature::of("no space left"),
                &deviation,
                "такого раньше не было",
            )
            .await
            .unwrap()
            .0
    }

    /// Ждёт первой заявки по инциденту.
    async fn inquiry(&self, incident: i64) -> Option<sre_store::Asked> {
        for _ in 0..100 {
            if let Ok(asked) = self.store.inquiries(incident).await
                && let Some(first) = asked.into_iter().next_back()
            {
                return Some(first);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }

    /// Ждёт вывода в заданном состоянии.
    async fn conclusion(&self, incident: i64, state: &str) -> Option<sre_store::Finding> {
        for _ in 0..150 {
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
async fn asks_the_duty_engineer_when_the_data_is_out_of_reach() {
    let stand = Stand::start("df -h /var").await;
    let incident = stand.incident().await;
    assert_eq!(
        stand.inquiry(incident).await.expect("заявки нет").command,
        "df -h /var"
    );
}

#[tokio::test]
async fn names_the_host_the_command_belongs_to() {
    let stand = Stand::start("df -h /var").await;
    let incident = stand.incident().await;
    assert_eq!(
        stand.inquiry(incident).await.expect("заявки нет").host,
        "node-01"
    );
}

#[tokio::test]
async fn stops_the_investigation_until_the_answer_comes() {
    let stand = Stand::start("df -h /var").await;
    let incident = stand.incident().await;
    stand.inquiry(incident).await.expect("заявки нет");
    assert_eq!(
        stand
            .conclusion(incident, "waiting")
            .await
            .expect("расследование не остановлено")
            .state,
        "waiting"
    );
}

#[tokio::test]
async fn digs_again_once_the_engineer_answered() {
    let stand = Stand::start("df -h /var").await;
    let incident = stand.incident().await;
    let inquiry = stand.inquiry(incident).await.expect("заявки нет").id;
    stand.conclusion(incident, "waiting").await;
    stand
        .store
        .reply(
            inquiry,
            "букин",
            "/var 98% занято",
            Minute::of(chrono::Utc::now()),
        )
        .await
        .unwrap();
    assert!(stand.conclusion(incident, "done").await.is_some());
}

#[tokio::test]
async fn puts_the_answer_into_the_next_dossier() {
    let stand = Stand::start("df -h /var").await;
    let incident = stand.incident().await;
    let inquiry = stand.inquiry(incident).await.expect("заявки нет").id;
    stand.conclusion(incident, "waiting").await;
    stand
        .store
        .reply(
            inquiry,
            "букин",
            "/var 98% занято",
            Minute::of(chrono::Utc::now()),
        )
        .await
        .unwrap();
    let found = stand
        .conclusion(incident, "done")
        .await
        .expect("вывода нет");
    let steps = stand.store.steps(found.id).await.unwrap();
    assert!(steps.iter().any(|(_, _, data)| data.contains("98% занято")));
}

#[tokio::test]
async fn refuses_to_offer_a_command_that_changes_the_host() {
    let stand = Stand::start("rm -rf /var/log/old").await;
    let incident = stand.incident().await;
    assert!(stand.conclusion(incident, "done").await.is_some());
}

#[tokio::test]
async fn leaves_no_inquiry_after_a_refused_command() {
    let stand = Stand::start("rm -rf /var/log/old").await;
    let incident = stand.incident().await;
    stand
        .conclusion(incident, "done")
        .await
        .expect("вывода нет");
    assert!(stand.store.inquiries(incident).await.unwrap().is_empty());
}
