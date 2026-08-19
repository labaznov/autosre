use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use autosre_app::config::{Incidents, Knowledge};
use autosre_app::metrics::Metrics;
use autosre_app::reporter::{Reporter, daily, single, weekly};
use autosre_domain::{Detector, Deviation, Kind, Minute, Service, Signature, Stream, Thresholds};
use autosre_model::{Model, Settings};
use autosre_store::Store;
use axum::Router;
use axum::routing::post;
use tempfile::TempDir;

/// Поддельная модель: всегда пишет одну и ту же общую картину.
async fn answer(_body: String) -> &'static str {
    r#"{"choices":[{"message":{"content":"{\"picture\":\"Хозяйство в целом живо, шумит только orders-api\"}"}}]}"#
}

/// Стенд: база, каталог знаний, поддельная модель.
struct Desk {
    store: Store,
    reporter: Reporter,
    model: Arc<Model>,
    knowledge: TempDir,
    _directory: TempDir,
}

impl Desk {
    async fn open() -> Self {
        let directory = TempDir::new().expect("временный каталог не создан");
        let knowledge = TempDir::new().expect("каталог знаний не создан");
        let store = Store::open(&directory.path().join("autosre.db")).expect("база не открыта");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("порт не занят");
        let address: SocketAddr = listener.local_addr().expect("адрес не получен");
        tokio::spawn(async move {
            let router = Router::new().route("/v1/chat/completions", post(answer));
            axum::serve(listener, router).await.expect("сервер упал");
        });
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
        let settings = Knowledge::default().filing(knowledge.path().join("reports"));
        let reporter = Reporter::new(
            &store,
            &Arc::new(Metrics::new("тест")),
            &model,
            &settings,
            &Incidents::default(),
        );
        Self {
            store,
            reporter,
            model,
            knowledge,
            _directory: directory,
        }
    }

    /// Заводит инцидент в заданную минуту и отвечает его номером.
    async fn incident(&self, at: Minute, service: &str) -> i64 {
        let stream = Stream::new(format!("{{host=\"node-01\",service=\"{service}\"}}"));
        let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0], 91.0, Kind::Sum);
        let deviation = Deviation::new("logs", &stream, "15m", at, verdict);
        self.store.spot(&deviation, at).await.unwrap();
        let loose = self.store.loose(50).await.unwrap();
        let id = loose
            .into_iter()
            .find(|(_, it)| it.stream == stream && it.at == at)
            .expect("отклонение не найдено")
            .0;
        self.store
            .attach(
                id,
                &Service::new(service),
                &Signature::of(&format!("сбой {service}")),
                &deviation,
                "такого раньше не было",
            )
            .await
            .unwrap()
            .0
    }

    /// Вчерашняя полночь плюс сколько-то минут.
    fn yesterday(minutes: i64) -> Minute {
        let day = chrono::Utc::now()
            .date_naive()
            .pred_opt()
            .expect("вчера не бывает")
            .and_hms_opt(0, 0, 0)
            .expect("полночь не бывает")
            .and_utc()
            .timestamp();
        Minute::at(day).back(-minutes)
    }
}

#[tokio::test]
async fn builds_the_report_of_an_incident() {
    let desk = Desk::open().await;
    let incident = desk.incident(Desk::yesterday(600), "orders-api").await;
    assert!(single(&desk.reporter, incident).await.is_some());
}

#[tokio::test]
async fn tells_the_numbers_of_the_incident() {
    let desk = Desk::open().await;
    let incident = desk.incident(Desk::yesterday(600), "orders-api").await;
    let name = single(&desk.reporter, incident).await.expect("отчёта нет");
    let report = desk
        .store
        .report("incidents", &name)
        .await
        .unwrap()
        .expect("отчёт не сохранён");
    assert!(report.body.contains("подтверждений: 1"));
}

#[tokio::test]
async fn says_a_report_of_a_live_incident_is_partial() {
    let desk = Desk::open().await;
    let incident = desk.incident(Desk::yesterday(600), "orders-api").await;
    let name = single(&desk.reporter, incident).await.expect("отчёта нет");
    let report = desk
        .store
        .report("incidents", &name)
        .await
        .unwrap()
        .unwrap();
    assert!(report.body.contains("Отчёт неполон"));
}

#[tokio::test]
async fn tells_what_the_duty_engineer_did() {
    let desk = Desk::open().await;
    let at = Desk::yesterday(600);
    let incident = desk.incident(at, "orders-api").await;
    desk.store.judge(incident, true, "букин", at).await.unwrap();
    let name = single(&desk.reporter, incident).await.expect("отчёта нет");
    let report = desk
        .store
        .report("incidents", &name)
        .await
        .unwrap()
        .unwrap();
    assert!(report.body.contains("по делу"));
}

#[tokio::test]
async fn puts_the_report_into_the_knowledge_repository() {
    let desk = Desk::open().await;
    let incident = desk.incident(Desk::yesterday(600), "orders-api").await;
    let name = single(&desk.reporter, incident).await.expect("отчёта нет");
    assert!(
        desk.knowledge
            .path()
            .join("reports/incidents")
            .join(format!("{name}.md"))
            .exists()
    );
}

#[tokio::test]
async fn builds_the_report_of_a_day() {
    let desk = Desk::open().await;
    desk.incident(Desk::yesterday(600), "orders-api").await;
    daily(&desk.reporter).await;
    assert_eq!(desk.store.reports(10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn holds_all_four_parts_of_a_daily_report() {
    let desk = Desk::open().await;
    desk.incident(Desk::yesterday(600), "orders-api").await;
    daily(&desk.reporter).await;
    let report = desk
        .store
        .reports(10)
        .await
        .unwrap()
        .pop()
        .expect("отчёта нет");
    assert!(
        [
            "## Инциденты",
            "## Аномалии",
            "## Динамика",
            "## Общая картина"
        ]
        .iter()
        .all(|part| report.body.contains(part))
    );
}

#[tokio::test]
async fn lets_the_model_write_the_overall_picture() {
    let desk = Desk::open().await;
    desk.incident(Desk::yesterday(600), "orders-api").await;
    daily(&desk.reporter).await;
    let report = desk
        .store
        .reports(10)
        .await
        .unwrap()
        .pop()
        .expect("отчёта нет");
    assert!(report.body.contains("Хозяйство в целом живо"));
}

#[tokio::test]
async fn builds_a_daily_report_once() {
    let desk = Desk::open().await;
    desk.incident(Desk::yesterday(600), "orders-api").await;
    daily(&desk.reporter).await;
    daily(&desk.reporter).await;
    assert_eq!(desk.store.reports(10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn builds_a_daily_report_of_a_quiet_day() {
    let desk = Desk::open().await;
    daily(&desk.reporter).await;
    let report = desk
        .store
        .reports(10)
        .await
        .unwrap()
        .pop()
        .expect("отчёта нет");
    assert!(report.body.contains("Ни одного за сутки"));
}

#[tokio::test]
async fn shows_what_kept_happening_while_muted() {
    let desk = Desk::open().await;
    let at = Desk::yesterday(600);
    let incident = desk.incident(at, "orders-api").await;
    let mute = desk
        .store
        .mute(
            &Service::new("orders-api"),
            &Signature::of("сбой orders-api"),
            at.back(-30 * 24 * 60),
            ("букин", "ругается каждую ночь"),
            at,
        )
        .await
        .unwrap();
    let _ = incident;
    let (id, _) = {
        let stream = Stream::new("{host=\"node-01\",service=\"orders-api\"}");
        let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0], 91.0, Kind::Sum);
        let deviation = Deviation::new("logs", &stream, "15m", at.back(-1), verdict);
        desk.store.spot(&deviation, at.back(-1)).await.unwrap();
        let id = desk
            .store
            .loose(50)
            .await
            .unwrap()
            .into_iter()
            .find(|(_, it)| it.at == at.back(-1))
            .expect("отклонение не найдено")
            .0;
        (id, deviation)
    };
    desk.store.hush_deviation(id, mute).await.unwrap();
    daily(&desk.reporter).await;
    let report = desk
        .store
        .reports(10)
        .await
        .unwrap()
        .pop()
        .expect("отчёта нет");
    assert!(report.body.contains("Приглушено, но продолжается"));
}

#[tokio::test]
async fn builds_the_report_of_a_week() {
    let desk = Desk::open().await;
    weekly(&desk.reporter).await;
    let report = desk
        .store
        .reports(10)
        .await
        .unwrap()
        .pop()
        .expect("отчёта нет");
    assert_eq!(report.kind, "weekly");
}

#[tokio::test]
async fn tells_the_manager_what_people_did() {
    let desk = Desk::open().await;
    weekly(&desk.reporter).await;
    let report = desk
        .store
        .reports(10)
        .await
        .unwrap()
        .pop()
        .expect("отчёта нет");
    assert!(report.body.contains("## Что по этим проблемам делали люди"));
}

#[tokio::test]
async fn builds_a_weekly_report_once() {
    let desk = Desk::open().await;
    weekly(&desk.reporter).await;
    weekly(&desk.reporter).await;
    assert_eq!(desk.store.reports(10).await.unwrap().len(), 1);
}

/// Стенд с недоступной моделью: общая картина написана не будет.
async fn mute_desk() -> Desk {
    let desk = Desk::open().await;
    Desk {
        reporter: Reporter::new(
            &desk.store,
            &Arc::new(Metrics::new("тест")),
            &Arc::new(
                Model::new(Settings {
                    url: "http://127.0.0.1:1/".parse().expect("адрес некорректен"),
                    key: "sk-lab".to_owned(),
                    name: "недоступная".to_owned(),
                    temperature: 0.2,
                    tokens: 300,
                    timeout: Duration::from_millis(300),
                })
                .expect("клиент не собрался"),
            ),
            &Knowledge::default().filing(desk.knowledge.path().join("reports")),
            &Incidents::default(),
        ),
        ..desk
    }
}

#[tokio::test]
async fn files_a_report_even_without_the_model() {
    let desk = mute_desk().await;
    daily(&desk.reporter).await;
    assert_eq!(desk.store.reports(10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn marks_a_report_without_a_picture_as_partial() {
    let desk = mute_desk().await;
    daily(&desk.reporter).await;
    assert!(!desk.store.reports(10).await.unwrap()[0].whole);
}

#[tokio::test]
async fn builds_a_partial_report_again_when_the_model_returns() {
    let desk = mute_desk().await;
    daily(&desk.reporter).await;
    let whole = Desk::open().await;
    // Та же база, но модель отвечает: отчёт обязан пересобраться.
    let reporter = Reporter::new(
        &desk.store,
        &Arc::new(Metrics::new("тест")),
        &whole.model,
        &Knowledge::default().filing(desk.knowledge.path().join("reports")),
        &Incidents::default(),
    );
    daily(&reporter).await;
    assert!(desk.store.reports(10).await.unwrap()[0].whole);
}
