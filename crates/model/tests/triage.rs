use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use serde_json::Value;
use sre_domain::{Detector, Deviation, Group, Kind, Minute, Signature, Stream, Thresholds};
use sre_model::{Model, ModelError, Settings, prompt};

#[derive(Clone)]
struct Reply {
    seen: Arc<Mutex<Vec<Value>>>,
    status: StatusCode,
    body: &'static str,
    delay: Duration,
}

/// Поддельный `LiteLLM` на эфемерном порту.
struct Fake {
    address: SocketAddr,
    seen: Arc<Mutex<Vec<Value>>>,
}

impl Fake {
    async fn start(status: StatusCode, body: &'static str) -> Self {
        Self::slow(status, body, Duration::ZERO).await
    }

    async fn slow(status: StatusCode, body: &'static str, delay: Duration) -> Self {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("порт не занят");
        let address = listener.local_addr().expect("адрес не получен");
        let router = Router::new()
            .route("/v1/chat/completions", post(answer))
            .with_state(Reply {
                seen: Arc::clone(&seen),
                status,
                body,
                delay,
            });
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("сервер упал");
        });
        Self { address, seen }
    }

    fn asked(&self) -> Value {
        self.seen.lock().expect("журнал заблокирован")[0].clone()
    }

    fn model(&self, timeout: Duration) -> Model {
        Model::new(Settings {
            url: format!("http://{}/", self.address)
                .parse()
                .expect("адрес некорректен"),
            key: "sk-lab".to_owned(),
            name: "gemma-4-12B-it-qat-q4_0-gguf".to_owned(),
            temperature: 0.2,
            tokens: 300,
            timeout,
        })
        .expect("клиент не собрался")
    }
}

async fn answer(State(reply): State<Reply>, body: String) -> (StatusCode, String) {
    reply
        .seen
        .lock()
        .expect("журнал заблокирован")
        .push(serde_json::from_str(&body).unwrap_or(Value::Null));
    tokio::time::sleep(reply.delay).await;
    (reply.status, reply.body.to_owned())
}

const WORTH: &str = r#"{"choices":[{"message":{"content":"{\"worth\":true,\"because\":\"такого раньше не было\"}"}}]}"#;
const NOISE: &str = r#"{"choices":[{"message":{"content":"{\"worth\":false,\"because\":\"ночная выгрузка, каждый день\"}"}}]}"#;

fn deviation() -> Deviation {
    let stream = Stream::new("{host=\"node-01\",service=\"orders-api\"}");
    let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0, 5.0], 91.0, Kind::Sum);
    Deviation::new("logs", &stream, "15m", Minute::at(1_786_968_660), verdict)
}

fn groups() -> Vec<Group> {
    vec![Group {
        signature: Signature::of("upstream 192.0.2.19 timed out after 30s"),
        count: 63,
        sample: "upstream 192.0.2.19 timed out after 30s".to_owned(),
    }]
}

#[tokio::test]
async fn reads_the_decision_to_dig() {
    let fake = Fake::start(StatusCode::OK, WORTH).await;
    let about = prompt::about(&deviation(), "orders-api", &groups());
    assert!(
        fake.model(Duration::from_secs(5))
            .triage(&about)
            .await
            .unwrap()
            .worth
    );
}

#[tokio::test]
async fn reads_the_decision_to_let_it_be() {
    let fake = Fake::start(StatusCode::OK, NOISE).await;
    let about = prompt::about(&deviation(), "orders-api", &groups());
    assert!(
        !fake
            .model(Duration::from_secs(5))
            .triage(&about)
            .await
            .unwrap()
            .worth
    );
}

#[tokio::test]
async fn keeps_the_reason_for_the_card() {
    let fake = Fake::start(StatusCode::OK, NOISE).await;
    let about = prompt::about(&deviation(), "orders-api", &groups());
    assert_eq!(
        fake.model(Duration::from_secs(5))
            .triage(&about)
            .await
            .unwrap()
            .because,
        "ночная выгрузка, каждый день"
    );
}

#[tokio::test]
async fn binds_the_answer_to_a_schema() {
    let fake = Fake::start(StatusCode::OK, WORTH).await;
    let about = prompt::about(&deviation(), "orders-api", &groups());
    fake.model(Duration::from_secs(5))
        .triage(&about)
        .await
        .unwrap();
    assert_eq!(fake.asked()["response_format"]["type"], "json_schema");
}

#[tokio::test]
async fn keeps_the_sifting_prompt_short() {
    let about = prompt::about(&deviation(), "orders-api", &groups());
    assert!(about.len() + prompt::SIFTER.len() < 1500);
}

#[tokio::test]
async fn sends_signatures_instead_of_raw_lines() {
    let about = prompt::about(&deviation(), "orders-api", &groups());
    assert!(about.contains("63 × upstream <addr> timed out after <n>s"));
}

#[tokio::test]
async fn says_plainly_when_there_are_no_samples() {
    let about = prompt::about(&deviation(), "llama-server", &[]);
    assert!(about.contains("образцов нет"));
}

#[tokio::test]
async fn survives_a_fenced_answer() {
    let fake = Fake::start(
        StatusCode::OK,
        r#"{"choices":[{"message":{"content":"```json\n{\"worth\":true,\"because\":\"растёт\"}\n```"}}]}"#,
    )
    .await;
    let about = prompt::about(&deviation(), "orders-api", &groups());
    assert!(
        fake.model(Duration::from_secs(5))
            .triage(&about)
            .await
            .unwrap()
            .worth
    );
}

#[tokio::test]
async fn fails_on_an_answer_beside_the_schema() {
    let fake = Fake::start(
        StatusCode::OK,
        r#"{"choices":[{"message":{"content":"{\"worth\":\"наверное\"}"}}]}"#,
    )
    .await;
    let about = prompt::about(&deviation(), "orders-api", &groups());
    let failure = fake
        .model(Duration::from_secs(5))
        .triage(&about)
        .await
        .unwrap_err();
    assert!(matches!(failure, ModelError::Shape(_)));
}

#[tokio::test]
async fn fails_on_a_busy_gateway() {
    let fake = Fake::start(StatusCode::TOO_MANY_REQUESTS, "no slots available").await;
    let about = prompt::about(&deviation(), "orders-api", &groups());
    let failure = fake
        .model(Duration::from_secs(5))
        .triage(&about)
        .await
        .unwrap_err();
    assert!(matches!(failure, ModelError::Status { status: 429, .. }));
}

#[tokio::test]
async fn gives_up_on_a_thinking_model() {
    let fake = Fake::slow(StatusCode::OK, WORTH, Duration::from_secs(30)).await;
    let about = prompt::about(&deviation(), "orders-api", &groups());
    let failure = fake
        .model(Duration::from_millis(150))
        .triage(&about)
        .await
        .unwrap_err();
    assert!(matches!(failure, ModelError::Transport(_)));
}
