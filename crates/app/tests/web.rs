use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use sre_app::metrics::Metrics;
use sre_app::web::{Shared, routes};

/// Агент, поднятый на эфемерном порту.
struct Agent {
    address: SocketAddr,
    shared: Shared,
}

impl Agent {
    async fn start() -> Self {
        let shared = Shared::new(Arc::new(Metrics::new("0.1.0-тест")), "0.1.0-тест");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("порт не занят");
        let address = listener.local_addr().expect("адрес не получен");
        let router = routes(shared.clone());
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("морда упала");
        });
        Self { address, shared }
    }

    async fn get(&self, path: &str) -> reqwest::Response {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("клиент не собрался")
            .get(format!("http://{}{path}", self.address))
            .send()
            .await
            .expect("запрос не дошёл")
    }

    async fn text(&self, path: &str) -> String {
        self.get(path)
            .await
            .text()
            .await
            .expect("ответ не прочитан")
    }
}

#[tokio::test]
async fn answers_the_health_check() {
    let agent = Agent::start().await;
    let health: serde_json::Value =
        serde_json::from_str(&agent.text("/api/health").await).expect("ответ не разобран");
    assert_eq!(health["status"], "ok");
}

#[tokio::test]
async fn tells_its_version() {
    let agent = Agent::start().await;
    let health: serde_json::Value =
        serde_json::from_str(&agent.text("/api/health").await).expect("ответ не разобран");
    assert_eq!(health["version"], "0.1.0-тест");
}

#[tokio::test]
async fn exposes_the_prometheus_format() {
    let agent = Agent::start().await;
    assert!(agent.text("/metrics").await.contains("# TYPE sre_up gauge"));
}

#[tokio::test]
async fn names_its_version_in_the_metrics() {
    let agent = Agent::start().await;
    assert!(
        agent
            .text("/metrics")
            .await
            .contains("sre_build_info{version=\"0.1.0-тест\"} 1")
    );
}

#[tokio::test]
async fn hides_the_bucket_moment_until_the_first_bucket() {
    let agent = Agent::start().await;
    assert!(
        !agent
            .text("/metrics")
            .await
            .contains("sre_last_bucket_timestamp_seconds")
    );
}

#[tokio::test]
async fn shows_the_bucket_moment_once_there_is_one() {
    let agent = Agent::start().await;
    agent.shared.metrics().bucket(SystemTime::now());
    assert!(
        agent
            .text("/metrics")
            .await
            .contains("sre_last_bucket_timestamp_seconds")
    );
}

#[tokio::test]
async fn counts_the_failures_of_a_source() {
    let agent = Agent::start().await;
    agent.shared.metrics().failure();
    agent.shared.metrics().failure();
    assert!(
        agent
            .text("/metrics")
            .await
            .contains("sre_source_failures_total 2")
    );
}

#[tokio::test]
async fn serves_the_metrics_as_plain_text() {
    let agent = Agent::start().await;
    let kind = agent.get("/metrics").await;
    assert_eq!(
        kind.headers()["content-type"],
        "text/plain; version=0.0.4; charset=utf-8"
    );
}

#[tokio::test]
async fn knows_nothing_of_an_unknown_path() {
    let agent = Agent::start().await;
    assert_eq!(
        agent.get("/api/findings").await.status(),
        reqwest::StatusCode::NOT_FOUND
    );
}
