use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autosre_domain::{Kind, Minute, Span};
use autosre_metrics::{Metrics, Settings};
use autosre_source::{Source, SourceError};
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;

#[derive(Clone)]
struct Reply {
    seen: Arc<Mutex<Vec<String>>>,
    status: StatusCode,
    body: &'static str,
}

/// Поддельная `VictoriaMetrics` на эфемерном порту.
struct Fake {
    address: SocketAddr,
    seen: Arc<Mutex<Vec<String>>>,
}

impl Fake {
    async fn start(status: StatusCode, body: &'static str) -> Self {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("порт не занят");
        let address = listener.local_addr().expect("адрес не получен");
        let router = Router::new()
            .route("/api/v1/query_range", get(answer))
            .with_state(Reply {
                seen: Arc::clone(&seen),
                status,
                body,
            });
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("сервер упал");
        });
        Self { address, seen }
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().expect("журнал заблокирован").clone()
    }

    fn metrics(&self, select: &[&str]) -> Metrics {
        Metrics::new(&Settings {
            url: format!("http://{}/", self.address)
                .parse()
                .expect("адрес некорректен"),
            timeout: Duration::from_secs(5),
            select: select.iter().map(|it| (*it).to_owned()).collect(),
            labels: vec!["job".to_owned(), "instance".to_owned()],
        })
        .expect("коннектор не собрался")
    }
}

async fn answer(
    State(reply): State<Reply>,
    Query(params): Query<HashMap<String, String>>,
) -> (StatusCode, String) {
    reply
        .seen
        .lock()
        .expect("журнал заблокирован")
        .push(params.get("query").cloned().unwrap_or_default());
    (reply.status, reply.body.to_owned())
}

fn quarter() -> Span {
    let from = Minute::at(1_786_968_000);
    Span::new(from, from.back(-15), 96).expect("промежуток некорректен")
}

const MEMORY: &str = r#"{"status":"success","data":{"resultType":"matrix","result":[
 {"metric":{"job":"llama-server"},"values":[[1786968060,"457000000"],[1786968120,"458000000"]]}
]}}"#;

#[tokio::test]
async fn reads_a_value_for_every_minute() {
    let fake = Fake::start(StatusCode::OK, MEMORY).await;
    let buckets = fake
        .metrics(&["process_resident_memory_bytes"])
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(buckets.len(), 2);
}

#[tokio::test]
async fn treats_a_level_as_a_level() {
    let fake = Fake::start(StatusCode::OK, MEMORY).await;
    let buckets = fake
        .metrics(&["process_resident_memory_bytes"])
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(buckets[0].kind, Kind::Mean);
}

#[tokio::test]
async fn treats_a_counter_as_a_counter() {
    let fake = Fake::start(StatusCode::OK, MEMORY).await;
    let buckets = fake
        .metrics(&["http_requests_total"])
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(buckets[0].kind, Kind::Sum);
}

#[tokio::test]
async fn averages_a_level_across_instances() {
    let fake = Fake::start(StatusCode::OK, MEMORY).await;
    fake.metrics(&["process_resident_memory_bytes"])
        .buckets(quarter())
        .await
        .unwrap();
    assert!(fake.seen()[0].starts_with("avg by (job,instance)"));
}

#[tokio::test]
async fn takes_the_increase_of_a_counter() {
    let fake = Fake::start(StatusCode::OK, MEMORY).await;
    fake.metrics(&["http_requests_total"])
        .buckets(quarter())
        .await
        .unwrap();
    assert!(fake.seen()[0].contains("increase(http_requests_total[1m])"));
}

#[tokio::test]
async fn builds_a_selector_like_the_logs_do() {
    let fake = Fake::start(StatusCode::OK, MEMORY).await;
    let buckets = fake
        .metrics(&["process_resident_memory_bytes"])
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(
        buckets[0].stream.as_str(),
        "{__series__=\"process_resident_memory_bytes\",job=\"llama-server\"}"
    );
}

#[tokio::test]
async fn asks_once_for_every_selected_series() {
    let fake = Fake::start(StatusCode::OK, MEMORY).await;
    fake.metrics(&["a_bytes", "b_bytes", "c_total"])
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(fake.seen().len(), 3);
}

#[tokio::test]
async fn skips_a_value_that_is_not_a_number() {
    let fake = Fake::start(
        StatusCode::OK,
        r#"{"data":{"result":[{"metric":{},"values":[[1786968060,"NaN"],[1786968120,"7"]]}]}}"#,
    )
    .await;
    let buckets = fake.metrics(&["x_bytes"]).buckets(quarter()).await.unwrap();
    assert_eq!(buckets.len(), 1);
}

#[tokio::test]
async fn fails_on_a_server_error() {
    let fake = Fake::start(StatusCode::INTERNAL_SERVER_ERROR, "boom").await;
    let failure = fake
        .metrics(&["x_bytes"])
        .buckets(quarter())
        .await
        .unwrap_err();
    assert!(matches!(failure, SourceError::Status { status: 500, .. }));
}

#[tokio::test]
async fn fails_on_an_answer_without_results() {
    let fake = Fake::start(StatusCode::OK, "<html>proxy error</html>").await;
    let failure = fake
        .metrics(&["x_bytes"])
        .buckets(quarter())
        .await
        .unwrap_err();
    assert!(matches!(failure, SourceError::Shape(_)));
}

#[tokio::test]
async fn has_no_samples_to_give() {
    let fake = Fake::start(StatusCode::OK, MEMORY).await;
    let stream = autosre_domain::Stream::new("{job=\"llama-server\"}");
    assert!(
        fake.metrics(&["x_bytes"])
            .samples(&stream, quarter(), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn calls_itself_metrics() {
    let fake = Fake::start(StatusCode::OK, MEMORY).await;
    assert_eq!(fake.metrics(&["x_bytes"]).name(), "metrics");
}
