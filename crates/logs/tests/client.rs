use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autosre_domain::{Minute, Span, Stream};
use autosre_logs::{Filter, Logs, Settings};
use autosre_source::{Source, SourceError};
use axum::Router;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use chrono::{DateTime, Utc};

/// Что поддельная Victoria Logs увидела в запросе.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    query: String,
    limit: String,
    authorization: Option<String>,
}

#[derive(Clone)]
struct Reply {
    seen: Arc<Mutex<Vec<Seen>>>,
    status: StatusCode,
    body: &'static str,
    delay: Duration,
    failures: Arc<AtomicUsize>,
    /// Каким кодом отбивать первые запросы.
    busy: StatusCode,
}

/// Поддельная `Victoria Logs` на эфемерном порту.
struct Fake {
    address: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Fake {
    async fn start(status: StatusCode, body: &'static str) -> Self {
        Self::slow(status, body, Duration::ZERO).await
    }

    async fn slow(status: StatusCode, body: &'static str, delay: Duration) -> Self {
        Self::stand(status, body, delay, (0, status)).await
    }

    /// Источник, отбивающий первые `fails` запросов кодом `status`.
    async fn flaky(fails: usize, status: StatusCode, body: &'static str) -> Self {
        Self::stand(StatusCode::OK, body, Duration::ZERO, (fails, status)).await
    }

    async fn stand(
        status: StatusCode,
        body: &'static str,
        delay: Duration,
        (fails, busy): (usize, StatusCode),
    ) -> Self {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("порт не занят");
        let address = listener.local_addr().expect("адрес не получен");
        let router = Router::new()
            .route("/select/logsql/query", get(answer))
            .with_state(Reply {
                seen: Arc::clone(&seen),
                status,
                body,
                delay,
                failures: Arc::new(AtomicUsize::new(fails)),
                busy,
            });
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("сервер упал");
        });
        Self { address, seen }
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("журнал заблокирован").clone()
    }

    fn logs(&self, timeout: Duration, username: &str) -> Logs {
        Logs::new(
            &Settings {
                url: format!("http://{}/", self.address)
                    .parse()
                    .expect("адрес некорректен"),
                username: username.to_owned(),
                password: "гладиолус".to_owned(),
                timeout,
                rows: 500,
            },
            Filter::new("i(error*)", vec![]).expect("фильтр некорректен"),
        )
        .expect("коннектор не собрался")
    }
}

async fn answer(
    State(reply): State<Reply>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> (StatusCode, String) {
    reply.seen.lock().expect("журнал заблокирован").push(Seen {
        query: params.get("query").cloned().unwrap_or_default(),
        limit: params.get("limit").cloned().unwrap_or_default(),
        authorization: headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
    });
    tokio::time::sleep(reply.delay).await;
    if reply
        .failures
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
            left.checked_sub(1)
        })
        .is_ok()
    {
        return (reply.busy, "busy".to_owned());
    }
    (reply.status, reply.body.to_owned())
}

const QUICK: [Duration; 2] = [Duration::from_millis(5), Duration::from_millis(5)];

#[tokio::test]
async fn tries_again_after_a_busy_source() {
    let fake = Fake::flaky(1, StatusCode::BAD_GATEWAY, COUNTS).await;
    let buckets = fake
        .logs(Duration::from_secs(5), "")
        .retrying(&QUICK)
        .buckets(quarter())
        .await;
    assert!(buckets.is_ok(), "{buckets:?}");
}

#[tokio::test]
async fn does_not_try_again_on_a_refused_query() {
    let fake = Fake::flaky(1, StatusCode::BAD_REQUEST, COUNTS).await;
    let _ = fake
        .logs(Duration::from_secs(5), "")
        .retrying(&QUICK)
        .buckets(quarter())
        .await;
    assert_eq!(fake.seen().len(), 1);
}

fn moment(text: &str) -> DateTime<Utc> {
    text.parse().expect("момент времени некорректен")
}

fn quarter() -> Span {
    let from = Minute::of(moment("2026-08-17T10:00:00Z"));
    Span::new(from, from.back(-15), 96).expect("промежуток некорректен")
}

const COUNTS: &str = concat!(
    r#"{"_stream":"{container=\"orders-api\"}","_time":"2026-08-17T10:01:00.000Z","total":"7"}"#,
    "\n",
    r#"{"_stream":"{container=\"orders-api\"}","_time":"2026-08-17T10:02:00.000Z","total":"91"}"#,
    "\n",
    r#"{"_stream":"{container=\"billing-worker\"}","_time":"2026-08-17T10:01:00.000Z","total":3}"#,
    "\n",
);

#[tokio::test]
async fn reads_every_bucket_of_the_answer() {
    let fake = Fake::start(StatusCode::OK, COUNTS).await;
    let buckets = fake
        .logs(Duration::from_secs(5), "")
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(buckets.len(), 3);
}

#[tokio::test]
async fn puts_a_bucket_into_its_minute() {
    let fake = Fake::start(StatusCode::OK, COUNTS).await;
    let buckets = fake
        .logs(Duration::from_secs(5), "")
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(buckets[1].minute.start(), moment("2026-08-17T10:02:00Z"));
}

#[tokio::test]
async fn keeps_the_stream_selector_as_it_came() {
    let fake = Fake::start(StatusCode::OK, COUNTS).await;
    let buckets = fake
        .logs(Duration::from_secs(5), "")
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(buckets[0].stream, Stream::new("{container=\"orders-api\"}"));
}

#[tokio::test]
async fn reads_a_count_given_as_a_number() {
    let fake = Fake::start(StatusCode::OK, COUNTS).await;
    let buckets = fake
        .logs(Duration::from_secs(5), "")
        .buckets(quarter())
        .await
        .unwrap();
    assert!((buckets[2].value - 3.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn takes_one_request_for_the_whole_span() {
    let fake = Fake::start(StatusCode::OK, COUNTS).await;
    fake.logs(Duration::from_secs(5), "")
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(fake.seen().len(), 1);
}

#[tokio::test]
async fn takes_one_request_however_many_streams_answer() {
    let many: &'static str = Box::leak(
        (0..50)
            .map(|index| {
                format!(
                    "{{\"_stream\":\"{{container=\\\"s{index}\\\"}}\",\"_time\":\"2026-08-17T10:01:00.000Z\",\"total\":\"1\"}}"
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
            .into_boxed_str(),
    );
    let fake = Fake::start(StatusCode::OK, many).await;
    let buckets = fake
        .logs(Duration::from_secs(5), "")
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!((buckets.len(), fake.seen().len()), (50, 1));
}

#[tokio::test]
async fn carries_the_credentials() {
    let fake = Fake::start(StatusCode::OK, COUNTS).await;
    fake.logs(Duration::from_secs(5), "admin")
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(
        fake.seen()[0].authorization.as_deref(),
        Some("Basic YWRtaW460LPQu9Cw0LTQuNC+0LvRg9GB")
    );
}

#[tokio::test]
async fn asks_nothing_without_a_username() {
    let fake = Fake::start(StatusCode::OK, COUNTS).await;
    fake.logs(Duration::from_secs(5), "")
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(fake.seen()[0].authorization, None);
}

#[tokio::test]
async fn caps_the_rows_of_the_answer() {
    let fake = Fake::start(StatusCode::OK, COUNTS).await;
    fake.logs(Duration::from_secs(5), "")
        .buckets(quarter())
        .await
        .unwrap();
    assert_eq!(fake.seen()[0].limit, "500");
}

#[tokio::test]
async fn fails_on_a_server_error() {
    let fake = Fake::start(StatusCode::INTERNAL_SERVER_ERROR, "unexpected pipe").await;
    let failure = fake
        .logs(Duration::from_secs(5), "")
        .buckets(quarter())
        .await
        .unwrap_err();
    assert!(matches!(failure, SourceError::Status { status: 500, .. }));
}

#[tokio::test]
async fn fails_on_an_unreadable_row() {
    let fake = Fake::start(StatusCode::OK, "<html>gateway timeout</html>").await;
    let failure = fake
        .logs(Duration::from_secs(5), "")
        .buckets(quarter())
        .await
        .unwrap_err();
    assert!(matches!(failure, SourceError::Shape(_)));
}

#[tokio::test]
async fn fails_on_a_row_without_a_stream() {
    let fake = Fake::start(
        StatusCode::OK,
        r#"{"_time":"2026-08-17T10:01:00.000Z","total":"5"}"#,
    )
    .await;
    let failure = fake
        .logs(Duration::from_secs(5), "")
        .buckets(quarter())
        .await
        .unwrap_err();
    assert!(matches!(failure, SourceError::Shape(_)));
}

#[tokio::test]
async fn gives_up_on_a_silent_source() {
    let fake = Fake::slow(StatusCode::OK, COUNTS, Duration::from_secs(30)).await;
    let failure = fake
        .logs(Duration::from_millis(150), "")
        .buckets(quarter())
        .await
        .unwrap_err();
    assert!(matches!(failure, SourceError::Transport(_)));
}

#[tokio::test]
async fn calls_itself_logs() {
    let fake = Fake::start(StatusCode::OK, COUNTS).await;
    assert_eq!(fake.logs(Duration::from_secs(5), "").name(), "logs");
}

const BY_HOUR: &str = concat!(
    r#"{"_time":"2026-08-17T09:00:00Z","total":"12"}"#,
    "\n",
    r#"{"_time":"2026-08-17T10:00:00Z","total":"40"}"#,
    "\n",
);

#[tokio::test]
async fn runs_a_query_as_written() {
    let fake = Fake::start(StatusCode::OK, BY_HOUR).await;
    fake.logs(Duration::from_secs(5), "")
        .query(
            "_time:1h {a=\"b\"} | stats by (_time:1h) count() as total",
            quarter(),
            50,
        )
        .await
        .expect("запрос не прошёл");
    assert_eq!(
        fake.seen()[0].query,
        "_time:1h {a=\"b\"} | stats by (_time:1h) count() as total"
    );
}

#[tokio::test]
async fn passes_the_limit_of_a_query() {
    let fake = Fake::start(StatusCode::OK, BY_HOUR).await;
    fake.logs(Duration::from_secs(5), "")
        .query("*", quarter(), 37)
        .await
        .expect("запрос не прошёл");
    assert_eq!(fake.seen()[0].limit, "37");
}

#[tokio::test]
async fn renders_a_stats_row_as_fields() {
    let fake = Fake::start(StatusCode::OK, BY_HOUR).await;
    let lines = fake
        .logs(Duration::from_secs(5), "")
        .query("*", quarter(), 50)
        .await
        .expect("запрос не прошёл");
    assert_eq!(lines[1], "_time=2026-08-17T10:00:00Z total=40");
}

#[tokio::test]
async fn renders_a_message_row_as_the_message_alone() {
    let fake = Fake::start(
        StatusCode::OK,
        r#"{"_stream":"{a=\"b\"}","_stream_id":"0x1","_time":"2026-08-17T10:00:00Z","_msg":"upstream timed out"}"#,
    )
    .await;
    let lines = fake
        .logs(Duration::from_secs(5), "")
        .query("*", quarter(), 50)
        .await
        .expect("запрос не прошёл");
    assert_eq!(lines, vec!["upstream timed out".to_owned()]);
}

#[tokio::test]
async fn reports_a_refused_query() {
    let fake = Fake::start(StatusCode::BAD_REQUEST, "cannot parse query").await;
    let failure = fake
        .logs(Duration::from_secs(5), "")
        .query("| |", quarter(), 50)
        .await
        .expect_err("отказ пропущен");
    assert!(matches!(failure, SourceError::Status { status: 400, .. }));
}
