use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use argon2::Argon2;
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHasher, SaltString};
use sre_app::config::Account;
use sre_app::metrics::Metrics;
use sre_app::session::Doorman;
use sre_app::web::{Shared, routes};
use sre_store::Store;
use tempfile::TempDir;

/// Пароль дежурного в лаборатории.
const SECRET: &str = "гладиолус-9f3a";

/// Поднятая веб-морда с базой во временном каталоге.
struct Agent {
    address: SocketAddr,
    shared: Shared,
    store: Store,
    _directory: TempDir,
}

impl Agent {
    async fn start() -> Self {
        let directory = TempDir::new().expect("временный каталог не создан");
        let store = Store::open(&directory.path().join("sre.db")).expect("база не открыта");
        let doorman = Doorman::new(
            vec![Account {
                login: "duty".to_owned(),
                password: hashed(SECRET),
            }],
            "ключ-подписи-стенда",
        );
        let shared = Shared::new(
            Arc::new(Metrics::new("0.1.0-тест")),
            store.clone(),
            doorman,
            "0.1.0-тест",
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("порт не занят");
        let address = listener.local_addr().expect("адрес не получен");
        let router = routes(shared.clone());
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("морда упала");
        });
        Self {
            address,
            shared,
            store,
            _directory: directory,
        }
    }

    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("клиент не собрался")
    }

    fn at(&self, path: &str) -> String {
        format!("http://{}{path}", self.address)
    }

    async fn get(&self, path: &str) -> reqwest::Response {
        Self::client()
            .get(self.at(path))
            .send()
            .await
            .expect("запрос не дошёл")
    }

    /// Заходит под дежурным и возвращает куку сессии.
    async fn enter(&self, login: &str, password: &str) -> Option<String> {
        let answer = Self::client()
            .post(self.at("/login"))
            .form(&[("login", login), ("password", password)])
            .send()
            .await
            .expect("запрос не дошёл");
        answer
            .headers()
            .get("set-cookie")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
            .filter(|cookie| !cookie.starts_with("sreagent=;"))
    }

    /// Ставит оценку инциденту.
    async fn judge(&self, id: i64, useful: &str, cookie: &str) {
        Self::client()
            .post(self.at(&format!("/incident/{id}/verdict")))
            .header("cookie", cookie)
            .form(&[("useful", useful)])
            .send()
            .await
            .expect("запрос не дошёл");
    }

    async fn inside(&self, path: &str, cookie: &str) -> reqwest::Response {
        Self::client()
            .get(self.at(path))
            .header("cookie", cookie)
            .send()
            .await
            .expect("запрос не дошёл")
    }
}

fn hashed(password: &str) -> String {
    Argon2::default()
        .hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))
        .expect("хеш не посчитан")
        .to_string()
}

/// Заводит инцидент прямо в базе, минуя конвейер.
async fn incident(agent: &Agent) -> i64 {
    use sre_domain::{Detector, Deviation, Kind, Minute, Service, Signature, Stream, Thresholds};
    let at = Minute::at(1_786_968_660);
    let stream = Stream::new("{host=\"node-01\",service=\"orders-api\"}");
    let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0, 5.0], 91.0, Kind::Sum);
    let deviation = Deviation::new("logs", &stream, "15m", at, verdict);
    agent.store.spot(&deviation, at).await.unwrap();
    let id = agent.store.loose(10).await.unwrap()[0].0;
    agent
        .store
        .attach(
            id,
            &Service::new("orders-api"),
            &Signature::of("upstream 192.0.2.19 timed out"),
            &deviation,
        )
        .await
        .unwrap()
        .0
}

#[tokio::test]
async fn answers_the_health_check() {
    let agent = Agent::start().await;
    let health: serde_json::Value = agent.get("/api/health").await.json().await.unwrap();
    assert_eq!(health["status"], "ok");
}

#[tokio::test]
async fn opens_the_health_check_without_a_login() {
    let agent = Agent::start().await;
    assert_eq!(agent.get("/api/health").await.status(), 200);
}

#[tokio::test]
async fn opens_the_metrics_without_a_login() {
    let agent = Agent::start().await;
    assert!(
        agent
            .get("/metrics")
            .await
            .text()
            .await
            .unwrap()
            .contains("sre_up")
    );
}

#[tokio::test]
async fn shows_the_bucket_moment_once_there_is_one() {
    let agent = Agent::start().await;
    agent.shared.metrics().bucket(SystemTime::now());
    assert!(
        agent
            .get("/metrics")
            .await
            .text()
            .await
            .unwrap()
            .contains("sre_last_bucket_timestamp_seconds")
    );
}

#[tokio::test]
async fn sends_a_stranger_to_the_door() {
    let agent = Agent::start().await;
    assert_eq!(agent.get("/").await.status(), 303);
}

#[tokio::test]
async fn shows_the_door_to_a_stranger() {
    let agent = Agent::start().await;
    assert!(
        agent
            .get("/login")
            .await
            .text()
            .await
            .unwrap()
            .contains("Вход")
    );
}

#[tokio::test]
async fn lets_the_duty_engineer_in() {
    let agent = Agent::start().await;
    assert!(agent.enter("duty", SECRET).await.is_some());
}

#[tokio::test]
async fn refuses_a_wrong_password() {
    let agent = Agent::start().await;
    assert!(agent.enter("duty", "подобрал").await.is_none());
}

#[tokio::test]
async fn refuses_an_unknown_login() {
    let agent = Agent::start().await;
    assert!(agent.enter("посторонний", SECRET).await.is_none());
}

#[tokio::test]
async fn hides_the_session_from_scripts() {
    let agent = Agent::start().await;
    assert!(
        agent
            .enter("duty", SECRET)
            .await
            .unwrap()
            .contains("HttpOnly")
    );
}

#[tokio::test]
async fn shows_the_feed_to_the_one_who_entered() {
    let agent = Agent::start().await;
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    assert_eq!(agent.inside("/", &cookie).await.status(), 200);
}

#[tokio::test]
async fn refuses_a_forged_session() {
    let agent = Agent::start().await;
    let forged = "sreagent=duty:99999999999.поддельная-подпись";
    assert_eq!(agent.inside("/", forged).await.status(), 303);
}

#[tokio::test]
async fn says_the_feed_is_empty() {
    let agent = Agent::start().await;
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    let page = agent.inside("/", &cookie).await.text().await.unwrap();
    assert!(page.contains("Инцидентов нет"));
}

#[tokio::test]
async fn shows_an_incident_in_the_feed() {
    let agent = Agent::start().await;
    incident(&agent).await;
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    let page = agent.inside("/", &cookie).await.text().await.unwrap();
    assert!(page.contains("orders-api"));
}

#[tokio::test]
async fn masks_the_address_in_the_shown_signature() {
    let agent = Agent::start().await;
    incident(&agent).await;
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    let page = agent.inside("/", &cookie).await.text().await.unwrap();
    assert!(page.contains("upstream &#60;addr&#62; timed out"));
}

#[tokio::test]
async fn opens_the_card_of_an_incident() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    let page = agent
        .inside(&format!("/incident/{id}"), &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("Последнее подтверждение"));
}

#[tokio::test]
async fn knows_nothing_of_an_absent_incident() {
    let agent = Agent::start().await;
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    assert_eq!(agent.inside("/incident/404", &cookie).await.status(), 404);
}

#[tokio::test]
async fn keeps_the_card_from_a_stranger() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    assert_eq!(agent.get(&format!("/incident/{id}")).await.status(), 303);
}

#[tokio::test]
async fn takes_the_verdict_of_the_duty_engineer() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    agent.judge(id, "no", &cookie).await;
    let quality: serde_json::Value = agent.get("/api/metrics").await.json().await.unwrap();
    assert_eq!(quality["useless"], 1);
}

#[tokio::test]
async fn counts_the_wrong_share_over_the_judged() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    agent.judge(id, "no", &cookie).await;
    let quality: serde_json::Value = agent.get("/api/metrics").await.json().await.unwrap();
    assert_eq!(quality["wrong"], 1.0);
}

#[tokio::test]
async fn knows_no_share_before_the_first_verdict() {
    let agent = Agent::start().await;
    incident(&agent).await;
    let quality: serde_json::Value = agent.get("/api/metrics").await.json().await.unwrap();
    assert!(quality["wrong"].is_null());
}

#[tokio::test]
async fn shows_the_verdict_on_the_card() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    agent.judge(id, "yes", &cookie).await;
    let page = agent
        .inside(&format!("/incident/{id}"), &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("по делу"));
}

#[tokio::test]
async fn keeps_the_verdict_from_a_stranger() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let answer = Agent::client()
        .post(agent.at(&format!("/incident/{id}/verdict")))
        .form(&[("useful", "no")])
        .send()
        .await
        .expect("запрос не дошёл");
    assert_eq!(answer.status(), 303);
}
