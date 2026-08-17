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
    knowledge: sre_app::config::Knowledge,
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
        let knowledge = sre_app::config::Knowledge::default()
            .shelf(directory.path().join("notes"), Duration::from_mins(5))
            .drafting(directory.path().join("drafts"));
        let shared = Shared::new(
            Arc::new(Metrics::new("0.1.0-тест")),
            store.clone(),
            doorman,
            &knowledge,
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
            knowledge,
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

    /// Приглушает пару инцидента на заданное число суток.
    async fn mute(&self, id: i64, days: &str, cookie: &str) {
        Self::client()
            .post(self.at(&format!("/incident/{id}/mute")))
            .header("cookie", cookie)
            .form(&[("days", days), ("reason", "ругается каждую ночь")])
            .send()
            .await
            .expect("запрос не дошёл");
    }

    /// Принимает или отклоняет черновик.
    async fn settle(&self, id: i64, accept: &str, cookie: &str) {
        Self::client()
            .post(self.at(&format!("/draft/{id}/settle")))
            .header("cookie", cookie)
            .form(&[("accept", accept)])
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
            "такого раньше не было",
        )
        .await
        .unwrap()
        .0
}

/// Заводит второй инцидент — другого сервиса и другой сигнатуры.
async fn other(agent: &Agent) -> i64 {
    use sre_domain::{Detector, Deviation, Kind, Minute, Service, Signature, Stream, Thresholds};
    let at = Minute::at(1_786_968_720);
    let stream = Stream::new("{host=\"node-01\",service=\"billing-api\"}");
    let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0, 5.0], 91.0, Kind::Sum);
    let deviation = Deviation::new("logs", &stream, "15m", at, verdict);
    agent.store.spot(&deviation, at).await.unwrap();
    let id = agent
        .store
        .loose(10)
        .await
        .unwrap()
        .into_iter()
        .find(|(_, it)| it.stream == stream)
        .expect("отклонение не найдено")
        .0;
    agent
        .store
        .attach(
            id,
            &Service::new("billing-api"),
            &Signature::of("connection refused"),
            &deviation,
            "такого раньше не было",
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

#[tokio::test]
async fn shows_the_conclusion_on_the_card() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let dig = agent
        .store
        .dig(id, "error-burst", sre_domain::Minute::at(1_786_968_660))
        .await
        .unwrap();
    agent
        .store
        .conclude(
            dig,
            "апстрим перестал отвечать после выката",
            0.7,
            "проверить откат апстрима",
            None,
            sre_domain::Minute::at(1_786_968_720),
        )
        .await
        .unwrap();
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    let page = agent
        .inside(&format!("/incident/{id}"), &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("апстрим перестал отвечать после выката"));
}

#[tokio::test]
async fn says_plainly_when_the_queue_did_not_reach() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let dig = agent
        .store
        .dig(id, "—", sre_domain::Minute::at(1_786_968_660))
        .await
        .unwrap();
    agent.store.drop_dig(dig, "skipped").await.unwrap();
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    let page = agent
        .inside(&format!("/incident/{id}"), &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("вывод пропущен"));
}

#[tokio::test]
async fn breaks_a_long_number_into_groups() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let cookie = agent.enter("duty", SECRET).await.unwrap();
    let page = agent
        .inside(&format!("/incident/{id}"), &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("91"));
}

/// Заводит инцидент с открытой заявкой и отвечает её номером.
async fn asked(agent: &Agent) -> (i64, i64) {
    use sre_domain::{Inquiry, Minute};
    let incident = incident(agent).await;
    let at = Minute::at(1_786_968_660);
    let dig = agent.store.dig(incident, "error-burst", at).await.unwrap();
    let inquiry = agent
        .store
        .ask(
            incident,
            dig,
            &Inquiry::new("node-01", "df -h /var", "место на диске").unwrap(),
            at,
        )
        .await
        .unwrap();
    (incident, inquiry)
}

#[tokio::test]
async fn shows_the_command_on_the_card() {
    let agent = Agent::start().await;
    let (incident, _) = asked(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent
        .inside(&format!("/incident/{incident}"), &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("df -h /var"));
}

#[tokio::test]
async fn collects_what_waits_for_the_duty_engineer() {
    let agent = Agent::start().await;
    asked(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent
        .inside("/waiting", &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("df -h /var"));
}

#[tokio::test]
async fn says_there_is_nothing_to_wait_for() {
    let agent = Agent::start().await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent
        .inside("/waiting", &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("Ждать нечего"));
}

#[tokio::test]
async fn keeps_the_waiting_list_from_a_stranger() {
    let agent = Agent::start().await;
    assert_eq!(agent.get("/waiting").await.status(), 303);
}

#[tokio::test]
async fn counts_the_waiting_inquiries_in_the_header() {
    let agent = Agent::start().await;
    asked(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent.inside("/", &cookie).await.text().await.unwrap();
    assert!(page.contains("ждёт вас · 1"));
}

#[tokio::test]
async fn takes_the_answer_of_the_duty_engineer() {
    let agent = Agent::start().await;
    let (incident, inquiry) = asked(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    Agent::client()
        .post(agent.at(&format!("/inquiry/{inquiry}/answer")))
        .header("cookie", &cookie)
        .form(&[("answer", "/var 98% занято")])
        .send()
        .await
        .expect("запрос не дошёл");
    assert_eq!(
        agent.store.answers(incident).await.unwrap()[0].1,
        "/var 98% занято"
    );
}

#[tokio::test]
async fn drops_an_inquiry_answered_with_nothing() {
    let agent = Agent::start().await;
    let (_, inquiry) = asked(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    Agent::client()
        .post(agent.at(&format!("/inquiry/{inquiry}/answer")))
        .header("cookie", &cookie)
        .form(&[("answer", "  ")])
        .send()
        .await
        .expect("запрос не дошёл");
    assert!(agent.store.pending(10).await.unwrap().is_empty());
}

#[tokio::test]
async fn keeps_an_answer_from_a_stranger() {
    let agent = Agent::start().await;
    let (_, inquiry) = asked(&agent).await;
    let answer = Agent::client()
        .post(agent.at(&format!("/inquiry/{inquiry}/answer")))
        .form(&[("answer", "чужой ответ")])
        .send()
        .await
        .expect("запрос не дошёл");
    assert_eq!(answer.status(), 303);
}

/// Заводит инцидент с непринятым черновиком и отвечает его номером.
async fn drafted(agent: &Agent) -> (i64, i64) {
    use sre_domain::Minute;
    let incident = incident(agent).await;
    let drafts = agent.knowledge.drafts.clone();
    let path = sre_knowledge::draft::write(
        &drafts,
        &sre_knowledge::Draft {
            name: "2026-08-17-orders-api-1".to_owned(),
            title: "Апстрим orders-api перестал отвечать".to_owned(),
            kind: "incident".to_owned(),
            tags: vec!["таймаут".to_owned()],
            signatures: vec!["upstream timed out".to_owned()],
            services: vec!["orders-api".to_owned()],
            incident,
            confidence: 0.7,
            body: "## Что было\n\nАпстрим молчит.".to_owned(),
        },
    )
    .expect("черновик не записан");
    let draft = agent
        .store
        .draft(
            incident,
            "2026-08-17-orders-api-1",
            "Апстрим orders-api перестал отвечать",
            &path.to_string_lossy(),
            Minute::at(1_786_968_660),
        )
        .await
        .unwrap()
        .expect("черновик не учтён");
    (incident, draft)
}

#[tokio::test]
async fn shows_a_draft_on_the_card() {
    let agent = Agent::start().await;
    let (incident, _) = drafted(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent
        .inside(&format!("/incident/{incident}"), &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("принять в базу"));
}

#[tokio::test]
async fn counts_an_unsettled_draft_among_what_waits() {
    let agent = Agent::start().await;
    drafted(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent.inside("/", &cookie).await.text().await.unwrap();
    assert!(page.contains("ждёт вас · 1"));
}

#[tokio::test]
async fn moves_an_accepted_draft_into_the_knowledge_base() {
    let agent = Agent::start().await;
    let (_, draft) = drafted(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    agent.settle(draft, "yes", &cookie).await;
    assert!(
        agent
            .knowledge
            .notes
            .join("2026-08-17-orders-api-1.md")
            .exists()
    );
}

#[tokio::test]
async fn puts_an_accepted_draft_into_the_search_index() {
    let agent = Agent::start().await;
    let (_, draft) = drafted(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    agent.settle(draft, "yes", &cookie).await;
    assert_eq!(agent.store.notes().await.unwrap(), 1);
}

#[tokio::test]
async fn keeps_a_rejected_draft_out_of_the_knowledge_base() {
    let agent = Agent::start().await;
    let (_, draft) = drafted(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    agent.settle(draft, "no", &cookie).await;
    assert_eq!(agent.store.notes().await.unwrap(), 0);
}

#[tokio::test]
async fn names_the_one_who_settled_a_draft() {
    let agent = Agent::start().await;
    let (incident, draft) = drafted(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    agent.settle(draft, "no", &cookie).await;
    assert_eq!(
        agent.store.drafts(incident).await.unwrap()[0]
            .who
            .as_deref(),
        Some("duty")
    );
}

#[tokio::test]
async fn settles_a_draft_once() {
    let agent = Agent::start().await;
    let (_, draft) = drafted(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    agent.settle(draft, "yes", &cookie).await;
    assert!(agent.store.unsettled(10).await.unwrap().is_empty());
}

#[tokio::test]
async fn hushes_a_pair_the_duty_engineer_is_tired_of() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    agent.mute(id, "7", &cookie).await;
    let now = sre_domain::Minute::at(1_786_968_720);
    assert_eq!(agent.store.mutes(now, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn refuses_to_mute_forever() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    agent.mute(id, "100000", &cookie).await;
    let now = sre_domain::Minute::at(1_786_968_720);
    let until = agent.store.mutes(now, 10).await.unwrap()[0].until;
    assert!(until.stamp() - now.stamp() <= 91 * 24 * 60 * 60);
}

#[tokio::test]
async fn shows_the_muted_pairs_on_their_own_page() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    agent.mute(id, "7", &cookie).await;
    let page = agent.inside("/mutes", &cookie).await.text().await.unwrap();
    assert!(page.contains("orders-api"));
}

#[tokio::test]
async fn says_there_is_nothing_muted() {
    let agent = Agent::start().await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent.inside("/mutes", &cookie).await.text().await.unwrap();
    assert!(page.contains("Приглушений нет"));
}

#[tokio::test]
async fn lifts_a_mute_before_its_time() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    agent.mute(id, "7", &cookie).await;
    let now = sre_domain::Minute::at(1_786_968_720);
    let mute = agent.store.mutes(now, 10).await.unwrap()[0].id;
    Agent::client()
        .post(agent.at(&format!("/mute/{mute}/lift")))
        .header("cookie", &cookie)
        .send()
        .await
        .expect("запрос не дошёл");
    assert!(!agent.store.mutes(now, 10).await.unwrap()[0].live);
}

#[tokio::test]
async fn keeps_muting_from_a_stranger() {
    let agent = Agent::start().await;
    let id = incident(&agent).await;
    let answer = Agent::client()
        .post(agent.at(&format!("/incident/{id}/mute")))
        .form(&[("days", "7"), ("reason", "чужой")])
        .send()
        .await
        .expect("запрос не дошёл");
    assert_eq!(answer.status(), 303);
}

#[tokio::test]
async fn merges_two_incidents_at_the_word_of_the_duty_engineer() {
    let agent = Agent::start().await;
    let first = incident(&agent).await;
    let second = other(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    Agent::client()
        .post(agent.at(&format!("/incident/{first}/merge")))
        .header("cookie", &cookie)
        .form(&[("other", second.to_string())])
        .send()
        .await
        .expect("запрос не дошёл");
    assert_eq!(agent.store.merged(first).await.unwrap(), vec![second]);
}

#[tokio::test]
async fn shows_what_an_incident_was_built_from() {
    let agent = Agent::start().await;
    let first = incident(&agent).await;
    let second = other(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    Agent::client()
        .post(agent.at(&format!("/incident/{first}/merge")))
        .header("cookie", &cookie)
        .form(&[("other", second.to_string())])
        .send()
        .await
        .expect("запрос не дошёл");
    let page = agent
        .inside(&format!("/incident/{first}"), &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("Собран из"));
}

#[tokio::test]
async fn takes_a_merged_incident_off_the_feed() {
    let agent = Agent::start().await;
    let first = incident(&agent).await;
    let second = other(&agent).await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    Agent::client()
        .post(agent.at(&format!("/incident/{first}/merge")))
        .header("cookie", &cookie)
        .form(&[("other", second.to_string())])
        .send()
        .await
        .expect("запрос не дошёл");
    assert_eq!(agent.store.incidents(false, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn keeps_merging_from_a_stranger() {
    let agent = Agent::start().await;
    let first = incident(&agent).await;
    let second = other(&agent).await;
    let answer = Agent::client()
        .post(agent.at(&format!("/incident/{first}/merge")))
        .form(&[("other", second.to_string())])
        .send()
        .await
        .expect("запрос не дошёл");
    assert_eq!(answer.status(), 303);
}

#[tokio::test]
async fn shows_the_shelf_of_reports() {
    let agent = Agent::start().await;
    agent
        .store
        .file(
            sre_store::Filing {
                kind: "daily",
                name: "2026-08-16",
                title: "Сутки 2026-08-16",
                path: "reports/daily/2026-08-16.md",
                body: "# Сутки\n\n## Инциденты\n\nНи одного за сутки.",
                whole: true,
            },
            sre_domain::Minute::at(1_786_968_660),
        )
        .await
        .unwrap();
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent
        .inside("/reports", &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("Сутки 2026-08-16"));
}

#[tokio::test]
async fn opens_a_report() {
    let agent = Agent::start().await;
    agent
        .store
        .file(
            sre_store::Filing {
                kind: "daily",
                name: "2026-08-16",
                title: "Сутки 2026-08-16",
                path: "reports/daily/2026-08-16.md",
                body: "Ни одного за сутки",
                whole: true,
            },
            sre_domain::Minute::at(1_786_968_660),
        )
        .await
        .unwrap();
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent
        .inside("/report/daily/2026-08-16", &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("Ни одного за сутки"));
}

#[tokio::test]
async fn says_there_are_no_reports_yet() {
    let agent = Agent::start().await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent
        .inside("/reports", &cookie)
        .await
        .text()
        .await
        .unwrap();
    assert!(page.contains("Отчётов нет"));
}

#[tokio::test]
async fn keeps_the_reports_from_a_stranger() {
    let agent = Agent::start().await;
    assert_eq!(agent.get("/reports").await.status(), 303);
}

#[tokio::test]
async fn shows_what_the_agent_watches() {
    let agent = Agent::start().await;
    let at = sre_domain::Minute::at(1_786_968_660);
    let stream = sre_domain::Stream::new("{host=\"node-01\",service=\"orders-api\"}");
    agent
        .store
        .save(
            "logs",
            sre_domain::Span::single(at),
            vec![sre_domain::Bucket::counted(stream, at, 7.0)],
        )
        .await
        .unwrap();
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent.inside("/series", &cookie).await.text().await.unwrap();
    assert!(page.contains("orders-api"));
}

#[tokio::test]
async fn says_the_series_are_empty_before_the_first_bucket() {
    let agent = Agent::start().await;
    let cookie = agent
        .enter("duty", SECRET)
        .await
        .expect("вход не удался");
    let page = agent.inside("/series", &cookie).await.text().await.unwrap();
    assert!(page.contains("Ряд пуст"));
}

#[tokio::test]
async fn keeps_the_series_from_a_stranger() {
    let agent = Agent::start().await;
    assert_eq!(agent.get("/series").await.status(), 303);
}

#[tokio::test]
async fn shows_how_long_it_took_to_notice() {
    let agent = Agent::start().await;
    agent.shared.metrics().detected(240);
    let page = agent.get("/metrics").await.text().await.unwrap();
    assert!(page.contains("sre_detection_seconds_bucket{le=\"300\"} 1"));
}

#[tokio::test]
async fn leaves_a_slow_detection_out_of_the_quick_buckets() {
    let agent = Agent::start().await;
    agent.shared.metrics().detected(1200);
    let page = agent.get("/metrics").await.text().await.unwrap();
    assert!(page.contains("sre_detection_seconds_bucket{le=\"300\"} 0"));
}

#[tokio::test]
async fn counts_every_detection_whatever_it_took() {
    let agent = Agent::start().await;
    for seconds in [30, 1200, 9000] {
        agent.shared.metrics().detected(seconds);
    }
    let page = agent.get("/metrics").await.text().await.unwrap();
    assert!(page.contains("sre_detection_seconds_count 3"));
}

#[tokio::test]
async fn shows_how_long_it_took_to_explain() {
    let agent = Agent::start().await;
    agent.shared.metrics().explained(700);
    let page = agent.get("/metrics").await.text().await.unwrap();
    assert!(page.contains("sre_conclusion_seconds_bucket{le=\"900\"} 1"));
}

#[tokio::test]
async fn drops_a_measurement_of_a_clock_that_went_backwards() {
    let agent = Agent::start().await;
    agent.shared.metrics().detected(-60);
    let page = agent.get("/metrics").await.text().await.unwrap();
    assert!(page.contains("sre_detection_seconds_count 0"));
}
