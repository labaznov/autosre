use std::net::SocketAddr;
use std::path::PathBuf;

use autosre_app::check::{self, Check};
use autosre_app::config::{Config, Env};
use axum::Router;
use axum::routing::{get, post};
use tempfile::TempDir;

/// Окружение с ключами.
struct Keys;

impl Env for Keys {
    fn var(&self, key: &str) -> Option<String> {
        match key {
            "AUTOSRE_MODEL_KEY" => Some("sk-lab".to_owned()),
            "AUTOSRE_SESSION_KEY" => Some("9f3a1c7e0b".to_owned()),
            _ => None,
        }
    }
}

async fn logs(_: String) -> &'static str {
    ""
}

async fn metrics() -> &'static str {
    r#"{"status":"success","data":{"resultType":"matrix","result":[]}}"#
}

async fn model(_: String) -> &'static str {
    r#"{"choices":[{"message":{"content":"{\"picture\":\"Связь есть\"}"}}]}"#
}

/// Поддельные логи, метрики и модель на одном эфемерном порту.
async fn stand() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("порт не занят");
    let address = listener.local_addr().expect("адрес не получен");
    let router = Router::new()
        .route("/select/logsql/query", get(logs))
        .route("/api/v1/query_range", get(metrics))
        .route("/v1/chat/completions", post(model));
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("стенд упал");
    });
    address
}

/// Порт, на котором никто не слушает.
async fn silence() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("порт не занят");
    listener.local_addr().expect("адрес не получен")
}

/// Настройки, где всё указывает на стенд, кроме того, что подменено.
struct Setup {
    config: Config,
    _directory: TempDir,
}

impl Setup {
    fn of(logs: SocketAddr, metrics: SocketAddr, model: SocketAddr, skills: &str) -> Self {
        let directory = TempDir::new().expect("временный каталог не создан");
        let root = directory.path();
        std::fs::create_dir_all(root.join("knowledge/notes")).unwrap();
        std::fs::create_dir_all(root.join("knowledge/drafts")).unwrap();
        let body = format!(
            r#"
database = "{db}"

[logs]
url = "http://{logs}/"

[metrics]
url = "http://{metrics}/"
select = ["process_resident_memory_bytes"]

[model]
url = "http://{model}/"
name = "поддельная"
timeout = "5s"

[[horizon]]
name = "15m"
width = "15m"
period = "1m"

[[account]]
login = "duty"
password = "$argon2id$v=19$m=19456,t=2,p=1$c2FsdHNhbHQ$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

[digging]
skills = "{skills}"

[knowledge]
notes = "{notes}"
drafts = "{drafts}"
"#,
            db = root.join("autosre.db").display(),
            notes = root.join("knowledge/notes").display(),
            drafts = root.join("knowledge/drafts").display(),
        );
        let path = root.join("autosre.toml");
        std::fs::write(&path, body).unwrap();
        Self {
            config: Config::read(&path, &Keys).expect("настройки не прочитаны"),
            _directory: directory,
        }
    }
}

fn shipped() -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/knowledge/skills")
        .display()
        .to_string()
}

fn outcome<'a>(checks: &'a [Check], what: &str) -> &'a Result<String, String> {
    &checks
        .iter()
        .find(|it| it.what == what)
        .unwrap_or_else(|| panic!("проверки «{what}» нет"))
        .outcome
}

#[tokio::test]
async fn passes_when_everything_answers() {
    let stand = stand().await;
    let setup = Setup::of(stand, stand, stand, &shipped());
    let checks = check::run(&setup.config).await;
    assert_eq!(check::failed(&checks), 0, "{}", check::table(&checks));
}

#[tokio::test]
async fn names_the_silent_logs() {
    let stand = stand().await;
    let setup = Setup::of(silence().await, stand, stand, &shipped());
    let checks = check::run(&setup.config).await;
    assert!(outcome(&checks, "логи").is_err());
}

#[tokio::test]
async fn names_the_silent_metrics() {
    let stand = stand().await;
    let setup = Setup::of(stand, silence().await, stand, &shipped());
    let checks = check::run(&setup.config).await;
    assert!(outcome(&checks, "метрики").is_err());
}

#[tokio::test]
async fn names_the_silent_model() {
    let stand = stand().await;
    let setup = Setup::of(stand, stand, silence().await, &shipped());
    let checks = check::run(&setup.config).await;
    assert!(outcome(&checks, "модель").is_err());
}

#[tokio::test]
async fn keeps_the_rest_green_when_the_model_is_silent() {
    let stand = stand().await;
    let setup = Setup::of(stand, stand, silence().await, &shipped());
    let checks = check::run(&setup.config).await;
    assert_eq!(check::failed(&checks), 1);
}

#[tokio::test]
async fn names_a_missing_skills_directory() {
    let stand = stand().await;
    let setup = Setup::of(stand, stand, stand, "/nonexistent/skills");
    let checks = check::run(&setup.config).await;
    assert!(outcome(&checks, "знания").is_err());
}

#[tokio::test]
async fn counts_the_skills_it_found() {
    let stand = stand().await;
    let setup = Setup::of(stand, stand, stand, &shipped());
    let checks = check::run(&setup.config).await;
    assert!(
        outcome(&checks, "знания")
            .as_ref()
            .unwrap()
            .starts_with("скиллов 5")
    );
}

#[tokio::test]
async fn draws_a_cross_for_a_failure() {
    let stand = stand().await;
    let setup = Setup::of(silence().await, stand, stand, &shipped());
    let checks = check::run(&setup.config).await;
    assert!(check::table(&checks).contains("✗ логи"));
}
