use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;

use autosre_app::config::Account;
use autosre_app::metrics::Metrics;
use autosre_app::session::Doorman;
use autosre_app::tls;
use autosre_app::web::{Shared, routes};
use autosre_store::Store;
use tempfile::TempDir;

#[tokio::test]
async fn makes_a_certificate_when_there_is_none() {
    let directory = TempDir::new().unwrap();
    let cert = directory.path().join("tls/cert.pem");
    let key = directory.path().join("tls/key.pem");
    tls::certificate(&cert, &key).await.unwrap();
    assert!(cert.is_file() && key.is_file());
}

#[tokio::test]
async fn tells_that_the_certificate_is_fresh() {
    let directory = TempDir::new().unwrap();
    let cert = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    let (_, fresh) = tls::certificate(&cert, &key).await.unwrap();
    assert!(fresh);
}

#[tokio::test]
async fn keeps_the_key_to_itself() {
    let directory = TempDir::new().unwrap();
    let key = directory.path().join("key.pem");
    tls::certificate(&directory.path().join("cert.pem"), &key)
        .await
        .unwrap();
    assert_eq!(
        std::fs::metadata(&key).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[tokio::test]
async fn reuses_the_certificate_it_made() {
    let directory = TempDir::new().unwrap();
    let cert = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    tls::certificate(&cert, &key).await.unwrap();
    let made = std::fs::read(&cert).unwrap();
    let (_, fresh) = tls::certificate(&cert, &key).await.unwrap();
    assert!(!fresh && std::fs::read(&cert).unwrap() == made);
}

#[tokio::test]
async fn refuses_a_key_without_a_certificate() {
    let directory = TempDir::new().unwrap();
    let key = directory.path().join("key.pem");
    std::fs::write(&key, "ключ без пары").unwrap();
    assert!(
        tls::certificate(&directory.path().join("cert.pem"), &key)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn refuses_a_certificate_that_is_not_pem() {
    let directory = TempDir::new().unwrap();
    let cert = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&cert, "не сертификат").unwrap();
    std::fs::write(&key, "не ключ").unwrap();
    assert!(tls::certificate(&cert, &key).await.is_err());
}

#[tokio::test]
async fn answers_over_https() {
    let directory = TempDir::new().unwrap();
    let (config, _) = tls::certificate(
        &directory.path().join("cert.pem"),
        &directory.path().join("key.pem"),
    )
    .await
    .unwrap();
    let store = Store::open(&directory.path().join("autosre.db")).unwrap();
    let shared = Shared::new(
        Arc::new(Metrics::new("0.2.0-тест")),
        store,
        Doorman::new(Vec::<Account>::new(), "ключ"),
        &autosre_app::config::Knowledge::default(),
        "0.2.0-тест",
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(tls::serve(
        listener,
        config,
        routes(shared),
        std::future::pending(),
    ));
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let answer = client
        .get(format!("https://{address}/api/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(answer.status(), 200);
}
