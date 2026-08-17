//! Агент диагностики: смотрит логи и метрики, находит отклонения, расследует их.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use sre_app::config::{Config, Process};
use sre_app::metrics::Metrics;
use sre_app::{VERSION, collector, grouper, watcher, web};
use sre_logs::{Filter, Logs};
use sre_source::Source;
use sre_store::Store;
use tracing_subscriber::EnvFilter;

/// Путь к файлу настроек по умолчанию.
const CONFIG: &str = "/etc/sreagent/sreagent.toml";

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_env("SREAGENT_LOG").unwrap_or_else(|_| "info".into()))
        .init();
    match serve().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(failure) => {
            tracing::error!(%failure, "агент не запущен");
            ExitCode::FAILURE
        }
    }
}

/// Отказы запуска.
#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error(transparent)]
    Config(#[from] sre_app::config::ConfigError),
    #[error("порт не занят: {0}")]
    Bind(#[from] std::io::Error),
    #[error("база наблюдений не открыта: {0}")]
    Store(#[from] sre_store::StoreError),
    #[error("источник не собран: {0}")]
    Source(#[from] sre_source::SourceError),
    #[error("шаблон ошибок некорректен: {0}")]
    Filter(#[from] sre_logs::FilterError),
    #[error("адрес источника некорректен: {0}")]
    Address(#[from] url::ParseError),
}

async fn serve() -> Result<(), Failure> {
    let path = std::env::args()
        .nth(1)
        .map_or_else(|| PathBuf::from(CONFIG), PathBuf::from);
    let config = Config::read(&path, &Process)?;
    for key in &config.unknown {
        tracing::warn!(key, "настройка неизвестна агенту и пропущена");
    }

    let metrics = Arc::new(Metrics::new(VERSION));
    let store = Store::open(&config.file.database)?;
    let logs: Arc<dyn Source> = Arc::new(Logs::new(
        &sre_logs::Settings {
            url: config.file.logs.url.parse()?,
            username: config.file.logs.username.clone(),
            password: config.secrets.logs_password.clone(),
            timeout: config.file.logs.timeout,
            rows: 2000,
        },
        Filter::new(
            &config.file.logs.error_pattern,
            config.file.logs.self_streams.clone(),
        )?,
    )?);
    let sources = vec![logs];
    collector::collect(sources.clone(), &store, &metrics, &config.file.collector);
    collector::tidy(&sources, &store, &config.file.retention);
    watcher::watch(&sources, &store, &metrics, &config.file.enabled());
    grouper::group(&sources, &store, &metrics, &config.file.incidents);

    let shared = web::Shared::new(metrics, VERSION);
    let listener = tokio::net::TcpListener::bind(config.file.bind).await?;
    tracing::info!(
        address = %config.file.bind,
        horizons = config.file.enabled().len(),
        model = config.file.model.name,
        "агент поднят"
    );
    axum::serve(listener, web::routes(shared))
        .with_graceful_shutdown(stop())
        .await?;
    Ok(())
}

/// Ждёт сигнала остановки, чтобы дорисовать текущие запросы.
async fn stop() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("остановка по сигналу");
}
