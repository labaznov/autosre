//! Агент диагностики: смотрит логи и метрики, находит отклонения, расследует их.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use autosre_app::config::{Config, Process};
use autosre_app::metrics::Metrics;
use autosre_app::session::Doorman;
use autosre_app::{VERSION, collector, digger, grouper, librarian, reporter, scribe, watcher, web};
use autosre_logs::{Filter, Logs};
use autosre_source::Source;
use autosre_store::Store;
use tracing_subscriber::EnvFilter;

/// Путь к файлу настроек по умолчанию.
const CONFIG: &str = "/etc/autosre/autosre.toml";

/// Паузы между попытками после обрыва связи или занятого шлюза: две попытки
/// сверх первой. Дольше ждать незачем — минуту спустя съём придёт снова.
const PAUSES: [Duration; 2] = [Duration::from_secs(2), Duration::from_secs(5)];

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_env("AUTOSRE_LOG").unwrap_or_else(|_| "info".into()))
        .init();
    if let Some(password) = hashing() {
        let Ok(hash) = hash(&password) else {
            tracing::error!("пароль не захеширован");
            return ExitCode::FAILURE;
        };
        println!("{hash}");
        return ExitCode::SUCCESS;
    }
    match serve().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(failure) => {
            tracing::error!(%failure, "агент не запущен");
            ExitCode::FAILURE
        }
    }
}

/// Пароль, который просят захешировать: `autosre hash <пароль>`.
///
/// Учётку без этого не завести: в конфигурации лежит хеш, а не пароль, и
/// считать его где-то на стороне — верный способ отправить пароль не туда.
fn hashing() -> Option<String> {
    let mut args = std::env::args().skip(1);
    (args.next()? == "hash").then(|| args.next()).flatten()
}

/// Хеш пароля в форме, которую понимает конфигурация.
fn hash(password: &str) -> Result<String, argon2::password_hash::Error> {
    use argon2::password_hash::rand_core::OsRng;
    use argon2::password_hash::{PasswordHasher, SaltString};
    Ok(argon2::Argon2::default()
        .hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))?
        .to_string())
}

/// Отказы запуска.
#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error(transparent)]
    Config(#[from] autosre_app::config::ConfigError),
    #[error("порт не занят: {0}")]
    Bind(#[from] std::io::Error),
    #[error("база наблюдений не открыта: {0}")]
    Store(#[from] autosre_store::StoreError),
    #[error("источник не собран: {0}")]
    Source(#[from] autosre_source::SourceError),
    #[error("шаблон ошибок некорректен: {0}")]
    Filter(#[from] autosre_logs::FilterError),
    #[error("адрес источника некорректен: {0}")]
    Address(#[from] url::ParseError),
    #[error("модель не подключена: {0}")]
    Model(#[from] autosre_model::ModelError),
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
    let sources = sources(&config)?;
    let depth = config.file.depth();
    tracing::info!(minutes = depth.as_secs() / 60, "дозапрос истории на старте");
    collector::collect(
        sources.clone(),
        &store,
        &metrics,
        &config.file.collector,
        depth,
    );
    collector::tidy(&sources, &store, &config.file.retention);
    watcher::watch(&sources, &store, &metrics, &config.file.enabled());
    let talker = autosre_model::Model::new(autosre_model::Settings {
        url: config.file.model.url.parse()?,
        key: config.secrets.model.clone(),
        name: config.file.model.name.clone(),
        temperature: config.file.model.temperature,
        tokens: config.file.model.max_tokens,
        timeout: config.file.model.timeout,
    })?
    .retrying(&PAUSES);
    let model = Arc::new(if config.file.corpus.collect {
        tracing::info!(
            raw = config.file.corpus.raw,
            "сбор живых данных включён: каждый заход в модель ложится в корпус"
        );
        talker.recording(Arc::new(scribe::Scribe))
    } else {
        talker
    });
    grouper::group(&sources, &store, &metrics, &model, &config.file.incidents);

    librarian::learn(&store, &metrics, &config.file.knowledge).await;
    librarian::keep(&store, &metrics, &config.file.knowledge);

    let skills = autosre_skills::read(&config.file.digging.skills).unwrap_or_else(|failure| {
        tracing::warn!(
            path = %config.file.digging.skills.display(),
            %failure,
            "скиллы не прочитаны: разбирать будет нечем"
        );
        Vec::new()
    });
    tracing::info!(skills = skills.len(), "скиллы загружены");
    digger::dig(digger::Digger::new(
        &sources,
        &store,
        &metrics,
        &model,
        skills,
        &digger::Recipe {
            digging: &config.file.digging,
            incidents: &config.file.incidents,
            knowledge: &config.file.knowledge,
        },
    ));

    let scribe = reporter::Reporter::new(
        &store,
        &metrics,
        &model,
        &config.file.knowledge,
        &config.file.incidents,
    );
    reporter::report(scribe.clone());

    let doorman = Doorman::new(config.file.accounts.clone(), &config.secrets.session);
    if doorman.empty() {
        tracing::warn!("учётных записей нет: в веб-морду не войти никому");
    }
    let shared = web::Shared::new(
        metrics,
        store.clone(),
        doorman,
        &config.file.knowledge,
        VERSION,
    )
    .writing(scribe)
    .collecting(&config.file.corpus)
    .grouping(&config.file.incidents);
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

/// Источники наблюдений: логи и метрики.
///
/// Оба за одной границей ([ADR-0004](../../../docs/adr/0004-connectors-as-features.md)),
/// поэтому дальше по коду они неразличимы.
fn sources(config: &Config) -> Result<Vec<Arc<dyn Source>>, Failure> {
    let logs: Arc<dyn Source> = Arc::new(
        Logs::new(
            &autosre_logs::Settings {
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
        )?
        .retrying(&PAUSES),
    );
    let numbers: Arc<dyn Source> = Arc::new(
        autosre_metrics::Metrics::new(&autosre_metrics::Settings {
            url: config.file.metrics.url.parse()?,
            timeout: config.file.metrics.timeout,
            select: config.file.metrics.select.clone(),
            labels: config.file.incidents.service_labels.clone(),
        })?
        .retrying(&PAUSES),
    );
    Ok(vec![logs, numbers])
}

/// Ждёт сигнала остановки, чтобы дорисовать текущие запросы.
async fn stop() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("остановка по сигналу");
}
