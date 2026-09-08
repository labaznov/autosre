//! Агент диагностики: смотрит логи и метрики, находит отклонения, расследует их.

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use autosre_app::config::{Config, Process};
use autosre_app::metrics::Metrics;
use autosre_app::session::Doorman;
use autosre_app::{
    VERSION, collector, digger, grouper, librarian, reporter, rig, scribe, tls, watcher, web,
};
use autosre_store::Store;
use tracing_subscriber::EnvFilter;

/// Путь к файлу настроек по умолчанию.
const CONFIG: &str = "/etc/autosre/autosre.toml";

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let words: Vec<&str> = args.iter().map(String::as_str).collect();
    // Разовые команды печатают ответ сами; журнал им нужен только для бед.
    let quiet = matches!(words.first(), Some(&("check" | "backup" | "hash")));
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("AUTOSRE_LOG")
                .unwrap_or_else(|_| if quiet { "warn" } else { "info" }.into()),
        )
        .init();
    match words.as_slice() {
        ["--version" | "-V" | "version"] => {
            println!("{}", autosre_app::version());
            ExitCode::SUCCESS
        }
        ["hash", password] => {
            let Ok(hash) = hash(password) else {
                tracing::error!("пароль не захеширован");
                return ExitCode::FAILURE;
            };
            println!("{hash}");
            ExitCode::SUCCESS
        }
        ["check", rest @ ..] => check(rest.first().map_or(CONFIG, |it| it)).await,
        ["backup", target, rest @ ..] => backup(target, rest.first().map_or(CONFIG, |it| it)).await,
        [] | [_] if words.first().is_none_or(|it| !it.starts_with('-')) => {
            match serve(words.first().map_or(CONFIG, |it| it)).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(failure) => {
                    tracing::error!(%failure, "агент не запущен");
                    ExitCode::FAILURE
                }
            }
        }
        _ => {
            eprintln!("{USAGE}");
            ExitCode::FAILURE
        }
    }
}

/// Подсказка по командам: печатается, когда команда не разобрана.
const USAGE: &str = "autosre [файл настроек]          запустить агента
autosre check [файл настроек]    проверить настройки и связь, не запуская
autosre backup <куда> [файл]     снять копию базы на живом агенте
autosre hash <пароль>            хеш пароля для учётной записи
autosre --version                версия и коммит";

/// Проверка до старта: настройки, база, знания, источники, модель.
async fn check(path: &str) -> ExitCode {
    let config = match Config::read(Path::new(path), &Process) {
        Ok(config) => config,
        Err(failure) => {
            println!("  ✗ настройки  {failure}");
            return ExitCode::FAILURE;
        }
    };
    let checks = autosre_app::check::run(&config).await;
    println!("{}", autosre_app::check::table(&checks));
    match autosre_app::check::failed(&checks) {
        0 => {
            println!("всё на месте, можно запускать");
            ExitCode::SUCCESS
        }
        failed => {
            println!("не прошло проверок: {failed}");
            ExitCode::FAILURE
        }
    }
}

/// Копия базы средствами `SQLite`: годится на живом агенте.
async fn backup(target: &str, path: &str) -> ExitCode {
    let done = async {
        let config = Config::read(Path::new(path), &Process)?;
        let store = Store::open(&config.file.database)?;
        store.backup(Path::new(target)).await?;
        Ok::<_, Failure>(config.file.database)
    }
    .await;
    match done {
        Ok(source) => {
            println!("копия {} снята в {target}", source.display());
            ExitCode::SUCCESS
        }
        Err(failure) => {
            tracing::error!(%failure, "копия не снята");
            ExitCode::FAILURE
        }
    }
}

/// Хеш пароля в форме, которую понимает конфигурация.
///
/// Учётку без этого не завести: в конфигурации лежит хеш, а не пароль, и
/// считать его где-то на стороне — верный способ отправить пароль не туда.
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
    #[error(transparent)]
    Rig(#[from] autosre_app::rig::RigError),
    #[error("TLS не поднят: {0}")]
    Tls(#[from] autosre_app::tls::TlsError),
}

async fn serve(path: &str) -> Result<(), Failure> {
    let config = Config::read(Path::new(path), &Process)?;
    for key in &config.unknown {
        tracing::warn!(key, "настройка неизвестна агенту и пропущена");
    }

    let metrics = Arc::new(Metrics::new(VERSION));
    let store = Store::open(&config.file.database)?;
    let sources = rig::sources(&config)?;
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
    let talker = rig::model(&config)?;
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
    .grouping(&config.file.incidents)
    .securing(config.file.tls.enabled);
    tracing::info!(
        address = %config.file.bind,
        https = config.file.tls.enabled,
        horizons = config.file.enabled().len(),
        model = config.file.model.name,
        version = autosre_app::version(),
        "агент поднят"
    );
    listen(&config, shared).await
}

/// Веб-морда до сигнала остановки: по HTTPS, если TLS не выключен.
async fn listen(config: &Config, shared: web::Shared) -> Result<(), Failure> {
    let listener = tokio::net::TcpListener::bind(config.file.bind).await?;
    if config.file.tls.enabled {
        let (certificate, fresh) =
            tls::certificate(&config.file.tls.cert, &config.file.tls.key).await?;
        if fresh {
            tracing::warn!(
                cert = %config.file.tls.cert.display(),
                "сделан самоподписанный сертификат: браузер предупредит, подложите свой по тому же пути"
            );
        }
        tls::serve(
            listener.into_std()?,
            certificate,
            web::routes(shared),
            stop(),
        )
        .await?;
    } else {
        tracing::warn!(
            "TLS выключен: пароль дежурного идёт по сети открытым, снаружи нужен прокси с HTTPS"
        );
        axum::serve(listener, web::routes(shared))
            .with_graceful_shutdown(stop())
            .await?;
    }
    Ok(())
}

/// Ждёт сигнала остановки, чтобы дорисовать текущие запросы.
///
/// Слушает и SIGINT, и SIGTERM: первый шлёт терминал, второй — systemd и
/// Docker. Без второго остановка службы была бы убийством на месте, без
/// дорисовки запросов и без строки в журнале.
async fn stop() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("подписка на SIGTERM не удалась");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
    tracing::info!("остановка по сигналу");
}
