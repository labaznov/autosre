//! Проверка до старта: `autosre check <файл>`.
//!
//! На сервере без сети первая установка — это чтение журнала: агент поднялся,
//! что-то не так, и понять что — значит перебирать строки. Проверка делает
//! то же самое до старта и говорит таблицей: настройки, база, знания, логи,
//! метрики, модель — и что именно у каждого не так.
//!
//! Ничего не заводит и ничего не пишет, кроме схемы базы: открыть базу без
//! приведения схемы нельзя, а приведение обратимо
//! ([ADR-0023](../../../docs/adr/0023-deploy-with-downtime.md)).

use autosre_domain::{Minute, Span};
use autosre_store::Store;
use chrono::Utc;

use crate::config::Config;
use crate::rig;

/// Итог одной проверки: что проверяли и чем кончилось.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    pub what: &'static str,
    /// Что увидели — или почему не вышло.
    pub outcome: Result<String, String>,
}

impl Check {
    fn of(what: &'static str, outcome: Result<String, String>) -> Self {
        Self { what, outcome }
    }
}

/// Все проверки по порядку: сначала то, что не требует сети.
pub async fn run(config: &Config) -> Vec<Check> {
    let mut all = vec![settings(config), base(config), knowledge(config)];
    all.extend(sources(config).await);
    all.push(model(config).await);
    all
}

/// Сколько проверок не прошло.
#[must_use]
pub fn failed(checks: &[Check]) -> usize {
    checks.iter().filter(|it| it.outcome.is_err()).count()
}

/// Таблица для человека: галочка или крест, имя, что увидели.
#[must_use]
pub fn table(checks: &[Check]) -> String {
    checks
        .iter()
        .map(|check| match &check.outcome {
            Ok(seen) => format!("  ✓ {:<10} {seen}", check.what),
            Err(why) => format!("  ✗ {:<10} {why}", check.what),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn settings(config: &Config) -> Check {
    let horizons = config.file.enabled().len();
    let accounts = config.file.accounts.len();
    let seen = if config.unknown.is_empty() {
        format!("горизонтов {horizons}, учётных записей {accounts}")
    } else {
        format!(
            "горизонтов {horizons}, учётных записей {accounts}; неизвестные поля пропущены: {}",
            config.unknown.join(", ")
        )
    };
    if accounts == 0 {
        return Check::of(
            "настройки",
            Err(format!("{seen}; без учётной записи в веб-морду не войти")),
        );
    }
    Check::of("настройки", Ok(seen))
}

fn base(config: &Config) -> Check {
    Check::of(
        "база",
        Store::open(&config.file.database)
            .map(|_| {
                format!(
                    "{} открыта, схема в порядке",
                    config.file.database.display()
                )
            })
            .map_err(|failure| format!("{}: {failure}", config.file.database.display())),
    )
}

fn knowledge(config: &Config) -> Check {
    let skills = match autosre_skills::read(&config.file.digging.skills) {
        Ok(skills) => skills,
        Err(failure) => {
            return Check::of(
                "знания",
                Err(format!(
                    "{}: {failure}",
                    config.file.digging.skills.display()
                )),
            );
        }
    };
    let notes = config.file.knowledge.notes.is_dir();
    let drafts = config.file.knowledge.drafts.is_dir();
    if !notes || !drafts {
        return Check::of(
            "знания",
            Err(format!(
                "скиллов {}; каталога {} нет",
                skills.len(),
                if notes {
                    config.file.knowledge.drafts.display()
                } else {
                    config.file.knowledge.notes.display()
                }
            )),
        );
    }
    if skills.is_empty() {
        return Check::of(
            "знания",
            Err(format!(
                "в {} ни одного скилла: разбирать будет нечем",
                config.file.digging.skills.display()
            )),
        );
    }
    Check::of(
        "знания",
        Ok(format!("скиллов {}, каталоги на месте", skills.len())),
    )
}

/// Логи и метрики: по одному запросу за последнюю закрытую минуту.
async fn sources(config: &Config) -> Vec<Check> {
    let sources = match rig::sources(config) {
        Ok(sources) => sources,
        Err(failure) => return vec![Check::of("источники", Err(failure.to_string()))],
    };
    let minute = Span::single(Minute::of(Utc::now()).previous());
    let mut checks = Vec::new();
    for source in sources {
        let what: &'static str = match source.name() {
            "logs" => "логи",
            _ => "метрики",
        };
        checks.push(Check::of(
            what,
            source
                .buckets(minute)
                .await
                .map(|buckets| format!("отвечают, потоков за минуту: {}", buckets.len()))
                .map_err(|failure| failure.to_string()),
        ));
    }
    checks
}

/// Модель: один короткий заход со схемой ответа — так проверяется и связь,
/// и то, что сервер понимает `response_format`.
async fn model(config: &Config) -> Check {
    let model = match rig::model(config) {
        Ok(model) => model,
        Err(failure) => return Check::of("модель", Err(failure.to_string())),
    };
    Check::of(
        "модель",
        model
            .summary("Проверка связи. Инцидентов 0, отклонений 0, отсеяно 0.")
            .await
            .map(|answer| {
                format!(
                    "{} отвечает по схеме: «{}»",
                    model.name(),
                    answer.chars().take(60).collect::<String>()
                )
            })
            .map_err(|failure| failure.to_string()),
    )
}
