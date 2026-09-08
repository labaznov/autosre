//! Оснастка: источники и модель, собранные из настроек.
//!
//! Одно место на бинарь и на проверку `check`: собирать клиентов дважды по
//! одним и тем же настройкам значит однажды собрать их по-разному.

use std::sync::Arc;
use std::time::Duration;

use autosre_logs::{Filter, Logs};
use autosre_model::Model;
use autosre_source::Source;

use crate::config::Config;

/// Паузы между попытками после обрыва связи или занятого шлюза: две попытки
/// сверх первой. Дольше ждать незачем — минуту спустя съём придёт снова.
const PAUSES: [Duration; 2] = [Duration::from_secs(2), Duration::from_secs(5)];

/// Предел строк в ответе источника логов на один запрос.
const ROWS: usize = 2000;

/// Отказы сборки оснастки.
#[derive(Debug, thiserror::Error)]
pub enum RigError {
    #[error("источник не собран: {0}")]
    Source(#[from] autosre_source::SourceError),
    #[error("шаблон ошибок некорректен: {0}")]
    Filter(#[from] autosre_logs::FilterError),
    #[error("адрес некорректен: {0}")]
    Address(#[from] url::ParseError),
    #[error("модель не подключена: {0}")]
    Model(#[from] autosre_model::ModelError),
}

/// Источники наблюдений: логи и метрики.
///
/// Оба за одной границей ([ADR-0004](../../../docs/adr/0004-connectors-as-features.md)),
/// поэтому дальше по коду они неразличимы.
///
/// # Errors
/// [`RigError`] на неразбираемом адресе или шаблоне ошибок.
pub fn sources(config: &Config) -> Result<Vec<Arc<dyn Source>>, RigError> {
    let logs: Arc<dyn Source> = Arc::new(
        Logs::new(
            &autosre_logs::Settings {
                url: config.file.logs.url.parse()?,
                username: config.file.logs.username.clone(),
                password: config.secrets.logs_password.clone(),
                timeout: config.file.logs.timeout,
                rows: ROWS,
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

/// Клиент модели.
///
/// # Errors
/// [`RigError`] на неразбираемом адресе.
pub fn model(config: &Config) -> Result<Model, RigError> {
    Ok(Model::new(autosre_model::Settings {
        url: config.file.model.url.parse()?,
        key: config.secrets.model.clone(),
        name: config.file.model.name.clone(),
        temperature: config.file.model.temperature,
        tokens: config.file.model.max_tokens,
        timeout: config.file.model.timeout,
    })?
    .retrying(&PAUSES))
}
