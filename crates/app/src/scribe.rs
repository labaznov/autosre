//! Сбор живых данных: корпус для дообучения и подбора порогов.
//!
//! Агент и так помнит почти всё, чего требует калибровка: ряды, отклонения,
//! разборы, оценки дежурного. Не хранился только сам разговор с моделью — а он
//! и есть материал для дообучения ([ADR-0027](../../../docs/adr/0027-live-corpus.md)).
//!
//! Собирается **отправленное**, а не собранное заново: промпты меняются вместе
//! с агентом, и корпус, восстановленный по нынешним шаблонам из прошлогодних
//! данных, учит модель тому, чего никогда не происходило.
//!
//! Метку к примеру ставит не агент, а дежурный — своей оценкой инцидента.
//! Поэтому урок держит номер инцидента: без него это переписка с моделью, а с
//! ним — пример с ответом.

use std::future::Future;
use std::sync::Arc;

use chrono::Utc;
use sre_domain::Minute;
use sre_model::{Ledger, Told};
use sre_store::{Lesson, Store};

use crate::metrics::Metrics;

tokio::task_local! {
    /// Инцидент и расследование, ради которых сейчас спрашивают модель.
    ///
    /// Модель про них не знает и знать не должна: она отдаёт то, что отправила.
    /// Связь ставит тот, кто спрашивает, и здесь она живёт ровно на время
    /// одного захода.
    static ABOUT: (Option<i64>, Option<i64>);
}

/// Выполняет работу, пометив её инцидентом и расследованием.
pub async fn about<T>(
    incident: Option<i64>,
    investigation: Option<i64>,
    work: impl Future<Output = T>,
) -> T {
    ABOUT.scope((incident, investigation), work).await
}

/// Писарь: складывает заходы в модель в базу.
pub struct Scribe {
    store: Store,
    metrics: Arc<Metrics>,
}

impl Scribe {
    #[must_use]
    pub fn new(store: &Store, metrics: &Arc<Metrics>) -> Self {
        Self {
            store: store.clone(),
            metrics: Arc::clone(metrics),
        }
    }
}

impl Ledger for Scribe {
    fn keep(&self, told: Told) {
        let (incident, investigation) = ABOUT.try_with(|it| *it).unwrap_or((None, None));
        let (store, metrics) = (self.store.clone(), Arc::clone(&self.metrics));
        // Запись урока не должна задерживать разбор: она не нужна ни для
        // вывода, ни для карточки, и падать из-за неё тем более незачем.
        tokio::spawn(async move {
            let lesson = Lesson {
                kind: told.kind.to_owned(),
                model: told.model,
                incident,
                investigation,
                system: told.system,
                ask: told.ask,
                answer: told.answer,
            };
            match store.learn(&lesson, Minute::of(Utc::now())).await {
                Ok(_) => metrics.taught(),
                Err(failure) => tracing::warn!(%failure, "урок не записан"),
            }
        });
    }
}
