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
//! Метку к примеру ставит дежурный своей оценкой инцидента, поэтому урок
//! обязан знать инцидент. Знает его не модель и не писарь, а тот, кто
//! спрашивал, — и часто узнаёт **после** ответа: отсев зовут раньше, чем
//! инцидент заведён. Отсюда порядок: заходы копятся, а записываются, когда
//! номер известен.

use std::future::Future;
use std::sync::Mutex;

use chrono::Utc;
use sre_domain::Minute;
use sre_model::{Ledger, Told};
use sre_store::{Lesson, Store};

use crate::metrics::Metrics;

tokio::task_local! {
    /// Заходы в модель, сделанные внутри одной работы.
    static TOLD: Mutex<Vec<Told>>;
}

/// Выполняет работу, запоминая всё, что она сказала модели.
///
/// Ответ работы отдаётся как есть: сбор данных не имеет права ни менять его,
/// ни задерживать.
pub async fn watch<T>(work: impl Future<Output = T>) -> (T, Vec<Told>) {
    let slot = Mutex::new(Vec::new());
    TOLD.scope(slot, async {
        let done = work.await;
        let told = TOLD.with(|it| std::mem::take(&mut *guard(it)));
        (done, told)
    })
    .await
}

/// К чему относится урок: без этого его нечем разметить.
#[derive(Debug, Clone, Copy, Default)]
pub struct About {
    pub incident: Option<i64>,
    pub investigation: Option<i64>,
    pub deviation: Option<i64>,
}

/// Складывает заходы в корпус, пометив их тем, к чему они относятся.
pub async fn keep(store: &Store, metrics: &Metrics, told: Vec<Told>, about: About) {
    for one in told {
        let lesson = Lesson {
            kind: one.kind.to_owned(),
            model: one.model,
            incident: about.incident,
            investigation: about.investigation,
            deviation: about.deviation,
            system: one.system,
            ask: one.ask,
            answer: one.answer,
        };
        match store.learn(&lesson, Minute::of(Utc::now())).await {
            Ok(_) => metrics.taught(),
            Err(failure) => tracing::warn!(%failure, "урок не записан"),
        }
    }
}

/// Писарь: перехватывает заходы в модель и складывает их в текущую работу.
///
/// Ничего не хранит сам: где записывать и под каким номером — не его дело.
pub struct Scribe;

impl Ledger for Scribe {
    fn keep(&self, told: Told) {
        if TOLD.try_with(|slot| guard(slot).push(told)).is_err() {
            tracing::debug!("заход в модель вне наблюдаемой работы, в корпус не попал");
        }
    }
}

/// Берёт список заходов, восстанавливая его после паники другого потока:
/// терять корпус из-за одного сбоя незачем.
fn guard(slot: &Mutex<Vec<Told>>) -> std::sync::MutexGuard<'_, Vec<Told>> {
    slot.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
