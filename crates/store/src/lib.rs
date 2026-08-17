//! Хранилище ряда наблюдений (`SQLite`).
//!
//! Ряд накапливается у нас: горизонт считается поверх него, а не запросами к
//! источнику ([ADR-0013](../../../docs/adr/0013-minute-buckets.md)). Все
//! обращения к базе уходят в блокирующий пул `tokio` — синхронный драйвер в
//! обработчике останавливает цикл событий целиком.

pub mod schema;

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use rusqlite::{Connection, OptionalExtension, params};
use sre_domain::{Bucket, Minute, Span, Stream};
use tokio::task;

/// Предел ожидания на заблокированной базе.
const BUSY_TIMEOUT: &str = "5000";

/// Отказы хранилища.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("база недоступна: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("каталог базы не создан: {0}")]
    Directory(#[from] std::io::Error),
    #[error("задача хранилища не завершилась: {0}")]
    Task(#[from] task::JoinError),
}

/// База наблюдений.
#[derive(Debug, Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
}

impl Store {
    /// Открывает базу, создавая каталог и таблицы при необходимости.
    ///
    /// # Errors
    /// [`StoreError::Directory`] если каталог не создаётся, [`StoreError::Sqlite`]
    /// на отказе базы.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        if let Some(directory) = path.parent() {
            std::fs::create_dir_all(directory)?;
        }
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "busy_timeout", BUSY_TIMEOUT)?;
        connection.execute_batch(schema::SCHEMA)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Сохраняет снятые бакеты и отмечает минуты промежутка снятыми.
    ///
    /// Одной транзакцией: отметка «минута снята» появляется только вместе с её
    /// содержимым, иначе после падения посреди записи в ряду будет дыра,
    /// которую никто не считает дырой.
    ///
    /// Повторное снятие той же минуты не удваивает счётчики — значение
    /// замещается.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn save(
        &self,
        source: &str,
        span: Span,
        buckets: Vec<Bucket>,
    ) -> Result<usize, StoreError> {
        let source = source.to_owned();
        self.work(move |db| {
            let saved = buckets.len();
            let change = db.unchecked_transaction()?;
            {
                let mut bucket = change.prepare(
                    "INSERT INTO buckets (source, stream, at, value) VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (source, stream, at) DO UPDATE SET value = excluded.value",
                )?;
                let mut minute =
                    change.prepare("INSERT OR IGNORE INTO minutes (source, at) VALUES (?1, ?2)")?;
                for it in &buckets {
                    bucket.execute(params![
                        source,
                        it.stream.as_str(),
                        it.minute.stamp(),
                        it.value
                    ])?;
                }
                for it in span.minutes() {
                    minute.execute(params![source, it.stamp()])?;
                }
            }
            change.commit()?;
            Ok(saved)
        })
        .await
    }

    /// Последняя снятая минута источника.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn snapped(&self, source: &str) -> Result<Option<Minute>, StoreError> {
        let source = source.to_owned();
        self.work(move |db| {
            Ok(db
                .query_row(
                    "SELECT MAX(at) FROM minutes WHERE source = ?1",
                    params![source],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .optional()?
                .flatten()
                .map(Minute::at))
        })
        .await
    }

    /// Минуты промежутка, которые ещё не снимались.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn gaps(&self, source: &str, span: Span) -> Result<Vec<Minute>, StoreError> {
        let source = source.to_owned();
        self.work(move |db| {
            let mut known =
                db.prepare("SELECT at FROM minutes WHERE source = ?1 AND at >= ?2 AND at < ?3")?;
            let taken = known
                .query_map(
                    params![source, span.from().stamp(), span.to().stamp()],
                    |row| row.get::<_, i64>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(span
                .minutes()
                .into_iter()
                .filter(|minute| !taken.contains(&minute.stamp()))
                .collect())
        })
        .await
    }

    /// Значение бакета, если оно есть.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn value(
        &self,
        source: &str,
        stream: &Stream,
        minute: Minute,
    ) -> Result<Option<f64>, StoreError> {
        let source = source.to_owned();
        let stream = stream.as_str().to_owned();
        self.work(move |db| {
            Ok(db
                .query_row(
                    "SELECT value FROM buckets WHERE source = ?1 AND stream = ?2 AND at = ?3",
                    params![source, stream, minute.stamp()],
                    |row| row.get::<_, f64>(0),
                )
                .optional()?)
        })
        .await
    }

    /// Сколько бакетов сохранено источником.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn count(&self, source: &str) -> Result<u64, StoreError> {
        let source = source.to_owned();
        self.work(move |db| {
            Ok(db.query_row(
                "SELECT COUNT(*) FROM buckets WHERE source = ?1",
                params![source],
                |row| row.get::<_, u64>(0),
            )?)
        })
        .await
    }

    /// Уносит работу с базой в блокирующий пул.
    async fn work<T, F>(&self, job: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        task::spawn_blocking(move || job(&guard(&connection))).await?
    }
}

/// Берёт соединение, восстанавливая его после паники другого потока: `SQLite`
/// откатывает незавершённую транзакцию сам, терять базу из-за этого незачем.
fn guard(connection: &Mutex<Connection>) -> MutexGuard<'_, Connection> {
    connection.lock().unwrap_or_else(PoisonError::into_inner)
}
