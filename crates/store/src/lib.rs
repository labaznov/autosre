//! Хранилище ряда наблюдений (`SQLite`).
//!
//! Ряд накапливается у нас: горизонт считается поверх него, а не запросами к
//! источнику ([ADR-0013](../../../docs/adr/0013-minute-buckets.md)). Все
//! обращения к базе уходят в блокирующий пул `tokio` — синхронный драйвер в
//! обработчике останавливает цикл событий целиком.

pub mod schema;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use rusqlite::{Connection, OptionalExtension, params};
use sre_domain::{
    Bucket, Deviation, Hour, Incident, Minute, Service, Signature, Span, State, Stream,
};
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
        migrate(&connection)?;
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
                    "INSERT INTO buckets (source, stream, at, value, kind) VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT (source, stream, at)
                     DO UPDATE SET value = excluded.value, kind = excluded.kind",
                )?;
                let mut minute =
                    change.prepare("INSERT OR IGNORE INTO minutes (source, at) VALUES (?1, ?2)")?;
                for it in &buckets {
                    bucket.execute(params![
                        source,
                        it.stream.as_str(),
                        it.minute.stamp(),
                        it.value,
                        it.kind.code()
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

    /// Окна горизонта по всем потокам источника, свежие первыми.
    ///
    /// Один запрос на источник, а не на поток: двести сервисов — это двести
    /// потоков, и запрос на каждый вернул бы нас к тому, от чего ушли.
    ///
    /// Окно 0 — последнее, оканчивающееся в `end`; окно `k` отстоит на `k`
    /// ширин назад. Минуты без бакетов считаются нулями: их отсутствие в ряду
    /// означает тишину, а не незнание, потому что дыры закрывает дозапрос.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn windows(
        &self,
        source: &str,
        end: Minute,
        width: usize,
        count: usize,
    ) -> Result<BTreeMap<Stream, Vec<f64>>, StoreError> {
        let source = source.to_owned();
        let seconds = i64::try_from(width).unwrap_or(15) * 60;
        let start = end.stamp() - seconds * i64::try_from(count).unwrap_or(24);
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT stream, (?4 - at - 1) / ?5 AS window, SUM(value)
                   FROM buckets
                  WHERE source = ?1 AND at >= ?2 AND at < ?3
                  GROUP BY stream, window",
            )?;
            let rows = query.query_map(
                params![source, start, end.stamp(), end.stamp(), seconds],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, f64>(2)?,
                    ))
                },
            )?;
            let mut series: BTreeMap<Stream, Vec<f64>> = BTreeMap::new();
            for row in rows {
                let (stream, index, value) = row?;
                let line = series
                    .entry(Stream::new(stream))
                    .or_insert_with(|| vec![0.0; count]);
                if let Ok(index) = usize::try_from(index)
                    && index < count
                {
                    line[index] = value;
                }
            }
            Ok(series)
        })
        .await
    }

    /// Сколько минут каждого окна действительно снималось.
    ///
    /// Отсутствие бакета и отсутствие данных — разные вещи. Без этой проверки
    /// пустая история читается как «ошибок не было», медиана выходит нулевой, и
    /// после каждого запуска агент находит отклонение в любом живом сервисе.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn covered(
        &self,
        source: &str,
        end: Minute,
        width: usize,
        count: usize,
    ) -> Result<Vec<usize>, StoreError> {
        let source = source.to_owned();
        let seconds = i64::try_from(width).unwrap_or(15) * 60;
        let start = end.stamp() - seconds * i64::try_from(count).unwrap_or(24);
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT (?4 - at - 1) / ?5 AS window, COUNT(*)
                   FROM minutes
                  WHERE source = ?1 AND at >= ?2 AND at < ?3
                  GROUP BY window",
            )?;
            let rows = query.query_map(
                params![source, start, end.stamp(), end.stamp(), seconds],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?;
            let mut known = vec![0; count];
            for row in rows {
                let (index, minutes) = row?;
                if let Ok(index) = usize::try_from(index)
                    && index < count
                {
                    known[index] = usize::try_from(minutes).unwrap_or(0);
                }
            }
            Ok(known)
        })
        .await
    }

    /// Записывает отклонение; повторная оценка того же окна ничего не меняет.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn spot(&self, deviation: &Deviation, found: Minute) -> Result<bool, StoreError> {
        let deviation = deviation.clone();
        self.work(move |db| {
            Ok(db.execute(
                "INSERT OR IGNORE INTO deviations
                   (source, stream, horizon, at, value, baseline, score, weight, found)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    deviation.source,
                    deviation.stream.as_str(),
                    deviation.horizon,
                    deviation.at.stamp(),
                    deviation.value,
                    deviation.baseline,
                    deviation.score.is_finite().then_some(deviation.score),
                    deviation.weight,
                    found.stamp()
                ],
            )? > 0)
        })
        .await
    }

    /// Отклонения, самые тяжёлые первыми.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn deviations(&self, limit: usize) -> Result<Vec<Deviation>, StoreError> {
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT source, stream, horizon, at, value, baseline, score, weight
                   FROM deviations ORDER BY weight DESC, id DESC LIMIT ?1",
            )?;
            let rows = query.query_map(params![limit], |row| {
                Ok(Deviation {
                    source: row.get(0)?,
                    stream: Stream::new(row.get::<_, String>(1)?),
                    horizon: row.get(2)?,
                    at: Minute::at(row.get(3)?),
                    value: row.get(4)?,
                    baseline: row.get(5)?,
                    score: row.get::<_, Option<f64>>(6)?.unwrap_or(f64::INFINITY),
                    weight: row.get(7)?,
                })
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Отклонения, к которым ещё не привязан инцидент, самые тяжёлые первыми.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn loose(&self, limit: usize) -> Result<Vec<(i64, Deviation)>, StoreError> {
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT id, source, stream, horizon, at, value, baseline, score, weight
                   FROM deviations WHERE incident IS NULL
                  ORDER BY weight DESC, id ASC LIMIT ?1",
            )?;
            let rows = query.query_map(params![limit], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    Deviation {
                        source: row.get(1)?,
                        stream: Stream::new(row.get::<_, String>(2)?),
                        horizon: row.get(3)?,
                        at: Minute::at(row.get(4)?),
                        value: row.get(5)?,
                        baseline: row.get(6)?,
                        score: row.get::<_, Option<f64>>(7)?.unwrap_or(f64::INFINITY),
                        weight: row.get(8)?,
                    },
                ))
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Привязывает отклонение к инциденту: открывает новый или продлевает
    /// открытый с той же парой «сервис плюс сигнатура».
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn attach(
        &self,
        deviation: i64,
        service: &Service,
        signature: &Signature,
        found: &Deviation,
    ) -> Result<(i64, bool), StoreError> {
        let service = service.as_str().to_owned();
        let signature = signature.as_str().to_owned();
        let found = found.clone();
        self.work(move |db| {
            let change = db.unchecked_transaction()?;
            let open = change
                .query_row(
                    "SELECT id FROM incidents
                      WHERE service = ?1 AND signature = ?2 AND state = 'open'",
                    params![service, signature],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            let (id, fresh) = if let Some(id) = open {
                change.execute(
                    "UPDATE incidents
                        SET last = MAX(last, ?2), seen = seen + 1,
                            peak = MAX(peak, ?3), weight = MAX(weight, ?4)
                      WHERE id = ?1",
                    params![id, found.at.stamp(), found.value, found.weight],
                )?;
                (id, false)
            } else {
                change.execute(
                    "INSERT INTO incidents
                       (service, signature, stream, source, state, began, last,
                        seen, peak, weight)
                     VALUES (?1, ?2, ?3, ?4, 'open', ?5, ?5, 1, ?6, ?7)",
                    params![
                        service,
                        signature,
                        found.stream.as_str(),
                        found.source,
                        found.at.stamp(),
                        found.value,
                        found.weight
                    ],
                )?;
                (change.last_insert_rowid(), true)
            };
            change.execute(
                "UPDATE deviations SET incident = ?2 WHERE id = ?1",
                params![deviation, id],
            )?;
            change.commit()?;
            Ok((id, fresh))
        })
        .await
    }

    /// Закрывает инциденты, о которых давно нет вестей.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn hush(&self, before: Minute) -> Result<usize, StoreError> {
        self.work(move |db| {
            Ok(db.execute(
                "UPDATE incidents SET state = 'closed' WHERE state = 'open' AND last < ?1",
                params![before.stamp()],
            )?)
        })
        .await
    }

    /// Отмечает связь между инцидентами, начавшимися рядом во времени.
    ///
    /// Связь взаимна и хранится обеими сторонами: карточка любого из них
    /// должна показывать соседа, а не только тот, кто пришёл вторым.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn link(&self, incident: i64, apart: i64) -> Result<usize, StoreError> {
        self.work(move |db| {
            let change = db.unchecked_transaction()?;
            let near = change
                .prepare(
                    "SELECT id FROM incidents
                      WHERE id <> ?1 AND state = 'open'
                        AND ABS(began - (SELECT began FROM incidents WHERE id = ?1)) <= ?2",
                )?
                .query_map(params![incident, apart], |row| row.get::<_, i64>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            let mut tie = change
                .prepare("INSERT OR IGNORE INTO links (incident, related) VALUES (?1, ?2)")?;
            for other in &near {
                tie.execute(params![incident, other])?;
                tie.execute(params![other, incident])?;
            }
            drop(tie);
            change.commit()?;
            Ok(near.len())
        })
        .await
    }

    /// Инциденты, свежие сверху.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn incidents(&self, open: bool, limit: usize) -> Result<Vec<Incident>, StoreError> {
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT id, service, signature, stream, source, state, began, last,
                        seen, peak, weight, verdict
                   FROM incidents
                  WHERE (?1 = 0 OR state = 'open')
                  ORDER BY last DESC, id DESC LIMIT ?2",
            )?;
            let rows = query.query_map(params![i64::from(open), limit], |row| {
                Ok(Incident {
                    id: row.get(0)?,
                    service: Service::new(row.get::<_, String>(1)?),
                    signature: Signature::stored(row.get::<_, String>(2)?),
                    stream: Stream::new(row.get::<_, String>(3)?),
                    source: row.get(4)?,
                    state: State::of(&row.get::<_, String>(5)?),
                    began: Minute::at(row.get(6)?),
                    last: Minute::at(row.get(7)?),
                    seen: row.get(8)?,
                    peak: row.get(9)?,
                    weight: row.get(10)?,
                    verdict: row.get::<_, Option<i64>>(11)?.map(|it| it == 1),
                })
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Связанные инциденты.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn related(&self, incident: i64) -> Result<Vec<i64>, StoreError> {
        self.work(move |db| {
            let mut query =
                db.prepare("SELECT related FROM links WHERE incident = ?1 ORDER BY related")?;
            let rows = query.query_map(params![incident], |row| row.get::<_, i64>(0))?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Сворачивает один час минутных бакетов в часовые.
    ///
    /// По одному часу за вызов, а не всё разом: свёртка недели держала бы
    /// единственное соединение так долго, что съём минут встал бы, и агент
    /// ослеп бы ровно на время уборки.
    ///
    /// Счётчики складываются, уровни усредняются — как сказал источник
    /// (`Kind`), а не как удобно хранилищу.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе базы.
    pub async fn roll(&self, source: &str, before: Minute) -> Result<Rolled, StoreError> {
        let source = source.to_owned();
        self.work(move |db| {
            let Some(oldest) = db
                .query_row(
                    "SELECT MIN(at) FROM buckets WHERE source = ?1 AND at < ?2",
                    params![source, before.stamp()],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .optional()?
                .flatten()
            else {
                return Ok(Rolled::done());
            };
            let hour = Hour::at(oldest);
            let span = hour.span();
            let change = db.unchecked_transaction()?;
            let rolled = change.execute(
                "INSERT INTO hours (source, stream, at, value, kind)
                 SELECT ?1, stream, ?2,
                        CASE kind WHEN 1 THEN AVG(value) ELSE SUM(value) END,
                        kind
                   FROM buckets
                  WHERE source = ?1 AND at >= ?3 AND at < ?4
                  GROUP BY stream, kind
                 ON CONFLICT (source, stream, at)
                 DO UPDATE SET value = excluded.value, kind = excluded.kind",
                params![source, hour.stamp(), span.from().stamp(), span.to().stamp()],
            )?;
            change.execute(
                "DELETE FROM buckets WHERE source = ?1 AND at >= ?2 AND at < ?3",
                params![source, span.from().stamp(), span.to().stamp()],
            )?;
            change.commit()?;
            Ok(Rolled {
                hour: Some(hour),
                streams: rolled,
                more: true,
            })
        })
        .await
    }

    /// Забывает то, чей срок вышел.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе базы.
    pub async fn forget(
        &self,
        source: &str,
        minutes: Minute,
        hours: Hour,
    ) -> Result<usize, StoreError> {
        let source = source.to_owned();
        self.work(move |db| {
            let change = db.unchecked_transaction()?;
            let mut gone = change.execute(
                "DELETE FROM buckets WHERE source = ?1 AND at < ?2",
                params![source, minutes.stamp()],
            )?;
            gone += change.execute(
                "DELETE FROM minutes WHERE source = ?1 AND at < ?2",
                params![source, minutes.stamp()],
            )?;
            gone += change.execute(
                "DELETE FROM hours WHERE source = ?1 AND at < ?2",
                params![source, hours.stamp()],
            )?;
            change.commit()?;
            Ok(gone)
        })
        .await
    }

    /// Значение свёрнутого часа, если оно есть.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn hour(
        &self,
        source: &str,
        stream: &Stream,
        hour: Hour,
    ) -> Result<Option<f64>, StoreError> {
        let source = source.to_owned();
        let stream = stream.as_str().to_owned();
        self.work(move |db| {
            Ok(db
                .query_row(
                    "SELECT value FROM hours WHERE source = ?1 AND stream = ?2 AND at = ?3",
                    params![source, stream, hour.stamp()],
                    |row| row.get::<_, f64>(0),
                )
                .optional()?)
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

/// Чем кончилась свёртка одного часа.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rolled {
    /// Свёрнутый час; `None`, если сворачивать было нечего.
    pub hour: Option<Hour>,
    /// Сколько потоков свёрнуто.
    pub streams: usize,
    /// Осталось ли что-то ещё: свёртка идёт по часу за вызов.
    pub more: bool,
}

impl Rolled {
    #[must_use]
    fn done() -> Self {
        Self {
            hour: None,
            streams: 0,
            more: false,
        }
    }
}

/// Применяет недостающие шаги схемы.
fn migrate(connection: &Connection) -> Result<(), StoreError> {
    let applied: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let applied = usize::try_from(applied).unwrap_or(0);
    for (number, step) in schema::STEPS.iter().enumerate().skip(applied) {
        connection.execute_batch(step)?;
        connection.pragma_update(None, "user_version", number + 1)?;
        tracing::info!(step = number + 1, "схема базы обновлена");
    }
    Ok(())
}

/// Берёт соединение, восстанавливая его после паники другого потока: `SQLite`
/// откатывает незавершённую транзакцию сам, терять базу из-за этого незачем.
fn guard(connection: &Mutex<Connection>) -> MutexGuard<'_, Connection> {
    connection.lock().unwrap_or_else(PoisonError::into_inner)
}
