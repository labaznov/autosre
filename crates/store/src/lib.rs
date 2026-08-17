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
    Bucket, Deviation, Hour, Incident, Kind, Minute, Service, Signature, Span, State, Stream, Tally,
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

/// Вывод расследования в том виде, в каком его читают.
#[derive(Debug, Clone, PartialEq)]
pub struct Finding {
    pub id: i64,
    pub skill: String,
    pub state: String,
    pub cause: Option<String>,
    pub confidence: Option<f64>,
    pub advice: Option<String>,
    /// Заметка базы знаний, на которую опёрся вывод.
    pub note: Option<String>,
}

/// Заявка в том виде, в каком её читают.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asked {
    pub id: i64,
    pub incident: i64,
    /// Сервис инцидента: заявка без него — команда неизвестно про что.
    pub service: String,
    pub host: String,
    pub command: String,
    pub reason: String,
    pub state: String,
    pub asked: Minute,
    pub who: Option<String>,
    pub answer: Option<String>,
}

/// Заметка в том виде, в каком её принимает индекс.
///
/// Пять строк, и ни одного знания о том, откуда они взялись: хранилище не
/// читает файлов и не знает про фронтматтер.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Memory {
    pub name: String,
    pub title: String,
    pub tags: String,
    pub marks: String,
    pub body: String,
}

/// Черновик в том виде, в каком его читают.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    pub id: i64,
    pub incident: i64,
    pub name: String,
    pub title: String,
    pub path: String,
    pub state: String,
    pub written: Minute,
    pub who: Option<String>,
}

/// Найденная заметка.
#[derive(Debug, Clone, PartialEq)]
pub struct Recalled {
    pub name: String,
    pub title: String,
    pub body: String,
    /// Оценка BM25: чем меньше, тем ближе. Так считает `SQLite`.
    pub score: f64,
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
    /// Счётчики складываются, уровни усредняются — иначе «окно» уровня было бы
    /// пятнадцатикратной памятью, числом без смысла.
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
    ) -> Result<BTreeMap<Stream, (Kind, Vec<f64>)>, StoreError> {
        let source = source.to_owned();
        let seconds = i64::try_from(width).unwrap_or(15) * 60;
        let start = end.stamp() - seconds * i64::try_from(count).unwrap_or(24);
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT stream, (?4 - at - 1) / ?5 AS window, kind,
                        CASE kind WHEN 1 THEN AVG(value) ELSE SUM(value) END
                   FROM buckets
                  WHERE source = ?1 AND at >= ?2 AND at < ?3
                  GROUP BY stream, window, kind",
            )?;
            let rows = query.query_map(
                params![source, start, end.stamp(), end.stamp(), seconds],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, f64>(3)?,
                    ))
                },
            )?;
            let mut series: BTreeMap<Stream, (Kind, Vec<f64>)> = BTreeMap::new();
            for row in rows {
                let (stream, index, kind, value) = row?;
                let line = series
                    .entry(Stream::new(stream))
                    .or_insert_with(|| (Kind::of(kind), vec![0.0; count]));
                if let Ok(index) = usize::try_from(index)
                    && index < count
                {
                    line.1[index] = value;
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
                let value: f64 = row.get(4)?;
                let baseline: f64 = row.get(5)?;
                Ok(Deviation {
                    source: row.get(0)?,
                    stream: Stream::new(row.get::<_, String>(1)?),
                    horizon: row.get(2)?,
                    at: Minute::at(row.get(3)?),
                    value,
                    baseline,
                    score: row
                        .get::<_, Option<f64>>(6)?
                        .unwrap_or_else(|| endless(value, baseline)),
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
                   FROM deviations WHERE incident IS NULL AND sifted IS NULL
                  ORDER BY weight DESC, id ASC LIMIT ?1",
            )?;
            let rows = query.query_map(params![limit], |row| {
                let value: f64 = row.get(5)?;
                let baseline: f64 = row.get(6)?;
                Ok((
                    row.get::<_, i64>(0)?,
                    Deviation {
                        source: row.get(1)?,
                        stream: Stream::new(row.get::<_, String>(2)?),
                        horizon: row.get(3)?,
                        at: Minute::at(row.get(4)?),
                        value,
                        baseline,
                        score: row
                            .get::<_, Option<f64>>(7)?
                            .unwrap_or_else(|| endless(value, baseline)),
                        weight: row.get(8)?,
                    },
                ))
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Отмечает отклонение отсеянным: модель сочла его привычным шумом.
    ///
    /// Пометка, а не удаление: иначе агент будет спрашивать модель об одном и
    /// том же каждую минуту, а разобрать потом, что он гасил, станет нечем.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn sift(&self, deviation: i64, why: &str) -> Result<bool, StoreError> {
        let why = why.to_owned();
        self.work(move |db| {
            Ok(db.execute(
                "UPDATE deviations SET sifted = ?2 WHERE id = ?1",
                params![deviation, why],
            )? > 0)
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
        because: &str,
    ) -> Result<(i64, bool), StoreError> {
        let service = service.as_str().to_owned();
        let signature = signature.as_str().to_owned();
        let found = found.clone();
        let because = because.to_owned();
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
                        seen, peak, weight, because)
                     VALUES (?1, ?2, ?3, ?4, 'open', ?5, ?5, 1, ?6, ?7, ?8)",
                    params![
                        service,
                        signature,
                        found.stream.as_str(),
                        found.source,
                        found.at.stamp(),
                        found.value,
                        found.weight,
                        because
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

    /// Инциденты, ждущие разбора: открытые, без расследования, самые тяжёлые
    /// первыми.
    ///
    /// Расследование, на заявку которого ответили, инцидент не держит: ответ
    /// дежурного — это новые данные, и разбор идёт заново, уже с ними
    /// ([ADR-0011](../../../docs/adr/0011-diagnostic-requests.md)).
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn awaiting(&self, limit: usize) -> Result<Vec<Incident>, StoreError> {
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT id, service, signature, stream, source, state, began, last,
                        seen, peak, weight, verdict, because
                   FROM incidents
                  WHERE state = 'open'
                    AND id NOT IN (SELECT incident FROM investigations
                                    WHERE state <> 'answered')
                  ORDER BY weight DESC, id ASC LIMIT ?1",
            )?;
            let rows = query.query_map(params![limit], read_incident)?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Заводит расследование инцидента.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn dig(&self, incident: i64, skill: &str, at: Minute) -> Result<i64, StoreError> {
        let skill = skill.to_owned();
        self.work(move |db| {
            db.execute(
                "INSERT INTO investigations (incident, skill, state, started)
                 VALUES (?1, ?2, 'running', ?3)",
                params![incident, skill, at.stamp()],
            )?;
            Ok(db.last_insert_rowid())
        })
        .await
    }

    /// Записывает шаг расследования сразу, как он сделан.
    ///
    /// По шагу за раз, а не пачкой в конце: убитый на середине агент должен
    /// знать, где остановился.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn step(
        &self,
        investigation: i64,
        ord: usize,
        tool: &str,
        about: &str,
        data: &str,
    ) -> Result<(), StoreError> {
        let (tool, about, data) = (tool.to_owned(), about.to_owned(), data.to_owned());
        let ord = i64::try_from(ord).unwrap_or(0);
        self.work(move |db| {
            db.execute(
                "INSERT OR REPLACE INTO steps (investigation, ord, tool, about, data)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![investigation, ord, tool, about, data],
            )?;
            Ok(())
        })
        .await
    }

    /// Заканчивает расследование выводом.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn conclude(
        &self,
        investigation: i64,
        cause: &str,
        confidence: f64,
        advice: &str,
        note: Option<&str>,
        at: Minute,
    ) -> Result<(), StoreError> {
        let (cause, advice) = (cause.to_owned(), advice.to_owned());
        let note = note.map(ToOwned::to_owned);
        self.work(move |db| {
            db.execute(
                "UPDATE investigations
                    SET state = 'done', finished = ?2, cause = ?3, confidence = ?4,
                        advice = ?5, note = ?6
                  WHERE id = ?1",
                params![investigation, at.stamp(), cause, confidence, advice, note],
            )?;
            Ok(())
        })
        .await
    }

    /// Оставляет заявку и останавливает расследование до ответа человека.
    ///
    /// Одной транзакцией: расследование, помеченное ожиданием без заявки,
    /// молчит вечно, а заявка без пометки возвращает инцидент в очередь и
    /// заводит вторую такую же.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn ask(
        &self,
        incident: i64,
        investigation: i64,
        inquiry: &sre_domain::Inquiry,
        at: Minute,
    ) -> Result<i64, StoreError> {
        let inquiry = inquiry.clone();
        self.work(move |db| {
            let change = db.unchecked_transaction()?;
            change.execute(
                "INSERT INTO inquiries
                   (incident, investigation, host, command, reason, state, asked)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'open', ?6)",
                params![
                    incident,
                    investigation,
                    inquiry.host,
                    inquiry.command,
                    inquiry.reason,
                    at.stamp()
                ],
            )?;
            let id = change.last_insert_rowid();
            change.execute(
                "UPDATE investigations SET state = 'waiting', finished = ?2 WHERE id = ?1",
                params![investigation, at.stamp()],
            )?;
            change.commit()?;
            Ok(id)
        })
        .await
    }

    /// Записывает ответ дежурного и возвращает инцидент в очередь.
    ///
    /// Отвечает номером инцидента: тому, кто ответил, надо вернуться на его
    /// карточку, а закрытая или чужая заявка не отвечает ничем.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn reply(
        &self,
        inquiry: i64,
        who: &str,
        answer: &str,
        at: Minute,
    ) -> Result<Option<i64>, StoreError> {
        let (who, answer) = (who.to_owned(), answer.to_owned());
        self.work(move |db| {
            close(
                db,
                inquiry,
                "UPDATE inquiries
                    SET state = 'answered', answer = ?2, who = ?3, answered = ?4
                  WHERE id = ?1 AND state = 'open'",
                params![inquiry, answer, who, at.stamp()],
                "answered",
            )
        })
        .await
    }

    /// Снимает заявку: ответить нечем или незачем.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn shush(
        &self,
        inquiry: i64,
        who: &str,
        at: Minute,
    ) -> Result<Option<i64>, StoreError> {
        let who = who.to_owned();
        self.work(move |db| {
            close(
                db,
                inquiry,
                "UPDATE inquiries SET state = 'dropped', who = ?2, answered = ?3
                  WHERE id = ?1 AND state = 'open'",
                params![inquiry, who, at.stamp()],
                "dropped",
            )
        })
        .await
    }

    /// Гасит заявки, на которые давно никто не ответил.
    ///
    /// Заявка без срока — это инцидент, замерший навсегда: расследование ждёт
    /// человека, а человек про него уже забыл.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn fade(&self, before: Minute) -> Result<usize, StoreError> {
        self.work(move |db| {
            let change = db.unchecked_transaction()?;
            let stale = change
                .prepare("SELECT investigation FROM inquiries WHERE state = 'open' AND asked < ?1")?
                .query_map(params![before.stamp()], |row| row.get::<_, i64>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            change.execute(
                "UPDATE inquiries SET state = 'faded' WHERE state = 'open' AND asked < ?1",
                params![before.stamp()],
            )?;
            let mut mark =
                change.prepare("UPDATE investigations SET state = 'unanswered' WHERE id = ?1")?;
            for investigation in &stale {
                mark.execute(params![investigation])?;
            }
            drop(mark);
            change.commit()?;
            Ok(stale.len())
        })
        .await
    }

    /// Заявки инцидента, свежие первыми.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn inquiries(&self, incident: i64) -> Result<Vec<Asked>, StoreError> {
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT i.id, i.incident, n.service, i.host, i.command, i.reason,
                        i.state, i.asked, i.who, i.answer
                   FROM inquiries i JOIN incidents n ON n.id = i.incident
                  WHERE i.incident = ?1 ORDER BY i.id DESC",
            )?;
            let rows = query.query_map(params![incident], read_inquiry)?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Заявки, ждущие человека, самые старые первыми.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn pending(&self, limit: usize) -> Result<Vec<Asked>, StoreError> {
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT i.id, i.incident, n.service, i.host, i.command, i.reason,
                        i.state, i.asked, i.who, i.answer
                   FROM inquiries i JOIN incidents n ON n.id = i.incident
                  WHERE i.state = 'open' ORDER BY i.asked ASC, i.id ASC LIMIT ?1",
            )?;
            let rows = query.query_map(params![limit], read_inquiry)?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Отвеченные заявки инцидента: команда и то, что она показала.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn answers(&self, incident: i64) -> Result<Vec<(String, String)>, StoreError> {
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT command, answer FROM inquiries
                  WHERE incident = ?1 AND state = 'answered' AND answer IS NOT NULL
                  ORDER BY id",
            )?;
            let rows = query.query_map(params![incident], |row| Ok((row.get(0)?, row.get(1)?)))?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Заводит учёт черновика, написанного по инциденту.
    ///
    /// Второй черновик по тому же инциденту не заводится: имя файла и есть
    /// признак единственности, и переписывать заметку на каждом новом выводе
    /// значит заваливать дежурного одним и тем же.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn draft(
        &self,
        incident: i64,
        name: &str,
        title: &str,
        path: &str,
        at: Minute,
    ) -> Result<Option<i64>, StoreError> {
        let (name, title, path) = (name.to_owned(), title.to_owned(), path.to_owned());
        self.work(move |db| {
            let written = db.execute(
                "INSERT OR IGNORE INTO drafts (incident, name, title, path, state, written)
                 VALUES (?1, ?2, ?3, ?4, 'open', ?5)",
                params![incident, name, title, path, at.stamp()],
            )?;
            Ok((written > 0).then(|| db.last_insert_rowid()))
        })
        .await
    }

    /// Закрывает черновик: принят или отклонён.
    ///
    /// Отвечает инцидентом и путём к файлу — тому, кто закрывает, надо и
    /// вернуться на карточку, и знать, какой файл трогать.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn settle(
        &self,
        draft: i64,
        state: &str,
        who: &str,
        at: Minute,
    ) -> Result<Option<(i64, String)>, StoreError> {
        let (state, who) = (state.to_owned(), who.to_owned());
        self.work(move |db| {
            let change = db.unchecked_transaction()?;
            let touched = change.execute(
                "UPDATE drafts SET state = ?2, who = ?3, settled = ?4
                  WHERE id = ?1 AND state = 'open'",
                params![draft, state, who, at.stamp()],
            )?;
            if touched == 0 {
                return Ok(None);
            }
            let found = change.query_row(
                "SELECT incident, path FROM drafts WHERE id = ?1",
                params![draft],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )?;
            change.commit()?;
            Ok(Some(found))
        })
        .await
    }

    /// Черновики инцидента, свежие первыми.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn drafts(&self, incident: i64) -> Result<Vec<Written>, StoreError> {
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT id, incident, name, title, path, state, written, who
                   FROM drafts WHERE incident = ?1 ORDER BY id DESC",
            )?;
            let rows = query.query_map(params![incident], read_draft)?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Черновики, ждущие приёмки, самые старые первыми.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn unsettled(&self, limit: usize) -> Result<Vec<Written>, StoreError> {
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT id, incident, name, title, path, state, written, who
                   FROM drafts WHERE state = 'open' ORDER BY written ASC, id ASC LIMIT ?1",
            )?;
            let rows = query.query_map(params![limit], read_draft)?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Пересобирает поисковый индекс базы знаний.
    ///
    /// Целиком, а не по одной заметке: правки приходят из чужого репозитория
    /// пачкой, и разбираться, что там изменилось, дороже, чем переписать
    /// индекс на сотне заметок.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn remember(&self, notes: Vec<Memory>) -> Result<usize, StoreError> {
        self.work(move |db| {
            let change = db.unchecked_transaction()?;
            change.execute("DELETE FROM notes", [])?;
            {
                let mut put = change.prepare(
                    "INSERT INTO notes (name, title, tags, marks, body)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )?;
                for note in &notes {
                    put.execute(params![
                        note.name, note.title, note.tags, note.marks, note.body
                    ])?;
                }
            }
            change.commit()?;
            Ok(notes.len())
        })
        .await
    }

    /// Ищет заметки, похожие на описанное словами.
    ///
    /// Слова приходят из сигнатуры, имени сервиса и подозрения; редкие токены
    /// вроде `ECONNRESET` и делают попадание точным. Вес колонок неравный:
    /// сигнатура весит вчетверо против тела.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn recall(&self, words: &str, limit: usize) -> Result<Vec<Recalled>, StoreError> {
        let Some(query) = terms(words) else {
            return Ok(Vec::new());
        };
        self.work(move |db| {
            let mut search = db.prepare(
                "SELECT name, title, body, bm25(notes, 1.0, 2.0, 4.0, 8.0, 1.0)
                   FROM notes WHERE notes MATCH ?1
                  ORDER BY bm25(notes, 1.0, 2.0, 4.0, 8.0, 1.0) LIMIT ?2",
            )?;
            let rows = search.query_map(params![query, limit], |row| {
                Ok(Recalled {
                    name: row.get(0)?,
                    title: row.get(1)?,
                    body: row.get(2)?,
                    score: row.get(3)?,
                })
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Сколько заметок в индексе.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn notes(&self) -> Result<u64, StoreError> {
        self.work(move |db| {
            Ok(db.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get::<_, u64>(0))?)
        })
        .await
    }

    /// Помечает расследование неудавшимся или брошенным.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn drop_dig(&self, investigation: i64, state: &str) -> Result<(), StoreError> {
        let state = state.to_owned();
        self.work(move |db| {
            db.execute(
                "UPDATE investigations SET state = ?2 WHERE id = ?1",
                params![investigation, state],
            )?;
            Ok(())
        })
        .await
    }

    /// Вывод расследования инцидента, если он есть.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn conclusion(&self, incident: i64) -> Result<Option<Finding>, StoreError> {
        self.work(move |db| {
            Ok(db
                .query_row(
                    "SELECT id, skill, state, cause, confidence, advice, note
                       FROM investigations WHERE incident = ?1
                      ORDER BY id DESC LIMIT 1",
                    params![incident],
                    |row| {
                        Ok(Finding {
                            id: row.get(0)?,
                            skill: row.get(1)?,
                            state: row.get(2)?,
                            cause: row.get(3)?,
                            confidence: row.get(4)?,
                            advice: row.get(5)?,
                            note: row.get(6)?,
                        })
                    },
                )
                .optional()?)
        })
        .await
    }

    /// Расследования, оборвавшиеся на середине.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn unfinished(&self) -> Result<Vec<(i64, i64, String, Minute)>, StoreError> {
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT id, incident, skill, started FROM investigations
                  WHERE state = 'running' ORDER BY id",
            )?;
            let rows = query.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    Minute::at(row.get(3)?),
                ))
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Шаги расследования по порядку.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn steps(
        &self,
        investigation: i64,
    ) -> Result<Vec<(String, String, String)>, StoreError> {
        self.work(move |db| {
            let mut query = db.prepare(
                "SELECT tool, about, data FROM steps WHERE investigation = ?1 ORDER BY ord",
            )?;
            let rows = query.query_map(params![investigation], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
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
                        seen, peak, weight, verdict, because
                   FROM incidents
                  WHERE (?1 = 0 OR state = 'open')
                  ORDER BY last DESC, id DESC LIMIT ?2",
            )?;
            let rows = query.query_map(params![i64::from(open), limit], read_incident)?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Записывает оценку дежурного; отвечает, нашёлся ли инцидент.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе записи.
    pub async fn judge(
        &self,
        incident: i64,
        useful: bool,
        who: &str,
        when: Minute,
    ) -> Result<bool, StoreError> {
        let who = who.to_owned();
        self.work(move |db| {
            Ok(db.execute(
                "UPDATE incidents SET verdict = ?2, judge = ?3, judged = ?4 WHERE id = ?1",
                params![incident, i64::from(useful), who, when.stamp()],
            )? > 0)
        })
        .await
    }

    /// Счёт оценённых инцидентов — основание метрики точности.
    ///
    /// # Errors
    /// [`StoreError::Sqlite`] на отказе чтения.
    pub async fn tally(&self) -> Result<Tally, StoreError> {
        self.work(move |db| {
            Ok(db.query_row(
                "SELECT COUNT(*),
                        COALESCE(SUM(verdict = 1), 0),
                        COALESCE(SUM(verdict = 0), 0),
                        COALESCE(SUM(state = 'open'), 0)
                   FROM incidents",
                [],
                |row| {
                    Ok(Tally {
                        total: row.get(0)?,
                        useful: row.get(1)?,
                        useless: row.get(2)?,
                        open: row.get(3)?,
                    })
                },
            )?)
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
///
/// Шаг применяется по предложению, а уже сделанное пропускается: номер
/// применённого шага можно потерять — восстановлением из бэкапа, ручной
/// правкой, копией файла, — и агент обязан подняться, а не упасть с «столбец
/// уже есть». Откат из [ADR-0023](../../../docs/adr/0023-deploy-with-downtime.md)
/// без этого перестаёт быть откатом.
fn migrate(connection: &Connection) -> Result<(), StoreError> {
    let applied: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let applied = usize::try_from(applied).unwrap_or(0);
    for (number, step) in schema::STEPS.iter().enumerate().skip(applied) {
        for sentence in step.split(';').map(str::trim).filter(|it| !it.is_empty()) {
            match connection.execute_batch(sentence) {
                Ok(()) => {}
                Err(failure) if done(&failure) => {
                    tracing::debug!(step = number + 1, "часть шага схемы уже применена");
                }
                Err(failure) => return Err(failure.into()),
            }
        }
        connection.pragma_update(None, "user_version", number + 1)?;
        tracing::info!(step = number + 1, "схема базы обновлена");
    }
    Ok(())
}

/// Отказ, означающий «это уже сделано».
fn done(failure: &rusqlite::Error) -> bool {
    let text = failure.to_string();
    text.contains("duplicate column name") || text.contains("already exists")
}

/// Инцидент из строки таблицы.
fn read_incident(row: &rusqlite::Row<'_>) -> rusqlite::Result<Incident> {
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
        because: row.get(12)?,
    })
}

/// Закрывает открытую заявку и переводит её расследование.
///
/// Одной транзакцией: заявка, закрытая без пометки на расследовании, оставляет
/// инцидент вне очереди навсегда.
fn close(
    db: &Connection,
    inquiry: i64,
    sentence: &str,
    values: impl rusqlite::Params,
    state: &str,
) -> Result<Option<i64>, StoreError> {
    let change = db.unchecked_transaction()?;
    if change.execute(sentence, values)? == 0 {
        return Ok(None);
    }
    change.execute(
        "UPDATE investigations SET state = ?2
          WHERE id = (SELECT investigation FROM inquiries WHERE id = ?1)",
        params![inquiry, state],
    )?;
    let incident = change.query_row(
        "SELECT incident FROM inquiries WHERE id = ?1",
        params![inquiry],
        |row| row.get::<_, i64>(0),
    )?;
    change.commit()?;
    Ok(Some(incident))
}

/// Сколько слов уходит в поисковый запрос.
///
/// Больше дюжины — и запрос начинает находить всё подряд: редкие токены тонут
/// среди частых, и ранжирование перестаёт что-либо значить.
const WORDS: usize = 12;

/// Превращает описание словами в запрос к полнотекстовому индексу.
///
/// Единственный путь чужому тексту попасть в `MATCH`. Сигнатура ошибки полна
/// кавычек, звёздочек и скобок — всё это синтаксис FTS5, и без разбора на
/// слова запрос либо не разберётся, либо найдёт не то.
fn terms(words: &str) -> Option<String> {
    let mut seen = Vec::new();
    for word in words
        .split(|it: char| !it.is_alphanumeric())
        .map(str::to_lowercase)
        .filter(|it| it.chars().count() >= 3)
    {
        if !seen.contains(&word) {
            seen.push(word);
        }
        if seen.len() == WORDS {
            break;
        }
    }
    (!seen.is_empty()).then(|| {
        seen.iter()
            .map(|word| format!("\"{word}\""))
            .collect::<Vec<_>>()
            .join(" OR ")
    })
}

/// Черновик из строки таблицы.
fn read_draft(row: &rusqlite::Row<'_>) -> rusqlite::Result<Written> {
    Ok(Written {
        id: row.get(0)?,
        incident: row.get(1)?,
        name: row.get(2)?,
        title: row.get(3)?,
        path: row.get(4)?,
        state: row.get(5)?,
        written: Minute::at(row.get(6)?),
        who: row.get(7)?,
    })
}

/// Заявка из строки таблицы.
fn read_inquiry(row: &rusqlite::Row<'_>) -> rusqlite::Result<Asked> {
    Ok(Asked {
        id: row.get(0)?,
        incident: row.get(1)?,
        service: row.get(2)?,
        host: row.get(3)?,
        command: row.get(4)?,
        reason: row.get(5)?,
        state: row.get(6)?,
        asked: Minute::at(row.get(7)?),
        who: row.get(8)?,
        answer: row.get(9)?,
    })
}

/// Бесконечная оценка со знаком: в базе она пустая, а направление видно по
/// числам, которые лежат рядом.
fn endless(value: f64, baseline: f64) -> f64 {
    if value < baseline {
        f64::NEG_INFINITY
    } else {
        f64::INFINITY
    }
}

/// Берёт соединение, восстанавливая его после паники другого потока: `SQLite`
/// откатывает незавершённую транзакцию сам, терять базу из-за этого незачем.
fn guard(connection: &Mutex<Connection>) -> MutexGuard<'_, Connection> {
    connection.lock().unwrap_or_else(PoisonError::into_inner)
}
