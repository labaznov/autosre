//! Схема базы и её изменения.
//!
//! Изменения применяются по порядку, номер применённого хранится в
//! `user_version`. Каждый шаг обязан оставлять базу пригодной для предыдущей
//! версии агента: откат — это возврат прежнего образа, и он должен работать
//! без плясок с данными ([ADR-0023](../../../docs/adr/0023-deploy-with-downtime.md)).

/// Шаги изменения схемы. Добавлять только в конец, порядок не менять.
pub const STEPS: &[&str] = &[
    // 1. Ряд наблюдений: минутные бакеты, отметки снятых минут, свёрнутые часы.
    //
    // Таблица `minutes` отвечает на вопрос, который по `buckets` не задать:
    // минута без строк — это минута без ошибок или минута, которую не снимали?
    // Без неё дозапрос дыр невозможен, а тишина неотличима от слепоты.
    "
    CREATE TABLE IF NOT EXISTS buckets (
        source TEXT    NOT NULL,
        stream TEXT    NOT NULL,
        at     INTEGER NOT NULL,
        value  REAL    NOT NULL,
        kind   INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (source, stream, at)
    ) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS buckets_at ON buckets (source, at);

    CREATE TABLE IF NOT EXISTS minutes (
        source TEXT    NOT NULL,
        at     INTEGER NOT NULL,
        PRIMARY KEY (source, at)
    ) WITHOUT ROWID;

    CREATE TABLE IF NOT EXISTS hours (
        source TEXT    NOT NULL,
        stream TEXT    NOT NULL,
        at     INTEGER NOT NULL,
        value  REAL    NOT NULL,
        kind   INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (source, stream, at)
    ) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS hours_at ON hours (source, at);
    ",
];
