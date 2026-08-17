//! Схема базы наблюдений.

/// Таблицы и индексы; выполняется при каждом открытии базы.
///
/// Таблица `minutes` отвечает на вопрос, который по `buckets` не задать:
/// минута без строк — это минута без ошибок или минута, которую не снимали?
/// Без неё дозапрос дыр невозможен, а тишина неотличима от слепоты.
pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS buckets (
    source TEXT    NOT NULL,
    stream TEXT    NOT NULL,
    at     INTEGER NOT NULL,
    value  REAL    NOT NULL,
    PRIMARY KEY (source, stream, at)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS buckets_at ON buckets (source, at);

CREATE TABLE IF NOT EXISTS minutes (
    source TEXT    NOT NULL,
    at     INTEGER NOT NULL,
    PRIMARY KEY (source, at)
) WITHOUT ROWID;
";
