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
    // 2. Отклонения: окна, вышедшие за пороги детектора.
    //
    // Уникальность по «источник, поток, горизонт, конец окна» — не украшение:
    // окно скользит каждую минуту, и повторная оценка того же окна не должна
    // плодить строки.
    "
    CREATE TABLE IF NOT EXISTS deviations (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        source   TEXT    NOT NULL,
        stream   TEXT    NOT NULL,
        horizon  TEXT    NOT NULL,
        at       INTEGER NOT NULL,
        value    REAL    NOT NULL,
        baseline REAL    NOT NULL,
        -- Пусто, когда оценка бесконечна: ошибок раньше не было вовсе.
        -- Хранить в этом случае предельное число значит показать дежурному
        -- «оценка 1,8e308» вместо «такого не бывало».
        score    REAL,
        weight   REAL    NOT NULL,
        found    INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS deviations_once
        ON deviations (source, stream, horizon, at);
    CREATE INDEX IF NOT EXISTS deviations_weight ON deviations (weight DESC);
    ",
    // 3. Инциденты и связи между ними.
    //
    // Открытый инцидент на пару «сервис плюс сигнатура» должен быть один: на
    // этом держится вся группировка. Уникальность частичная — закрытые пары
    // могут повторяться, и новая беда с той же сигнатурой заведёт новую
    // карточку, а не воскресит прошлогоднюю.
    "
    CREATE TABLE IF NOT EXISTS incidents (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        service   TEXT    NOT NULL,
        signature TEXT    NOT NULL,
        stream    TEXT    NOT NULL,
        source    TEXT    NOT NULL,
        state     TEXT    NOT NULL,
        began     INTEGER NOT NULL,
        last      INTEGER NOT NULL,
        seen      INTEGER NOT NULL,
        peak      REAL    NOT NULL,
        weight    REAL    NOT NULL,
        verdict   INTEGER
    );
    CREATE UNIQUE INDEX IF NOT EXISTS incidents_open
        ON incidents (service, signature) WHERE state = 'open';
    CREATE INDEX IF NOT EXISTS incidents_last ON incidents (last DESC);

    CREATE TABLE IF NOT EXISTS links (
        incident INTEGER NOT NULL,
        related  INTEGER NOT NULL,
        PRIMARY KEY (incident, related)
    ) WITHOUT ROWID;

    ALTER TABLE deviations ADD COLUMN incident INTEGER;
    ",
    // 4. Кто поставил оценку и когда.
    //
    // Имя, а не «дежурный»: на подписи держится вся ценность обратной связи.
    // Разбирать через месяц, кто и почему счёл находку ложной, придётся всерьёз.
    "
    ALTER TABLE incidents ADD COLUMN judge TEXT;
    ALTER TABLE incidents ADD COLUMN judged INTEGER;
    ",
    // 5. Отсев: почему отклонение не пошло дальше и почему инцидент завёлся.
    //
    // Отсеянное помечается, а не удаляется: иначе агент будет спрашивать модель
    // об одном и том же каждую минуту, а разобрать потом, что он гасил, будет
    // невозможно.
    "
    ALTER TABLE deviations ADD COLUMN sifted TEXT;
    ALTER TABLE incidents ADD COLUMN because TEXT;
    ",
];
