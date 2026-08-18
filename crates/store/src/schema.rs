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
    // 6. Расследования и их шаги.
    //
    // Шаг — строка в базе, а не переменная в памяти: убитый на середине агент
    // должен знать, где остановился ([ADR-0021](../../../docs/adr/0021-state-survives-restart.md)).
    "
    CREATE TABLE IF NOT EXISTS investigations (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        incident   INTEGER NOT NULL,
        skill      TEXT    NOT NULL,
        state      TEXT    NOT NULL,
        started    INTEGER NOT NULL,
        finished   INTEGER,
        cause      TEXT,
        confidence REAL,
        advice     TEXT
    );
    CREATE INDEX IF NOT EXISTS investigations_incident ON investigations (incident);
    CREATE INDEX IF NOT EXISTS investigations_state ON investigations (state);

    CREATE TABLE IF NOT EXISTS steps (
        investigation INTEGER NOT NULL,
        ord           INTEGER NOT NULL,
        tool          TEXT    NOT NULL,
        about         TEXT    NOT NULL,
        data          TEXT    NOT NULL,
        PRIMARY KEY (investigation, ord)
    ) WITHOUT ROWID;
    ",
    // 7. Заявки на диагностику: вопрос человеку и его ответ.
    //
    // Заявка держит номер расследования, а не только инцидента: ответ должен
    // вернуться туда, откуда спрашивали, даже если инцидент за это время
    // успел набрать других расследований ([ADR-0011](../../../docs/adr/0011-diagnostic-requests.md)).
    "
    CREATE TABLE IF NOT EXISTS inquiries (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        incident      INTEGER NOT NULL,
        investigation INTEGER NOT NULL,
        host          TEXT    NOT NULL,
        command       TEXT    NOT NULL,
        reason        TEXT    NOT NULL,
        state         TEXT    NOT NULL,
        asked         INTEGER NOT NULL,
        answered      INTEGER,
        who           TEXT,
        answer        TEXT
    );
    CREATE INDEX IF NOT EXISTS inquiries_open ON inquiries (state, asked);
    CREATE INDEX IF NOT EXISTS inquiries_incident ON inquiries (incident);
    ",
    // 8. Поисковый индекс базы знаний.
    //
    // Не хранилище заметок, а именно индекс: сами заметки лежат файлами в чужом
    // репозитории ([ADR-0015](../../../docs/adr/0015-knowledge-repository.md)),
    // и правда о них там. Индекс собирается заново при старте и после правок,
    // поэтому потерять его не страшно.
    //
    // Колонки разделены не ради красоты: у сигнатур и тегов свой вес в BM25.
    // `ECONNRESET` в сигнатуре означает «заметка про это», в теле — «здесь
    // такое упоминалось» ([ADR-0008](../../../docs/adr/0008-full-text-knowledge-search.md)).
    "
    CREATE VIRTUAL TABLE IF NOT EXISTS notes USING fts5 (
        name, title, tags, marks, body, tokenize = 'unicode61 remove_diacritics 2'
    );
    ",
    // 9. Заметка, на которую опёрся вывод.
    //
    // Имя, а не текст: заметка живёт в чужом репозитории и меняется там. Копия
    // в базе разошлась бы с оригиналом в первый же день.
    "
    ALTER TABLE investigations ADD COLUMN note TEXT;
    ",
    // 10. Черновики заметок: учёт того, что ждёт приёмки.
    //
    // Содержимое черновика лежит файлом в репозитории знаний, здесь только
    // учёт: чей, откуда, где лежит и что с ним стало
    // ([ADR-0009](../../../docs/adr/0009-drafts-before-knowledge.md)). Считать
    // непринятые обходом каталога значит читать сотню файлов ради одного числа
    // в шапке.
    "
    CREATE TABLE IF NOT EXISTS drafts (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        incident INTEGER NOT NULL,
        name     TEXT    NOT NULL,
        title    TEXT    NOT NULL,
        path     TEXT    NOT NULL,
        state    TEXT    NOT NULL,
        written  INTEGER NOT NULL,
        settled  INTEGER,
        who      TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS drafts_once ON drafts (name);
    CREATE INDEX IF NOT EXISTS drafts_state ON drafts (state, written);
    ",
    // 11. Приглушения: пара «сервис плюс сигнатура», замолкшая на срок.
    //
    // Срок обязателен и в схеме, и в правилах: вечное приглушение — это
    // слепота, оформленная как настройка
    // ([ADR-0019](../../../docs/adr/0019-muting-instead-of-per-service-thresholds.md)).
    //
    // Отклонение помечается номером приглушения, а не отсевом: отсев — решение
    // модели, приглушение — решение человека, и путать их в отчёте нельзя.
    "
    CREATE TABLE IF NOT EXISTS mutes (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        service   TEXT    NOT NULL,
        signature TEXT    NOT NULL,
        until     INTEGER NOT NULL,
        made      INTEGER NOT NULL,
        author    TEXT    NOT NULL,
        reason    TEXT    NOT NULL,
        lifted    INTEGER
    );
    CREATE INDEX IF NOT EXISTS mutes_pair ON mutes (service, signature, until);

    ALTER TABLE deviations ADD COLUMN muted INTEGER;
    ",
    // 12. Правка группировки: во что инцидент влит и из чего выделен.
    //
    // Правило группировки узкое и ошибается в обе стороны
    // ([ADR-0017](../../../docs/adr/0017-narrow-grouping.md)), поэтому у
    // дежурного должны быть обе кнопки. Без них любая ошибка группировки
    // становится неисправимой ([ADR-0010](../../../docs/adr/0010-incident-aggregate.md)).
    //
    // Влитый инцидент не удаляется: он несёт своё время первого наблюдения, а
    // на нём держится метрика времени до обнаружения.
    "
    ALTER TABLE incidents ADD COLUMN merged INTEGER;
    ALTER TABLE incidents ADD COLUMN split INTEGER;
    ",
    // 13. Момент закрытия и учёт отчётов.
    //
    // Без момента закрытия суточный отчёт не ответит на простой вопрос: что
    // сегодня кончилось. Инциденты, закрытые до этого шага, останутся без
    // момента — это честнее, чем выдумать им время задним числом.
    //
    // Отчёт лежит файлом в репозитории знаний, здесь — учёт: какой, за что и
    // где ([SPEC §9](../../../docs/SPEC.md)).
    "
    ALTER TABLE incidents ADD COLUMN closed INTEGER;

    CREATE TABLE IF NOT EXISTS reports (
        id     INTEGER PRIMARY KEY AUTOINCREMENT,
        kind   TEXT    NOT NULL,
        name   TEXT    NOT NULL,
        title  TEXT    NOT NULL,
        path   TEXT    NOT NULL,
        made   INTEGER NOT NULL,
        body   TEXT    NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS reports_once ON reports (kind, name);
    ",
    // 14. Отчёт, собранный не целиком.
    //
    // Модель бывает недоступна, и общая картина не пишется. Такой отчёт всё
    // равно нужен — числа в нём настоящие, — но он обязан пересобраться, когда
    // модель вернётся. Без этой отметки первый же отказ модели оставлял бы
    // сутки без картины навсегда: имя занято, значит собирать нечего.
    "
    ALTER TABLE reports ADD COLUMN whole INTEGER NOT NULL DEFAULT 1;
    ",
    // 15. Важность инцидента.
    //
    // Ставит её модель в выводе, поэтому до первого вывода она пуста — и это
    // честнее, чем «средняя по умолчанию»: неразобранный инцидент может
    // оказаться и страшнее, и безобиднее любого разобранного
    // ([SPEC §3](../../../docs/SPEC.md)).
    "
    ALTER TABLE incidents ADD COLUMN severity TEXT;
    ",
    // 16. Уроки: что уходило в модель и что она ответила.
    //
    // Хранится **то, что было отправлено**, а не то, что можно собрать заново:
    // промпты меняются вместе с агентом, и корпус, собранный по нынешним
    // шаблонам из прошлогодних данных, учит модель тому, чего никогда не было.
    //
    // Метку к уроку ставит не агент, а дежурный — своей оценкой инцидента.
    // Поэтому урок держит номер инцидента: без него это просто переписка с
    // моделью, а с ним — пример с ответом, годный для дообучения.
    "
    CREATE TABLE IF NOT EXISTS lessons (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        at            INTEGER NOT NULL,
        kind          TEXT    NOT NULL,
        model         TEXT    NOT NULL,
        incident      INTEGER,
        investigation INTEGER,
        system        TEXT    NOT NULL,
        ask           TEXT    NOT NULL,
        answer        TEXT    NOT NULL
    );
    CREATE INDEX IF NOT EXISTS lessons_at ON lessons (at);
    CREATE INDEX IF NOT EXISTS lessons_incident ON lessons (incident);
    ",
    // 17. Оценка отсеянного.
    //
    // Отсеянное дежурный не видел никогда, поэтому у отрицательных примеров
    // корпуса не было метки: «модель сказала шум» — и всё, проверить некому
    // ([ADR-0027](../../../docs/adr/0027-live-corpus.md)). Теперь у отклонения
    // есть та же оценка, что у инцидента, и урок отсева знает своё отклонение.
    "
    ALTER TABLE deviations ADD COLUMN verdict INTEGER;
    ALTER TABLE deviations ADD COLUMN judge TEXT;
    ALTER TABLE deviations ADD COLUMN judged INTEGER;
    ALTER TABLE lessons ADD COLUMN deviation INTEGER;
    CREATE INDEX IF NOT EXISTS deviations_sifted ON deviations (sifted, found);
    ",
];
