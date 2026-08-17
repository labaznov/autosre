use chrono::{DateTime, Utc};
use sre_domain::{Bucket, Minute, Span, Stream};
use sre_store::Store;
use tempfile::TempDir;

/// База во временном каталоге вместе с ним самим.
struct Base {
    store: Store,
    _directory: TempDir,
}

impl Base {
    fn open() -> Self {
        let directory = TempDir::new().expect("временный каталог не создан");
        let store = Store::open(&directory.path().join("nested/sre.db")).expect("база не открыта");
        Self {
            store,
            _directory: directory,
        }
    }
}

fn moment(text: &str) -> DateTime<Utc> {
    text.parse().expect("момент времени некорректен")
}

fn minute(text: &str) -> Minute {
    Minute::of(moment(text))
}

fn wifi() -> Stream {
    Stream::new("{container=\"orders-api\"}")
}

#[tokio::test]
async fn saves_what_was_snapped() {
    let base = Base::open();
    let at = minute("2026-08-17T10:01:00Z");
    base.store
        .save("logs", Span::single(at), vec![Bucket::new(wifi(), at, 7.0)])
        .await
        .unwrap();
    assert_eq!(
        base.store.value("logs", &wifi(), at).await.unwrap(),
        Some(7.0)
    );
}

#[tokio::test]
async fn replaces_a_repeated_minute_instead_of_doubling_it() {
    let base = Base::open();
    let at = minute("2026-08-17T10:01:00Z");
    for value in [7.0, 7.0] {
        base.store
            .save(
                "logs",
                Span::single(at),
                vec![Bucket::new(wifi(), at, value)],
            )
            .await
            .unwrap();
    }
    assert_eq!(
        base.store.value("logs", &wifi(), at).await.unwrap(),
        Some(7.0)
    );
}

#[tokio::test]
async fn keeps_one_row_after_a_repeated_minute() {
    let base = Base::open();
    let at = minute("2026-08-17T10:01:00Z");
    for _ in 0..3 {
        base.store
            .save("logs", Span::single(at), vec![Bucket::new(wifi(), at, 7.0)])
            .await
            .unwrap();
    }
    assert_eq!(base.store.count("logs").await.unwrap(), 1);
}

#[tokio::test]
async fn separates_the_sources() {
    let base = Base::open();
    let at = minute("2026-08-17T10:01:00Z");
    base.store
        .save("logs", Span::single(at), vec![Bucket::new(wifi(), at, 7.0)])
        .await
        .unwrap();
    assert_eq!(base.store.count("metrics").await.unwrap(), 0);
}

#[tokio::test]
async fn remembers_the_last_snapped_minute() {
    let base = Base::open();
    let at = minute("2026-08-17T10:01:00Z");
    base.store
        .save("logs", Span::single(at), vec![])
        .await
        .unwrap();
    assert_eq!(base.store.snapped("logs").await.unwrap(), Some(at));
}

#[tokio::test]
async fn knows_no_minute_before_the_first_one() {
    assert_eq!(Base::open().store.snapped("logs").await.unwrap(), None);
}

#[tokio::test]
async fn counts_a_silent_minute_as_snapped() {
    let base = Base::open();
    let at = minute("2026-08-17T10:01:00Z");
    base.store
        .save("logs", Span::single(at), vec![])
        .await
        .unwrap();
    assert!(
        base.store
            .gaps("logs", Span::single(at))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn names_the_minutes_that_were_never_snapped() {
    let base = Base::open();
    let from = minute("2026-08-17T10:00:00Z");
    let span = Span::new(from, from.back(-5), 96).unwrap();
    base.store
        .save("logs", Span::single(from), vec![])
        .await
        .unwrap();
    assert_eq!(base.store.gaps("logs", span).await.unwrap().len(), 4);
}

#[tokio::test]
async fn survives_a_reopen_of_the_same_file() {
    let directory = TempDir::new().expect("временный каталог не создан");
    let path = directory.path().join("sre.db");
    let at = minute("2026-08-17T10:01:00Z");
    Store::open(&path)
        .unwrap()
        .save("logs", Span::single(at), vec![Bucket::new(wifi(), at, 7.0)])
        .await
        .unwrap();
    assert_eq!(Store::open(&path).unwrap().count("logs").await.unwrap(), 1);
}
