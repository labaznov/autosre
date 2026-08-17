use chrono::{DateTime, Utc};
use sre_domain::{
    Bucket, Detector, Deviation, Hour, Minute, Service, Signature, Span, Stream, Thresholds,
};
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
        .save(
            "logs",
            Span::single(at),
            vec![Bucket::counted(wifi(), at, 7.0)],
        )
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
                vec![Bucket::counted(wifi(), at, value)],
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
            .save(
                "logs",
                Span::single(at),
                vec![Bucket::counted(wifi(), at, 7.0)],
            )
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
        .save(
            "logs",
            Span::single(at),
            vec![Bucket::counted(wifi(), at, 7.0)],
        )
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
        .save(
            "logs",
            Span::single(at),
            vec![Bucket::counted(wifi(), at, 7.0)],
        )
        .await
        .unwrap();
    assert_eq!(Store::open(&path).unwrap().count("logs").await.unwrap(), 1);
}

#[tokio::test]
async fn rolls_the_minutes_of_an_hour_into_one() {
    let base = Base::open();
    let hour = minute("2026-08-17T10:00:00Z");
    for step in 0..60 {
        let at = hour.back(-step);
        base.store
            .save(
                "logs",
                Span::single(at),
                vec![Bucket::counted(wifi(), at, 2.0)],
            )
            .await
            .unwrap();
    }
    base.store.roll("logs", hour.back(-60)).await.unwrap();
    assert_eq!(
        base.store
            .hour("logs", &wifi(), Hour::of(hour))
            .await
            .unwrap(),
        Some(120.0)
    );
}

#[tokio::test]
async fn averages_a_level_instead_of_summing_it() {
    let base = Base::open();
    let hour = minute("2026-08-17T10:00:00Z");
    for step in 1..=4 {
        let at = hour.back(-i64::from(step));
        base.store
            .save(
                "metrics",
                Span::single(at),
                vec![Bucket::level(wifi(), at, f64::from(step))],
            )
            .await
            .unwrap();
    }
    base.store.roll("metrics", hour.back(-60)).await.unwrap();
    assert_eq!(
        base.store
            .hour("metrics", &wifi(), Hour::of(hour))
            .await
            .unwrap(),
        Some(2.5)
    );
}

#[tokio::test]
async fn takes_the_rolled_minutes_out_of_the_series() {
    let base = Base::open();
    let hour = minute("2026-08-17T10:00:00Z");
    base.store
        .save(
            "logs",
            Span::single(hour),
            vec![Bucket::counted(wifi(), hour, 2.0)],
        )
        .await
        .unwrap();
    base.store.roll("logs", hour.back(-60)).await.unwrap();
    assert_eq!(base.store.count("logs").await.unwrap(), 0);
}

#[tokio::test]
async fn rolls_one_hour_at_a_time() {
    let base = Base::open();
    let first = minute("2026-08-17T10:00:00Z");
    for at in [first, first.back(-60), first.back(-120)] {
        base.store
            .save(
                "logs",
                Span::single(at),
                vec![Bucket::counted(wifi(), at, 1.0)],
            )
            .await
            .unwrap();
    }
    let rolled = base.store.roll("logs", first.back(-180)).await.unwrap();
    assert_eq!(rolled.hour, Some(Hour::of(first)));
}

#[tokio::test]
async fn says_when_there_is_nothing_left_to_roll() {
    let base = Base::open();
    let at = minute("2026-08-17T10:00:00Z");
    base.store
        .save(
            "logs",
            Span::single(at),
            vec![Bucket::counted(wifi(), at, 1.0)],
        )
        .await
        .unwrap();
    base.store.roll("logs", at.back(-60)).await.unwrap();
    assert!(!base.store.roll("logs", at.back(-60)).await.unwrap().more);
}

#[tokio::test]
async fn leaves_the_fresh_minutes_alone() {
    let base = Base::open();
    let at = minute("2026-08-17T10:00:00Z");
    base.store
        .save(
            "logs",
            Span::single(at),
            vec![Bucket::counted(wifi(), at, 1.0)],
        )
        .await
        .unwrap();
    base.store.roll("logs", at).await.unwrap();
    assert_eq!(base.store.count("logs").await.unwrap(), 1);
}

#[tokio::test]
async fn forgets_the_marks_of_expired_minutes() {
    let base = Base::open();
    let at = minute("2026-08-17T10:00:00Z");
    base.store
        .save("logs", Span::single(at), vec![])
        .await
        .unwrap();
    base.store
        .forget("logs", at.back(-1), Hour::of(at).next())
        .await
        .unwrap();
    assert_eq!(base.store.snapped("logs").await.unwrap(), None);
}

#[tokio::test]
async fn forgets_the_hours_whose_time_has_passed() {
    let base = Base::open();
    let at = minute("2026-08-17T10:00:00Z");
    base.store
        .save(
            "logs",
            Span::single(at),
            vec![Bucket::counted(wifi(), at, 5.0)],
        )
        .await
        .unwrap();
    base.store.roll("logs", at.back(-60)).await.unwrap();
    base.store
        .forget("logs", at.back(-60), Hour::of(at).next())
        .await
        .unwrap();
    assert_eq!(
        base.store
            .hour("logs", &wifi(), Hour::of(at))
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn sums_the_minutes_of_a_window() {
    let base = Base::open();
    let end = minute("2026-08-17T10:15:00Z");
    for step in 1..=15 {
        let at = end.back(step);
        base.store
            .save(
                "logs",
                Span::single(at),
                vec![Bucket::counted(wifi(), at, 2.0)],
            )
            .await
            .unwrap();
    }
    let series = base.store.windows("logs", end, 15, 4).await.unwrap();
    assert!((series[&wifi()][0] - 30.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn puts_the_older_window_further_along() {
    let base = Base::open();
    let end = minute("2026-08-17T10:30:00Z");
    let older = end.back(20);
    base.store
        .save(
            "logs",
            Span::single(older),
            vec![Bucket::counted(wifi(), older, 7.0)],
        )
        .await
        .unwrap();
    let series = base.store.windows("logs", end, 15, 4).await.unwrap();
    assert!((series[&wifi()][1] - 7.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn counts_a_window_without_buckets_as_zero() {
    let base = Base::open();
    let end = minute("2026-08-17T10:30:00Z");
    let at = end.back(1);
    base.store
        .save(
            "logs",
            Span::single(at),
            vec![Bucket::counted(wifi(), at, 3.0)],
        )
        .await
        .unwrap();
    let series = base.store.windows("logs", end, 15, 4).await.unwrap();
    assert!(series[&wifi()][2].abs() < f64::EPSILON);
}

#[tokio::test]
async fn takes_one_query_for_every_stream() {
    let base = Base::open();
    let end = minute("2026-08-17T10:15:00Z");
    let at = end.back(1);
    let many: Vec<Bucket> = (0..50)
        .map(|index| Bucket::counted(Stream::new(format!("{{service=\"s{index}\"}}")), at, 1.0))
        .collect();
    base.store
        .save("logs", Span::single(at), many)
        .await
        .unwrap();
    assert_eq!(
        base.store.windows("logs", end, 15, 4).await.unwrap().len(),
        50
    );
}

#[tokio::test]
async fn writes_down_a_deviation() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0, 5.0], 91.0);
    base.store
        .spot(&Deviation::new("logs", &wifi(), "15m", at, verdict), at)
        .await
        .unwrap();
    assert_eq!(base.store.deviations(10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn writes_the_same_window_down_once() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0, 5.0], 91.0);
    let deviation = Deviation::new("logs", &wifi(), "15m", at, verdict);
    base.store.spot(&deviation, at).await.unwrap();
    assert!(!base.store.spot(&deviation, at).await.unwrap());
}

#[tokio::test]
async fn puts_the_heaviest_deviation_first() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    let detector = Detector::new(Thresholds::default());
    for (stream, value) in [("light", 40.0), ("heavy", 900.0)] {
        let verdict = detector.verdict(&[4.0, 4.0, 5.0], value);
        let stream = Stream::new(format!("{{service=\"{stream}\"}}"));
        base.store
            .spot(&Deviation::new("logs", &stream, "15m", at, verdict), at)
            .await
            .unwrap();
    }
    assert_eq!(
        base.store.deviations(10).await.unwrap()[0].stream,
        Stream::new("{service=\"heavy\"}")
    );
}

#[tokio::test]
async fn keeps_the_numbers_of_a_deviation() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0, 5.0], 91.0);
    base.store
        .spot(&Deviation::new("logs", &wifi(), "15m", at, verdict), at)
        .await
        .unwrap();
    assert!((base.store.deviations(10).await.unwrap()[0].baseline - 4.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn counts_the_minutes_a_window_really_holds() {
    let base = Base::open();
    let end = minute("2026-08-17T10:15:00Z");
    for step in 1..=10 {
        base.store
            .save("logs", Span::single(end.back(step)), vec![])
            .await
            .unwrap();
    }
    assert_eq!(base.store.covered("logs", end, 15, 4).await.unwrap()[0], 10);
}

#[tokio::test]
async fn counts_an_unsnapped_window_as_empty() {
    let base = Base::open();
    let end = minute("2026-08-17T10:15:00Z");
    base.store
        .save("logs", Span::single(end.back(1)), vec![])
        .await
        .unwrap();
    assert_eq!(base.store.covered("logs", end, 15, 4).await.unwrap()[2], 0);
}

#[tokio::test]
async fn keeps_an_endless_score_endless() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    let verdict = Detector::new(Thresholds::default()).verdict(&[0.0, 0.0, 0.0], 91.0);
    base.store
        .spot(&Deviation::new("logs", &wifi(), "15m", at, verdict), at)
        .await
        .unwrap();
    assert!(
        base.store.deviations(10).await.unwrap()[0]
            .score
            .is_infinite()
    );
}

/// Отклонение потока в заданную минуту — для проверок группировки.
async fn spotted(base: &Base, stream: &Stream, at: Minute, value: f64) -> (i64, Deviation) {
    let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0, 5.0], value);
    let deviation = Deviation::new("logs", stream, "15m", at, verdict);
    base.store.spot(&deviation, at).await.unwrap();
    let loose = base.store.loose(10).await.unwrap();
    let mine = loose
        .into_iter()
        .find(|(_, it)| it.stream == *stream && it.at == at)
        .expect("отклонение не найдено");
    (mine.0, mine.1)
}

/// Поток сервиса с заданным именем.
fn service(name: &str) -> Stream {
    Stream::new(format!("{{host=\"node-01\",service=\"{name}\"}}"))
}

#[tokio::test]
async fn opens_an_incident_for_a_loose_deviation() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    let (id, found) = spotted(&base, &wifi(), at, 91.0).await;
    let (_, fresh) = base
        .store
        .attach(
            id,
            &Service::new("orders-api"),
            &Signature::of("timed out"),
            &found,
        )
        .await
        .unwrap();
    assert!(fresh);
}

#[tokio::test]
async fn keeps_one_incident_for_the_same_service_and_signature() {
    let base = Base::open();
    let first = minute("2026-08-17T10:15:00Z");
    for step in 0..3 {
        let at = first.back(-step);
        let (id, found) = spotted(&base, &wifi(), at, 91.0).await;
        base.store
            .attach(
                id,
                &Service::new("orders-api"),
                &Signature::of("timed out"),
                &found,
            )
            .await
            .unwrap();
    }
    assert_eq!(base.store.incidents(true, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn counts_every_deviation_of_an_incident() {
    let base = Base::open();
    let first = minute("2026-08-17T10:15:00Z");
    for step in 0..3 {
        let at = first.back(-step);
        let (id, found) = spotted(&base, &wifi(), at, 91.0).await;
        base.store
            .attach(
                id,
                &Service::new("orders-api"),
                &Signature::of("timed out"),
                &found,
            )
            .await
            .unwrap();
    }
    assert_eq!(base.store.incidents(true, 10).await.unwrap()[0].seen, 3);
}

#[tokio::test]
async fn separates_incidents_of_different_signatures() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    for (step, text) in ["timed out", "no space left"].iter().enumerate() {
        let when = at.back(-i64::try_from(step).unwrap());
        let (id, found) = spotted(&base, &wifi(), when, 91.0).await;
        base.store
            .attach(id, &Service::new("orders-api"), &Signature::of(text), &found)
            .await
            .unwrap();
    }
    assert_eq!(base.store.incidents(true, 10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn takes_a_deviation_out_of_the_loose_pile() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    let (id, found) = spotted(&base, &wifi(), at, 91.0).await;
    base.store
        .attach(
            id,
            &Service::new("orders-api"),
            &Signature::of("timed out"),
            &found,
        )
        .await
        .unwrap();
    assert!(base.store.loose(10).await.unwrap().is_empty());
}

#[tokio::test]
async fn closes_an_incident_that_went_quiet() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    let (id, found) = spotted(&base, &wifi(), at, 91.0).await;
    base.store
        .attach(
            id,
            &Service::new("orders-api"),
            &Signature::of("timed out"),
            &found,
        )
        .await
        .unwrap();
    base.store.hush(at.back(-30)).await.unwrap();
    assert!(base.store.incidents(true, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn keeps_a_closed_incident_in_the_history() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    let (id, found) = spotted(&base, &wifi(), at, 91.0).await;
    base.store
        .attach(
            id,
            &Service::new("orders-api"),
            &Signature::of("timed out"),
            &found,
        )
        .await
        .unwrap();
    base.store.hush(at.back(-30)).await.unwrap();
    assert_eq!(base.store.incidents(false, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn ties_incidents_that_began_together() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    let mut ids = Vec::new();
    for name in ["orders-api", "billing-api"] {
        let (id, found) = spotted(&base, &service(name), at, 91.0).await;
        ids.push(
            base.store
                .attach(id, &Service::new(name), &Signature::of("timed out"), &found)
                .await
                .unwrap()
                .0,
        );
    }
    base.store.link(ids[1], 300).await.unwrap();
    assert_eq!(base.store.related(ids[0]).await.unwrap(), vec![ids[1]]);
}

#[tokio::test]
async fn leaves_distant_incidents_untied() {
    let base = Base::open();
    let at = minute("2026-08-17T10:15:00Z");
    let mut ids = Vec::new();
    for (step, name) in ["orders-api", "billing-api"].iter().enumerate() {
        let when = at.back(-i64::try_from(step).unwrap() * 60);
        let (id, found) = spotted(&base, &service(name), when, 91.0).await;
        ids.push(
            base.store
                .attach(
                    id,
                    &Service::new(*name),
                    &Signature::of("timed out"),
                    &found,
                )
                .await
                .unwrap()
                .0,
        );
    }
    base.store.link(ids[1], 300).await.unwrap();
    assert!(base.store.related(ids[0]).await.unwrap().is_empty());
}
