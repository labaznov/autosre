use autosre_domain::{Hour, Minute, Span, SpanError};
use chrono::{DateTime, Utc};

fn moment(text: &str) -> DateTime<Utc> {
    text.parse().expect("момент времени некорректен")
}

#[test]
fn aligns_a_moment_down_to_its_minute() {
    assert_eq!(
        Minute::of(moment("2026-08-17T10:07:59.900Z")).start(),
        moment("2026-08-17T10:07:00Z")
    );
}

#[test]
fn ends_where_the_next_one_starts() {
    let minute = Minute::of(moment("2026-08-17T10:07:23Z"));
    assert_eq!(minute.end(), minute.next().start());
}

#[test]
fn steps_back_the_asked_number_of_minutes() {
    assert_eq!(
        Minute::of(moment("2026-08-17T10:07:00Z")).back(15).start(),
        moment("2026-08-17T09:52:00Z")
    );
}

#[test]
fn aligns_a_stamp_that_sits_inside_a_minute() {
    assert_eq!(Minute::at(1_786_919_135).stamp(), 1_786_919_100);
}

#[test]
fn counts_the_minutes_of_a_span() {
    let from = Minute::of(moment("2026-08-17T10:00:00Z"));
    assert_eq!(Span::new(from, from.back(-15), 96).unwrap().len(), 15);
}

#[test]
fn walks_the_minutes_in_order() {
    let from = Minute::of(moment("2026-08-17T10:00:00Z"));
    let span = Span::new(from, from.back(-3), 96).unwrap();
    assert_eq!(
        span.minutes().last().copied().map(Minute::start),
        Some(moment("2026-08-17T10:02:00Z"))
    );
}

#[test]
fn holds_a_single_minute() {
    let minute = Minute::of(moment("2026-08-17T10:00:00Z"));
    assert_eq!(Span::single(minute).len(), 1);
}

#[test]
fn keeps_the_last_minute_outside() {
    let from = Minute::of(moment("2026-08-17T10:00:00Z"));
    let span = Span::new(from, from.back(-3), 96).unwrap();
    assert!(!span.contains(from.back(-3)));
}

#[test]
fn refuses_an_inverted_span() {
    let from = Minute::of(moment("2026-08-17T10:00:00Z"));
    assert_eq!(
        Span::new(from, from.back(5), 96).unwrap_err(),
        SpanError::Empty
    );
}

#[test]
fn refuses_a_span_beyond_the_limit() {
    let from = Minute::of(moment("2026-08-17T10:00:00Z"));
    assert_eq!(
        Span::new(from, from.back(-200), 96).unwrap_err(),
        SpanError::Oversized(200, 96)
    );
}

#[test]
fn folds_neighbouring_minutes_into_one_run() {
    let from = Minute::of(moment("2026-08-17T10:00:00Z"));
    let holes = vec![from, from.back(-1), from.back(-2)];
    assert_eq!(Span::runs(&holes, 60).len(), 1);
}

#[test]
fn breaks_a_run_where_the_series_is_whole() {
    let from = Minute::of(moment("2026-08-17T10:00:00Z"));
    let holes = vec![from, from.back(-1), from.back(-5), from.back(-6)];
    assert_eq!(Span::runs(&holes, 60).len(), 2);
}

#[test]
fn cuts_a_long_run_by_the_limit() {
    let from = Minute::of(moment("2026-08-17T10:00:00Z"));
    let holes: Vec<Minute> = (0..150).map(|step| from.back(-step)).collect();
    assert_eq!(Span::runs(&holes, 60).len(), 3);
}

#[test]
fn keeps_every_minute_of_the_runs() {
    let from = Minute::of(moment("2026-08-17T10:00:00Z"));
    let holes: Vec<Minute> = (0..150).map(|step| from.back(-step)).collect();
    assert_eq!(
        Span::runs(&holes, 60)
            .iter()
            .map(|run| run.len())
            .sum::<usize>(),
        150
    );
}

#[test]
fn puts_a_minute_into_its_hour() {
    assert_eq!(
        Hour::of(Minute::of(moment("2026-08-17T10:47:00Z"))).stamp(),
        Minute::of(moment("2026-08-17T10:00:00Z")).stamp()
    );
}

#[test]
fn holds_sixty_minutes_in_an_hour() {
    assert_eq!(
        Hour::of(Minute::of(moment("2026-08-17T10:47:00Z")))
            .span()
            .len(),
        60
    );
}
