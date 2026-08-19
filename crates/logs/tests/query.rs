use autosre_domain::{Minute, Span};
use autosre_logs::{Filter, FilterError};
use chrono::{DateTime, Utc};

fn moment(text: &str) -> DateTime<Utc> {
    text.parse().expect("момент времени некорректен")
}

fn quarter() -> Span {
    let from = Minute::of(moment("2026-08-17T10:00:00Z"));
    Span::new(from, from.back(-15), 96).expect("промежуток некорректен")
}

#[test]
fn refuses_an_empty_pattern() {
    assert_eq!(Filter::new("   ", vec![]).unwrap_err(), FilterError::Empty);
}

#[test]
fn wraps_the_error_pattern_in_parentheses() {
    let filter = Filter::new("i(error*) OR i(panic*)", vec![]).unwrap();
    assert!(
        filter
            .buckets(quarter())
            .contains("(i(error*) OR i(panic*))")
    );
}

#[test]
fn bounds_the_span_from_both_ends() {
    let filter = Filter::new("i(error*)", vec![]).unwrap();
    assert!(
        filter
            .buckets(quarter())
            .starts_with("_time:[2026-08-17T10:00:00Z, 2026-08-17T10:15:00Z)")
    );
}

#[test]
fn excludes_the_own_streams_of_the_agent() {
    let filter = Filter::new("i(error*)", vec!["{container=\"autosre\"}".to_owned()]).unwrap();
    assert!(
        filter
            .buckets(quarter())
            .contains("NOT (_stream:{container=\"autosre\"})")
    );
}

#[test]
fn excludes_every_own_stream() {
    let filter = Filter::new(
        "i(error*)",
        vec![
            "{container=\"autosre\"}".to_owned(),
            "{container=\"litellm\"}".to_owned(),
        ],
    )
    .unwrap();
    assert!(
        filter
            .buckets(quarter())
            .contains("NOT (_stream:{container=\"autosre\"} OR _stream:{container=\"litellm\"})")
    );
}

#[test]
fn groups_counts_by_stream_and_minute() {
    let filter = Filter::new("i(error*)", vec![]).unwrap();
    assert!(
        filter
            .buckets(quarter())
            .ends_with("| stats by (_stream, _time:1m) count() as total")
    );
}
