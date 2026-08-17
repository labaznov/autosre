use sre_domain::{Detector, Thresholds};

/// Пороги, при которых видно поведение детектора, а не пороги.
fn plain() -> Thresholds {
    Thresholds {
        minimum: 20.0,
        score: 3.5,
        ratio: 2.0,
    }
}

#[test]
fn finds_a_burst_over_a_flat_baseline() {
    assert!(
        Detector::new(plain())
            .verdict(&[4.0, 3.0, 4.0, 5.0, 4.0, 3.0, 4.0], 91.0)
            .deviates
    );
}

#[test]
fn stays_quiet_over_a_calm_series() {
    assert!(
        !Detector::new(plain())
            .verdict(&[31.0, 29.0, 33.0, 30.0, 28.0, 32.0], 34.0)
            .deviates
    );
}

#[test]
fn survives_an_outlier_inside_the_baseline() {
    assert!(
        Detector::new(plain())
            .verdict(&[3.0, 5.0, 4.0, 120.0, 6.0, 4.0], 90.0)
            .deviates
    );
}

#[test]
fn keeps_the_baseline_where_the_mean_would_run_away() {
    let history = [3.0, 5.0, 4.0, 120.0, 6.0, 4.0];
    assert!(Detector::new(plain()).verdict(&history, 90.0).baseline < 10.0);
}

#[test]
fn ignores_a_burst_under_the_minimum() {
    assert!(
        !Detector::new(plain())
            .verdict(&[0.0, 0.0, 1.0, 0.0, 0.0], 17.0)
            .deviates
    );
}

#[test]
fn ignores_a_rise_short_of_the_ratio() {
    assert!(
        !Detector::new(Thresholds {
            ratio: 3.0,
            ..plain()
        })
        .verdict(&[20.0, 21.0, 19.0, 20.0], 45.0)
        .deviates
    );
}

#[test]
fn reports_the_median_as_the_baseline() {
    assert!(
        (Detector::new(plain())
            .verdict(&[7.0, 2.0, 9.0, 4.0], 61.0)
            .baseline
            - 5.5)
            .abs()
            < f64::EPSILON
    );
}

#[test]
fn scores_a_flat_baseline_as_endless() {
    assert!(
        Detector::new(plain())
            .verdict(&[6.0, 6.0, 6.0, 6.0], 77.0)
            .score
            .is_infinite()
    );
}

#[test]
fn keeps_the_numbers_that_led_to_the_verdict() {
    let verdict = Detector::new(plain()).verdict(&[4.0, 4.0, 5.0, 4.0], 91.0);
    assert!((verdict.value - 91.0).abs() < f64::EPSILON);
}

#[test]
fn weighs_a_bigger_excess_heavier() {
    let detector = Detector::new(plain());
    let small = detector.verdict(&[4.0, 4.0, 5.0, 4.0], 40.0).weight;
    assert!(detector.verdict(&[4.0, 4.0, 5.0, 4.0], 400.0).weight > small);
}

#[test]
fn weighs_the_unusual_heavier_than_the_merely_big() {
    let detector = Detector::new(plain());
    let usual = detector
        .verdict(&[300.0, 310.0, 290.0, 305.0], 400.0)
        .weight;
    let unusual = detector.verdict(&[4.0, 4.0, 5.0, 4.0], 400.0).weight;
    assert!(unusual > usual);
}

#[test]
fn keeps_a_silent_baseline_from_outweighing_a_real_burst() {
    let detector = Detector::new(plain());
    let tiny = detector.verdict(&[0.0, 0.0, 0.0, 0.0], 21.0).weight;
    let real = detector.verdict(&[10.0, 12.0, 9.0, 11.0], 900.0).weight;
    assert!(real > tiny);
}
