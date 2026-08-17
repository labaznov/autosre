use sre_domain::{Detector, Kind, Thresholds};

/// Пороги, при которых видно поведение детектора, а не пороги.
fn plain() -> Thresholds {
    Thresholds {
        minimum: 20.0,
        score: 3.5,
        ratio: 2.0,
        drift: 0.15,
    }
}

#[test]
fn finds_a_burst_over_a_flat_baseline() {
    assert!(
        Detector::new(plain())
            .verdict(&[4.0, 3.0, 4.0, 5.0, 4.0, 3.0, 4.0], 91.0, Kind::Sum)
            .deviates
    );
}

#[test]
fn stays_quiet_over_a_calm_series() {
    assert!(
        !Detector::new(plain())
            .verdict(&[31.0, 29.0, 33.0, 30.0, 28.0, 32.0], 34.0, Kind::Sum)
            .deviates
    );
}

#[test]
fn survives_an_outlier_inside_the_baseline() {
    assert!(
        Detector::new(plain())
            .verdict(&[3.0, 5.0, 4.0, 120.0, 6.0, 4.0], 90.0, Kind::Sum)
            .deviates
    );
}

#[test]
fn keeps_the_baseline_where_the_mean_would_run_away() {
    let history = [3.0, 5.0, 4.0, 120.0, 6.0, 4.0];
    assert!(
        Detector::new(plain())
            .verdict(&history, 90.0, Kind::Sum)
            .baseline
            < 10.0
    );
}

#[test]
fn ignores_a_burst_under_the_minimum() {
    assert!(
        !Detector::new(plain())
            .verdict(&[0.0, 0.0, 1.0, 0.0, 0.0], 17.0, Kind::Sum)
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
        .verdict(&[20.0, 21.0, 19.0, 20.0], 45.0, Kind::Sum)
        .deviates
    );
}

#[test]
fn reports_the_median_as_the_baseline() {
    assert!(
        (Detector::new(plain())
            .verdict(&[7.0, 2.0, 9.0, 4.0], 61.0, Kind::Sum)
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
            .verdict(&[6.0, 6.0, 6.0, 6.0], 77.0, Kind::Sum)
            .score
            .is_infinite()
    );
}

#[test]
fn keeps_the_numbers_that_led_to_the_verdict() {
    let verdict = Detector::new(plain()).verdict(&[4.0, 4.0, 5.0, 4.0], 91.0, Kind::Sum);
    assert!((verdict.value - 91.0).abs() < f64::EPSILON);
}

#[test]
fn weighs_a_bigger_excess_heavier() {
    let detector = Detector::new(plain());
    let small = detector
        .verdict(&[4.0, 4.0, 5.0, 4.0], 40.0, Kind::Sum)
        .weight;
    assert!(
        detector
            .verdict(&[4.0, 4.0, 5.0, 4.0], 400.0, Kind::Sum)
            .weight
            > small
    );
}

#[test]
fn weighs_the_unusual_heavier_than_the_merely_big() {
    let detector = Detector::new(plain());
    let usual = detector
        .verdict(&[300.0, 310.0, 290.0, 305.0], 400.0, Kind::Sum)
        .weight;
    let unusual = detector
        .verdict(&[4.0, 4.0, 5.0, 4.0], 400.0, Kind::Sum)
        .weight;
    assert!(unusual > usual);
}

#[test]
fn keeps_a_silent_baseline_from_outweighing_a_real_burst() {
    let detector = Detector::new(plain());
    let tiny = detector
        .verdict(&[0.0, 0.0, 0.0, 0.0], 21.0, Kind::Sum)
        .weight;
    let real = detector
        .verdict(&[10.0, 12.0, 9.0, 11.0], 900.0, Kind::Sum)
        .weight;
    assert!(real > tiny);
}

#[test]
fn finds_a_level_that_crept_up() {
    let history = [457.0, 460.0, 463.0, 466.0, 470.0, 474.0];
    assert!(
        Detector::new(plain())
            .verdict(&history, 564.0, Kind::Mean)
            .deviates
    );
}

#[test]
fn misses_that_creep_by_the_counter_rule() {
    let history = [457.0, 460.0, 463.0, 466.0, 470.0, 474.0];
    assert!(
        !Detector::new(plain())
            .verdict(&history, 564.0, Kind::Sum)
            .deviates
    );
}

#[test]
fn ignores_a_level_that_barely_moved() {
    let history = [457.0, 460.0, 463.0, 466.0, 470.0, 474.0];
    assert!(
        !Detector::new(plain())
            .verdict(&history, 480.0, Kind::Mean)
            .deviates
    );
}

#[test]
fn notices_a_level_that_fell() {
    let history = [2000.0, 1900.0, 1800.0, 1700.0, 1600.0, 1500.0];
    assert!(
        Detector::new(plain())
            .verdict(&history, 900.0, Kind::Mean)
            .deviates
    );
}

#[test]
fn weighs_a_falling_level_like_a_rising_one() {
    let detector = Detector::new(plain());
    let up = detector
        .verdict(&[100.0, 100.0, 100.0], 150.0, Kind::Mean)
        .weight;
    let down = detector
        .verdict(&[100.0, 100.0, 100.0], 50.0, Kind::Mean)
        .weight;
    assert!((up - down).abs() < f64::EPSILON);
}

#[test]
fn leaves_a_falling_counter_alone() {
    assert!(
        !Detector::new(plain())
            .verdict(&[100.0, 100.0, 100.0], 10.0, Kind::Sum)
            .deviates
    );
}

#[test]
fn finds_a_steady_creep_the_score_would_miss() {
    let history = [583.0, 529.0, 475.0, 421.0];
    assert!(
        Detector::new(plain())
            .verdict(&history, 637.0, Kind::Mean)
            .deviates
    );
}

#[test]
fn finds_a_disk_that_keeps_filling() {
    let history = [1200.0, 1500.0, 1800.0, 2000.0];
    assert!(
        Detector::new(plain())
            .verdict(&history, 900.0, Kind::Mean)
            .deviates
    );
}

#[test]
fn ignores_a_level_that_only_jitters() {
    let history = [500.0, 495.0, 505.0, 498.0];
    assert!(
        !Detector::new(plain())
            .verdict(&history, 503.0, Kind::Mean)
            .deviates
    );
}

#[test]
fn finds_a_sudden_jump_of_a_level() {
    let history = [500.0, 495.0, 505.0, 498.0];
    assert!(
        Detector::new(plain())
            .verdict(&history, 900.0, Kind::Mean)
            .deviates
    );
}
