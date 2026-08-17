use sre_domain::{Detector, Deviation, Kind, Minute, Stream, Thresholds};
use sre_skills::{Skill, read};
use tempfile::TempDir;

/// Скилл, годный во всём: от него отталкиваются проверки.
const PLAIN: &str = r"---
name: error-burst
title: Всплеск ошибок в потоке
horizon: 15m
when:
  signal: errors
collect:
  - id: signatures
    vl: '_time:[{start}, {end}) {stream} | fields _msg'
    limit: 200
---

Сравни сигнатуры окна с прошлым окном.
";

fn deviation(source: &str, horizon: &str, stream: &str) -> Deviation {
    let stream = Stream::new(stream);
    let verdict = Detector::new(Thresholds::default()).verdict(&[4.0, 4.0], 91.0, Kind::Sum);
    Deviation::new(source, &stream, horizon, Minute::at(1_786_968_660), verdict)
}

fn wifi() -> Deviation {
    deviation("logs", "15m", "{host=\"node-01\",service=\"orders-api\"}")
}

#[test]
fn reads_the_name_of_a_skill() {
    assert_eq!(Skill::parse(PLAIN).unwrap().front.name, "error-burst");
}

#[test]
fn keeps_the_body_as_the_author_wrote_it() {
    assert_eq!(
        Skill::parse(PLAIN).unwrap().body,
        "Сравни сигнатуры окна с прошлым окном."
    );
}

#[test]
fn reads_the_queries_of_the_first_dossier() {
    assert_eq!(Skill::parse(PLAIN).unwrap().front.collect.len(), 1);
}

#[test]
fn refuses_a_file_without_frontmatter() {
    assert!(Skill::parse("просто текст без фронтматтера").is_err());
}

#[test]
fn refuses_a_frontmatter_without_a_name() {
    let broken = "---\ntitle: без имени\nhorizon: 15m\nwhen:\n  signal: errors\n---\nтело\n";
    assert!(Skill::parse(broken).is_err());
}

#[test]
fn fits_the_deviation_it_was_written_for() {
    assert!(Skill::parse(PLAIN).unwrap().fits(&wifi()));
}

#[test]
fn keeps_away_from_another_horizon() {
    let deviation = deviation("logs", "1h", "{service=\"orders-api\"}");
    assert!(!Skill::parse(PLAIN).unwrap().fits(&deviation));
}

#[test]
fn keeps_away_from_another_signal() {
    let deviation = deviation("metrics", "15m", "{job=\"llama\"}");
    assert!(!Skill::parse(PLAIN).unwrap().fits(&deviation));
}

#[test]
fn takes_a_metric_the_way_people_write_it() {
    let about = PLAIN
        .replace("  signal: errors", "  signal: metric")
        .replace("horizon: 15m", "horizon: 1h");
    let deviation = deviation("metrics", "1h", "{__series__=\"go_goroutines\"}");
    assert!(Skill::parse(&about).unwrap().fits(&deviation));
}

#[test]
fn hears_the_signals_it_knows() {
    assert!(Skill::parse(PLAIN).unwrap().heard());
}

#[test]
fn says_it_never_heard_of_a_made_up_signal() {
    let odd = PLAIN.replace("  signal: errors", "  signal: metrics");
    assert!(!Skill::parse(&odd).unwrap().heard());
}

#[test]
fn narrows_itself_by_the_stream() {
    let narrow = PLAIN.replace(
        "  signal: errors",
        "  signal: errors\n  stream: 'service=\"billing-api\"'",
    );
    assert!(!Skill::parse(&narrow).unwrap().fits(&wifi()));
}

#[test]
fn takes_the_stream_it_asked_for() {
    let narrow = PLAIN.replace(
        "  signal: errors",
        "  signal: errors\n  stream: 'service=\"orders-api\"'",
    );
    assert!(Skill::parse(&narrow).unwrap().fits(&wifi()));
}

#[test]
fn names_an_unknown_field_instead_of_choking_on_it() {
    let newer = PLAIN.replace("horizon: 15m", "horizon: 15m\ntelepathy: true");
    assert_eq!(Skill::parse(&newer).unwrap().unknown(), vec!["telepathy"]);
}

#[test]
fn reads_every_skill_of_a_directory() {
    let directory = TempDir::new().expect("временный каталог не создан");
    for name in ["one", "two"] {
        std::fs::write(
            directory.path().join(format!("{name}.md")),
            PLAIN.replace("error-burst", name),
        )
        .expect("скилл не записан");
    }
    assert_eq!(read(directory.path()).unwrap().len(), 2);
}

#[test]
fn skips_a_broken_skill_and_keeps_the_rest() {
    let directory = TempDir::new().expect("временный каталог не создан");
    std::fs::write(directory.path().join("good.md"), PLAIN).expect("скилл не записан");
    std::fs::write(directory.path().join("broken.md"), "мусор").expect("скилл не записан");
    assert_eq!(read(directory.path()).unwrap().len(), 1);
}

#[test]
fn ignores_files_that_are_not_skills() {
    let directory = TempDir::new().expect("временный каталог не создан");
    std::fs::write(directory.path().join("good.md"), PLAIN).expect("скилл не записан");
    std::fs::write(directory.path().join("README.txt"), "не скилл").expect("файл не записан");
    assert_eq!(read(directory.path()).unwrap().len(), 1);
}

#[test]
fn fills_the_window_into_a_query() {
    let filled = sre_skills::fill(
        "_time:[{start}, {end}) {stream}",
        &wifi(),
        "orders-api",
        "2026-08-17T10:00:00Z",
        "2026-08-17T10:15:00Z",
    );
    assert!(filled.starts_with("_time:[2026-08-17T10:00:00Z, 2026-08-17T10:15:00Z)"));
}

#[test]
fn keeps_a_stray_pipe_out_of_a_query() {
    let deviation = deviation("logs", "15m", "{service=\"a|drop\"}");
    let filled = sre_skills::fill("{stream}", &deviation, "a", "x", "y");
    assert!(!filled.contains('|'));
}

/// Скиллы поставки: стартовый набор, который едет вместе с агентом.
fn shipped() -> Vec<Skill> {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/knowledge/skills");
    read(&path).expect("скиллы поставки не прочитаны")
}

#[test]
fn reads_the_shipped_skills() {
    assert_eq!(shipped().len(), 5);
}

#[test]
fn covers_every_horizon_the_agent_raises_by_default() {
    let raised = ["15m", "1h", "24h"];
    assert!(
        raised
            .iter()
            .all(|horizon| shipped().iter().any(|it| it.front.horizon == *horizon))
    );
}

#[test]
fn hears_every_signal_of_the_shipped_skills() {
    assert!(shipped().iter().all(Skill::heard));
}
