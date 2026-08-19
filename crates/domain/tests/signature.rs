use autosre_domain::signature::groups;
use autosre_domain::{Service, Signature, Stream};

#[test]
fn masks_numbers() {
    assert_eq!(
        Signature::of("retry 7 of 30 failed").as_str(),
        "retry <n> of <n> failed"
    );
}

#[test]
fn masks_a_uuid() {
    assert_eq!(
        Signature::of("session 3f2504e0-4f89-11d3-9a0c-0305e82c3301 expired").as_str(),
        "session <uuid> expired"
    );
}

#[test]
fn masks_a_network_address() {
    assert_eq!(
        Signature::of("connect 192.0.2.12:9428 refused").as_str(),
        "connect <addr> refused"
    );
}

#[test]
fn masks_a_path() {
    assert_eq!(
        Signature::of("cannot write to /var/lib/docker/overlay2: no space").as_str(),
        "cannot write to <path> no space"
    );
}

#[test]
fn masks_a_long_hex_token() {
    assert_eq!(
        Signature::of("trace deadbeefcafebabe lost").as_str(),
        "trace <hex> lost"
    );
}

#[test]
fn squeezes_whitespace() {
    assert_eq!(
        Signature::of("  timeout\n\twhile\u{00a0}reading  ").as_str(),
        "timeout while reading"
    );
}

#[test]
fn clips_an_overlong_message() {
    assert_eq!(
        Signature::of(&"я".repeat(400)).as_str().chars().count(),
        201
    );
}

#[test]
fn joins_messages_differing_only_in_numbers() {
    let messages = vec![
        "upstream 192.0.2.19 timed out after 30s".to_owned(),
        "upstream 192.0.2.22 timed out after 45s".to_owned(),
    ];
    assert_eq!(groups(&messages, 5)[0].count, 2);
}

#[test]
fn ranks_the_frequent_group_first() {
    let messages = vec![
        "disk quota exceeded".to_owned(),
        "connection reset by peer".to_owned(),
        "connection reset by peer".to_owned(),
    ];
    assert_eq!(
        groups(&messages, 5)[0].signature.as_str(),
        "connection reset by peer"
    );
}

#[test]
fn keeps_at_most_the_asked_number_of_groups() {
    let messages: Vec<String> = (0..40)
        .map(|index| format!("failure kind {} raised", "z".repeat(index + 1)))
        .collect();
    assert_eq!(groups(&messages, 7).len(), 7);
}

#[test]
fn keeps_an_original_line_as_the_sample() {
    let messages = vec!["panic at worker 12: nil map".to_owned()];
    assert_eq!(
        groups(&messages, 5)[0].sample,
        "panic at worker 12: nil map"
    );
}

#[test]
fn takes_the_service_from_the_selector() {
    let stream = Stream::new("{host=\"node-01\",service=\"orders-api\"}");
    assert_eq!(
        Service::of(&stream, &["service".to_owned()]).as_str(),
        "orders-api"
    );
}

#[test]
fn prefers_the_earlier_label() {
    let stream = Stream::new("{container=\"proxy\",service=\"orders-api\"}");
    let labels = vec!["service".to_owned(), "container".to_owned()];
    assert_eq!(Service::of(&stream, &labels).as_str(), "orders-api");
}

#[test]
fn falls_back_to_the_container_label() {
    let stream = Stream::new("{host=\"node-01\",container=\"proxy\"}");
    let labels = vec!["service".to_owned(), "container".to_owned()];
    assert_eq!(Service::of(&stream, &labels).as_str(), "proxy");
}

#[test]
fn keeps_the_whole_selector_when_no_label_fits() {
    let stream = Stream::new("{host=\"node-01\"}");
    assert_eq!(
        Service::of(&stream, &["service".to_owned()]).as_str(),
        "{host=\"node-01\"}"
    );
}

#[test]
fn hides_the_address_of_a_host_in_a_lesson() {
    assert!(!autosre_domain::signature::hide("upstream 192.0.2.19 timed out").contains("192.0.2"));
}

#[test]
fn keeps_the_numbers_a_lesson_is_learned_from() {
    let hidden = autosre_domain::signature::hide("Значение за окно: 936500000, обычно: 1371500000");
    assert!(hidden.contains("936500000"));
}

#[test]
fn keeps_the_width_of_a_horizon_in_a_lesson() {
    assert!(autosre_domain::signature::hide("горизонт: 15m").contains("15m"));
}

#[test]
fn hides_a_token_that_only_looks_like_a_number() {
    assert!(!autosre_domain::signature::hide("session deadbeefcafe1234").contains("deadbeef"));
}

#[test]
fn keeps_the_path_that_ran_out_of_space() {
    assert!(
        autosre_domain::signature::hide("no space left on /var/lib/docker").contains("/var/lib")
    );
}
