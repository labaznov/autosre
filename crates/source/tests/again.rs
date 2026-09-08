use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autosre_source::again;

const QUICK: [Duration; 2] = [Duration::from_millis(5), Duration::from_millis(5)];

#[tokio::test]
async fn returns_the_first_success() {
    let tries = AtomicUsize::new(0);
    let got: Result<u32, &str> = again(&QUICK, Result::is_err, || async {
        tries.fetch_add(1, Ordering::Relaxed);
        Ok(7)
    })
    .await;
    assert_eq!((got, tries.load(Ordering::Relaxed)), (Ok(7), 1));
}

#[tokio::test]
async fn tries_again_while_the_failure_is_passing() {
    let tries = AtomicUsize::new(0);
    let got: Result<u32, &str> = again(
        &QUICK,
        |_| true,
        || async {
            if tries.fetch_add(1, Ordering::Relaxed) < 2 {
                Err("занято")
            } else {
                Ok(3)
            }
        },
    )
    .await;
    assert_eq!(got, Ok(3));
}

#[tokio::test]
async fn gives_up_when_the_pauses_run_out() {
    let tries = AtomicUsize::new(0);
    let got: Result<u32, &str> = again(
        &QUICK,
        |_| true,
        || async {
            tries.fetch_add(1, Ordering::Relaxed);
            Err("занято")
        },
    )
    .await;
    assert_eq!((got, tries.load(Ordering::Relaxed)), (Err("занято"), 3));
}

#[tokio::test]
async fn does_not_repeat_a_lasting_failure() {
    let tries = AtomicUsize::new(0);
    let got: Result<u32, &str> = again(
        &QUICK,
        |_| false,
        || async {
            tries.fetch_add(1, Ordering::Relaxed);
            Err("схема")
        },
    )
    .await;
    assert_eq!((got, tries.load(Ordering::Relaxed)), (Err("схема"), 1));
}

#[tokio::test]
async fn waits_the_pause_between_tries() {
    let began = std::time::Instant::now();
    let _: Result<u32, &str> = again(
        &[Duration::from_millis(40)],
        |_| true,
        || async { Err("занято") },
    )
    .await;
    assert!(began.elapsed() >= Duration::from_millis(40));
}
