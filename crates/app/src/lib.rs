//! Агент диагностики: сборка настроек, веб-морды и собственных метрик.
//!
//! Крейт отдаёт наружу то, что нужно и бинарю, и тестам, чтобы поднять агента
//! на эфемерном порту и проверить его настоящими запросами.

pub mod check;
pub mod collector;
pub mod config;
pub mod digger;
pub mod grouper;
pub mod librarian;
pub mod metrics;
pub mod reporter;
pub mod rig;
pub mod scribe;
pub mod session;
pub mod tls;
pub mod view;
pub mod watcher;
pub mod web;

/// Версия агента, попадающая в здоровье и в метрики.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Коммит, из которого собран бинарь: его подставляет сборка бандла.
pub const COMMIT: Option<&str> = option_env!("AUTOSRE_COMMIT");

/// Версия словами: `autosre 0.2.0 (a1b2c3d)` или без коммита, если собрано
/// на месте.
#[must_use]
pub fn version() -> String {
    match COMMIT {
        Some(commit) => format!("autosre {VERSION} ({commit})"),
        None => format!("autosre {VERSION}"),
    }
}
