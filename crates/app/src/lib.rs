//! Агент диагностики: сборка настроек, веб-морды и собственных метрик.
//!
//! Крейт отдаёт наружу то, что нужно и бинарю, и тестам, чтобы поднять агента
//! на эфемерном порту и проверить его настоящими запросами.

pub mod collector;
pub mod config;
pub mod grouper;
pub mod metrics;
pub mod watcher;
pub mod web;

/// Версия агента, попадающая в здоровье и в метрики.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
