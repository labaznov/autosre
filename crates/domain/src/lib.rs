//! Предметная область агента: время, потоки и числа.
//!
//! Крейт не знает ни про источники, ни про базу, ни про модель. Всё, что здесь
//! есть, проверяется без сети и без файлов.

pub mod bucket;
pub mod detector;
pub mod deviation;
pub mod incident;
pub mod inquiry;
pub mod minute;
pub mod signature;

pub use bucket::{Bucket, Kind, Stream};
pub use detector::{Detector, Thresholds, Verdict};
pub use deviation::Deviation;
pub use incident::{Incident, Service, State, Tally};
pub use inquiry::{Inquiry, Meddles};
pub use minute::{Hour, Minute, Span, SpanError};
pub use signature::{Group, Signature};
