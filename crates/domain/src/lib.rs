//! Предметная область агента: время, потоки и числа.
//!
//! Крейт не знает ни про источники, ни про базу, ни про модель. Всё, что здесь
//! есть, проверяется без сети и без файлов.

pub mod bucket;
pub mod minute;

pub use bucket::{Bucket, Kind, Stream};
pub use minute::{Hour, Minute, Span, SpanError};
