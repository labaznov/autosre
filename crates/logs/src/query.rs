//! Сборка запросов `LogsQL`.
//!
//! Единственный путь к тексту запроса: голого шаблона ошибок снаружи крейта
//! нет. В пилоте запрос собирался в двух местах, и в одном из них потерялись
//! скобки вокруг OR-цепочки — фильтр времени приклеился только к первому терму,
//! а остальные ветки считались по всей базе. Здесь такой развилки нет.

use autosre_domain::Span;
use chrono::{DateTime, Utc};

/// Шаблон ошибок и селекторы собственных потоков агента.
#[derive(Debug, Clone)]
pub struct Filter {
    pattern: String,
    exclude: Vec<String>,
}

/// Отказы построения фильтра.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FilterError {
    #[error("шаблон ошибок пуст")]
    Empty,
}

impl Filter {
    /// Собирает фильтр из шаблона ошибок и селекторов собственных потоков.
    ///
    /// Агент пишет в те же логи, которые читает: без исключения собственных
    /// потоков он находит свои записи об ошибках чужих сервисов, разбирает их и
    /// пишет об этом снова.
    ///
    /// # Errors
    /// [`FilterError::Empty`] на пустом шаблоне.
    pub fn new(pattern: &str, exclude: Vec<String>) -> Result<Self, FilterError> {
        if pattern.trim().is_empty() {
            return Err(FilterError::Empty);
        }
        Ok(Self {
            pattern: pattern.trim().to_owned(),
            exclude,
        })
    }

    /// Запрос счётчиков по всем потокам, разложенных по минутам.
    ///
    /// Одно обращение приносит и все потоки, и все минуты промежутка: считает
    /// источник, а не агент.
    #[must_use]
    pub fn buckets(&self, span: Span) -> String {
        format!(
            "_time:[{}, {}) ({}){} | stats by (_stream, _time:1m) count() as total",
            stamp(span.from().start()),
            stamp(span.to().start()),
            self.pattern,
            self.foreign()
        )
    }

    /// Запрос живых записей одного потока за промежуток.
    #[must_use]
    pub fn samples(&self, stream: &str, span: Span) -> String {
        format!(
            "_time:[{}, {}) ({}){} AND _stream:{stream} | fields _msg",
            stamp(span.from().start()),
            stamp(span.to().start()),
            self.pattern,
            self.foreign()
        )
    }

    /// Отсечение собственных потоков; пустая строка, когда исключать нечего.
    fn foreign(&self) -> String {
        if self.exclude.is_empty() {
            return String::new();
        }
        let own = self
            .exclude
            .iter()
            .map(|selector| format!("_stream:{selector}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        format!(" NOT ({own})")
    }
}

/// Момент времени в форме, которую понимает `LogsQL`.
fn stamp(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}
