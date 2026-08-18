//! Сигнатура — сообщение об ошибке без переменных частей.
//!
//! Половина ключа инцидента и содержимое промпта. Сорок сырых строк в промпте
//! весят под тридцать тысяч токенов и заставляют модель срываться в повторы;
//! пять сигнатур со счётчиками говорят ровно то же самое.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::bucket::Stream;

/// Предел длины сигнатуры в символах.
const LIMIT: usize = 200;

/// Чем сигнатура выделенного потока отличается от родительской.
///
/// Пара «сервис плюс сигнатура» обязана оставаться единственной среди открытых
/// ([ADR-0017](../../../docs/adr/0017-narrow-grouping.md)), поэтому выделенное
/// должно отличаться хоть чем-то. Разделитель живёт здесь, в одном месте: по
/// нему же инцидент узнаёт свои подтверждения, а приглушение — своих детей.
const APART: &str = " · ";

static UUID: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
        .expect("шаблон uuid некорректен")
});
static ADDRESS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new("[0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}(:[0-9]+)?")
        .expect("шаблон адреса некорректен")
});
static PATH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("/[^ \t]{3,}").expect("шаблон пути некорректен"));
static HEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("(0x)?[0-9a-fA-F]{8,}").expect("шаблон hex некорректен"));
static NUMBER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("[0-9]+").expect("шаблон числа некорректен"));

/// Нормализованное сообщение об ошибке.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Signature(String);

impl Signature {
    /// Маскирует изменчивые части сообщения и подрезает длину.
    ///
    /// Порядок важен: сначала то, что длиннее и специфичнее, иначе маска чисел
    /// съест половину адреса и склеит несклеиваемое.
    #[must_use]
    pub fn of(message: &str) -> Self {
        let squeezed = message.split_whitespace().collect::<Vec<_>>().join(" ");
        Self(clip(&mask(&squeezed), LIMIT))
    }

    /// Сигнатура выделенного потока: родительская плюс сам поток.
    #[must_use]
    pub fn apart(&self, stream: &Stream) -> Self {
        Self(format!("{}{APART}{}", self.0, stream.as_str()))
    }

    /// Восстанавливает сигнатуру, нормализованную когда-то раньше.
    #[must_use]
    pub fn stored(text: String) -> Self {
        Self(text)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Signature {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str(&self.0)
    }
}

/// Заменяет изменчивые части текста заглушками.
///
/// Порядок важен: сначала то, что длиннее и специфичнее, иначе маска чисел
/// съест половину адреса и склеит несклеиваемое.
///
/// Отдельно от [`Signature::of`], потому что маскировать приходится не только
/// сообщение: в корпусе для дообучения тем же способом гасятся адреса и
/// идентификаторы, а длину там резать нельзя — обрезанный пример учит плохому.
#[must_use]
pub fn mask(text: &str) -> String {
    let masked = UUID.replace_all(text, "<uuid>");
    let masked = ADDRESS.replace_all(&masked, "<addr>");
    let masked = PATH.replace_all(&masked, "<path>");
    let masked = HEX.replace_all(&masked, "<hex>");
    NUMBER.replace_all(&masked, "<n>").into_owned()
}

/// Прячет то, что указывает на конкретную машину и человека, — и только это.
///
/// Не то же самое, что [`mask`], и разница принципиальная. Для сигнатуры число
/// — шум: «таймаут 30s» и «таймаут 45s» об одном и том же. Для обучения число
/// — единственный сигнал: отсев решает по величинам, и пример, в котором вместо
/// «936 500 000 против 1 371 500 000» стоит «столько-то против столько-то»,
/// не учит ничему ([ADR-0027](../../../docs/adr/0027-live-corpus.md)).
///
/// Поэтому здесь гасятся адреса, идентификаторы и длинные шестнадцатеричные
/// токены — то, по чему узнают хозяйство, — а числа и пути остаются: без них
/// не понять ни величины, ни какой раздел кончился.
#[must_use]
pub fn hide(text: &str) -> String {
    let hidden = UUID.replace_all(text, "<uuid>");
    let hidden = ADDRESS.replace_all(&hidden, "<addr>");
    HEX.replace_all(&hidden, |caught: &regex::Captures| {
        // Длинное число — это число, а не идентификатор. Токеном его делает
        // буква: `deadbeef` прячем, `936500000` оставляем.
        let token = &caught[0];
        if token.chars().any(|it| it.is_ascii_alphabetic()) {
            "<hex>".to_owned()
        } else {
            token.to_owned()
        }
    })
    .into_owned()
}

/// Группа сообщений с одной сигнатурой: счётчик и живой образец.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Group {
    pub signature: Signature,
    pub count: usize,
    pub sample: String,
}

/// Схлопывает сообщения в группы, отдавая `limit` самых частых.
///
/// Порядок при равных счётчиках задаётся сигнатурой: промпт не должен меняться
/// от запуска к запуску на одних и тех же данных.
#[must_use]
pub fn groups(messages: &[String], limit: usize) -> Vec<Group> {
    let mut counts: HashMap<Signature, (usize, &String)> = HashMap::new();
    for message in messages {
        let entry = counts.entry(Signature::of(message)).or_insert((0, message));
        entry.0 += 1;
    }
    let mut groups: Vec<Group> = counts
        .into_iter()
        .map(|(signature, (count, sample))| Group {
            signature,
            count,
            sample: clip(sample, LIMIT),
        })
        .collect();
    groups.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.signature.cmp(&right.signature))
    });
    groups.truncate(limit);
    groups
}

/// Обрезает текст по границе символа, отмечая усечение многоточием.
fn clip(text: &str, limit: usize) -> String {
    match text.char_indices().nth(limit) {
        None => text.to_owned(),
        Some((cut, _)) => format!("{}…", &text[..cut]),
    }
}
