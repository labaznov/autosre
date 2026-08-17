//! Сигнатура — сообщение об ошибке без переменных частей.
//!
//! Половина ключа инцидента и содержимое промпта. Сорок сырых строк в промпте
//! весят под тридцать тысяч токенов и заставляют модель срываться в повторы;
//! пять сигнатур со счётчиками говорят ровно то же самое.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

/// Предел длины сигнатуры в символах.
const LIMIT: usize = 200;

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
        let masked = UUID.replace_all(&squeezed, "<uuid>");
        let masked = ADDRESS.replace_all(&masked, "<addr>");
        let masked = PATH.replace_all(&masked, "<path>");
        let masked = HEX.replace_all(&masked, "<hex>");
        let masked = NUMBER.replace_all(&masked, "<n>");
        Self(clip(&masked, LIMIT))
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
