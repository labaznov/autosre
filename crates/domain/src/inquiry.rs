//! Заявка на диагностику: что выполнить, где и зачем.
//!
//! Части диагностики нет ни в логах, ни в метриках, а доступа на хосты у
//! агента нет и не будет на этом этапе
//! ([ADR-0011](../../../docs/adr/0011-diagnostic-requests.md)). Поэтому агент
//! оформляет заявку, а выполняет её человек.
//!
//! Отсюда единственное правило этого модуля: **предлагать можно только то, что
//! читает**. Дежурный в аварию копирует команду не читая, и цена ошибки здесь
//! не «неверный вывод», а «упавший прод по совету агента».

/// Команда, которая меняет систему, а не смотрит на неё.
#[derive(Debug, Clone, thiserror::Error)]
#[error("команда меняет систему, такое дежурному не предлагают: {0}")]
pub struct Meddles(pub String);

/// Слова, после которых команда перестаёт быть чтением.
///
/// Список ловит и глагол, и подкоманду: `systemctl status` — чтение,
/// `systemctl restart` — уже нет, и различить их можно только по второму слову.
///
/// Список заведомо строже нужного: `grep restart /var/log/syslog` он тоже
/// отвергнет. Это осознанный перекос — отвергнутая заявка стоит одного лишнего
/// захода в модель, пропущенная стоит прода.
const MEDDLING: &[&str] = &[
    "rm", "rmdir", "mv", "cp", "dd", "mkfs", "fsck", "truncate", "shred", "chmod", "chown",
    "chgrp", "ln", "kill", "pkill", "killall", "reboot", "shutdown", "halt", "poweroff",
    "iptables", "nft", "tee", "apt", "apt-get", "yum", "dnf", "apk", "pip", "npm", "restart",
    "stop", "start", "reload", "enable", "disable", "delete", "drop", "flush", "prune", "purge",
    "install", "remove", "write", "set",
];

/// Заявка: команда для человека и причина, зачем она расследованию.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inquiry {
    /// Где выполнить: имя хоста или контейнера словами.
    pub host: String,
    /// Что выполнить — одной строкой, готовой к копипасту.
    pub command: String,
    /// Зачем это расследованию.
    pub reason: String,
}

impl Inquiry {
    /// Оформляет заявку, если предложенная команда только читает.
    ///
    /// # Errors
    /// [`Meddles`], если команда меняет систему или пуста.
    pub fn new(host: &str, command: &str, reason: &str) -> Result<Self, Meddles> {
        let command = command.trim();
        if command.is_empty() || !reads(command) {
            return Err(Meddles(command.to_owned()));
        }
        Ok(Self {
            host: host.trim().to_owned(),
            command: command.to_owned(),
            reason: reason.trim().to_owned(),
        })
    }
}

/// Только ли читает команда.
///
/// Перенаправление вывода — запись, каким бы безобидным ни было то, что слева
/// от стрелки.
#[must_use]
pub fn reads(command: &str) -> bool {
    if command.contains('>') {
        return false;
    }
    !command
        .split(|it: char| it.is_whitespace() || "|;&()'\"`".contains(it))
        .map(|word| word.rsplit('/').next().unwrap_or(word))
        .map(|word| word.trim_start_matches('-'))
        .any(|word| MEDDLING.contains(&word))
}
