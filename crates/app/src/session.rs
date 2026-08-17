//! Вход по логину и паролю.
//!
//! Ролей нет: вошедший может всё ([ADR-0020](../../../docs/adr/0020-login-and-password.md)).
//! Учёток несколько, а не одна на смену: приглушения и приёмки черновиков
//! подписываются именем, и подпись «дежурный» обесценила бы обе.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordVerifier};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::config::Account;

/// Имя куки с сессией.
pub const COOKIE: &str = "sreagent";

/// Сколько живёт сессия.
const LIFE: Duration = Duration::from_hours(12);

/// Хранитель входа: знает учётки и умеет подписывать сессии.
#[derive(Debug, Clone)]
pub struct Doorman {
    accounts: Vec<Account>,
    key: Vec<u8>,
}

impl Doorman {
    #[must_use]
    pub fn new(accounts: Vec<Account>, key: &str) -> Self {
        Self {
            accounts,
            key: key.as_bytes().to_vec(),
        }
    }

    /// Проверяет пароль и выдаёт подписанную сессию.
    ///
    /// Ответ один на все случаи: нет такой учётки или неверный пароль — снаружи
    /// это неразличимо, иначе форма входа начинает подсказывать, какие имена
    /// существуют.
    #[must_use]
    pub fn admit(&self, login: &str, password: &str) -> Option<String> {
        let account = self.accounts.iter().find(|it| it.login == login)?;
        let hash = PasswordHash::new(&account.password).ok()?;
        Argon2::default()
            .verify_password(password.as_bytes(), &hash)
            .ok()?;
        Some(self.sign(login))
    }

    /// Имя вошедшего, если сессия цела и не просрочена.
    #[must_use]
    pub fn who(&self, session: &str) -> Option<String> {
        let (body, signature) = session.rsplit_once('.')?;
        if self.seal(body) != signature {
            return None;
        }
        let (login, until) = body.rsplit_once(':')?;
        let until: u64 = until.parse().ok()?;
        (until > now()).then(|| login.to_owned())
    }

    /// Есть ли вообще кого пускать.
    #[must_use]
    pub fn empty(&self) -> bool {
        self.accounts.is_empty()
    }

    fn sign(&self, login: &str) -> String {
        let body = format!("{login}:{}", now() + LIFE.as_secs());
        let signature = self.seal(&body);
        format!("{body}.{signature}")
    }

    fn seal(&self, body: &str) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).expect("ключ подписи любой длины годится");
        mac.update(body.as_bytes());
        BASE64.encode(mac.finalize().into_bytes())
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default()
}
