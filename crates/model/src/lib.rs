//! Модель за OpenAI-совместимым API.
//!
//! Форма ответа задаётся схемой, а не описывается словами в промпте:
//! `llama.cpp` превращает её в грамматику и удерживает на уровне декодирования
//! ([ADR-0007](../../../docs/adr/0007-local-inference.md)). Пилот описывал
//! форму текстом и выкусывал из ответа первый попавшийся `{...}` жадным
//! регулярным выражением — отсюда и разъезжающийся язык, и развал на двух
//! объектах.

pub mod prompt;
pub mod schema;

use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};
use url::Url;

/// Путь дополнения в OpenAI-совместимом API.
const ENDPOINT: &str = "v1/chat/completions";

/// Настройки доступа к модели.
#[derive(Debug, Clone)]
pub struct Settings {
    pub url: Url,
    pub key: String,
    pub name: String,
    pub temperature: f32,
    pub tokens: u32,
    pub timeout: Duration,
}

/// Отказы при обращении к модели.
#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    #[error("модель недоступна: {0}")]
    Transport(String),
    #[error("модель ответила {status}: {body}")]
    Status { status: u16, body: String },
    #[error("ответ модели пуст")]
    Empty,
    #[error("ответ модели не отвечает схеме: {0}")]
    Shape(String),
}

/// Решение отсева: стоит ли разбираться.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Triage {
    /// Разбирать или это привычный шум.
    pub worth: bool,
    /// Одна фраза почему — попадёт в карточку.
    pub because: String,
}

/// Вывод расследования.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Conclusion {
    /// Версия причины.
    pub cause: String,
    /// Насколько модель себе верит, от нуля до единицы.
    pub confidence: f64,
    /// Куда смотреть дежурному.
    pub advice: String,
    /// Чего не хватило: `logs`, `metrics`, `neighbours` или `nothing`.
    #[serde(default)]
    pub need: Option<String>,
}

/// Клиент модели.
#[derive(Debug, Clone)]
pub struct Model {
    http: reqwest::Client,
    endpoint: Url,
    settings: Settings,
}

impl Model {
    /// Собирает клиента под заданные настройки.
    ///
    /// # Errors
    /// [`ModelError::Transport`] на неразбираемом адресе или несобравшемся
    /// HTTP-клиенте.
    pub fn new(settings: Settings) -> Result<Self, ModelError> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(settings.timeout)
                .build()
                .map_err(|cause| ModelError::Transport(cause.to_string()))?,
            endpoint: settings
                .url
                .join(ENDPOINT)
                .map_err(|cause| ModelError::Transport(cause.to_string()))?,
            settings,
        })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.settings.name
    }

    /// Дешёвый отсев: стоит ли тратить на это расследование.
    ///
    /// # Errors
    /// [`ModelError`] при недоступности модели или ответе мимо схемы.
    pub async fn triage(&self, about: &str) -> Result<Triage, ModelError> {
        self.ask(prompt::SIFTER, about, schema::triage()).await
    }

    /// Разбор всплеска: причина, уверенность, рекомендация.
    ///
    /// # Errors
    /// [`ModelError`] при недоступности модели или ответе мимо схемы.
    pub async fn conclude(&self, skill: &str, dossier: &str) -> Result<Conclusion, ModelError> {
        let system = format!("{}\n\n{skill}", prompt::ANALYST);
        self.ask(&system, dossier, schema::conclusion()).await
    }

    /// Один заход в модель со схемой ответа.
    async fn ask<T: serde::de::DeserializeOwned>(
        &self,
        system: &str,
        user: &str,
        schema: Value,
    ) -> Result<T, ModelError> {
        tracing::debug!(
            model = self.settings.name,
            chars = user.len(),
            "запрос к модели"
        );
        let response = self
            .http
            .post(self.endpoint.clone())
            .bearer_auth(&self.settings.key)
            .json(&json!({
                "model": self.settings.name,
                "temperature": self.settings.temperature,
                "max_tokens": self.settings.tokens,
                "response_format": schema,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user}
                ]
            }))
            .send()
            .await
            .map_err(|cause| ModelError::Transport(cause.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|cause| ModelError::Transport(cause.to_string()))?;
        if !status.is_success() {
            return Err(ModelError::Status {
                status: status.as_u16(),
                body: clip(&body),
            });
        }
        serde_json::from_str::<Answer>(&body)
            .map_err(|cause| ModelError::Shape(cause.to_string()))?
            .content()
            .and_then(|content| {
                serde_json::from_str(bare(&content))
                    .map_err(|cause| ModelError::Shape(cause.to_string()))
            })
    }
}

/// Ответ API — ровно та его часть, что нужна агенту.
#[derive(Debug, Deserialize)]
struct Answer {
    choices: Vec<Choice>,
}

#[derive(Debug, Deserialize)]
struct Choice {
    message: Message,
}

#[derive(Debug, Deserialize)]
struct Message {
    content: Option<String>,
}

impl Answer {
    fn content(self) -> Result<String, ModelError> {
        self.choices
            .into_iter()
            .next()
            .and_then(|choice| choice.message.content)
            .filter(|content| !content.trim().is_empty())
            .ok_or(ModelError::Empty)
    }
}

/// Снимает обрамление тройными кавычками, если сервер проигнорировал схему.
fn bare(content: &str) -> &str {
    content
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim()
}

fn clip(text: &str) -> String {
    text.chars().take(300).collect()
}
