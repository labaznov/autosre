//! Схемы ответов модели.
//!
//! Схема уходит в запрос как `response_format`. Описывать форму словами и
//! надеяться — это пилот; там из ответа выкусывался первый попавшийся `{...}`,
//! и на двух объектах разбор разваливался.

use serde_json::{Value, json};

/// Схема решения отсева.
#[must_use]
pub fn triage() -> Value {
    object(
        "triage",
        &json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["worth", "because"],
            "properties": {
                "worth": {"type": "boolean"},
                "because": {"type": "string", "maxLength": 200}
            }
        }),
    )
}

/// Схема вывода расследования.
///
/// `need` — просьба добрать данные. Меню закрытое: выдумать инструмент модель
/// не может, а неизвестное имя — отказ шага, а не расследования
/// ([ADR-0002](../../../docs/adr/0002-bounded-tools.md)).
#[must_use]
pub fn conclusion() -> Value {
    object(
        "conclusion",
        &json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["cause", "confidence", "advice"],
            "properties": {
                "cause": {"type": "string", "maxLength": 600},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "advice": {"type": "string", "maxLength": 400},
                "need": {
                    "type": "string",
                    "enum": ["logs", "metrics", "neighbours", "nothing"]
                }
            }
        }),
    )
}

/// Оборачивает схему в `response_format`, понятный OpenAI-совместимому API.
fn object(name: &str, schema: &Value) -> Value {
    json!({
        "type": "json_schema",
        "json_schema": {"name": name, "strict": true, "schema": schema}
    })
}
