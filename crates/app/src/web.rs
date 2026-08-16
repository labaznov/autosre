//! Веб-морда агента.
//!
//! Пока только то, что доступно без входа: здоровье и собственные метрики.
//! Всё остальное появится вместе с лентой инцидентов и потребует логина.

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use crate::metrics::Metrics;

/// Разделяемое обработчиками состояние.
#[derive(Clone)]
pub struct Shared {
    metrics: Arc<Metrics>,
    version: &'static str,
}

impl Shared {
    #[must_use]
    pub fn new(metrics: Arc<Metrics>, version: &'static str) -> Self {
        Self { metrics, version }
    }

    #[must_use]
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

/// Маршруты, открытые без входа.
pub fn routes(shared: Shared) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/metrics", get(expose))
        .with_state(shared)
}

async fn health(State(shared): State<Shared>) -> Response {
    Json(serde_json::json!({
        "status": "ok",
        "version": shared.version,
    }))
    .into_response()
}

async fn expose(State(shared): State<Shared>) -> Response {
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        shared.metrics.expose(),
    )
        .into_response()
}
