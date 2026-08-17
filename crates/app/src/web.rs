//! Веб-морда агента.
//!
//! Здоровье и собственные метрики открыты — их спрашивает мониторинг. Всё
//! остальное требует входа: на экране содержимое прод-логов
//! ([ADR-0020](../../../docs/adr/0020-login-and-password.md)).

use std::sync::Arc;

use askama::Template;
use axum::extract::{Form, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use sre_domain::Minute;
use sre_store::Store;

use crate::config::Knowledge;
use crate::metrics::Metrics;
use crate::session::{COOKIE, Doorman};
use crate::view::{Card, Paper, Question};

/// Стили: вшиты в бинарь, чтобы образ оставался одним файлом.
const STYLE: &str = include_str!("../static/style.css");

/// Разделяемое обработчиками состояние.
#[derive(Clone)]
pub struct Shared {
    metrics: Arc<Metrics>,
    store: Store,
    doorman: Arc<Doorman>,
    knowledge: Knowledge,
    version: &'static str,
}

impl Shared {
    #[must_use]
    pub fn new(
        metrics: Arc<Metrics>,
        store: Store,
        doorman: Doorman,
        knowledge: &Knowledge,
        version: &'static str,
    ) -> Self {
        Self {
            metrics,
            store,
            doorman: Arc::new(doorman),
            knowledge: knowledge.clone(),
            version,
        }
    }

    #[must_use]
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

/// Все маршруты агента.
pub fn routes(shared: Shared) -> Router {
    Router::new()
        .route("/", get(feed))
        .route("/incident/{id}", get(card))
        .route("/incident/{id}/verdict", post(verdict))
        .route("/inquiry/{id}/answer", post(answer))
        .route("/waiting", get(waiting))
        .route("/draft/{id}/settle", post(settle))
        .route("/api/metrics", get(quality))
        .route("/login", get(door).post(enter))
        .route("/logout", post(leave))
        .route("/static/style.css", get(style))
        .route("/api/health", get(health))
        .route("/metrics", get(expose))
        .with_state(shared)
}

#[derive(Template)]
#[template(path = "feed.html")]
struct Feed {
    incidents: Vec<Card>,
    who: String,
    waiting: usize,
}

#[derive(Template)]
#[template(path = "card.html")]
struct Single {
    incident: Card,
    who: String,
    waiting: usize,
}

#[derive(Template)]
#[template(path = "waiting.html")]
struct Duty {
    inquiries: Vec<Question>,
    drafts: Vec<Paper>,
    who: String,
    waiting: usize,
}

#[derive(Template)]
#[template(path = "login.html")]
struct Door {
    failed: bool,
}

#[derive(Debug, Deserialize)]
struct Credentials {
    login: String,
    password: String,
}

/// Лента инцидентов.
async fn feed(State(shared): State<Shared>, headers: HeaderMap) -> Response {
    let Some(who) = guard(&shared, &headers) else {
        return Redirect::to("/login").into_response();
    };
    match shared.store.incidents(false, 100).await {
        Ok(incidents) => {
            let mut cards = Vec::with_capacity(incidents.len());
            for incident in incidents {
                let related = shared.store.related(incident.id).await.unwrap_or_default();
                let finding = shared
                    .store
                    .conclusion(incident.id)
                    .await
                    .unwrap_or_default();
                cards.push(Card::of(incident, related, finding.as_ref()));
            }
            render(&Feed {
                incidents: cards,
                who,
                waiting: waits(&shared).await,
            })
        }
        Err(broken) => failure(StatusCode::INTERNAL_SERVER_ERROR, &broken.to_string()),
    }
}

/// Карточка одного инцидента.
async fn card(State(shared): State<Shared>, headers: HeaderMap, Path(id): Path<i64>) -> Response {
    let Some(who) = guard(&shared, &headers) else {
        return Redirect::to("/login").into_response();
    };
    match shared.store.incidents(false, 500).await {
        Ok(incidents) => match incidents.into_iter().find(|it| it.id == id) {
            Some(incident) => {
                let related = shared.store.related(id).await.unwrap_or_default();
                let finding = shared.store.conclusion(id).await.unwrap_or_default();
                let asked = shared.store.inquiries(id).await.unwrap_or_default();
                let steps = match &finding {
                    Some(found) => shared.store.steps(found.id).await.unwrap_or_default(),
                    None => Vec::new(),
                };
                let drafts = shared.store.drafts(id).await.unwrap_or_default();
                render(&Single {
                    incident: Card::of(incident, related, finding.as_ref())
                        .asking(&asked)
                        .reading(&steps)
                        .drafting(&drafts),
                    who,
                    waiting: waits(&shared).await,
                })
            }
            None => failure(StatusCode::NOT_FOUND, "инцидент не найден"),
        },
        Err(broken) => failure(StatusCode::INTERNAL_SERVER_ERROR, &broken.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct Judgement {
    useful: String,
}

/// Оценка инцидента дежурным — единственный источник метрики точности.
async fn verdict(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Form(given): Form<Judgement>,
) -> Response {
    let Some(who) = guard(&shared, &headers) else {
        return Redirect::to("/login").into_response();
    };
    let useful = given.useful == "yes";
    match shared
        .store
        .judge(id, useful, &who, Minute::of(Utc::now()))
        .await
    {
        Ok(true) => {
            tracing::info!(incident = id, useful, who, "инцидент оценён");
            Redirect::to(&format!("/incident/{id}")).into_response()
        }
        Ok(false) => failure(StatusCode::NOT_FOUND, "инцидент не найден"),
        Err(broken) => failure(StatusCode::INTERNAL_SERVER_ERROR, &broken.to_string()),
    }
}

/// Всё, что ждёт руки дежурного.
///
/// Одно место на все заявки: искать их по карточкам — значит не находить.
async fn waiting(State(shared): State<Shared>, headers: HeaderMap) -> Response {
    let Some(who) = guard(&shared, &headers) else {
        return Redirect::to("/login").into_response();
    };
    let asked = pending(&shared).await;
    let drafts = shared.store.unsettled(100).await.unwrap_or_default();
    let waiting = asked.len() + drafts.len();
    render(&Duty {
        inquiries: asked.iter().map(Question::of).collect(),
        drafts: drafts.iter().map(Paper::of).collect(),
        who,
        waiting,
    })
}

/// Открытые заявки, самые старые первыми.
async fn pending(shared: &Shared) -> Vec<sre_store::Asked> {
    shared.store.pending(100).await.unwrap_or_default()
}

/// Сколько всего ждёт руки дежурного: заявки плюс непринятые черновики.
async fn waits(shared: &Shared) -> usize {
    pending(shared).await.len() + shared.store.unsettled(100).await.unwrap_or_default().len()
}

#[derive(Debug, Deserialize)]
struct Settling {
    accept: String,
}

/// Приёмка черновика: только принятая заметка попадает в базу и в индекс.
///
/// Правит текст дежурный сам, в своём редакторе: агент не умеет писать за него
/// и не должен делать вид ([ADR-0009](../../../docs/adr/0009-drafts-before-knowledge.md)).
async fn settle(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Form(given): Form<Settling>,
) -> Response {
    let Some(who) = guard(&shared, &headers) else {
        return Redirect::to("/login").into_response();
    };
    let accepted = given.accept == "yes";
    let state = if accepted { "accepted" } else { "rejected" };
    match shared
        .store
        .settle(id, state, &who, Minute::of(Utc::now()))
        .await
    {
        Ok(Some((incident, path))) => {
            move_draft(&shared, &path, &who, accepted).await;
            tracing::info!(draft = id, who, accepted, "черновик разобран");
            Redirect::to(&format!("/incident/{incident}")).into_response()
        }
        Ok(None) => failure(StatusCode::NOT_FOUND, "черновик не найден или уже разобран"),
        Err(broken) => failure(StatusCode::INTERNAL_SERVER_ERROR, &broken.to_string()),
    }
}

/// Переносит файл черновика и обновляет индекс поиска.
async fn move_draft(shared: &Shared, path: &str, who: &str, accepted: bool) {
    let (draft, notes, who) = (
        std::path::PathBuf::from(path),
        shared.knowledge.notes.clone(),
        who.to_owned(),
    );
    let done = tokio::task::spawn_blocking(move || {
        if accepted {
            sre_knowledge::draft::accept(&draft, &notes, &who).map(|_| ())
        } else {
            sre_knowledge::draft::reject(&draft, &who)
        }
    })
    .await;
    match done {
        Ok(Ok(())) if accepted => {
            crate::librarian::learn(&shared.store, &shared.metrics, &shared.knowledge).await;
        }
        Ok(Ok(())) => {}
        Ok(Err(failure)) => tracing::error!(%failure, path, "файл черновика не тронут"),
        Err(failure) => tracing::error!(%failure, "перенос черновика не завершился"),
    }
}

#[derive(Debug, Deserialize)]
struct Reply {
    answer: String,
}

/// Ответ дежурного на заявку. Пустой ответ снимает её: сказать «нечем» — тоже
/// ответ, и он лучше молчания, потому что возвращает инцидент в работу.
async fn answer(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Form(given): Form<Reply>,
) -> Response {
    let Some(who) = guard(&shared, &headers) else {
        return Redirect::to("/login").into_response();
    };
    let now = Minute::of(Utc::now());
    let text = given.answer.trim();
    let done = if text.is_empty() {
        shared.store.shush(id, &who, now).await
    } else {
        shared.store.reply(id, &who, text, now).await
    };
    match done {
        Ok(Some(incident)) => {
            if !text.is_empty() {
                shared.metrics.answered();
            }
            tracing::info!(
                inquiry = id,
                who,
                dropped = text.is_empty(),
                "заявка закрыта"
            );
            Redirect::to(&format!("/incident/{incident}")).into_response()
        }
        Ok(None) => failure(StatusCode::NOT_FOUND, "заявка не найдена или уже закрыта"),
        Err(broken) => failure(StatusCode::INTERNAL_SERVER_ERROR, &broken.to_string()),
    }
}

/// Метрики качества: доля ложных считается по оценённым, а не по всем.
async fn quality(State(shared): State<Shared>) -> Response {
    match shared.store.tally().await {
        Ok(tally) => Json(serde_json::json!({
            "incidents": tally.total,
            "open": tally.open,
            "useful": tally.useful,
            "useless": tally.useless,
            "wrong": tally.wrong(),
        }))
        .into_response(),
        Err(broken) => failure(StatusCode::INTERNAL_SERVER_ERROR, &broken.to_string()),
    }
}

async fn door() -> Response {
    render(&Door { failed: false })
}

async fn enter(State(shared): State<Shared>, Form(given): Form<Credentials>) -> Response {
    let Some(session) = shared.doorman.admit(&given.login, &given.password) else {
        tracing::warn!(login = given.login, "вход не удался");
        return render(&Door { failed: true });
    };
    (
        [(
            header::SET_COOKIE,
            format!("{COOKIE}={session}; Path=/; HttpOnly; SameSite=Lax"),
        )],
        Redirect::to("/"),
    )
        .into_response()
}

async fn leave() -> Response {
    (
        [(
            header::SET_COOKIE,
            format!("{COOKIE}=; Path=/; HttpOnly; Max-Age=0"),
        )],
        Redirect::to("/login"),
    )
        .into_response()
}

async fn style() -> Response {
    ([("content-type", "text/css; charset=utf-8")], STYLE).into_response()
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

/// Имя вошедшего или отказ.
fn guard(shared: &Shared, headers: &HeaderMap) -> Option<String> {
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    let session = cookies
        .split(';')
        .map(str::trim)
        .find_map(|it| it.strip_prefix(&format!("{COOKIE}=")))?;
    shared.doorman.who(session)
}

fn render<T: Template>(page: &T) -> Response {
    match page.render() {
        Ok(html) => Html(html).into_response(),
        Err(broken) => failure(StatusCode::INTERNAL_SERVER_ERROR, &broken.to_string()),
    }
}

fn failure(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}
