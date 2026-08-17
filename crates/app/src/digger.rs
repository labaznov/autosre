//! Очередь расследований и сам разбор.
//!
//! Расследований одновременно идёт не больше, чем тянет стенд, а порядок задаёт
//! вес: в аварию дежурный должен увидеть главное первым, а не первое пришедшее
//! ([ADR-0018](../../../docs/adr/0018-investigation-queue.md)).
//!
//! Инцидент, простоявший в очереди дольше терпения, остаётся на ленте голыми
//! числами с пометкой «вывод пропущен». Молчать в этом случае нельзя: агент
//! нужнее всего именно тогда, когда не успевает.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sre_domain::{Incident, Minute, Span};
use sre_model::{Model, prompt};
use sre_skills::Skill;
use sre_source::Source;
use sre_store::Store;
use tokio::sync::Semaphore;

use crate::config::{Digging, Incidents, Knowledge};
use crate::metrics::Metrics;

/// Всё, что нужно одному расследованию.
#[derive(Clone)]
pub struct Digger {
    sources: Vec<Arc<dyn Source>>,
    store: Store,
    metrics: Arc<Metrics>,
    model: Arc<Model>,
    skills: Arc<Vec<Skill>>,
    settings: Digging,
    incidents: Incidents,
    knowledge: Knowledge,
}

/// Настройки, из которых собирается разбор.
///
/// Три раздела конфигурации вместе, чтобы не передавать их по одному: разбор
/// касается и очереди, и инцидентов, и базы знаний.
pub struct Recipe<'a> {
    pub digging: &'a Digging,
    pub incidents: &'a Incidents,
    pub knowledge: &'a Knowledge,
}

impl Digger {
    #[must_use]
    pub fn new(
        sources: &[Arc<dyn Source>],
        store: &Store,
        metrics: &Arc<Metrics>,
        model: &Arc<Model>,
        skills: Vec<Skill>,
        recipe: &Recipe<'_>,
    ) -> Self {
        Self {
            sources: sources.to_vec(),
            store: store.clone(),
            metrics: Arc::clone(metrics),
            model: Arc::clone(model),
            skills: Arc::new(skills),
            settings: recipe.digging.clone(),
            incidents: recipe.incidents.clone(),
            knowledge: recipe.knowledge.clone(),
        }
    }

    fn source(&self, name: &str) -> Option<&Arc<dyn Source>> {
        self.sources.iter().find(|it| it.name() == name)
    }
}

/// Через сколько минут оборванное расследование считается устаревшим.
const STALE: i64 = 24 * 60;

/// Заводит очередь разбора.
pub fn dig(digger: Digger) {
    let slots = Arc::new(Semaphore::new(digger.settings.parallel.max(1)));
    tokio::spawn(async move {
        resume(&digger).await;
        let mut ticker = tokio::time::interval(digger.settings.tick.max(Duration::from_secs(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            turn(&digger, &slots).await;
        }
    });
}

/// Один проход очереди: раздать свободные места самым тяжёлым.
async fn turn(digger: &Digger, slots: &Arc<Semaphore>) {
    fade(digger).await;
    let waiting = match digger.store.awaiting(digger.settings.batch).await {
        Ok(waiting) => waiting,
        Err(failure) => {
            digger.metrics.failure();
            tracing::error!(%failure, "очередь не прочитана");
            return;
        }
    };
    for incident in waiting {
        let patience = i64::try_from(digger.settings.patience.as_secs() / 60).unwrap_or(15);
        if incident.began.stamp() < Minute::of(Utc::now()).back(patience).stamp() {
            skip(digger, &incident).await;
            continue;
        }
        let Ok(place) = Arc::clone(slots).try_acquire_owned() else {
            tracing::debug!("мест для разбора нет, ждём следующего круга");
            return;
        };
        let digger = digger.clone();
        tokio::spawn(async move {
            let _place = place;
            investigate(&digger, &incident).await;
        });
    }
}

/// Гасит заявки, на которые никто не ответил за отпущенный срок.
///
/// Заявка без срока — это инцидент, замерший навсегда: агент ждёт человека, а
/// человек про заявку забыл на другой день.
pub async fn fade(digger: &Digger) {
    let waited = i64::try_from(digger.settings.answer.as_secs() / 60).unwrap_or(24 * 60);
    match digger.store.fade(Minute::of(Utc::now()).back(waited)).await {
        Ok(0) => {}
        Ok(gone) => tracing::warn!(gone, "заявки погасли без ответа"),
        Err(failure) => tracing::error!(%failure, "заявки не проверены"),
    }
}

/// Инцидент, до которого очередь не дошла вовремя.
async fn skip(digger: &Digger, incident: &Incident) {
    let now = Minute::of(Utc::now());
    match digger.store.dig(incident.id, "—", now).await {
        Ok(investigation) => {
            if let Err(failure) = digger.store.drop_dig(investigation, "skipped").await {
                tracing::error!(%failure, "пометка о пропуске не записана");
            }
            digger.metrics.skipped();
            tracing::warn!(
                incident = incident.id,
                service = incident.service.as_str(),
                "вывод пропущен: очередь не дошла за отпущенное время"
            );
        }
        Err(failure) => tracing::error!(%failure, "пропуск не записан"),
    }
}

/// Инцидент, для которого не нашлось скилла.
async fn unskilled(digger: &Digger, incident: &Incident) {
    let now = Minute::of(Utc::now());
    if let Ok(investigation) = digger.store.dig(incident.id, "—", now).await {
        let _ = digger.store.drop_dig(investigation, "unskilled").await;
    }
    tracing::info!(
        incident = incident.id,
        source = incident.source,
        signature = incident.signature.as_str(),
        "подходящего скилла нет: разбирать нечем"
    );
}

/// Расследование одного инцидента.
async fn investigate(digger: &Digger, incident: &Incident) {
    let Some(skill) = pick(digger, incident) else {
        // Помечаем один раз, а не жалуемся каждую минуту: без пометки инцидент
        // возвращается в очередь бесконечно, а дежурный так и не узнает, что
        // разбирать его нечем.
        unskilled(digger, incident).await;
        return;
    };
    let now = Minute::of(Utc::now());
    let Ok(investigation) = digger.store.dig(incident.id, &skill.front.name, now).await else {
        digger.metrics.failure();
        return;
    };

    let mut dossier = collect(digger, incident, skill, investigation).await;
    let head = head(incident);
    let steps = skill.front.steps.unwrap_or(digger.settings.steps);

    for step in 0..=steps {
        let asked = prompt::dossier(&head, &dossier);
        match digger.model.conclude(&skill.body, &asked).await {
            Ok(conclusion) => {
                let need = conclusion.need.clone().unwrap_or_default();
                if need == "ask" && inquire(digger, incident, investigation, &conclusion).await {
                    return;
                }
                let wanted = step < steps && !matches!(need.as_str(), "nothing" | "ask" | "");
                let extra = if wanted {
                    more(digger, incident, &need, investigation, dossier.len()).await
                } else {
                    None
                };
                if let Some(part) = extra {
                    tracing::info!(incident = incident.id, need, "добор данных");
                    dossier.push(part);
                    continue;
                }
                finish(digger, incident, investigation, &conclusion).await;
                return;
            }
            Err(failure) => {
                digger.metrics.failure();
                tracing::error!(incident = incident.id, %failure, "разбор не удался");
                let _ = digger.store.drop_dig(investigation, "failed").await;
                return;
            }
        }
    }
}

/// Пишет черновик заметки по готовому выводу.
///
/// Не на каждый вывод: черновик пишется, только если агент себе верит и если
/// вывод не пересказывает уже принятую заметку. Иначе очередь на приёмку
/// заполняется мусором, а неразобранный черновик через месяц бесполезен —
/// контекст забыт ([ADR-0009](../../../docs/adr/0009-drafts-before-knowledge.md)).
///
/// Текст собирается из того, что уже сказано: причина, совет, числа. Второй
/// заход в модель ради красивой заметки стоит дороже, чем стоит сама заметка.
async fn jot(
    digger: &Digger,
    incident: &Incident,
    conclusion: &sre_model::Conclusion,
    note: Option<&str>,
) {
    if note.is_some() || conclusion.confidence < digger.knowledge.worth {
        return;
    }
    let draft = sre_knowledge::Draft {
        name: format!(
            "{}-{}-{}",
            Utc::now().format("%Y-%m-%d"),
            latin(incident.service.as_str()),
            incident.id
        ),
        title: clipped(&conclusion.cause, 90),
        kind: "incident".to_owned(),
        tags: tags(incident),
        signatures: vec![incident.signature.to_string()],
        services: vec![incident.service.to_string()],
        incident: incident.id,
        confidence: conclusion.confidence,
        body: format!(
            "## Что было\n\n{}\n\nСервис {}, сигнатура «{}». Подтверждений {}, пик за окно {:.0} против обычного.\n\n## Что делать\n\n{}\n",
            conclusion.cause,
            incident.service,
            incident.signature,
            incident.seen,
            incident.peak,
            conclusion.advice,
        ),
    };
    let drafts = digger.knowledge.drafts.clone();
    let written = tokio::task::spawn_blocking(move || sre_knowledge::draft::write(&drafts, &draft))
        .await
        .ok()
        .and_then(Result::ok);
    let Some(path) = written else {
        tracing::warn!(incident = incident.id, "черновик заметки не записан");
        return;
    };
    let name = path.file_stem().map_or_else(String::new, |it| {
        it.to_string_lossy()
            .split('.')
            .next()
            .unwrap_or("")
            .to_owned()
    });
    match digger
        .store
        .draft(
            incident.id,
            &name,
            &clipped(&conclusion.cause, 90),
            &path.to_string_lossy(),
            Minute::of(Utc::now()),
        )
        .await
    {
        Ok(Some(id)) => {
            digger.metrics.drafted();
            tracing::info!(
                incident = incident.id,
                draft = id,
                "черновик заметки написан"
            );
        }
        Ok(None) => tracing::debug!(incident = incident.id, "черновик по инциденту уже есть"),
        Err(failure) => tracing::error!(%failure, "черновик не учтён"),
    }
}

/// Имя сервиса, годное для имени файла: латиница, цифры и дефис.
///
/// Имя файла попадает в ссылки и в журналы, где кириллица только мешает —
/// [`KNOWLEDGE.md`](../../../docs/KNOWLEDGE.md) требует латиницы.
fn latin(service: &str) -> String {
    let name: String = service
        .chars()
        .map(|it| {
            if it.is_ascii_alphanumeric() {
                it.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let name = name.trim_matches('-').to_owned();
    if name.is_empty() {
        "incident".to_owned()
    } else {
        name
    }
}

/// Слова для поиска: сервис и слова сигнатуры.
fn tags(incident: &Incident) -> Vec<String> {
    let mut tags = vec![incident.service.to_string()];
    tags.extend(
        incident
            .signature
            .as_str()
            .split(|it: char| !it.is_alphanumeric())
            .filter(|it| it.chars().count() >= 4)
            .take(6)
            .map(str::to_lowercase),
    );
    tags
}

/// Первые знаки строки: заголовок заметки — это заголовок, а не абзац.
fn clipped(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_owned();
    }
    text.chars().take(limit).collect::<String>() + "…"
}

/// Заметка, на которую опёрся вывод, — если такая заметка есть.
///
/// Имя проверяется по индексу: модель называет то, что видела в досье, но
/// «видела» и «перепечатала верно» — разные вещи, а ссылка в карточке обязана
/// вести на существующую заметку.
async fn leaned(digger: &Digger, conclusion: &sre_model::Conclusion) -> Option<String> {
    let named = conclusion.note.as_deref()?.trim();
    if named.is_empty() {
        return None;
    }
    let known = digger
        .store
        .recall(named, digger.knowledge.recall)
        .await
        .ok()?
        .into_iter()
        .any(|note| note.name == named);
    if !known {
        tracing::warn!(note = named, "модель сослалась на заметку, которой нет");
        return None;
    }
    Some(named.to_owned())
}

/// Оставляет заявку дежурному. Отвечает, удалось ли.
///
/// Не удалось — расследование заканчивается тем, что есть: агент, который
/// вместо вывода промолчал и ничего не спросил, бесполезен вдвойне.
async fn inquire(
    digger: &Digger,
    incident: &Incident,
    investigation: i64,
    conclusion: &sre_model::Conclusion,
) -> bool {
    let host = conclusion
        .host
        .clone()
        .unwrap_or_else(|| incident.service.to_string());
    let Some(command) = conclusion.command.as_deref() else {
        tracing::warn!(
            incident = incident.id,
            "модель просит команду, но не назвала её"
        );
        return false;
    };
    let inquiry = match sre_domain::Inquiry::new(&host, command, &conclusion.advice) {
        Ok(inquiry) => inquiry,
        Err(refused) => {
            tracing::warn!(incident = incident.id, %refused, "заявка отклонена");
            return false;
        }
    };
    match digger
        .store
        .ask(incident.id, investigation, &inquiry, Minute::of(Utc::now()))
        .await
    {
        Ok(id) => {
            digger.metrics.inquiry();
            tracing::info!(
                incident = incident.id,
                inquiry = id,
                host = inquiry.host,
                command = inquiry.command,
                "заявка дежурному оставлена"
            );
            true
        }
        Err(failure) => {
            digger.metrics.failure();
            tracing::error!(%failure, "заявка не записана");
            false
        }
    }
}

/// Записывает вывод и отмечает расследование законченным.
async fn finish(
    digger: &Digger,
    incident: &Incident,
    investigation: i64,
    conclusion: &sre_model::Conclusion,
) {
    let now = Minute::of(Utc::now());
    let note = leaned(digger, conclusion).await;
    match digger
        .store
        .conclude(
            investigation,
            &conclusion.cause,
            conclusion.confidence,
            &conclusion.advice,
            note.as_deref(),
            now,
        )
        .await
    {
        Ok(()) => {
            digger.metrics.concluded();
            jot(digger, incident, conclusion, note.as_deref()).await;
            tracing::info!(
                incident = incident.id,
                service = incident.service.as_str(),
                confidence = conclusion.confidence,
                "вывод готов"
            );
        }
        Err(failure) => {
            digger.metrics.failure();
            tracing::error!(%failure, "вывод не записан");
        }
    }
}

/// Скилл, подходящий инциденту. Подходят несколько — берётся первый по имени.
fn pick<'a>(digger: &'a Digger, incident: &Incident) -> Option<&'a Skill> {
    digger
        .skills
        .iter()
        .find(|skill| skill.fits(&about(incident)))
}

/// Отклонение, каким его видит скилл: инцидент несёт те же поля.
fn about(incident: &Incident) -> sre_domain::Deviation {
    sre_domain::Deviation {
        source: incident.source.clone(),
        stream: incident.stream.clone(),
        horizon: horizon(incident),
        at: incident.last,
        value: incident.peak,
        baseline: 0.0,
        score: f64::INFINITY,
        weight: incident.weight,
    }
}

/// Горизонт инцидента: он записан в сигнатуре метрик и неизвестен для логов.
///
/// Скиллы объявляют горизонт, а инцидент его не хранит — он собран из
/// отклонений, которые могли прийти с разных. Берём тот, на котором инцидент
/// открылся: это и есть первое наблюдение.
fn horizon(incident: &Incident) -> String {
    incident
        .signature
        .as_str()
        .rsplit_once("на горизонте ")
        .map_or_else(|| "15m".to_owned(), |(_, it)| it.trim().to_owned())
}

/// Заголовок досье: числа инцидента.
fn head(incident: &Incident) -> String {
    format!(
        "Сервис: {}\nСигнатура: {}\nИсточник: {}\nНачалось: {}\nПодтверждений: {}\nПик за окно: {:.0}",
        incident.service,
        incident.signature,
        incident.source,
        incident.began.start().format("%Y-%m-%dT%H:%M:%SZ"),
        incident.seen,
        incident.peak,
    )
}

/// Первое досье: то, что велел собрать скилл.
async fn collect(
    digger: &Digger,
    incident: &Incident,
    skill: &Skill,
    investigation: i64,
) -> Vec<(String, String)> {
    let mut parts = Vec::new();
    let window = i64::try_from(digger.incidents.sample_window.as_secs() / 60).unwrap_or(15);
    let span = Span::new(incident.last.back(window), incident.last, window + 1)
        .unwrap_or_else(|_| Span::single(incident.last));
    for (ord, want) in skill.front.collect.iter().enumerate() {
        let Some(source) = digger.source(if want.vl.is_some() { "logs" } else { "metrics" }) else {
            continue;
        };
        let text = match source
            .samples(
                &incident.stream,
                span,
                want.limit.unwrap_or(digger.incidents.samples),
            )
            .await
        {
            Ok(lines) => join(&lines),
            Err(failure) => format!("не собрано: {failure}"),
        };
        let _ = digger
            .store
            .step(investigation, ord, "collect", &want.id, &text)
            .await;
        parts.push((want.id.clone(), text));
    }
    if let Some(part) = similar(digger, incident, investigation, parts.len()).await {
        parts.push(part);
    }
    if let Some(part) = replies(digger, incident, investigation, parts.len()).await {
        parts.push(part);
    }
    parts
}

/// Похожие случаи из базы знаний.
///
/// Запрос строится из сигнатуры, сервиса и слов отсева: редкие токены вроде
/// `ECONNRESET` и делают попадание точным
/// ([ADR-0008](../../../docs/adr/0008-full-text-knowledge-search.md)). Пустой
/// результат расследованию не мешает — база знаний бывает и пустой.
async fn similar(
    digger: &Digger,
    incident: &Incident,
    investigation: i64,
    ord: usize,
) -> Option<(String, String)> {
    let words = format!(
        "{} {} {}",
        incident.signature,
        incident.service,
        incident.because.clone().unwrap_or_default()
    );
    let found = digger
        .store
        .recall(&words, digger.knowledge.recall)
        .await
        .ok()?;
    if found.is_empty() {
        return None;
    }
    let text = found
        .iter()
        .map(|note| format!("## {} — {}\n{}", note.name, note.title, clip(&note.body)))
        .collect::<Vec<_>>()
        .join("\n\n");
    let names = found
        .iter()
        .map(|note| note.name.clone())
        .collect::<Vec<_>>()
        .join(", ");
    let _ = digger
        .store
        .step(
            investigation,
            ord,
            "knowledge",
            &format!("по словам «{}» нашлись: {names}", short(&words)),
            &text,
        )
        .await;
    Some(("похожие случаи из базы знаний".to_owned(), text))
}

/// Сколько знаков заметки уходит в промпт.
///
/// Заметка пишется для человека и бывает на две страницы. Три таких — и досье
/// вытеснит собой те самые числа, ради которых расследование затевалось.
const CLIP: usize = 1200;

/// Начало заметки: столько, сколько влезает в бюджет промпта.
fn clip(body: &str) -> String {
    if body.chars().count() <= CLIP {
        return body.to_owned();
    }
    let head: String = body.chars().take(CLIP).collect();
    format!("{head}…")
}

/// Слова запроса в виде, годном для чтения человеком.
fn short(words: &str) -> String {
    words
        .split_whitespace()
        .take(8)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Ответы дежурного на прежние заявки — их в досье кладут последними.
///
/// Ради них заявка и заводилась: без этой части петля не замкнута, и
/// расследование заново упрётся в то же самое место
/// ([ADR-0011](../../../docs/adr/0011-diagnostic-requests.md)).
async fn replies(
    digger: &Digger,
    incident: &Incident,
    investigation: i64,
    ord: usize,
) -> Option<(String, String)> {
    let answers = digger.store.answers(incident.id).await.ok()?;
    if answers.is_empty() {
        return None;
    }
    let text = answers
        .iter()
        .map(|(command, answer)| format!("$ {command}\n{answer}"))
        .collect::<Vec<_>>()
        .join("\n\n");
    let _ = digger
        .store
        .step(investigation, ord, "ask", "ответ дежурного", &text)
        .await;
    Some(("ответы дежурного".to_owned(), text))
}

/// Добор данных по просьбе модели. Меню закрытое: чего нет в нём, того нет.
async fn more(
    digger: &Digger,
    incident: &Incident,
    need: &str,
    investigation: i64,
    ord: usize,
) -> Option<(String, String)> {
    let window = i64::try_from(digger.settings.wider.as_secs() / 60).unwrap_or(60);
    let span = Span::new(incident.last.back(window), incident.last, window + 1).ok()?;
    let source = digger.source(match need {
        "metrics" => "metrics",
        _ => "logs",
    })?;
    let text = source
        .samples(&incident.stream, span, digger.incidents.samples)
        .await
        .map(|lines| join(&lines))
        .ok()?;
    let _ = digger
        .store
        .step(investigation, ord, need, "добор по просьбе модели", &text)
        .await;
    Some((format!("добор: {need}"), text))
}

/// Разбирается с расследованиями, оборванными перезапуском.
///
/// Продолжить с середины нельзя: досье собрано, а рассуждение модели нигде не
/// хранится — оно и не должно храниться. Поэтому оборванное закрывается, а
/// инцидент возвращается в очередь и разбирается заново, если ещё жив.
///
/// Старше суток не возобновляется вовсе: данные уже другие, и вывод по ним
/// будет хуже отсутствия вывода ([ADR-0021](../../../docs/adr/0021-state-survives-restart.md)).
pub async fn resume(digger: &Digger) {
    let Ok(unfinished) = digger.store.unfinished().await else {
        return;
    };
    let edge = Minute::of(Utc::now()).back(STALE);
    for (dig, incident, skill, started) in unfinished {
        // Расследование старше суток не возобновляется: данные уже другие, и
        // вывод по ним будет хуже отсутствия вывода.
        let state = if started < edge { "stale" } else { "failed" };
        let _ = digger.store.drop_dig(dig, state).await;
        tracing::info!(
            dig,
            incident,
            skill,
            state,
            "расследование, оборванное перезапуском, закрыто"
        );
    }
}

/// Строки в текст, схлопнутые по сигнатурам.
fn join(lines: &[String]) -> String {
    if lines.is_empty() {
        return "(пусто)".to_owned();
    }
    sre_domain::signature::groups(lines, 7)
        .iter()
        .map(|group| {
            format!(
                "{} × {}\n    пример: {}",
                group.count, group.signature, group.sample
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}
