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

use crate::config::{Digging, Incidents};
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
}

impl Digger {
    #[must_use]
    pub fn new(
        sources: &[Arc<dyn Source>],
        store: &Store,
        metrics: &Arc<Metrics>,
        model: &Arc<Model>,
        skills: Vec<Skill>,
        settings: &Digging,
        incidents: &Incidents,
    ) -> Self {
        Self {
            sources: sources.to_vec(),
            store: store.clone(),
            metrics: Arc::clone(metrics),
            model: Arc::clone(model),
            skills: Arc::new(skills),
            settings: settings.clone(),
            incidents: incidents.clone(),
        }
    }

    fn source(&self, name: &str) -> Option<&Arc<dyn Source>> {
        self.sources.iter().find(|it| it.name() == name)
    }
}

/// Заводит очередь разбора.
pub fn dig(digger: Digger) {
    let slots = Arc::new(Semaphore::new(digger.settings.parallel.max(1)));
    tokio::spawn(async move {
        resume(&digger).await;
        let mut ticker = tokio::time::interval(Duration::from_mins(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            turn(&digger, &slots).await;
        }
    });
}

/// Один проход очереди: раздать свободные места самым тяжёлым.
async fn turn(digger: &Digger, slots: &Arc<Semaphore>) {
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

/// Расследование одного инцидента.
async fn investigate(digger: &Digger, incident: &Incident) {
    let Some(skill) = pick(digger, incident) else {
        tracing::info!(
            incident = incident.id,
            source = incident.source,
            "подходящего скилла нет, разбор не начат"
        );
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
                let wanted = step < steps && need != "nothing" && !need.is_empty();
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

/// Записывает вывод и отмечает расследование законченным.
async fn finish(
    digger: &Digger,
    incident: &Incident,
    investigation: i64,
    conclusion: &sre_model::Conclusion,
) {
    let now = Minute::of(Utc::now());
    match digger
        .store
        .conclude(
            investigation,
            &conclusion.cause,
            conclusion.confidence,
            &conclusion.advice,
            now,
        )
        .await
    {
        Ok(()) => {
            digger.metrics.concluded();
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
    parts
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

/// Возобновляет расследования, оборванные перезапуском.
async fn resume(digger: &Digger) {
    let Ok(unfinished) = digger.store.unfinished().await else {
        return;
    };
    let edge = Minute::of(Utc::now()).back(24 * 60);
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
