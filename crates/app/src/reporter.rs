//! Отчёты: по инциденту, суточный, недельный.
//!
//! Три читателя и три разговора ([SPEC §9](../../../docs/SPEC.md)): дежурному —
//! разбор полётов, дежурному же — сутки, руководителю — неделя. Общее у них
//! одно: всё собирается по накопленным рядам, без единого запроса к источнику
//! ([ADR-0013](../../../docs/adr/0013-minute-buckets.md)).
//!
//! Отчёт ложится и в базу, и файлом в репозиторий знаний. В базе он для
//! веб-морды, в репозитории — для людей, которые читают историю без агента.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use chrono::{Datelike, TimeZone, Utc};
use sre_domain::{Incident, Minute, Service};
use sre_model::Model;
use sre_store::{Digest, Store};

use crate::config::{Incidents, Knowledge};
use crate::metrics::Metrics;

/// Всё, что нужно отчётам.
#[derive(Clone)]
pub struct Reporter {
    store: Store,
    metrics: Arc<Metrics>,
    model: Arc<Model>,
    knowledge: Knowledge,
    incidents: Incidents,
}

impl Reporter {
    #[must_use]
    pub fn new(
        store: &Store,
        metrics: &Arc<Metrics>,
        model: &Arc<Model>,
        knowledge: &Knowledge,
        incidents: &Incidents,
    ) -> Self {
        Self {
            store: store.clone(),
            metrics: Arc::clone(metrics),
            model: Arc::clone(model),
            knowledge: knowledge.clone(),
            incidents: incidents.clone(),
        }
    }
}

/// Заводит расписание отчётов.
///
/// Проверка по часам, а не по будильнику на полночь: агент, перезапущенный в
/// 00:03, обязан собрать вчерашний отчёт, а не пропустить его до следующих
/// суток. Уже собранный отчёт не пересобирается — он в базе под своим именем.
pub fn report(reporter: Reporter) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_mins(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            daily(&reporter).await;
            weekly(&reporter).await;
        }
    });
}

/// Суточный отчёт за вчера, если его ещё нет.
pub async fn daily(reporter: &Reporter) {
    let day = Utc::now().date_naive().pred_opt().unwrap_or_default();
    let name = day.format("%Y-%m-%d").to_string();
    if let Ok(Some(_)) = reporter.store.report("daily", &name).await {
        return;
    }
    let Some(from) = midnight(day.and_hms_opt(0, 0, 0).map(|it| it.and_utc().timestamp())) else {
        return;
    };
    let to = from.back(-24 * 60);
    let Ok(digest) = reporter.store.digest(from, to).await else {
        tracing::error!("суточный отчёт не собран: свод не прочитан");
        return;
    };
    let body = day_page(reporter, &digest, &name).await;
    keep(reporter, "daily", &name, &format!("Сутки {name}"), &body).await;
}

/// Недельный отчёт за прошлую неделю, если его ещё нет.
pub async fn weekly(reporter: &Reporter) {
    let now = Utc::now();
    let past = now - chrono::Duration::days(7);
    let name = format!("{}-W{:02}", past.iso_week().year(), past.iso_week().week());
    if let Ok(Some(_)) = reporter.store.report("weekly", &name).await {
        return;
    }
    let start = past
        .date_naive()
        .week(chrono::Weekday::Mon)
        .first_day()
        .and_hms_opt(0, 0, 0)
        .map(|it| it.and_utc().timestamp());
    let Some(from) = midnight(start) else {
        return;
    };
    let to = from.back(-7 * 24 * 60);
    let Ok(digest) = reporter.store.digest(from, to).await else {
        tracing::error!("недельный отчёт не собран: свод не прочитан");
        return;
    };
    let body = week_page(reporter, &digest, &name).await;
    keep(reporter, "weekly", &name, &format!("Неделя {name}"), &body).await;
}

/// Отчёт по инциденту: что было, что выяснил агент, что делал человек.
///
/// Собирается по требованию и при закрытии. По открытому — тоже собирается, но
/// честно помечен неполным: разбор полётов часто начинают, не дожидаясь тишины.
pub async fn single(reporter: &Reporter, incident: i64) -> Option<String> {
    let found = reporter.store.one(incident).await.ok().flatten()?;
    let finding = reporter.store.conclusion(incident).await.ok().flatten();
    let asked = reporter.store.inquiries(incident).await.unwrap_or_default();
    let drafts = reporter.store.drafts(incident).await.unwrap_or_default();

    let mut page = String::new();
    let _ = writeln!(page, "# {} — {}\n", found.service, found.signature);
    if found.state == sre_domain::State::Open {
        page.push_str(
            "> Инцидент ещё идёт. Отчёт неполон: подтверждения продолжают приходить.\n\n",
        );
    }
    page.push_str(&happened(&found));
    page.push_str(&learned(finding.as_ref()));
    page.push_str(&questions(&asked));
    page.push_str(&handled(&found, &drafts));

    let name = format!(
        "{}-{}-{}",
        short(found.began),
        incident,
        latin(&found.service)
    );
    keep(
        reporter,
        "incidents",
        &name,
        &format!("{} — {}", found.service, found.signature),
        &page,
    )
    .await;
    Some(name)
}

/// Часть «что было»: числа инцидента.
fn happened(found: &Incident) -> String {
    let mut part = String::from("## Что было\n\n");
    let _ = writeln!(
        part,
        "- начался: {}\n- последнее подтверждение: {}\n- подтверждений: {}\n- пик за окно: {:.0} против обычного\n- источник: {}\n- поток: `{}`",
        stamp(found.began),
        stamp(found.last),
        found.seen,
        found.peak,
        found.source,
        found.stream,
    );
    if let Some(because) = &found.because {
        let _ = writeln!(part, "- отсев решил разбираться: {because}");
    }
    part
}

/// Часть «что выяснил агент».
fn learned(finding: Option<&sre_store::Finding>) -> String {
    let mut part = String::from("\n## Что выяснил агент\n\n");
    match finding {
        Some(finding) if finding.cause.is_some() => {
            let _ = writeln!(
                part,
                "{}\n\n**Куда смотреть:** {}\n\nУверенность {:.0}%, скилл `{}`.",
                finding.cause.clone().unwrap_or_default(),
                finding.advice.clone().unwrap_or_default(),
                finding.confidence.unwrap_or_default() * 100.0,
                finding.skill,
            );
            if let Some(note) = &finding.note {
                let _ = writeln!(part, "Опирается на заметку `{note}`.");
            }
        }
        Some(finding) => {
            let _ = writeln!(part, "Вывода нет: разбор в состоянии «{}»", finding.state);
        }
        None => part.push_str("Вывода нет: разбор не начинался.\n"),
    }
    part
}

/// Часть «что спрашивали у человека».
fn questions(asked: &[sre_store::Asked]) -> String {
    if asked.is_empty() {
        return String::new();
    }
    let mut part = String::from("\n## Что спрашивали у человека\n\n");
    for inquiry in asked {
        let _ = writeln!(
            part,
            "**{}** на `{}`\n\n```\n{}\n```\n",
            inquiry.reason, inquiry.host, inquiry.command
        );
        match (&inquiry.answer, &inquiry.who) {
            (Some(answer), Some(who)) => {
                let _ = writeln!(part, "Ответил {who}:\n\n```\n{answer}\n```\n");
            }
            _ => part.push_str("Ответа не было.\n\n"),
        }
    }
    part
}

/// Часть «что делал человек»: оценка и судьба черновиков.
///
/// Без неё отчёт превращается в список жалоб — [SPEC §9](../../../docs/SPEC.md)
/// говорит это про недельный, но верно оно про любой.
fn handled(found: &Incident, drafts: &[sre_store::Written]) -> String {
    let mut part = String::from("\n## Что делал человек\n\n");
    match found.verdict {
        Some(true) => part.push_str("Дежурный счёл инцидент по делу.\n"),
        Some(false) => part.push_str("Дежурный счёл инцидент ложным.\n"),
        None => part.push_str("Оценки нет.\n"),
    }
    for draft in drafts {
        let _ = writeln!(
            part,
            "- черновик `{}`: {}{}",
            draft.name,
            match draft.state.as_str() {
                "accepted" => "принят в базу знаний",
                "rejected" => "отклонён",
                _ => "ждёт приёмки",
            },
            draft
                .who
                .as_ref()
                .map(|who| format!(", {who}"))
                .unwrap_or_default(),
        );
    }
    part
}

/// Суточный отчёт: четыре части.
async fn day_page(reporter: &Reporter, digest: &Digest, name: &str) -> String {
    let mut page = format!("# Сутки {name}\n\n");
    page.push_str(&incidents_part(digest));
    page.push_str(&anomalies_part(digest));
    page.push_str(&dynamics_part(reporter, digest));
    page.push_str(&overall(reporter, digest, "сутки").await);
    page
}

/// Недельный отчёт: четыре части для читателя, не знающего устройства агента.
async fn week_page(reporter: &Reporter, digest: &Digest, name: &str) -> String {
    let mut page = format!("# Неделя {name}\n\n");
    let seen = digest.opened.len() + digest.still.len();
    let _ = writeln!(
        page,
        "## Как прошла неделя\n\nЗаведено инцидентов: {}. Закрыто: {}. Осталось открытыми: {}.\nОтклонений замечено {}, из них отсеяно как привычный шум {}, приглушено человеком {}.\nВыводов готово {}.\n",
        digest.opened.len(),
        digest.closed.len(),
        digest.still.len(),
        digest.deviations,
        digest.sifted,
        digest.hushed,
        digest.conclusions,
    );
    page.push_str("## Главные проблемы\n\n");
    if digest.opened.is_empty() {
        page.push_str("Ни одного инцидента за неделю.\n\n");
    } else {
        for incident in digest.opened.iter().take(7) {
            let _ = writeln!(
                page,
                "- **{}** — {} (подтверждений {}, {})",
                incident.service,
                incident.signature,
                incident.seen,
                lasted(incident),
            );
        }
        page.push('\n');
    }
    page.push_str(&dynamics_part(reporter, digest));
    let _ = writeln!(
        page,
        "## Что по этим проблемам делали люди\n\nОценено инцидентов: {}. Заявок на диагностику: {}. Принято заметок в базу знаний: {}.\n",
        digest
            .opened
            .iter()
            .chain(digest.closed.iter())
            .filter(|it| it.verdict.is_some())
            .count(),
        digest.inquiries,
        digest.notes,
    );
    page.push_str(&overall(reporter, digest, "неделю").await);
    if seen == 0 {
        page.push_str("\nЗа неделю не заведено ни одного инцидента. Это либо спокойная неделя, либо ослепший агент — второе проверяется метрикой `sre_last_bucket_timestamp_seconds`.\n");
    }
    page
}

/// Часть «инциденты».
fn incidents_part(digest: &Digest) -> String {
    let mut part = String::from("## Инциденты\n\n");
    if digest.opened.is_empty() && digest.still.is_empty() {
        part.push_str("Ни одного за сутки.\n\n");
        return part;
    }
    for incident in &digest.opened {
        let _ = writeln!(
            part,
            "- **{}** — {} · подтверждений {} · {}",
            incident.service,
            incident.signature,
            incident.seen,
            if incident.state == sre_domain::State::Open {
                "идёт".to_owned()
            } else {
                format!("закрыт, {}", lasted(incident))
            },
        );
    }
    let hanging = digest
        .still
        .iter()
        .filter(|it| !digest.opened.iter().any(|new| new.id == it.id))
        .collect::<Vec<_>>();
    if !hanging.is_empty() {
        part.push_str("\nОстались открытыми с прошлых суток:\n\n");
        for incident in hanging {
            let _ = writeln!(
                part,
                "- **{}** — {} · с {}",
                incident.service,
                incident.signature,
                stamp(incident.began)
            );
        }
    }
    part.push('\n');
    part
}

/// Часть «аномалии»: то, что не стало инцидентом.
fn anomalies_part(digest: &Digest) -> String {
    let mut part = format!(
        "## Аномалии\n\nОтклонений за сутки: {}. Отсеяно моделью как привычный шум: {}. Приглушено человеком: {}.\n",
        digest.deviations, digest.sifted, digest.hushed
    );
    let loud = digest
        .mutes
        .iter()
        .filter(|mute| mute.seen > 0)
        .collect::<Vec<_>>();
    if !loud.is_empty() {
        part.push_str("\nПриглушено, но продолжается:\n\n");
        for mute in loud {
            let _ = writeln!(
                part,
                "- **{}** — {} · {} раз · молчит до {} по слову {}",
                mute.service,
                mute.signature,
                mute.seen,
                stamp(mute.until),
                mute.author,
            );
        }
    }
    part.push('\n');
    part
}

/// Часть «динамика»: что растёт.
fn dynamics_part(reporter: &Reporter, digest: &Digest) -> String {
    let mut rising = digest
        .streams
        .iter()
        .filter(|(_, now, was)| *now > was + was / 2 && *now >= 3)
        .collect::<Vec<_>>();
    rising.sort_by_key(|(_, now, was)| std::cmp::Reverse(now - was));
    if rising.is_empty() {
        return "## Динамика\n\nНичего не растёт: числа держатся на уровне прошлого окна.\n\n"
            .to_owned();
    }
    let mut part = String::from("## Динамика\n\nРастёт по сравнению с прошлым окном:\n\n");
    for (stream, now, was) in rising.iter().take(10) {
        let service = Service::of(stream, &reporter.incidents.service_labels);
        let _ = writeln!(
            part,
            "- **{service}** — {now} против {was}{}",
            if *was == 0 {
                ", раньше не бывало".to_owned()
            } else {
                format!(", в {:.1} раза больше", ratio(*now, *was))
            }
        );
    }
    part.push('\n');
    part
}

/// Во сколько раз выросло: без точности, но и без обмана.
fn ratio(now: u64, was: u64) -> f64 {
    let (now, was) = (
        u32::try_from(now).unwrap_or(u32::MAX),
        u32::try_from(was).unwrap_or(u32::MAX),
    );
    f64::from(now) / f64::from(was.max(1))
}

/// Часть «общая картина»: её пишет модель по трём первым.
///
/// Модель молчит — часть не выдумывается, а честно отсутствует: числа выше
/// сами по себе полезны, а придуманный вывод хуже, чем никакого.
async fn overall(reporter: &Reporter, digest: &Digest, span: &str) -> String {
    let facts = format!(
        "Промежуток: {span}.\nИнцидентов заведено: {}, закрыто: {}, осталось открытыми: {}.\nОтклонений: {}, отсеяно: {}, приглушено: {}.\nВыводов: {}, заявок: {}.\nСамые шумные: {}.",
        digest.opened.len(),
        digest.closed.len(),
        digest.still.len(),
        digest.deviations,
        digest.sifted,
        digest.hushed,
        digest.conclusions,
        digest.inquiries,
        digest
            .opened
            .iter()
            .take(5)
            .map(|it| format!("{} ({})", it.service, it.signature))
            .collect::<Vec<_>>()
            .join(", "),
    );
    match reporter.model.summary(&facts).await {
        Ok(picture) => format!("## Общая картина\n\n{picture}\n"),
        Err(failure) => {
            reporter.metrics.failure();
            tracing::warn!(%failure, "общая картина отчёта не написана");
            "## Общая картина\n\nНе собрана: модель не ответила. Числа выше — всё, что есть.\n"
                .to_owned()
        }
    }
}

/// Кладёт отчёт в базу и файлом в репозиторий знаний.
async fn keep(reporter: &Reporter, kind: &str, name: &str, title: &str, body: &str) {
    let path = reporter
        .knowledge
        .reports
        .join(kind)
        .join(format!("{name}.md"));
    let (written, text) = (path.clone(), body.to_owned());
    let done = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(written.parent().unwrap_or(&written))?;
        std::fs::write(&written, text)
    })
    .await;
    if !matches!(done, Ok(Ok(()))) {
        tracing::warn!(path = %path.display(), "отчёт не записан файлом, останется только в базе");
    }
    match reporter
        .store
        .file(
            kind,
            name,
            title,
            &path.to_string_lossy(),
            body,
            Minute::of(Utc::now()),
        )
        .await
    {
        Ok(_) => {
            reporter.metrics.reported();
            tracing::info!(kind, name, "отчёт собран");
        }
        Err(failure) => tracing::error!(%failure, kind, name, "отчёт не сохранён"),
    }
}

/// Момент из отметки времени, если она разбирается.
fn midnight(stamp: Option<i64>) -> Option<Minute> {
    stamp.map(Minute::at)
}

/// Момент в виде, годном для чтения человеком.
fn stamp(minute: Minute) -> String {
    Utc.timestamp_opt(minute.stamp(), 0)
        .single()
        .map_or_else(|| "—".to_owned(), |at| at.format("%d.%m %H:%M").to_string())
}

/// Дата в виде, годном для имени файла.
fn short(minute: Minute) -> String {
    Utc.timestamp_opt(minute.stamp(), 0).single().map_or_else(
        || "0000-00-00".to_owned(),
        |at| at.format("%Y-%m-%d").to_string(),
    )
}

/// Сколько инцидент длился.
fn lasted(incident: &Incident) -> String {
    let minutes = (incident.last.stamp() - incident.began.stamp()) / 60;
    if minutes < 60 {
        format!("длился {minutes} мин")
    } else {
        format!("длился {} ч {} мин", minutes / 60, minutes % 60)
    }
}

/// Имя сервиса, годное для имени файла.
fn latin(service: &Service) -> String {
    let name: String = service
        .as_str()
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
