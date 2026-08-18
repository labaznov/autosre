# sreagent

> **In English.** A second diagnostic loop for on-call SRE. The agent watches
> VictoriaLogs and VictoriaMetrics, finds deviations with a robust detector,
> sifts routine noise with a cheap model call, investigates what survives using
> human-written skills, and shows the on-call engineer a write-up: what
> happened, what it resembles among known cases, where to look next.
>
> **It never fixes anything and never touches your hosts.** When data is out of
> reach it asks a human to run a read-only command and waits for the answer.
>
> Rust 2024, ten crates, 417 tests, `clippy::pedantic` clean. Works end to end
> on the bundled test lab; **not yet calibrated on production data**. Try it
> with `cd testlab && make up && make agent`, then open
> `http://127.0.0.1:8096/` and log in as `duty` / `sreagent-lab`.
>
> Everything else — code comments, docs, and the web UI — is in Russian, and
> that is deliberate: this is a tool for a Russian-speaking on-call team.
> Design rationale lives in [`docs/adr/`](docs/adr/README.md), 27 decisions
> with the alternatives that were rejected and why.

Второй контур диагностики: агент смотрит логи и метрики, замечает отклонения,
проводит первичное расследование и показывает дежурному SRE разбор — что
случилось, на что это похоже из уже известного, куда смотреть дальше.

**Агент не чинит и не ходит на хосты.** Он сокращает время до обнаружения и
время до понимания. Всё, чего он не может увидеть сам, он просит выполнить
человека и ждёт ответа.

## Что он делает

1. **Наблюдает.** Снимает минутные бакеты из VictoriaLogs и VictoriaMetrics,
   накапливает ряд у себя и переживает перезапуск без дыр.
2. **Замечает.** Робастный детектор на трёх горизонтах — 15 минут, час,
   сутки. Для счётчиков одно правило, для уровней другое: ползучий тренд
   робастная оценка не видит по построению.
3. **Отсеивает.** Дешёвый запрос к модели гасит привычный шум. Что он погасил,
   видно отдельной страницей — и там же дежурный может сказать, что зря.
4. **Расследует.** Скилл, написанный человеком, задаёт первое досье; модель
   может попросить добрать данных из закрытого меню или задать вопрос
   дежурному. Похожие случаи приезжают из базы знаний.
5. **Помнит.** Заметки и скиллы — markdown в отдельном git-репозитории. Агент
   пишет туда черновики, принимает их человек.
6. **Отчитывается.** По инциденту, за сутки, за неделю.

## Состояние

Работает и проверено на тестовой лаборатории целиком: от пустой базы до вывода
на карточке. **На живых прод-данных не калибровалось** — пороги выставлены на
глаз, промпты не проверены ни на одном настоящем инциденте. Первое, что стоит
сделать после развёртывания, — неделя работы и калибровка; для этого агент
умеет копить корпус живых данных и мерить время до обнаружения и до вывода.

Rust 2024, десять крейтов, 417 тестов, `clippy::pedantic` без предупреждений.

## Попробовать

```bash
cd testlab
make up        # VictoriaLogs, VictoriaMetrics, генератор потока, поддельная модель
make agent     # агент поверх них
```

Веб-морда — `http://127.0.0.1:8096/`, вход `duty` / `sreagent-lab`. Живого
инференса в лаборатории нет: проверяется конвейер, а не качество разбора,
поэтому ответы модели предсказуемы и прогон повторим. Подробности —
[`testlab/README.md`](testlab/README.md).

## Развернуть

Роль Ansible в [`deploy/auto-sre/`](deploy/auto-sre/README.md): образ на
Alpine, обновление с остановкой, настройки в TOML, секреты из окружения.

## Документы

**Начните со своей роли — читать всё не нужно.**

| Вы | Вам сюда | Объём |
| :--- | :--- | :--- |
| Ставите и эксплуатируете | [`docs/OPERATIONS.md`](docs/OPERATIONS.md) | 15 минут |
| Дежурите | [`docs/DUTY.md`](docs/DUTY.md) | 10 минут |
| Настраиваете мониторинг | [`docs/METRICS.md`](docs/METRICS.md) | 5 минут |
| Пишете скиллы и заметки | [`docs/KNOWLEDGE.md`](docs/KNOWLEDGE.md) | 10 минут |
| Разрабатываете | [`AGENTS.md`](AGENTS.md), дальше по ссылкам оттуда | час |

### Эксплуатация

| Файл | О чём |
| :--- | :--- |
| [`docs/OPERATIONS.md`](docs/OPERATIONS.md) | что нужно до установки, расчёт ресурсов, первый день, что открыто наружу, резервное копирование, обновление и откат, разбор отказов |
| [`docs/DUTY.md`](docs/DUTY.md) | страницы веб-морды, что делает дежурный и что означает каждое действие, как читать карточку, чего от агента не ждать |
| [`docs/METRICS.md`](docs/METRICS.md) | все 20 метрик с пояснениями, запросы для процентилей приёмки, с чего начать алерты |
| [`deploy/auto-sre/README.md`](deploy/auto-sre/README.md) | роль Ansible: переменные, запуск, обновление, HTTPS |
| [`examples/sreagent.toml`](examples/sreagent.toml) | все настройки с пояснениями, годится как образец для стенда |
| [`testlab/README.md`](testlab/README.md) | тестовая лаборатория: что генерируется, какие сценарии, как смотреть |

### Что и почему построено

| Файл | О чём |
| :--- | :--- |
| [`docs/SPEC.md`](docs/SPEC.md) | задача, масштаб, словарь терминов, конвейер, горизонты, отчёты, критерии приёмки |
| [`docs/adr/`](docs/adr/README.md) | 27 решений с отвергнутыми вариантами: [два контура](docs/adr/0001-two-contours.md), [скиллы как markdown](docs/adr/0003-skills-as-markdown.md), [минутный бакет](docs/adr/0013-minute-buckets.md), [заявка вместо SSH](docs/adr/0011-diagnostic-requests.md), [приглушение](docs/adr/0019-muting-instead-of-per-service-thresholds.md), [сбор живых данных](docs/adr/0027-live-corpus.md) |
| [`docs/NAMES.md`](docs/NAMES.md) | термин из словаря в имя типа, таблицы и поля, плюс запрещённые синонимы |
| [`docs/KNOWLEDGE.md`](docs/KNOWLEDGE.md) | устройство репозитория знаний: форма скиллов, заметок, черновиков и отчётов |
| [`examples/knowledge/`](examples/knowledge/README.md) | рабочий образец репозитория знаний: пять скиллов, две заметки, черновик, отчёты |

### Как ведётся проект

| Файл | О чём |
| :--- | :--- |
| [`AGENTS.md`](AGENTS.md) | правила работы над проектом: спека раньше кода, один термин, решение до реализации |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | из-за чего вернут пулл-реквест, что прогнать перед отправкой |
| [`docs/PLAN.md`](docs/PLAN.md) | срезы: что входит, чего не входит, когда срез считается готовым |
| [`docs/TASKS.md`](docs/TASKS.md) | 24 задачи с критериями готовности и отметками, где сделано иначе |
| [`docs/JOURNAL.md`](docs/JOURNAL.md) | журнал по сессиям: какой вопрос задавали, что ответили, что из этого вышло |
| [`docs/PROCESS.md`](docs/PROCESS.md) | какой документ когда правится и по каким правилам делится спека |

## Как это сделано

Конвейер из трёх решателей по возрастанию цены: числовой детектор находит
отклонения, короткий запрос к модели отсеивает шум, полное расследование
разбирает то, что осталось. Дорогое зовётся только тогда, когда дешёвое не
справилось.

Знание системы живёт вне кода: скиллы анализа и база известных ошибок — это
markdown в git, который пишут сами SRE, а не разработчики агента.

Стек: Rust, VictoriaLogs и VictoriaMetrics, локальная модель за
OpenAI-совместимым API, выкладка Ansible. Коннекторы — за одним узким трейтом,
второй источник лёг без правок первого.

## Лицензия

MIT — [`LICENSE`](LICENSE).
