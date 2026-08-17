#!/usr/bin/env python3
"""Генератор логов и метрик для тестовой лаборатории.

Пишет в VictoriaLogs и VictoriaMetrics поток, похожий на прод: ровный фон и
сценарии, ради которых агент и затевался — всплеск ошибок, ползучая утечка,
заполняющийся диск, замолчавший сервис.

Умеет засевать историю за прошедшие часы: без базовой линии детектору не с чем
сравнивать, а ждать шесть часов на каждом запуске лаборатории незачем.
"""

import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
import zlib
from datetime import UTC, datetime, timedelta

LOGS = os.environ.get("LOGS_URL", "http://victorialogs:9428")
METRICS = os.environ.get("METRICS_URL", "http://victoriametrics:8428")
SEED_HOURS = int(os.environ.get("SEED_HOURS", "6"))
SEED = int(os.environ.get("RANDOM_SEED", "20260817"))
HOST = "lab-01"

# Сервисы лаборатории: сколько строк в минуту и какая доля из них — ошибки.
SERVICES = {
    "orders-api": {"rate": 120, "errors": 0.01},
    "billing-api": {"rate": 80, "errors": 0.02},
    "litellm": {"rate": 40, "errors": 0.01},
    "llama-server": {"rate": 20, "errors": 0.005},
    "victorialogs": {"rate": 15, "errors": 0.0},
    "zabbix-proxy": {"rate": 30, "errors": 0.05},
}

# Обычные сообщения фона.
CHATTER = [
    "handled request in {ms}ms",
    "cache hit ratio {ratio}",
    "connection pool size {n}",
    "scheduled job finished in {ms}ms",
]

# Ошибки фона: то, что бывает всегда и никого не будит.
NOISE = {
    "orders-api": ["upstream 192.0.2.{n} timed out after 30s"],
    "billing-api": ["retry {n} of 3 for downstream call"],
    "litellm": ["Request timed out after 180s"],
    "llama-server": ["slot {n} released early"],
    "victorialogs": ["cannot parse field value"],
    "zabbix-proxy": ["connection reset by peer"],
}

# Сценарии: беды, которые агент обязан заметить.
STORMS = {
    "orders-api": "upstream 192.0.2.{n} timed out after 30s",
    "litellm": "no slots available, request queued for {ms}ms",
    "victorialogs": "cannot write data to /data: no space left on device",
}


def jsonline(lines):
    """Отправляет строки логов в VictoriaLogs."""
    body = "\n".join(json.dumps(line, ensure_ascii=False) for line in lines)
    url = (
        f"{LOGS}/insert/jsonline"
        "?_stream_fields=service,host&_time_field=_time&_msg_field=_msg"
    )
    send(url, body.encode("utf-8"), "application/x-ndjson")


def prometheus(samples):
    """Отправляет метрики в VictoriaMetrics."""
    send(
        f"{METRICS}/api/v1/import/prometheus",
        "\n".join(samples).encode("utf-8"),
        "text/plain",
    )


def send(url, body, kind):
    request = urllib.request.Request(url, data=body, headers={"Content-Type": kind})
    try:
        with urllib.request.urlopen(request, timeout=30) as answer:
            answer.read()
    except urllib.error.URLError as failure:
        print(f"не отправлено: {failure}", file=sys.stderr, flush=True)


def wait_for(url, patience=120):
    """Ждёт, пока источник начнёт отвечать.

    Готовность проверяет тот, кому она нужна: в образах Victoria нет ни wget,
    ни curl, поэтому средствами compose дождаться их нельзя.
    """
    until = time.time() + patience
    while time.time() < until:
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=3) as answer:
                answer.read()
                return
        except (urllib.error.URLError, OSError):
            time.sleep(1)
    print(f"источник {url} так и не ответил за {patience}с", file=sys.stderr, flush=True)


def storm(minute):
    """Идёт ли сейчас всплеск и у кого.

    Расписание жёсткое, чтобы прогон повторялся: каждые полчаса свой сервис
    штормит четыре минуты.
    """
    slot = (minute.hour * 60 + minute.minute) % 30
    if slot >= 4:
        return None
    which = (minute.hour * 60 + minute.minute) // 30 % len(STORMS)
    return list(STORMS)[which]


def lines(minute):
    """Строки логов за одну минуту."""
    loud = storm(minute)
    out = []
    for service, shape in SERVICES.items():
        rate = shape["rate"]
        errors = int(rate * shape["errors"])
        if service == loud:
            # Треть строк сервиса становится ошибками: против фона в один-два
            # это виден издалека, но сервис не превращается в сплошной поток
            # ошибок, чего в жизни почти не бывает.
            errors = max(int(rate * 0.35), 12)
        for _ in range(rate - min(errors, rate)):
            out.append(entry(minute, service, "info", random.choice(CHATTER)))
        for _ in range(errors):
            template = STORMS[service] if service == loud else random.choice(NOISE[service])
            out.append(entry(minute, service, "error", template))
    return out


def entry(minute, service, level, template):
    at = minute + timedelta(seconds=random.randint(0, 59))
    message = template.format(
        ms=random.randint(3, 900),
        n=random.randint(1, 250),
        ratio=round(random.uniform(0.5, 0.99), 2),
    )
    return {
        "_time": at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_msg": f"{level.upper()} {message}",
        "service": service,
        "host": HOST,
        "level": level,
    }


def samples(minute, step):
    """Метрики за одну минуту.

    Память `llama-server` ползёт вверх, свободное место на диске под логи —
    вниз. Ни то, ни другое не даёт всплеска ни в одном окне: это то, ради чего
    заведён часовой горизонт.
    """
    stamp = int(minute.timestamp() * 1000)
    out = []
    for service, shape in SERVICES.items():
        # crc32, а не hash(): встроенный хеш строк рандомизируется между
        # запусками, и базовая линия памяти уезжала бы при каждом старте.
        base = 200_000_000 + zlib.crc32(service.encode()) % 100_000_000
        creep = step * 900_000 if service == "llama-server" else 0
        jitter = random.randint(0, 5_000_000)
        out.append(
            f'process_resident_memory_bytes{{job="{service}",instance="{HOST}"}} '
            f"{base + creep + jitter} {stamp}"
        )
        out.append(
            f'http_requests_total{{job="{service}",instance="{HOST}"}} '
            f"{step * SERVICES[service]['rate']} {stamp}"
        )
    free = max(2_000_000_000 - step * 3_000_000, 50_000_000)
    out.append(
        f'node_filesystem_avail_bytes{{mountpoint="/data",instance="{HOST}"}} '
        f"{free} {stamp}"
    )
    return out


def minute_of(at):
    return at.replace(second=0, microsecond=0)


def main():
    random.seed(SEED)
    wait_for(LOGS)
    wait_for(METRICS)
    now = minute_of(datetime.now(UTC))
    start = now - timedelta(hours=SEED_HOURS)

    step = 0
    walk = start
    while walk < now:
        jsonline(lines(walk))
        prometheus(samples(walk, step))
        walk += timedelta(minutes=1)
        step += 1
    print(f"история засеяна: {SEED_HOURS} ч, {step} минут", flush=True)

    while True:
        walk = minute_of(datetime.now(UTC))
        jsonline(lines(walk))
        prometheus(samples(walk, step))
        loud = storm(walk)
        print(
            f"{walk:%H:%M} записано" + (f", штормит {loud}" if loud else ""),
            flush=True,
        )
        step += 1
        time.sleep(60 - datetime.now(UTC).second)


if __name__ == "__main__":
    main()
