#!/usr/bin/env python3
"""Поддельный OpenAI-совместимый сервер для лаборатории.

Живого инференса тут нет и не нужно: лаборатория проверяет конвейер, а не
качество модели. Ответы предсказуемы, поэтому прогон повторяем.

Правило отсева простое и объяснимое: привычным шумом считается то, что уже
попадалось. Первое появление сигнатуры проходит дальше, повторы гасятся —
ровно то поведение, ради которого отсев и заведён.
"""

import http.server
import json
import re
import sys

SEEN = set()
ASKED = []


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        size = int(self.headers.get("content-length", 0))
        asked = json.loads(self.rfile.read(size) or b"{}")
        user = asked["messages"][-1]["content"]
        kind = asked.get("response_format", {}).get("json_schema", {}).get("name", "triage")

        if kind == "conclusion":
            answer = self.conclusion(user)
        elif kind == "picture":
            answer = self.picture(user)
        else:
            answer = self.triage(user)

        body = json.dumps(
            {"choices": [{"message": {"content": json.dumps(answer, ensure_ascii=False)}}]}
        ).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def triage(self, user):
        """Первое появление сигнатуры проходит, повторы гасятся."""
        signature = re.search(r"× (.+)", user)
        key = signature.group(1) if signature else user[:80]
        worth = key not in SEEN
        SEEN.add(key)
        print(f"отсев: {'разбирать' if worth else 'шум'} — {key[:60]}", file=sys.stderr, flush=True)
        return {
            "worth": worth,
            "because": "такого раньше не видел" if worth else "уже попадалось, привычный шум",
        }

    def conclusion(self, user):
        """Разбор: пересказ фактов плюс одна просьба добрать данные.

        Просьба ровно одна за расследование: так проверяется, что ограничение
        шагов работает и что агент не зацикливается.

        Первое расследование за прогон вместо добора просит человека выполнить
        команду: так в лаборатории проверяется петля заявки. Одно на прогон —
        иначе агент застрянет в ожидании ответов и ничего не разберёт.
        """
        service = re.search(r"Сервис: (.+)", user)
        service = service.group(1) if service else "сервис"
        answered = "ответы дежурного" in user
        wider = "добор:" in user
        if not ASKED and not answered:
            ASKED.append(service)
            print(f"разбор: {service} — заявка дежурному", file=sys.stderr, flush=True)
            return {
                "cause": f"{service}: похоже на нехватку места, но метрики диска нет",
                "confidence": 0.3,
                "advice": "посмотреть, сколько осталось на разделе с данными",
                "severity": "medium",
                "need": "ask",
                "host": service,
                "command": "df -h /var",
            }
        found = re.search(r"^## ([\w-]+) —", user, re.M)
        print(
            f"разбор: {service} {'после ответа' if answered else 'после добора' if wider else 'первый заход'}",
            file=sys.stderr,
            flush=True,
        )
        answer = {
            "cause": f"{service}: судя по сигнатурам, отвечает не он, а то, от чего он зависит",
            "confidence": 0.8 if answered else 0.7 if wider else 0.4,
            "advice": "проверить соседей по цепочке и последние выкаты",
            "severity": "high" if answered else "medium",
            "need": "nothing" if (wider or answered) else "logs",
        }
        if found:
            answer["note"] = found.group(1)
        return answer

    def picture(self, user):
        """Общая картина отчёта: пересказ чисел одной фразой."""
        print("отчёт: общая картина", file=sys.stderr, flush=True)
        first = user.splitlines()[1] if len(user.splitlines()) > 1 else user[:80]
        return {
            "picture": f"Хозяйство держится. {first} Смотреть завтра на самых шумных.",
        }

    def log_message(self, *args):
        pass


http.server.HTTPServer(("0.0.0.0", 4000), Handler).serve_forever()
