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


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        size = int(self.headers.get("content-length", 0))
        asked = json.loads(self.rfile.read(size) or b"{}")
        user = asked["messages"][-1]["content"]
        kind = asked.get("response_format", {}).get("json_schema", {}).get("name", "triage")

        if kind == "conclusion":
            answer = self.conclusion(user)
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
        """
        service = re.search(r"Сервис: (.+)", user)
        service = service.group(1) if service else "сервис"
        wider = "добор:" in user
        print(f"разбор: {service} {'после добора' if wider else 'первый заход'}", file=sys.stderr, flush=True)
        return {
            "cause": f"{service}: судя по сигнатурам, отвечает не он, а то, от чего он зависит",
            "confidence": 0.7 if wider else 0.4,
            "advice": "проверить соседей по цепочке и последние выкаты",
            "need": "nothing" if wider else "logs",
        }

    def log_message(self, *args):
        pass


http.server.HTTPServer(("0.0.0.0", 4000), Handler).serve_forever()
