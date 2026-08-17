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
        signature = re.search(r"× (.+)", user)
        key = signature.group(1) if signature else user[:80]

        worth = key not in SEEN
        SEEN.add(key)
        answer = {
            "worth": worth,
            "because": "такого раньше не видел" if worth else "уже попадалось, привычный шум",
        }
        print(f"отсев: {'разбирать' if worth else 'шум'} — {key[:60]}", file=sys.stderr, flush=True)

        body = json.dumps(
            {"choices": [{"message": {"content": json.dumps(answer, ensure_ascii=False)}}]}
        ).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


http.server.HTTPServer(("0.0.0.0", 4000), Handler).serve_forever()
