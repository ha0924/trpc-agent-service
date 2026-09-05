#!/usr/bin/env python3
"""Collect outbound replies from the mock channel.

Stands in for the IM platform's push endpoint so an end-to-end run can assert
that the user actually received an answer, not merely that the agent produced
one. Writes each reply as a JSON line to the file given as the second argument.

    python3 scripts/reply_collector.py 9090 /tmp/replies.jsonl
"""
import json
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9090
OUT = sys.argv[2] if len(sys.argv) > 2 else "/tmp/replies.jsonl"


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw": raw.decode("utf-8", "replace")}
        payload["received_at"] = datetime.now().isoformat()

        with open(OUT, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

        print(f"[reply] target={payload.get('target')} text={payload.get('text', '')[:80]!r}",
              flush=True)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, *args):
        pass  # keep stdout to the replies themselves


if __name__ == "__main__":
    print(f"reply collector on :{PORT}, writing {OUT}", flush=True)
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
