#!/usr/bin/env python3
"""Minimal OTLP/HTTP trace receiver, for observing what the platform reports.

Stands in for a real collector so an end-to-end run can be checked without
installing Jaeger. It decodes just enough of the protobuf wire format to
recover span names, ids and parent links, then prints the resulting tree —
which is the thing actually worth verifying: whether Gateway and Worker spans
belong to one trace or two.

    python3 scripts/otlp_receiver.py 4318

Point the platform at it with:

    telemetry:
      enabled: true
      protocol: http
      endpoint: "127.0.0.1:4318"
"""
import struct
import sys
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 4318

# trace_id -> list of (span_id, parent_id, name, service)
TRACES = defaultdict(list)


def read_varint(buf, i):
    result = shift = 0
    while i < len(buf):
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7
    return result, i


def fields(buf):
    """Yield (field_number, wire_type, value) for one protobuf message."""
    i = 0
    while i < len(buf):
        try:
            key, i = read_varint(buf, i)
        except Exception:
            return
        fnum, wtype = key >> 3, key & 0x07
        if wtype == 0:
            val, i = read_varint(buf, i)
            yield fnum, wtype, val
        elif wtype == 2:
            ln, i = read_varint(buf, i)
            yield fnum, wtype, buf[i:i + ln]
            i += ln
        elif wtype == 5:
            yield fnum, wtype, buf[i:i + 4]
            i += 4
        elif wtype == 1:
            yield fnum, wtype, buf[i:i + 8]
            i += 8
        else:
            return


def string_attr(buf, want_key):
    """Pull a string attribute value by key from a KeyValue list."""
    for fnum, wtype, val in fields(buf):
        if fnum == 1 and wtype == 2:  # key
            key = val.decode("utf-8", "replace")
        elif fnum == 2 and wtype == 2 and key == want_key:  # AnyValue
            for f2, w2, v2 in fields(val):
                if f2 == 1 and w2 == 2:
                    return v2.decode("utf-8", "replace")
    return ""


def parse_span(buf):
    trace_id = span_id = parent_id = b""
    name = ""
    for fnum, wtype, val in fields(buf):
        if fnum == 1 and wtype == 2:
            trace_id = val
        elif fnum == 2 and wtype == 2:
            span_id = val
        elif fnum == 4 and wtype == 2:
            parent_id = val
        elif fnum == 5 and wtype == 2:
            name = val.decode("utf-8", "replace")
    return trace_id.hex(), span_id.hex(), parent_id.hex(), name


def parse_request(body):
    for fnum, wtype, val in fields(body):
        if fnum != 1 or wtype != 2:  # ResourceSpans
            continue
        service = "?"
        for f2, w2, v2 in fields(val):
            if f2 == 1 and w2 == 2:  # Resource
                for f3, w3, v3 in fields(v2):
                    if f3 == 1 and w3 == 2:
                        found = string_attr(v3, "service.name")
                        if found:
                            service = found
            elif f2 == 2 and w2 == 2:  # ScopeSpans
                for f3, w3, v3 in fields(v2):
                    if f3 == 2 and w3 == 2:  # Span
                        tid, sid, pid, name = parse_span(v3)
                        if tid:
                            TRACES[tid].append((sid, pid, name, service))


def print_tree(trace_id, out=sys.stdout):
    spans = TRACES[trace_id]
    children = defaultdict(list)
    by_id = {}
    for sid, pid, name, svc in spans:
        children[pid].append(sid)
        by_id[sid] = (name, svc, pid)

    say = lambda s: print(s, file=out, flush=True)
    say(f"\ntrace {trace_id}  ({len(spans)} spans)")

    def walk(sid, depth):
        name, svc, _ = by_id[sid]
        say(f"  {'  ' * depth}└─ {name:26} [{svc}]  {sid}")
        for c in children.get(sid, []):
            walk(c, depth + 1)

    roots = [sid for sid, (_, _, pid) in by_id.items() if pid not in by_id]
    for r in roots:
        walk(r, 0)
    if len(roots) > 1:
        say(f"  X {len(roots)} 个根 span —— 链路没串起来")
    else:
        services = {svc for _, _, _, svc in spans}
        say(f"  OK 单根，跨 {len(services)} 个服务：{', '.join(sorted(services))}")


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            parse_request(body)
            total = sum(len(v) for v in TRACES.values())
            print(f"[otlp] {length}B, {len(TRACES)} traces / {total} spans", flush=True)
        except Exception as exc:
            print(f"[otlp] parse failed: {exc}", flush=True)

        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.end_headers()
        self.wfile.write(b"")

    def do_GET(self):
        """GET /dump writes every collected trace tree to /tmp/traces.txt."""
        with open("/tmp/traces.txt", "w", encoding="utf-8") as fh:
            for tid in TRACES:
                print_tree(tid, out=fh)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    print(f"otlp receiver on :{PORT} (POST /v1/traces, GET /dump to print)", flush=True)
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
