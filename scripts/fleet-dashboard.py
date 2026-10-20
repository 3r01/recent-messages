#!/usr/bin/env python3
"""Independent, read-only Recentmessages fleet dashboard; no chat or tokens."""
import argparse
import concurrent.futures
import ipaddress
import json
import math
from pathlib import Path
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.error
import urllib.parse
import urllib.request

ASSETS = Path(__file__).resolve().parents[1] / "deploy/dashboard"
METRICS = {
    "process_start_time_seconds": "process_start",
    "process_resident_memory_bytes": "rss_bytes",
    "process_cpu_user_seconds_total": "cpu_user",
    "process_cpu_system_seconds_total": "cpu_system",
    "recentmessages_storage_physical_bytes": "cache_bytes",
    "recentmessages_storage_effective_max_bytes": "cache_limit_bytes",
    "recentmessages_store_messages": "messages",
    "recentmessages_store_channels": "channels",
    "recentmessages_ingest_queue_depth": "queue",
    "recentmessages_ingest_accepted_records_total": "accepted",
    "recentmessages_peer_requests_total": "peer_attempts",
}
SOURCE_CONNECTED = re.compile(r'^recentmessages_raw_firehose_connected\{source="(raw-firehose-[0-9]+)"\} (\S+)$')
SOURCE_FRESH = re.compile(r'^recentmessages_raw_firehose_last_event_timestamp_seconds\{source="(raw-firehose-[0-9]+)"\} (\S+)$')
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def private_address(address):
    ip = ipaddress.ip_address(address)
    return ip.is_private or ip.is_loopback or ip in ipaddress.ip_network("100.64.0.0/10")


def load_inventory(path):
    nodes = json.loads(Path(path).read_text())
    if not isinstance(nodes, list) or not 1 <= len(nodes) <= 25:
        raise ValueError("inventory must contain 1..25 nodes")
    seen = set()
    for node in nodes:
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", node["name"]) or node["name"] in seen:
            raise ValueError("node names must be unique and simple")
        seen.add(node["name"])
        url = urllib.parse.urlsplit(node["url"])
        if url.scheme not in ("http", "https") or not private_address(url.hostname) or url.username or url.password or url.query or url.fragment or url.path not in ("", "/"):
            raise ValueError("use a literal private IP origin without credentials or a path")
        node["url"] = node["url"].rstrip("/")
    return nodes


def parse_metrics(body):
    values, sources = {}, {}
    for line in body.splitlines():
        parts = line.split()
        if len(parts) != 2 or line.startswith("#"):
            continue
        try:
            value = float(parts[1])
        except ValueError:
            continue
        if not math.isfinite(value):
            continue
        key = parts[0].split("{", 1)[0]
        if key in METRICS:
            dest = METRICS[key]
            values[dest] = values.get(dest, 0) + value
        if key == "http_requests_total" and not any(s in parts[0] for s in ("/metrics", "/health/", "/internal/")):
            values["http_requests"] = values.get("http_requests", 0) + value
        match = SOURCE_CONNECTED.fullmatch(line)
        if match:
            sources.setdefault(match[1], {})["connected"] = value == 1
        match = SOURCE_FRESH.fullmatch(line)
        if match:
            sources.setdefault(match[1], {})["last_event"] = value
    values["sources"] = sources
    return values


def fetch(node):
    result = {"name": node["name"], "sample_time": time.time(), "healthy": False}
    try:
        try:
            with OPENER.open(node["url"] + "/health/ready", timeout=3) as response:
                result["ready_http"] = response.status
        except urllib.error.HTTPError as error:
            result["ready_http"] = error.code
        with OPENER.open(node["url"] + "/api/v2/metrics", timeout=3) as response:
            body = response.read(2 * 1024 * 1024 + 1)
            if len(body) > 2 * 1024 * 1024:
                raise ValueError("metrics response exceeds cap")
            result.update(parse_metrics(body.decode()))
        result["healthy"] = result.get("ready_http") == 204
        result["last_success"] = result["sample_time"]
    except (OSError, ValueError, urllib.error.URLError):
        result["error"] = "Node unreachable or metrics unavailable"
    return result


def collect(nodes, previous=None):
    previous = {row["name"]: row for row in (previous or {}).get("nodes", [])}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(nodes), 16)) as executor:
        rows = list(executor.map(fetch, nodes))
    for row in rows:
        old = previous.get(row["name"], {})
        elapsed = row["sample_time"] - old.get("sample_time", row["sample_time"])
        if row.get("error"):
            for key, value in old.items():
                if key not in ("healthy", "sample_time", "ready_http", "cpu_cores", "requests_per_second", "ingest_per_second"):
                    row.setdefault(key, value)
        elif elapsed > 0 and old.get("process_start") == row.get("process_start"):
            for key, inputs in {"cpu_cores": ("cpu_user", "cpu_system"), "requests_per_second": ("http_requests",), "ingest_per_second": ("accepted",)}.items():
                if all(k in old and k in row for k in inputs) and not old.get("error"):
                    delta = sum(row[k] - old[k] for k in inputs)
                    if delta >= 0:
                        row[key] = delta / elapsed
    return {"sample_time": time.time(), "nodes": rows}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("inventory")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2788)
    args = parser.parse_args()
    if not ipaddress.ip_address(args.listen).is_loopback:
        parser.error("dashboard binds to loopback; use SSH forwarding for operator access")
    nodes = load_inventory(args.inventory)
    snapshot = collect(nodes)
    if args.once:
        print(json.dumps(snapshot, indent=2))
        return

    def poll():
        nonlocal snapshot
        while True:
            time.sleep(10)
            snapshot = collect(nodes, snapshot)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            paths = {"/": ("index.html", "text/html"), "/app.js": ("app.js", "text/javascript"), "/style.css": ("style.css", "text/css")}
            if self.path == "/api/status":
                body, content_type = json.dumps(snapshot).encode(), "application/json"
            elif self.path in paths:
                name, content_type = paths[self.path]
                body = (ASSETS / name).read_bytes()
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type + "; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; base-uri 'none'")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    threading.Thread(target=poll, daemon=True).start()
    print(f"Recentmessages dashboard: http://{args.listen}:{args.port}", flush=True)
    ThreadingHTTPServer((args.listen, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
