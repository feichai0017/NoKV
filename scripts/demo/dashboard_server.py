#!/usr/bin/env python3
"""Minimal HTTP server for the NoKV demo dashboard.

Serves scripts/demo/dashboard.html at /, exposes a POST /api/redis endpoint
that forwards commands to `redis-cli -p <port>` on the local machine, and
proxies POST /api/docker/{stop,start}/<container> to docker CLI so the
failure-drill buttons on the dashboard can work without touching a terminal.

Security: bound to 127.0.0.1 by default. Anyone who can reach the bind
address can run arbitrary Redis commands and (with /api/docker) stop/start
containers whose name starts with "nokv-". Do not expose this service to
an untrusted network. For a public demo, put it behind an authenticated
tunnel.
"""
import http.server
import json
import os
import shlex
import socketserver
import subprocess
import sys
import urllib.error
import urllib.request
from urllib.parse import urlparse


REDIS_DEFAULT_PORT = "6380"
ALLOWED_CONTAINER_PREFIX = "nokv-"
ALLOWED_EXPVAR_PORTS = {9100, 9101, 9102, 9200, 9201, 9202, 9300, 9380, 9381, 9382}


class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("", "/", "/dashboard"):
            return self._redirect("/dashboard.html")
        if parsed.path.startswith("/api/expvar/"):
            return self._handle_expvar(parsed.path)
        return super().do_GET()

    def do_HEAD(self):
        parsed = urlparse(self.path)
        if parsed.path in ("", "/", "/dashboard"):
            self.send_response(302)
            self.send_header("Location", "/dashboard.html")
            self.end_headers()
            return
        return super().do_HEAD()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/redis":
            return self._handle_redis()
        if parsed.path.startswith("/api/docker/"):
            return self._handle_docker(parsed.path)
        self._send_json(404, {"error": "not found: " + parsed.path})

    def _handle_expvar(self, path):
        parts = path.split("/")
        if len(parts) != 4:
            return self._send_json(400, {"error": "path is /api/expvar/<port>"})
        try:
            port = int(parts[3])
        except ValueError:
            return self._send_json(400, {"error": f"invalid port: {parts[3]}"})
        if port not in ALLOWED_EXPVAR_PORTS:
            return self._send_json(400, {"error": f"port {port} is not an allowed expvar target"})
        target = f"http://127.0.0.1:{port}/debug/vars"
        try:
            with urllib.request.urlopen(target, timeout=1.5) as resp:
                body = resp.read()
                status = resp.status
                content_type = resp.headers.get("Content-Type", "application/json")
        except urllib.error.HTTPError as exc:
            return self._send_json(exc.code, {"error": f"upstream HTTP {exc.code}", "target": target})
        except urllib.error.URLError as exc:
            return self._send_json(502, {"error": f"upstream unavailable: {exc.reason}", "target": target})
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, location):
        self.send_response(302)
        self.send_header("Location", location)
        self.end_headers()

    def _handle_redis(self):
        body = self._read_body()
        try:
            data = json.loads(body) if body else {}
        except json.JSONDecodeError as exc:
            return self._send_json(400, {"error": f"invalid JSON: {exc}"})
        cmd = (data.get("cmd") or "").strip()
        port = str(data.get("port") or REDIS_DEFAULT_PORT)
        if not cmd:
            return self._send_json(400, {"error": "empty command"})
        try:
            argv = shlex.split(cmd)
        except ValueError as exc:
            return self._send_json(400, {"error": f"parse error: {exc}"})
        if not argv:
            return self._send_json(400, {"error": "no command tokens"})
        try:
            proc = subprocess.run(
                ["redis-cli", "-p", port, *argv],
                capture_output=True,
                text=True,
                timeout=4,
            )
        except FileNotFoundError:
            return self._send_json(500, {"error": "redis-cli not found on PATH"})
        except subprocess.TimeoutExpired:
            return self._send_json(504, {"error": "redis-cli timed out after 4s"})
        return self._send_json(200, {
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "returncode": proc.returncode,
            "argv": argv,
        })

    def _handle_docker(self, path):
        # path shapes:
        #   /api/docker/stop/nokv-coordinator-1
        #   /api/docker/start/nokv-meta-root-2
        parts = path.split("/")
        if len(parts) != 5:
            return self._send_json(400, {"error": "path is /api/docker/<stop|start>/<container>"})
        action = parts[3]
        name = parts[4]
        if action not in ("stop", "start", "restart"):
            return self._send_json(400, {"error": f"unsupported action: {action}"})
        if not name.startswith(ALLOWED_CONTAINER_PREFIX):
            return self._send_json(400, {"error": f"container name must start with '{ALLOWED_CONTAINER_PREFIX}'"})
        try:
            proc = subprocess.run(
                ["docker", action, name],
                capture_output=True,
                text=True,
                timeout=30,
            )
        except FileNotFoundError:
            return self._send_json(500, {"error": "docker not found on PATH"})
        except subprocess.TimeoutExpired:
            return self._send_json(504, {"error": "docker timed out"})
        return self._send_json(200, {
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "returncode": proc.returncode,
            "action": action,
            "container": name,
        })

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length <= 0:
            return b""
        return self.rfile.read(length)

    def _send_json(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # Silence access log chatter. Comment out to re-enable.
    def log_message(self, fmt, *args):
        pass


class ThreadingServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 18080
    bind = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print(f"NoKV dashboard server listening on http://{bind}:{port}/dashboard.html", flush=True)
    print(
        "  /api/redis                  POST JSON {cmd, port}   → redis-cli wrapper",
        flush=True,
    )
    print(
        "  /api/docker/<stop|start|restart>/<container>        → docker wrapper (nokv-* only)",
        flush=True,
    )
    print(
        "  /api/expvar/<port>                                  → localhost:<port>/debug/vars proxy",
        flush=True,
    )
    print("Press Ctrl-C to stop.\n", flush=True)
    with ThreadingServer((bind, port), Handler) as server:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nstopped")


if __name__ == "__main__":
    main()
