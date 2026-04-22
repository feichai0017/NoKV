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


REDIS_DEFAULT_PORT = "6380"
ALLOWED_CONTAINER_PREFIX = "nokv-"


class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def do_POST(self):
        if self.path == "/api/redis":
            return self._handle_redis()
        if self.path.startswith("/api/docker/"):
            return self._handle_docker()
        self._send_json(404, {"error": "not found: " + self.path})

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

    def _handle_docker(self):
        # path shapes:
        #   /api/docker/stop/nokv-coordinator-1
        #   /api/docker/start/nokv-meta-root-2
        parts = self.path.split("/")
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
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
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
    print("Press Ctrl-C to stop.\n", flush=True)
    with ThreadingServer((bind, port), Handler) as server:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nstopped")


if __name__ == "__main__":
    main()
