#!/usr/bin/env python3

import argparse
import http.server
import queue
import socketserver
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlparse


BASE_DIR = Path(__file__).resolve().parent
DIST_DIR = BASE_DIR / "dist"
WATCH_PATHS = [
    BASE_DIR / "src",
    BASE_DIR / "construir.py",
    BASE_DIR / "generar_descubrir.py",
    BASE_DIR / "generar_archivar.py",
]
IGNORE_PARTS = {"__pycache__", ".DS_Store"}


class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


class LiveReloadHandler(http.server.SimpleHTTPRequestHandler):
    server_version = "GeoDatosBetaDev/1.0"

    def __init__(self, *args, directory=None, **kwargs):
        super().__init__(*args, directory=str(DIST_DIR), **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/__reload":
            self.handle_reload_stream()
            return
        super().do_GET()

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, fmt, *args):
        sys.stdout.write("http: " + (fmt % args) + "\n")

    def handle_reload_stream(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        client = queue.Queue()
        self.server.reload_clients.add(client)
        try:
            self.wfile.write(b"retry: 1000\n\n")
            self.wfile.flush()
            while True:
                try:
                    event = client.get(timeout=30)
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                payload = f"event: reload\ndata: {event}\n\n".encode("utf-8")
                self.wfile.write(payload)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            self.server.reload_clients.discard(client)


def snapshot_paths(paths: list[Path]) -> dict[str, float]:
    snapshot = {}
    for path in paths:
        if not path.exists():
            continue
        if path.is_file():
            snapshot[str(path)] = path.stat().st_mtime
            continue
        for child in path.rglob("*"):
            if any(part in IGNORE_PARTS for part in child.parts):
                continue
            if child.is_file():
                snapshot[str(child)] = child.stat().st_mtime
    return snapshot


def run_build() -> bool:
    print("build: ejecutando publicar/construir.py")
    result = subprocess.run(
        [sys.executable, str(BASE_DIR / "construir.py")],
        cwd=BASE_DIR.parent,
        text=True,
    )
    return result.returncode == 0


def notify_reload(server: ThreadingHTTPServer, reason: str) -> None:
    stale = []
    for client in list(server.reload_clients):
        try:
            client.put_nowait(reason)
        except Exception:
            stale.append(client)
    for client in stale:
        server.reload_clients.discard(client)


def watch_and_rebuild(server: ThreadingHTTPServer, interval: float) -> None:
    previous = snapshot_paths(WATCH_PATHS)
    while True:
        time.sleep(interval)
        current = snapshot_paths(WATCH_PATHS)
        if current == previous:
            continue
        previous = current
        print("watch: cambio detectado, reconstruyendo…")
        if run_build():
            notify_reload(server, str(int(time.time())))
            print("watch: build ok, recargando navegador")
        else:
            print("watch: build falló, esperando nuevos cambios")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Servidor de desarrollo para publicar.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4173)
    parser.add_argument("--interval", type=float, default=1.0, help="Segundos entre chequeos de cambios.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not run_build():
        return 1

    server = ThreadingHTTPServer((args.host, args.port), LiveReloadHandler)
    server.reload_clients = set()

    watcher = threading.Thread(
        target=watch_and_rebuild,
        args=(server, args.interval),
        daemon=True,
    )
    watcher.start()

    print(f"dev: sirviendo {DIST_DIR} en http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\ndev: detenido")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
