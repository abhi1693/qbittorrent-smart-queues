"""Exercise both image entrypoints against isolated loopback fixtures only."""

import hashlib
import importlib
import importlib.util
import json
import os
import platform
import socket
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

from qbittorrent_smart_queues import __version__


class QbitFixture(BaseHTTPRequestHandler):
    """An empty synthetic qBittorrent; unknown endpoints always fail the smoke."""

    def do_GET(self):
        path = urlsplit(self.path).path
        self.server.requests.append(("GET", path))
        replies = {
            "/api/v2/app/version": b"v5.2.1",
            "/api/v2/torrents/info": b"[]",
            "/api/v2/torrents/tags": b"[]",
            "/api/v2/app/preferences": b'{"queueing_enabled":true,"max_active_downloads":1,"max_active_torrents":1,"preallocate_all":false}',
            "/api/v2/transfer/info": b'{"dl_info_speed":0,"up_info_speed":0}',
        }
        if path not in replies:
            self.server.unexpected.append(("GET", path))
        self.reply(200 if path in replies else 500, replies.get(path, b"unexpected"))

    def do_POST(self):
        path = urlsplit(self.path).path
        self.server.requests.append(("POST", path))
        length = int(self.headers.get("Content-Length", "0"))
        assert 0 < length <= 4096
        body = self.rfile.read(length)
        allowed = {
            "/api/v2/auth/login",
            "/api/v2/torrents/createTags",
            "/api/v2/transfer/setDownloadLimit",
            "/api/v2/transfer/setUploadLimit",
            "/api/v2/app/setPreferences",
            "/api/v2/torrents/stop",
        }
        if path == "/api/v2/torrents/stop":
            # The normal controller may stop its empty synthetic queue; no
            # torrent is created, resumed, deleted or sent to a real service.
            assert parse_qs(body.decode()) == {"hashes": ["all"]}
        if path not in allowed:
            self.server.unexpected.append(("POST", path))
        self.reply(200 if path in allowed else 500, b"Ok.")

    def reply(self, status, body):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", "application/json")
        if self.path == "/api/v2/auth/login":
            self.send_header("Set-Cookie", "SID=local-fixture; Path=/")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


def request(port, method, path, payload=None, key=None):
    connection = HTTPConnection("127.0.0.1", port, timeout=2)
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-Api-Key"] = key
    try:
        connection.request(
            method, path, json.dumps(payload) if payload is not None else None, headers
        )
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def available_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@contextmanager
def application(module, environment):
    # Inherit no configured upstreams, credentials or policy from the runner.
    environment = {
        "PATH": os.defpath,
        "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
        **environment,
    }
    with tempfile.TemporaryFile() as logs:
        process = subprocess.Popen(
            [sys.executable, "-m", module], env=environment, stdout=logs, stderr=logs
        )
        try:
            yield process
        except BaseException:
            # Only generated loopback fixture configuration reaches the child.
            logs.seek(0)
            sys.stderr.write(logs.read(8192).decode("utf-8", errors="replace"))
            raise
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            logs.seek(0)
            records = logs.read().decode("utf-8", errors="replace")
            assert "Traceback (most recent call last)" not in records


def wait_for(process, port, predicate):
    deadline = time.monotonic() + 10
    while True:
        assert process.poll() is None, "Application exited during startup"
        try:
            if predicate():
                return
        except OSError:
            pass
        assert time.monotonic() < deadline, (
            f"Application on local port {port} did not become ready"
        )
        time.sleep(0.05)


def exercise_controller():
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), QbitFixture)
    upstream.requests = []
    upstream.unexpected = []
    worker = threading.Thread(target=upstream.serve_forever, daemon=True)
    worker.start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            port = available_port()
            environment = {
                "QBT_URLS": f"http://127.0.0.1:{upstream.server_port}",
                "QBT_USER": "local-fixture",
                "QBT_PASSWORD": "local-fixture-only",
                "QBT_USAGE_PROVIDER": "none",
                "QBT_FALLBACK_AGGREGATE_DOWNLOAD_LIMIT_BYTES_PER_SEC": "1048576",
                "QBT_DOWNLOAD_STORAGE_PATH": directory,
                "QBT_DOWNLOAD_STORAGE_MIN_FREE_BYTES": "0",
                "QBT_DOWNLOAD_STORAGE_MIN_FREE_FRACTION": "0",
                "QBT_FULL_GUARD_THERMAL_CHECK_ENABLED": "false",
                "QBT_RPI_COOLING_ENABLED": "false",
                "QBT_TIMEOUT": "2",
                "QBT_REQUEST_ATTEMPTS": "1",
                "QBT_GUARD_POLL_SECONDS": "60",
                "QBT_GUARD_LOOP_EXIT_ON_ERROR": "true",
                "QBT_STOP_DOWNLOADS_ON_SHUTDOWN": "false",
                "QBT_STATUS_HTTP_ENABLED": "true",
                "QBT_STATUS_HTTP_HOST": "127.0.0.1",
                "QBT_STATUS_HTTP_PORT": str(port),
                "QBT_TORRENT_HEALTH_STATE_PATH": str(Path(directory, "health.json")),
                "QBT_RPI_COOLING_STATE_PATH": str(Path(directory, "cooling.json")),
            }
            with application("qbittorrent_smart_queues.guard", environment) as process:

                def completed_cycle():
                    status, body = request(port, "GET", "/status")
                    return status == 200 and bool(json.loads(body).get("last_loop_at"))

                wait_for(process, port, completed_cycle)
                assert request(port, "GET", "/healthz") == (200, b"ok\n")
                status, body = request(port, "GET", "/status")
                snapshot = json.loads(body)
                assert status == 200 and snapshot["loop_result"] == 0
                assert snapshot["last_decision_event"], (
                    "No actual controller decision was recorded"
                )
                status, metrics = request(port, "GET", "/metrics")
                assert status == 200 and b"qbt_guard_status_up 1" in metrics
                assert request(port, "GET", "/missing")[0] == 404
                assert not upstream.unexpected, (
                    "Controller attempted an unexpected fixture operation"
                )
                for path in [
                    "/api/v2/auth/login",
                    "/api/v2/app/version",
                    "/api/v2/torrents/info",
                ]:
                    assert any(
                        actual == path for _method, actual in upstream.requests
                    ), path
    finally:
        upstream.shutdown()
        upstream.server_close()
        worker.join(timeout=5)
    return {
        "health": True,
        "status": True,
        "metrics": True,
        "cycle_result": 0,
        "torrent_mutations": 0,
    }


def reconciler_fixture(directory):
    database = Path(directory, "ryokan.db")
    media = Path(directory, "anime")
    target = media / "Fixture Show" / "Season 01" / "Fixture - S01E01.mkv"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"x" * 101)
    with sqlite3.connect(database) as connection:
        connection.executescript("""
            CREATE TABLE grabbed_torrents (id INTEGER PRIMARY KEY, hash TEXT, torrent_name TEXT,
                series_id INTEGER, episode_numbers TEXT, state TEXT, grabbed_at TEXT,
                imported_at TEXT, imported_source_paths TEXT);
            CREATE TABLE series (id INTEGER PRIMARY KEY, folder_name TEXT);
            CREATE TABLE episode_grab_history (id INTEGER PRIMARY KEY, series_id INTEGER,
                episode_number INTEGER, release_title TEXT, file_name TEXT, state TEXT, grabbed_at TEXT);
            CREATE TABLE episode_quality_tags (series_id INTEGER, episode_number INTEGER, state TEXT);
            INSERT INTO series VALUES (1, 'Fixture Show');
            INSERT INTO grabbed_torrents VALUES (1, 'abc123', 'Fixture Batch', 1, '[1]', 'imported',
                '2026-01-01 00:00:00', '2026-01-01 00:01:00', '["/downloads/Fixture.mkv"]');
            INSERT INTO episode_grab_history VALUES (1, 1, 1, 'Fixture Batch', 'Fixture - S01E01.mkv',
                'completed', '2026-01-01 00:00:00');
            INSERT INTO episode_quality_tags VALUES (1, 1, 'completed');
        """)
    return database, media


def exercise_reconciler():
    with tempfile.TemporaryDirectory() as directory:
        database, media = reconciler_fixture(directory)
        before = hashlib.sha256(database.read_bytes()).digest()
        port = available_port()
        key = "local-fixture-key"
        environment = {
            "RYOKAN_RECONCILER_HOST": "127.0.0.1",
            "RYOKAN_RECONCILER_PORT": str(port),
            "RYOKAN_RECONCILER_DB_PATH": str(database),
            "RYOKAN_RECONCILER_MEDIA_ROOT": str(media),
            "RYOKAN_RECONCILER_API_KEY": key,
        }
        with application(
            "qbittorrent_smart_queues.ryokan_reconciler", environment
        ) as process:
            wait_for(
                process,
                port,
                lambda: request(port, "GET", "/healthz") == (200, b'{"ok": true}'),
            )
            payload = {
                "hash": "abc123",
                "expected_files": [
                    {
                        "source_path_candidates": ["/downloads/Fixture.mkv"],
                        "size_bytes": 101,
                    },
                ],
            }
            assert request(port, "POST", "/v1/imports/reconcile", payload)[0] == 401
            assert (
                request(port, "POST", "/v1/imports/reconcile", payload, "incorrect")[0]
                == 401
            )
            status, body = request(port, "POST", "/v1/imports/reconcile", payload, key)
            evidence = json.loads(body)
            assert status == 200 and evidence["status"] == "complete"
            assert (
                evidence["delete_allowed"]
                and evidence["sizes_match"]
                and evidence["targets_match"]
            )
            assert request(port, "GET", "/missing")[0] == 404
        assert hashlib.sha256(database.read_bytes()).digest() == before, (
            "Verification changed SQLite"
        )
        assert target_count(media) == 1, "Verification changed the fixture library"
    return {
        "health": True,
        "unauthorized": 401,
        "complete_receipts": True,
        "database_mutations": 0,
    }


def target_count(media):
    return sum(path.is_file() for path in media.rglob("*"))


def main():
    assert os.getuid() == os.getgid() == 65532
    assert platform.python_version() == os.environ["EXPECTED_PYTHON_VERSION"]
    assert (
        platform.machine()
        == {"linux/amd64": "x86_64", "linux/arm64": "aarch64"}[
            os.environ["EXPECTED_PLATFORM"]
        ]
    )
    assert __version__ == os.environ["EXPECTED_APP_VERSION"]
    with Path("/app/pyproject.toml").open("rb") as metadata:
        project = tomllib.load(metadata)
    assert project["project"]["dynamic"] == ["version"]
    assert (
        project["tool"]["setuptools"]["dynamic"]["version"]["attr"]
        == "qbittorrent_smart_queues.__version__"
    )
    for module in ["pip", "ensurepip", "setuptools"]:
        assert importlib.util.find_spec(module) is None, (
            f"Unexpected installer {module}"
        )
    assert not list(Path("/usr/local/bin").glob("pip*"))
    for module in ["_sqlite3", "_ssl", "_hashlib", "zlib", "_lzma", "_bz2", "_decimal"]:
        assert importlib.import_module(module).__file__.endswith(".so")
    assert ZoneInfo("Asia/Kolkata").key == "Asia/Kolkata"
    roots = ssl.create_default_context().get_ca_certs(binary_form=True)
    assert roots, "Default TLS root store is empty"
    forbidden = {
        "86a1ecba089c4a8d3bbe2734c612ba341d813e043cf9e8a862cd5c57a36bbe6b",
        "d7a7a0fb5d7e2731d771e9484ebcdef71d5f0c3e0a2948782bc83ee0ea699ef4",
    }
    assert forbidden.isdisjoint(hashlib.sha256(root).hexdigest() for root in roots)
    controller = exercise_controller()
    reconciler = exercise_reconciler()
    print(
        json.dumps(
            {
                "version": __version__,
                "python": platform.python_version(),
                "tls_roots": len(roots),
                "controller": controller,
                "reconciler": reconciler,
            }
        )
    )


if __name__ == "__main__":
    main()
