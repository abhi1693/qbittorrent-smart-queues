"""Narrow Ryokan import-receipt verifier used by Smart Queues.

The service is intended to run as a sidecar in Ryokan's pod so it can read the
live SQLite database and the anime library without sharing Ryokan's RWO volume
with another pod.  Its only mutation is an idempotent repair: an imported grab
whose source and destination receipts do not cover every qBittorrent-selected
media file is moved back to ``pending`` for Ryokan's normal post-processor.
"""

from __future__ import annotations

import hmac
import json
import os
import posixpath
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath


MAX_REQUEST_BYTES = 2 * 1024 * 1024


class ReconcileError(ValueError):
    """The caller supplied a request that cannot be reconciled safely."""


def normalize_absolute_path(value):
    value = str(value or "").strip().replace("\\", "/")
    if not value or not value.startswith("/"):
        return ""
    normalized = posixpath.normpath(value)
    return normalized if normalized.startswith("/") else ""


def normalize_expected_files(expected_files):
    normalized = []
    for item in expected_files or []:
        if not isinstance(item, dict):
            raise ReconcileError("expected_files entries must be objects")
        candidates = {
            path
            for path in (
                normalize_absolute_path(candidate)
                for candidate in item.get("source_path_candidates", [])
            )
            if path
        }
        if not candidates:
            raise ReconcileError("every expected file needs an absolute source path candidate")
        try:
            size_bytes = int(item.get("size_bytes", 0))
        except (TypeError, ValueError) as exc:
            raise ReconcileError("every expected file needs a valid size") from exc
        if size_bytes <= 0:
            raise ReconcileError("every expected file needs a positive size")
        normalized.append({"candidates": candidates, "size_bytes": size_bytes})
    if not normalized:
        raise ReconcileError("at least one expected media file is required")
    return normalized


def source_receipts_match(expected_candidates, imported_source_paths):
    receipts = [normalize_absolute_path(path) for path in imported_source_paths or []]
    if any(not path for path in receipts):
        return False
    receipt_set = set(receipts)
    if len(receipts) != len(expected_candidates) or len(receipt_set) != len(expected_candidates):
        return False

    matched = set()
    for expected in expected_candidates:
        candidates = expected["candidates"]
        matches = candidates & receipt_set
        if len(matches) != 1:
            return False
        matched.update(matches)
    return len(matched) == len(expected_candidates)


def safe_library_target(media_root, folder_name, file_name):
    if not folder_name or not file_name:
        return None
    if PurePosixPath(file_name).name != file_name:
        return None
    root = Path(media_root).resolve()
    target = (root / folder_name / "Season 01" / file_name).resolve()
    try:
        target.relative_to(root)
    except ValueError:
        return None
    return target


def connect_database(db_path, writable=False):
    mode = "rw" if writable else "ro"
    database = sqlite3.connect(
        f"file:{Path(db_path)}?mode={mode}",
        uri=True,
        timeout=10,
    )
    database.row_factory = sqlite3.Row
    database.execute("PRAGMA busy_timeout = 10000")
    database.execute("PRAGMA foreign_keys = ON")
    return database


def active_grab(database, item_hash):
    rows = database.execute(
        """
        SELECT id, hash, torrent_name, series_id, episode_numbers, state,
               grabbed_at, imported_at, imported_source_paths
        FROM grabbed_torrents
        WHERE lower(hash) = lower(?)
          AND state IN ('pending', 'imported')
        ORDER BY id DESC
        LIMIT 2
        """,
        (item_hash,),
    ).fetchall()
    if len(rows) != 1:
        return None, "ambiguous" if rows else "not_found"
    return rows[0], ""


def completed_targets_for_grab(database, grab):
    return database.execute(
        """
        WITH ranked AS (
            SELECT h.id, h.series_id, h.episode_number, h.file_name, h.state,
                   s.folder_name,
                   ROW_NUMBER() OVER (
                       PARTITION BY h.series_id, h.episode_number
                       ORDER BY h.grabbed_at DESC, h.id DESC
                   ) AS receipt_rank
            FROM episode_grab_history h
            JOIN series s ON s.id = h.series_id
            WHERE h.release_title = ?
              AND h.grabbed_at >= ?
        )
        SELECT id, series_id, episode_number, file_name, state, folder_name
        FROM ranked
        WHERE receipt_rank = 1 AND state = 'completed'
        ORDER BY series_id, episode_number
        """,
        (grab["torrent_name"], grab["grabbed_at"]),
    ).fetchall()


def inspect_import(db_path, media_root, item_hash, expected_files):
    item_hash = str(item_hash or "").strip().lower()
    if not item_hash:
        raise ReconcileError("hash is required")
    expected_candidates = normalize_expected_files(expected_files)

    with connect_database(db_path) as database:
        grab, lookup_status = active_grab(database, item_hash)
        if grab is None:
            return {
                "status": lookup_status,
                "delete_allowed": False,
                "hash": item_hash,
                "expected_count": len(expected_candidates),
            }

        base = {
            "status": grab["state"],
            "delete_allowed": False,
            "hash": item_hash,
            "grab_id": grab["id"],
            "expected_count": len(expected_candidates),
        }
        if grab["state"] != "imported":
            return base
        if not grab["imported_at"]:
            base["status"] = "completed_without_import"
            return base

        try:
            imported_sources = json.loads(grab["imported_source_paths"] or "[]")
        except (TypeError, json.JSONDecodeError):
            imported_sources = []
        if not isinstance(imported_sources, list):
            imported_sources = []

        target_rows = completed_targets_for_grab(database, grab)
        target_paths = []
        invalid_target_count = 0
        for row in target_rows:
            target = safe_library_target(media_root, row["folder_name"], row["file_name"])
            if target is None:
                invalid_target_count += 1
            else:
                target_paths.append(target)

        distinct_episode_count = len({
            (row["series_id"], row["episode_number"])
            for row in target_rows
        })
        distinct_targets = {str(path) for path in target_paths}
        existing_target_sizes = {}
        for path in target_paths:
            if path.is_file():
                size_bytes = path.stat().st_size
                if size_bytes > 0:
                    existing_target_sizes[str(path)] = size_bytes
        expected_count = len(expected_candidates)
        sources_match = source_receipts_match(expected_candidates, imported_sources)
        sizes_match = sorted(existing_target_sizes.values()) == sorted(
            expected["size_bytes"] for expected in expected_candidates
        )
        targets_match = (
            invalid_target_count == 0
            and len(target_rows) == expected_count
            and distinct_episode_count == expected_count
            and len(distinct_targets) == expected_count
            and len(existing_target_sizes) == expected_count
            and sizes_match
        )

        base.update({
            "status": "complete" if sources_match and targets_match else "incomplete",
            "delete_allowed": bool(sources_match and targets_match),
            "receipt_source_count": len(imported_sources),
            "receipt_target_count": len(target_rows),
            "distinct_episode_count": distinct_episode_count,
            "distinct_target_count": len(distinct_targets),
            "existing_target_count": len(existing_target_sizes),
            "sources_match": sources_match,
            "targets_match": targets_match,
            "sizes_match": sizes_match,
            "history_receipt_ids": [row["id"] for row in target_rows],
            "episode_receipts": [
                [row["series_id"], row["episode_number"]]
                for row in target_rows
            ],
        })
        return base


def requeue_incomplete_import(db_path, evidence):
    if evidence.get("status") != "incomplete" or evidence.get("delete_allowed"):
        return evidence

    grab_id = int(evidence["grab_id"])
    history_ids = [int(value) for value in evidence.get("history_receipt_ids", [])]
    episode_receipts = [
        (int(series_id), int(episode_number))
        for series_id, episode_number in evidence.get("episode_receipts", [])
    ]

    database = connect_database(db_path, writable=True)
    try:
        database.execute("BEGIN IMMEDIATE")
        updated = database.execute(
            """
            UPDATE grabbed_torrents
            SET state = 'pending', imported_at = NULL, imported_source_paths = NULL
            WHERE id = ? AND state = 'imported'
            """,
            (grab_id,),
        ).rowcount
        if updated != 1:
            database.rollback()
            result = dict(evidence)
            result.update({"status": "pending", "delete_allowed": False})
            return result

        if history_ids:
            placeholders = ",".join("?" for _ in history_ids)
            database.execute(
                f"UPDATE episode_grab_history SET state = 'grabbed' WHERE id IN ({placeholders})",
                history_ids,
            )
        for series_id, episode_number in episode_receipts:
            database.execute(
                """
                UPDATE episode_quality_tags
                SET state = 'grabbed'
                WHERE series_id = ? AND episode_number = ? AND state = 'completed'
                """,
                (series_id, episode_number),
            )
        database.commit()
    except Exception:
        database.rollback()
        raise
    finally:
        database.close()

    result = dict(evidence)
    result.update({"status": "requeued", "delete_allowed": False})
    result.pop("history_receipt_ids", None)
    result.pop("episode_receipts", None)
    return result


def reconcile_import(db_path, media_root, item_hash, expected_files):
    evidence = inspect_import(db_path, media_root, item_hash, expected_files)
    return requeue_incomplete_import(db_path, evidence)


class ReconcilerHandler(BaseHTTPRequestHandler):
    server_version = "ryokan-import-reconciler/1"

    def log_message(self, message, *args):
        print(f"ryokan-import-reconciler: {self.address_string()} {message % args}", flush=True)

    def send_json(self, status, payload):
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def authorized(self):
        supplied = self.headers.get("X-Api-Key", "")
        return bool(supplied) and hmac.compare_digest(supplied, self.server.api_key)

    def do_GET(self):
        if self.path != "/healthz":
            self.send_json(404, {"error": "not found"})
            return
        healthy = Path(self.server.db_path).is_file() and Path(self.server.media_root).is_dir()
        self.send_json(200 if healthy else 503, {"ok": healthy})

    def do_POST(self):
        if self.path != "/v1/imports/reconcile":
            self.send_json(404, {"error": "not found"})
            return
        if not self.authorized():
            self.send_json(401, {"error": "unauthorized"})
            return
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length <= 0 or content_length > MAX_REQUEST_BYTES:
                raise ReconcileError("invalid request size")
            body = json.loads(self.rfile.read(content_length).decode("utf-8"))
            result = reconcile_import(
                self.server.db_path,
                self.server.media_root,
                body.get("hash"),
                body.get("expected_files"),
            )
        except (json.JSONDecodeError, ReconcileError, TypeError, ValueError) as exc:
            self.send_json(400, {"error": str(exc)})
            return
        except (OSError, sqlite3.Error) as exc:
            self.send_json(503, {"error": f"reconciliation unavailable: {exc}"})
            return
        self.send_json(200, result)


def main():
    host = os.environ.get("RYOKAN_RECONCILER_HOST", "0.0.0.0")
    port = int(os.environ.get("RYOKAN_RECONCILER_PORT", "8979"))
    api_key = os.environ.get("RYOKAN_RECONCILER_API_KEY", "")
    if not api_key:
        raise SystemExit("RYOKAN_RECONCILER_API_KEY is required")

    server = ThreadingHTTPServer((host, port), ReconcilerHandler)
    server.db_path = os.environ.get("RYOKAN_RECONCILER_DB_PATH", "/data/ryokan.db")
    server.media_root = os.environ.get("RYOKAN_RECONCILER_MEDIA_ROOT", "/media/anime")
    server.api_key = api_key
    print(f"ryokan-import-reconciler listening on {host}:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
