"""qBittorrent Web API adapter.

The queue policy deliberately depends on this narrow client instead of HTTP,
cookie, retry, and compatibility details.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar

from qbittorrent_smart_queues.config import env_float, env_int, env_str
from qbittorrent_smart_queues.errors import ApiError
from qbittorrent_smart_queues.http import join_url


class QbtClient:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")
        self.timeout = env_int("QBT_TIMEOUT", 30)
        self.request_attempts = env_int("QBT_REQUEST_ATTEMPTS", 3)
        self.retry_delay = env_float("QBT_REQUEST_RETRY_DELAY", 2.0)
        self.username = env_str(("QBT_USER", "QBT_USERNAME"))
        self.password = os.environ.get("QBT_PASSWORD", "")
        self.cookie_jar = CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar)
        )

    def request(self, method, path, form=None):
        body = None
        headers = {}
        if form is not None:
            body = urllib.parse.urlencode(form).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        url = join_url(self.base_url, path)
        last_error = None
        for attempt in range(1, self.request_attempts + 1):
            request = urllib.request.Request(
                url,
                data=body,
                method=method,
                headers=headers,
            )
            try:
                with self.opener.open(request, timeout=self.timeout) as response:
                    return response.read()
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
                last_error = exc
                if attempt < self.request_attempts:
                    time.sleep(self.retry_delay)
        raise ApiError(
            f"{method} {path} failed after {self.request_attempts} attempts: {last_error}"
        )

    def login(self):
        if not self.username:
            return
        response = self.request(
            "POST",
            "/api/v2/auth/login",
            {"username": self.username, "password": self.password},
        ).decode("utf-8", errors="replace")
        if response.strip().lower() not in {"", "ok."}:
            raise ApiError(f"qBittorrent login failed: {response}")

    def _json(self, path):
        return json.loads(self.request("GET", path).decode("utf-8"))

    def _post_hashes(self, path, hashes, **form):
        if hashes:
            self.request("POST", path, {"hashes": "|".join(hashes), **form})

    def set_download_limit(self, limit_bytes_per_second):
        self.request(
            "POST",
            "/api/v2/transfer/setDownloadLimit",
            {"limit": str(max(0, int(limit_bytes_per_second)))},
        )

    def set_upload_limit(self, limit_bytes_per_second):
        self.request(
            "POST",
            "/api/v2/transfer/setUploadLimit",
            {"limit": str(max(1, int(limit_bytes_per_second)))},
        )

    def set_preferences(self, preferences):
        self.request(
            "POST",
            "/api/v2/app/setPreferences",
            {"json": json.dumps(preferences)},
        )

    def app_preferences(self):
        preferences = self._json("/api/v2/app/preferences")
        if not isinstance(preferences, dict):
            raise ApiError(
                "qBittorrent preferences response has unexpected shape: "
                f"{type(preferences).__name__}"
            )
        return preferences

    def set_active_queue_limits(self, max_active_downloads, max_active_torrents=None):
        max_active_downloads = max(1, int(max_active_downloads))
        if max_active_torrents is None:
            max_active_torrents = max_active_downloads
        self.set_preferences({
            "queueing_enabled": True,
            "max_active_downloads": max_active_downloads,
            "max_active_torrents": max(1, int(max_active_torrents)),
        })

    def stop_all(self):
        try:
            self.request("POST", "/api/v2/torrents/stop", {"hashes": "all"})
        except ApiError:
            self.request("POST", "/api/v2/torrents/pause", {"hashes": "all"})

    def torrents_info(self, filter_name=None):
        path = "/api/v2/torrents/info"
        if filter_name:
            path += "?" + urllib.parse.urlencode({"filter": filter_name})
        return self._json(path)

    def torrent_info(self, item_hash):
        if not item_hash:
            return None
        path = "/api/v2/torrents/info?" + urllib.parse.urlencode({"hashes": item_hash})
        torrents = self._json(path)
        if not isinstance(torrents, list):
            raise ApiError(
                "qBittorrent torrent info response has unexpected shape: "
                f"{type(torrents).__name__}"
            )
        normalized_hash = str(item_hash).strip().lower()
        return next(
            (
                torrent
                for torrent in torrents
                if str(torrent.get("hash") or "").strip().lower() == normalized_hash
            ),
            None,
        )

    def transfer_info(self):
        return self._json("/api/v2/transfer/info")

    def torrent_files(self, item_hash):
        if not item_hash:
            return []
        return self._json(
            "/api/v2/torrents/files?" + urllib.parse.urlencode({"hash": item_hash})
        )

    def torrent_trackers(self, item_hash):
        if not item_hash:
            return []
        trackers = self._json(
            "/api/v2/torrents/trackers?" + urllib.parse.urlencode({"hash": item_hash})
        )
        if not isinstance(trackers, list):
            raise ApiError(
                "qBittorrent trackers response has unexpected shape: "
                f"{type(trackers).__name__}"
            )
        return trackers

    def set_file_priority(self, item_hash, file_ids, priority):
        if item_hash and file_ids:
            self.request("POST", "/api/v2/torrents/filePrio", {
                "hash": item_hash,
                "id": "|".join(str(file_id) for file_id in file_ids),
                "priority": str(int(priority)),
            })

    def start_hashes(self, hashes):
        if not hashes:
            return
        try:
            self._post_hashes("/api/v2/torrents/start", hashes)
        except ApiError:
            self._post_hashes("/api/v2/torrents/resume", hashes)

    def recheck_hashes(self, hashes):
        self._post_hashes("/api/v2/torrents/recheck", hashes)

    def stop_hashes(self, hashes):
        if not hashes:
            return
        try:
            self._post_hashes("/api/v2/torrents/stop", hashes)
        except ApiError:
            self._post_hashes("/api/v2/torrents/pause", hashes)

    def set_torrent_download_limit(self, hashes, limit_bytes_per_second):
        self._post_hashes(
            "/api/v2/torrents/setDownloadLimit",
            hashes,
            limit=str(max(0, int(limit_bytes_per_second))),
        )

    def set_torrent_upload_limit(self, hashes, limit_bytes_per_second):
        self._post_hashes(
            "/api/v2/torrents/setUploadLimit",
            hashes,
            limit=str(max(0, int(limit_bytes_per_second))),
        )

    def top_priority(self, hashes):
        self._post_hashes("/api/v2/torrents/topPrio", hashes)

    def delete_hashes(self, hashes, delete_files):
        self._post_hashes(
            "/api/v2/torrents/delete",
            hashes,
            deleteFiles=str(bool(delete_files)).lower(),
        )

    def reannounce_hashes(self, hashes):
        self._post_hashes("/api/v2/torrents/reannounce", hashes)

    def add_tags(self, hashes, tags):
        if tags:
            self._post_hashes("/api/v2/torrents/addTags", hashes, tags=",".join(tags))

    def remove_tags(self, hashes, tags):
        if tags:
            self._post_hashes("/api/v2/torrents/removeTags", hashes, tags=",".join(tags))

    def create_tags(self, tags):
        if tags:
            self.request("POST", "/api/v2/torrents/createTags", {"tags": ",".join(tags)})

    def all_tags(self):
        return self._json("/api/v2/torrents/tags")

    def delete_tags(self, tags):
        if tags:
            self.request("POST", "/api/v2/torrents/deleteTags", {"tags": ",".join(tags)})
