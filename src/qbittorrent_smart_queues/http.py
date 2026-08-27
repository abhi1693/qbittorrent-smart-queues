"""Small standard-library HTTP helpers used by service adapters."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from qbittorrent_smart_queues.errors import ApiError


def join_url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def request_json(opener, method, url, headers=None, body=None, timeout=30):
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers=headers or {},
    )
    try:
        with opener.open(request, timeout=timeout) as response:
            payload = response.read()
            if not payload:
                return {}, response
            return json.loads(payload.decode("utf-8")), response
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ApiError(
            f"{method} {url} failed with HTTP {exc.code}: {detail}"
        ) from exc
    except urllib.error.URLError as exc:
        raise ApiError(f"{method} {url} failed: {exc}") from exc


def response_rows(data, label, key="data"):
    if isinstance(data, dict):
        rows = data.get(key, [])
    elif isinstance(data, list):
        rows = data
    else:
        raise ApiError(
            f"{label} response has unexpected shape: {type(data).__name__}"
        )
    if not isinstance(rows, list):
        raise ApiError(
            f"{label} response has unexpected shape: {type(rows).__name__}"
        )
    return rows
