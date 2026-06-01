# qBittorrent Smart Queues

`qbittorrent-smart-queues` is the queue and quota controller used by the home-lab
media qBittorrent deployment.

It runs as a continuous Kubernetes controller and controls qBittorrent through
the Web API. The controller enforces WAN quota budgets, single-active-download
behavior, stall cooldowns, persistent torrent health scoring, storage headroom
checks, optional Sonarr queue-aware TV ordering, optional Jellyfin watch-aware
single-episode boosts, optional Radarr queue-aware movie ordering, and NVMe
thermal stops.

## Image

Images are published to:

```text
ghcr.io/abhi1693/qbittorrent-smart-queues
```

The container entrypoint is:

```bash
python -m qbittorrent_smart_queues.guard
```

## Local Checks

```bash
PYTHONPATH=src python -m unittest discover -s tests
docker build -t qbittorrent-smart-queues:dev .
```

## Runtime

The controller is configured entirely through environment variables. qBittorrent
credentials are read from `QBT_USER`/`QBT_PASSWORD` or compatible existing
variables. UDM and optional Sonarr/Radarr API credentials are expected to be
injected by Kubernetes Secrets in the consuming deployment.

Sonarr queue enrichment uses `SONARR_API_KEY` or
`QBT_TV_QUEUE_SONARR_API_KEY` with `QBT_TV_QUEUE_SONARR_URLS`. Radarr queue
enrichment uses `RADARR_API_KEY` or `QBT_MOVIE_QUEUE_RADARR_API_KEY` with
`QBT_MOVIE_QUEUE_RADARR_URLS`. Both integrations read `/api/v3/queue`, index
records by download ID and title, and fall back to torrent-name parsing/order
when credentials or queue records are unavailable.

Jellyfin watch enrichment uses `JELLYFIN_API_KEY` or
`QBT_TV_WATCH_JELLYFIN_API_KEY` with `QBT_TV_WATCH_JELLYFIN_URLS`. Active
episode sessions from `/Sessions` boost matching single-episode TV torrents for
later episodes in the same season. Full-season packs are deliberately excluded
because once a pack finishes, the entire season becomes available together.

Each controller pass checks NVMe thermal state before selecting or starting
torrents. Set `QBT_FULL_GUARD_THERMAL_CHECK_ENABLED=false` only if another
controller is responsible for thermal gating.

The entrypoint always runs the polling controller. `QBT_GUARD_POLL_SECONDS`
controls the normal poll interval, and `QBT_GUARD_ERROR_POLL_SECONDS` controls
the retry delay after an errored pass.

Logs default to plain text at `INFO` level, with routine poll telemetry kept at
`DEBUG` while compact behavior-changing decisions stay at `INFO`: pause,
throttle, try, keep, stop, and no-candidate outcomes. Set `QBT_LOG_LEVEL` to
`debug`, `info`, `warning`, or `error` to tune verbosity. Set `QBT_LOG_FORMAT=json`
when machine-readable JSON lines are preferred.

Full decision payloads are emitted at `DEBUG` by default and include the
selected torrent, rejection counts, budget, effective cap, UDM stats age,
storage headroom, and thermal state. Set `QBT_DECISION_LOG_LEVEL=info` when
tuning and `QBT_DECISION_LOGS_ENABLED=false` to disable them. The legacy
`QBT_STRUCTURED_DECISION_LOGS_ENABLED=false` switch is still accepted as a
compatibility alias for disabling decision logs.
