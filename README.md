# qBittorrent Smart Queues

`qbittorrent-smart-queues` is the queue and quota controller used by the home-lab
media qBittorrent deployment.

It runs as a Kubernetes CronJob and controls qBittorrent through the Web API. The
controller enforces WAN quota budgets, single-active-download behavior, stall
cooldowns, persistent torrent health scoring, storage headroom checks, optional
Sonarr queue-aware TV ordering, and NVMe thermal stops.

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
variables. UDM and optional Sonarr API credentials are expected to be injected by
Kubernetes Secrets in the consuming deployment.

Full guard mode checks NVMe thermal state before selecting or starting torrents.
Set `QBT_FULL_GUARD_THERMAL_CHECK_ENABLED=false` only if another controller is
responsible for thermal gating.

Structured decision logs are emitted as JSON lines by default. Set
`QBT_STRUCTURED_DECISION_LOGS_ENABLED=false` to disable them. Decision events
include the selected torrent, rejection counts, budget, effective cap, UDM stats
age, storage headroom, and thermal state.
