# qBittorrent Smart Queues

`qbittorrent-smart-queues` is a small qBittorrent Web API controller for
running a more deliberate download queue.

It can enforce quota-aware download rates, keep only one useful download active,
cool down stalled torrents, score torrent health over time, check local download
storage headroom, optionally order TV and movie downloads from Sonarr/Radarr,
optionally boost the next watched TV episode from Jellyfin activity, and
optionally stop downloads when NVMe temperatures reported by Prometheus are too
high.

The app is configured entirely with environment variables. It does not ship with
private network addresses, Kubernetes service names, or media-server defaults;
set the endpoints for the services you want it to control.

## Image

The GitHub Actions workflow publishes images to the repository package namespace:

```text
ghcr.io/<owner>/qbittorrent-smart-queues
```

The container entrypoint is:

```bash
python -m qbittorrent_smart_queues.guard
```

## Quick Start

Minimum qBittorrent-only configuration:

```bash
export QBT_URLS="http://qbittorrent.example:8080"
export QBT_USER="admin"
export QBT_PASSWORD="change-me"
python -m qbittorrent_smart_queues.guard
```

Container example:

```bash
docker run --rm \
  -e QBT_URLS="http://qbittorrent.example:8080" \
  -e QBT_USER="admin" \
  -e QBT_PASSWORD="change-me" \
  -v qbittorrent-smart-queues-state:/state \
  ghcr.io/<owner>/qbittorrent-smart-queues
```

## Configuration

Required for normal operation:

| Variable | Purpose |
| --- | --- |
| `QBT_URLS` | Comma-separated or newline-separated qBittorrent Web API base URLs. |
| `QBT_USER`, `QBT_PASSWORD` | qBittorrent credentials. `QBT_USERNAME` is also accepted. |

Quota control from UniFi Network / UDM is optional. When quota data is
unavailable and `UDM_FAIL_CLOSED=false`, the controller uses
`QBT_FALLBACK_AGGREGATE_DOWNLOAD_LIMIT_BYTES_PER_SEC`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `UDM_URL` | unset | UniFi Network / UDM base URL, for example `https://unifi.example`. |
| `UDM_API_KEY` | unset | API key authentication. |
| `UDM_USER`, `UDM_PASSWORD` | unset | Login authentication fallback. |
| `UDM_MONTHLY_DOWNLOAD_QUOTA_BYTES` | `2500000000000` | Monthly WAN download budget. |
| `UDM_MONTHLY_CAP_FRACTION` | `1.0` | Fraction of the monthly budget to expose to the guardrail. |
| `UDM_FAIL_CLOSED` | `false` | Pause downloads if quota data cannot be read. |

Optional media integrations only load when both URL(s) and an API key are set:

| Integration | URL variable(s) | API key variable(s) |
| --- | --- | --- |
| Sonarr TV queue | `QBT_TV_QUEUE_SONARR_URLS`, `SONARR_URLS`, `SONARR_URL` | `QBT_TV_QUEUE_SONARR_API_KEY`, `SONARR_API_KEY` |
| Radarr movie queue | `QBT_MOVIE_QUEUE_RADARR_URLS`, `RADARR_URLS`, `RADARR_URL` | `QBT_MOVIE_QUEUE_RADARR_API_KEY`, `RADARR_API_KEY` |
| Jellyfin watch state | `QBT_TV_WATCH_JELLYFIN_URLS`, `JELLYFIN_URLS`, `JELLYFIN_URL` | `QBT_TV_WATCH_JELLYFIN_API_KEY`, `JELLYFIN_API_KEY` |

When Sonarr TV queue metadata is available, TV torrents are constrained by a
hard per-series order. A later season or episode for the same show cannot be
selected while an older incomplete queued item for that show remains in
qBittorrent; priority tags and Jellyfin watch boosts do not bypass this rule.

Optional single-download selection tuning:

| Variable | Default | Purpose |
| --- | --- | --- |
| `QBT_SINGLE_DOWNLOAD_SELECTION_STRATEGY` | `tiered` | Use `balanced` to score candidates with extra weight for near-complete torrents, smaller remaining downloads, shorter ETA, current seeds, and availability. |
| `QBT_SINGLE_DOWNLOAD_PREEMPT_PRODUCTIVE_ENABLED` | `false` | Allow a productive active torrent to yield when a stopped candidate has a much better balanced score. |
| `QBT_SINGLE_DOWNLOAD_PREEMPT_PRODUCTIVE_SCORE_MARGIN` | `25.0` | Minimum balanced-score advantage required before preempting a productive torrent. |

Optional storage and thermal guards:

| Variable | Default | Purpose |
| --- | --- | --- |
| `QBT_DOWNLOAD_STORAGE_PATH` | `/downloads` | Filesystem path checked for free download headroom. |
| `QBT_DOWNLOAD_STORAGE_MIN_FREE_BYTES` | `32212254720` | Minimum free-space reserve. |
| `QBT_TORRENT_HEALTH_STATE_PATH` | `/state/torrent-health.json` | Persistent torrent health state file. |
| `PROMETHEUS_URL` | unset | Prometheus base URL for thermal checks. |
| `QBT_NVME_THERMAL_STOP_ENABLED` | enabled only when `PROMETHEUS_URL` is set | Enable NVMe thermal stop checks. |
| `QBT_NVME_THERMAL_QUERY` | generic node-exporter NVMe composite-temperature query | PromQL query returning temperature samples. |

Optional Raspberry Pi thermal coordinator:

| Variable | Default | Description |
| --- | --- | --- |
| `QBT_RPI_COOLING_ENABLED` | `false` | Enable Raspberry Pi thermal mitigation. |
| `QBT_RPI_COOLING_NODES` | `k8s-rpi1,k8s-rpi2,k8s-rpi3` | Nodes monitored for thermal mitigation. |
| `QBT_RPI_COOLING_CPU_THROTTLE_CELSIUS` | `70` | CPU threshold that applies qBittorrent throttle limits. |
| `QBT_RPI_COOLING_NVME_THROTTLE_CELSIUS` | `65` | NVMe threshold that applies qBittorrent throttle limits. |
| `QBT_RPI_COOLING_CPU_PAUSE_CELSIUS` | `74` | CPU threshold that pauses qBittorrent torrents. |
| `QBT_RPI_COOLING_NVME_PAUSE_CELSIUS` | `68` | NVMe threshold that pauses qBittorrent torrents. |
| `QBT_RPI_COOLING_CPU_RESUME_CELSIUS` | `65` | CPU temperature required before clearing mitigation. |
| `QBT_RPI_COOLING_NVME_RESUME_CELSIUS` | `60` | NVMe temperature required before clearing mitigation. |
| `QBT_RPI_COOLING_RESUME_HOLD_SECONDS` | `900` | Time all readings must remain below resume thresholds. |
| `QBT_RPI_COOLING_THROTTLE_DOWNLOAD_LIMIT_BYTES_PER_SEC` | `2097152` | Download limit used for RPi thermal throttle. |
| `QBT_RPI_COOLING_THROTTLE_UPLOAD_LIMIT_BYTES_PER_SEC` | `131072` | Upload limit used for RPi thermal throttle. |
| `QBT_RPI_COOLING_BATCH_SUSPEND_ENABLED` | `false` | Suspend configured Kubernetes CronJobs during mitigation. |
| `QBT_RPI_COOLING_BATCH_SUSPEND_TARGETS` | unset | Newline/comma list of `namespace/name` CronJobs to suspend. |
| `QBT_RPI_COOLING_SHUTDOWN_ENABLED` | `false` | Allow immediate clean shutdown when shutdown thresholds are reached. |
| `QBT_RPI_COOLING_LAST_RESORT_SHUTDOWN_ENABLED` | `false` | Allow clean shutdown only after sustained thermal pressure. |
| `QBT_RPI_COOLING_LAST_RESORT_MIN_ACTIVE_SECONDS` | `1800` | Minimum active mitigation time before last-resort shutdown. |
| `QBT_RPI_COOLING_CPU_SHUTDOWN_CELSIUS` | `85` | CPU last-resort shutdown threshold. |
| `QBT_RPI_COOLING_NVME_SHUTDOWN_CELSIUS` | `80` | NVMe last-resort shutdown threshold. |
| `QBT_RPI_COOLING_SHUTDOWN_URL_TEMPLATE` | `http://rpi-shutdown-{node}:8000/shutdown` | Per-node shutdown endpoint template. |
| `QBT_RPI_COOLING_POWER_OFF_URLS` | unset | Newline/comma list of `node=url` endpoints called after the node becomes NotReady. |
| `QBT_RPI_COOLING_POWER_ON_URLS` | unset | Newline/comma list of `node=url` endpoints called after the cooldown window. |
| `QBT_RPI_COOLING_STATE_PATH` | `/state/rpi-cooling.json` | Persistent cooling lock file. |

When enabled, the coordinator reads CPU and NVMe temperatures from Prometheus,
requires every configured node to be Kubernetes `Ready`, and starts with
service-preserving mitigations: qBittorrent throttle, qBittorrent pause, and
optional CronJob suspension. A persisted state file keeps the same mitigation
active until all temperatures remain below the resume thresholds for the hold
window. Clean shutdown is disabled by default and is intended as last-resort
protection; if enabled and power URLs are configured, the lock advances from
shutdown to cooling to booting and the controller powers the node back on after
the cooldown window. The coordinator does not cordon or drain nodes before
shutdown.

Logs default to plain text at `INFO` level. Set `QBT_LOG_FORMAT=json` for JSON
lines and `QBT_LOG_LEVEL=debug` for detailed decision telemetry. Repeated
critical decision summaries for unchanged actions are emitted every
`QBT_DECISION_SUMMARY_REPEAT_SECONDS` seconds, defaulting to `900`; set it to
`0` to emit every loop. Full decision payloads are emitted at `DEBUG` by
default; set `QBT_DECISION_LOG_LEVEL=info` while tuning, or
`QBT_DECISION_LOGS_ENABLED=false` to disable them.

When download storage is at or below the configured reserve and torrent-fit
checks are enabled, the controller enters a constrained recovery mode instead of
pausing every torrent. It only considers torrents whose selected remaining bytes
can fit in the currently free space, selects the smallest verified remaining
downloads first, temporarily raises qBittorrent's active queue limit up to
`QBT_DOWNLOAD_STORAGE_RECOVERY_MAX_ACTIVE` downloads, defaulting to `5`, and
keeps the recovery batch selected for the next poll even if individual torrents
are stalled or make no measured progress. Once storage is back above reserve,
the next controller pass restores the normal active download limit from
`QBT_SINGLE_DOWNLOAD_NORMAL_MAX_ACTIVE_DOWNLOADS`, defaulting to `1`. Torrents
with unknown remaining size or no selected files are blocked while storage is
constrained.

## Local Checks

```bash
PYTHONPATH=src python -m unittest discover -s tests
docker build -t qbittorrent-smart-queues:dev .
```
