# qBittorrent Smart Queues

`qbittorrent-smart-queues` is a small qBittorrent Web API controller for
running a more deliberate download queue.

It can enforce quota-aware download rates, keep only one useful download active,
cool down stalled torrents, score torrent health over time, clean up stale
Arr-managed download leftovers, check local download storage headroom,
optionally order TV and movie downloads from Sonarr/Radarr, optionally boost the
next watched TV episode from Jellyfin activity, and optionally stop downloads
when NVMe temperatures reported by Prometheus are too high.

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

Download-rate limits are integer bytes per second. Use binary examples when
translating ISP speed into qBittorrent caps: `10485760` = `10 MiB/s`,
`8388608` = `8 MiB/s`, `2097152` = `2 MiB/s`, and `524288` = `512 KiB/s`.
Set ISP usable caps no higher than the real sustained throughput available
after router/VPN/protocol overhead.

| Variable | Default | Purpose |
| --- | --- | --- |
| `UDM_URL` | unset | UniFi Network / UDM base URL, for example `https://unifi.example`. |
| `UDM_API_KEY` | unset | API key authentication. |
| `UDM_USER`, `UDM_PASSWORD` | unset | Login authentication fallback. |
| `UDM_MONTHLY_DOWNLOAD_QUOTA_BYTES` | `2500000000000` | Monthly WAN download budget. |
| `UDM_MONTHLY_CAP_FRACTION` | `1.0` | Fraction of the monthly budget to expose to the guardrail. |
| `UDM_FAIL_CLOSED` | `false` | Pause downloads if quota data cannot be read. |
| `QBT_ISP_USABLE_DOWNLOAD_LIMIT_BYTES_PER_SEC` | `10485760` | Hard ISP usable download cap in bytes/s. This caps smoothed quota rates, burst mode, and single-download mode. Example: `10485760` = `10 MiB/s`. Replaces `QBT_MAX_AGGREGATE_DOWNLOAD_LIMIT_BYTES_PER_SEC` and `QBT_SINGLE_DOWNLOAD_DOWNLOAD_LIMIT_BYTES_PER_SEC`, which remain accepted as fallback aliases. |
| `QBT_UNCAPPED_DOWNLOAD_WINDOW_ENABLED` | `false` | Set qBittorrent's download limit to `0` during the configured local-time window, which qBittorrent treats as unlimited. Monthly/daily quota stop guardrails, thermal checks, storage checks, and queue selection still apply. |
| `QBT_UNCAPPED_DOWNLOAD_WINDOW_TIMEZONE` | `Asia/Kolkata` | IANA timezone used for the uncapped window. |
| `QBT_UNCAPPED_DOWNLOAD_WINDOW_START_LOCAL` | `22:00` | Local start time for uncapped downloads, inclusive. Example: `22:00` = 10 PM. |
| `QBT_UNCAPPED_DOWNLOAD_WINDOW_END_LOCAL` | `05:00` | Local end time for uncapped downloads, exclusive. Windows that cross midnight are supported. Example: `05:00` = 5 AM. |
| `QBT_UNCAPPED_DOWNLOAD_WINDOW_MAX_ACTIVE_DOWNLOADS` | `QBT_SINGLE_DOWNLOAD_NORMAL_MAX_ACTIVE_DOWNLOADS` | Total qBittorrent active download limit used during the uncapped window. Parked stalled torrents stay active inside this ceiling instead of adding slots above it. |
| `QBT_QUOTA_BURST_ENABLED` | `false` | Allow faster downloads above the smoothed quota-safe rate while daily and monthly reserves remain. |
| `QBT_ISP_USABLE_BURST_DOWNLOAD_LIMIT_BYTES_PER_SEC` | `QBT_ISP_USABLE_DOWNLOAD_LIMIT_BYTES_PER_SEC` | Burst-mode ISP usable cap in bytes/s. Example: `10485760` = `10 MiB/s`. Replaces `QBT_QUOTA_BURST_DOWNLOAD_LIMIT_BYTES_PER_SEC`, which remains accepted as a fallback alias. |
| `QBT_QUOTA_BURST_MIN_MONTHLY_REMAINING_FRACTION` | `0.10` | Minimum monthly guardrail reserve required before burst mode is allowed. |
| `QBT_QUOTA_BURST_MIN_DAILY_REMAINING_FRACTION` | `0.20` | Minimum daily guardrail reserve required before burst mode is allowed. |

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

Optional stale torrent maintenance:

| Variable | Default | Purpose |
| --- | --- | --- |
| `QBT_STALE_TORRENT_MAINTENANCE_ENABLED` | `true` | Track stalled incomplete torrents in the health state and run stale maintenance. |
| `QBT_STALE_TORRENT_DAYS` | `14` | Age before a continuously stalled or parked incomplete torrent is considered stale. |
| `QBT_STALE_TORRENT_TAG_PREFIX` | `stale-stalled` | Prefix used for stale stalled torrent tags, for example `stale-stalled-20260601`. |
| `QBT_STALE_TORRENT_REANNOUNCE_ENABLED` | `true` | Reannounce stale stalled torrents so they can find peers without occupying active work slots. |
| `QBT_STALE_TORRENT_PARK_RUNNING_ENABLED` | `true` | Stop running stale stalled torrents after tagging/reannouncing so other downloads can run. |
| `QBT_STALE_TORRENT_REMOVE_IMPORTED_COMPLETED` | `true` | Remove completed Sonarr leftovers when every queue warning says the episode file was already imported. |
| `QBT_STALE_TORRENT_FAIL_PERMANENT_IMPORT_FAILURES` | `true` | Remove and blocklist completed Radarr downloads with permanent corrupt/sample-detection import failures. |
| `QBT_STALE_TORRENT_ARR_TIMEOUT` | `QBT_ARR_QUEUE_TIMEOUT` or `10` | Timeout for Sonarr/Radarr queue delete calls. |

Stale maintenance is intentionally conservative. It does not delete incomplete
14-day stalled torrents just because they are old; it tags, reannounces, and
parks them so they can resume later while the selector moves on to torrents that
can make progress. Destructive cleanup is limited to completed downloads where
Arr confirms that the media was already imported, or completed Radarr downloads
that Arr marks with permanent corrupt media/sample-detection failures.

Optional single-download selection tuning:

| Variable | Default | Purpose |
| --- | --- | --- |
| `QBT_SINGLE_DOWNLOAD_SELECTION_STRATEGY` | `tiered` | Use `balanced` to score candidates with extra weight for near-complete torrents, smaller remaining downloads, shorter ETA, current seeds, and availability. |
| `QBT_SINGLE_DOWNLOAD_PREEMPT_PRODUCTIVE_ENABLED` | `false` | Allow a productive active torrent to yield when a stopped candidate has a much better balanced score. |
| `QBT_SINGLE_DOWNLOAD_PREEMPT_PRODUCTIVE_SCORE_MARGIN` | `25.0` | Minimum balanced-score advantage required before preempting a productive torrent. |
| `QBT_SINGLE_DOWNLOAD_PARK_STALLED_ENABLED` | `true` | Keep stalled/no-progress torrents active instead of pausing them, and run replacement candidates beside them. |
| `QBT_SINGLE_DOWNLOAD_PARK_STALLED_SAMPLES` | storage recovery stall samples | No-progress samples required before a non-productive running torrent is parked. qBittorrent `stalledDL`/`metaDL` torrents park immediately. |
| `QBT_SINGLE_DOWNLOAD_MAX_PARKED_STALLED` | `0` | Maximum parked stalled torrents in normal mode. `0` means no cap, so stalled torrents are not paused just because the parked set is large. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_SECONDS` | `3600` | Base cooldown for torrents that fail a single-download attempt. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_NO_PROGRESS_SECONDS` | base cooldown | Cooldown for torrents that run but do not move enough bytes during the sample. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_METADATA_SECONDS` | min(base, 1800) | Cooldown honored for manually/future-applied metadata wait tags. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_TRACKER_DEAD_SECONDS` | max(base, 21600) | Cooldown for stalled torrents with no connected seeds, reported seeds, or availability. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_IMPORT_FAILED_SECONDS` | max(base, 86400) | Cooldown honored for manually/future-applied import-failed tags. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_MANUAL_HOLD_SECONDS` | max(base, 604800) | Cooldown honored for manually-applied hold tags. |
| `QBT_TRACKER_HEALTH_SCORING_ENABLED` | `true` | Read qBittorrent tracker responses for eligible candidates and include tracker health in selection scores. |
| `QBT_TRACKER_HEALTH_MAX_CANDIDATES_PER_PASS` | `50` | Maximum `/torrents/trackers` reads per controller pass. |
| `QBT_TRACKER_HEALTH_MIN_REFRESH_SECONDS` | `300` | Minimum age before refreshing a torrent's tracker health again. |
| `QBT_TRACKER_HEALTH_SCORE_MAX_AGE_SECONDS` | `21600` | Maximum tracker-health observation age used for scoring; `0` means no age limit. |
| `QBT_STATUS_HTTP_ENABLED` | `false` | Enable the in-process queue status endpoint. |
| `QBT_STATUS_HTTP_HOST` | `0.0.0.0` | Bind address for the status endpoint. |
| `QBT_STATUS_HTTP_PORT` | `8081` | Bind port for `/healthz`, `/status`, and `/metrics`. |

Cooldown tags are reasoned as
`<prefix>-<reason>-<timestamp>`, for example
`quota-stalled-tracker-dead-20260601T123456Z`. Legacy
`<prefix>-<timestamp>` tags are still honored with the base cooldown. The
normal selector writes `no-progress` and `tracker-dead` tags today; `metadata`,
`import-failed`, and `manual-hold` are parsed and cleaned so they can be applied
manually or by future integrations without being treated as generic stalls.

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

When `QBT_STATUS_HTTP_ENABLED=true`, the controller exposes:

- `/healthz`: plain `ok` health response.
- `/status`: JSON snapshot of the latest queue decision, loop result, selected torrent, rejection counts, and candidate counts.
- `/metrics`: Prometheus text metrics for the latest decision. The endpoint
  includes controller freshness, latest action labels, selected torrent
  progress/speed/ETA, selected and parked torrent info rows, queue funnel
  counts, rejection reasons, effective transfer caps, budget bytes, and
  storage headroom.

Single-download mode keeps an active torrent only when selected bytes or
downloaded bytes move by at least `QBT_SINGLE_DOWNLOAD_MIN_PROGRESS_BYTES`
during the `QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS` sample window. Instantaneous
download speed is telemetry only for normal queue decisions; a low speed does
not stop a torrent if it is making real progress. When
`QBT_SINGLE_DOWNLOAD_ADAPTIVE_PROGRESS_ENABLED=true`, defaulting to true, that
floor scales up for larger torrents using `QBT_SINGLE_DOWNLOAD_PROGRESS_SIZE_FRACTION`,
is capped by `QBT_SINGLE_DOWNLOAD_PROGRESS_MAX_BYTES`, and is relaxed for older
torrents using `QBT_SINGLE_DOWNLOAD_PROGRESS_AGE_RELIEF_DAYS` and
`QBT_SINGLE_DOWNLOAD_PROGRESS_AGE_RELIEF_FRACTION`.

By default, normal single-download mode now parks stalled/no-progress torrents
instead of pausing and cooldown-tagging them. Parked torrents stay active in
qBittorrent so they can resume immediately if a needed peer appears, while the
controller excludes them from replacement selection and raises qBittorrent's
active download limit enough to start another candidate beside them.

When download storage is at or below the configured reserve and torrent-fit
checks are enabled, the controller enters a constrained recovery mode instead of
pausing every torrent. It only considers torrents whose selected remaining bytes
can fit in the currently free space, selects the smallest verified remaining
downloads first, temporarily raises qBittorrent's active queue limit up to
`QBT_DOWNLOAD_STORAGE_RECOVERY_MAX_ACTIVE` downloads, defaulting to `5`, and
tracks no-progress samples for each recovery member. After
`QBT_DOWNLOAD_STORAGE_RECOVERY_STALL_SAMPLES` samples, defaulting to `2`, a
stalled member is parked: it stays active in qBittorrent so it can resume if
seeders appear, but it no longer consumes one of the active recovery worker
slots. The controller then refills open worker slots with other fitting
torrents while accounting for parked torrents in the storage headroom budget.
At most `QBT_DOWNLOAD_STORAGE_RECOVERY_MAX_PARKED_STALLED` stalled torrents are
parked, defaulting to `10`. Recovery workers also need to meet
`QBT_DOWNLOAD_STORAGE_RECOVERY_MIN_RATE_BYTES_PER_SEC`, defaulting to the normal
slow torrent floor of `QBT_SINGLE_DOWNLOAD_SLOW_MIN_RATE_BYTES_PER_SEC`. A
running torrent below that rate is treated as too slow for recovery and is
replaced instead of being parked. Once storage is back above reserve, the next
controller pass restores the normal active download limit from
`QBT_SINGLE_DOWNLOAD_NORMAL_MAX_ACTIVE_DOWNLOADS`, defaulting to `1`. Torrents
with unknown remaining size or no selected files are blocked while storage is
constrained.

## Local Checks

```bash
PYTHONPATH=src python -m unittest discover -s tests
docker build -t qbittorrent-smart-queues:dev .
```
