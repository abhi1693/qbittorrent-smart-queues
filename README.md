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

The same image can run the Ryokan database-side reconciliation adapter:

```bash
python -m qbittorrent_smart_queues.ryokan_reconciler
```

Run that adapter in Ryokan's pod with the live database at `/data/ryokan.db`
and the library at `/media/anime`. It requires `RYOKAN_RECONCILER_API_KEY` and
accepts optional `RYOKAN_RECONCILER_DB_PATH`, `RYOKAN_RECONCILER_MEDIA_ROOT`,
`RYOKAN_RECONCILER_HOST`, and `RYOKAN_RECONCILER_PORT` overrides. Its write
scope is limited to returning an exact, already-imported hash with incomplete
receipts to `pending`; it never changes pending, unknown, or ambiguous grabs.

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

Quota control from UniFi Network is optional. When quota data is
unavailable and `QBT_USAGE_FAIL_CLOSED=false`, the controller uses
`QBT_FALLBACK_AGGREGATE_DOWNLOAD_LIMIT_BYTES_PER_SEC`.

`QBT_USAGE_PROVIDER` selects the network API adapter. It defaults to `unifi`;
set it to `none` to run only with the fallback download limit. Providers return
the same typed billing-cycle/day snapshot, so another router brand can be added
by implementing the provider contract and registering its factory without
changing queue policy.

### Architecture and provider extension

The runtime has three explicit class boundaries:

- `SmartQueueApplication` owns process lifecycle, polling, and shutdown.
- `SmartQueueController` coordinates one policy cycle using injected factories.
- `NetworkUsageProvider` is the abstract base for vendor adapters;
  `BackupInternetProvider` is an optional second capability.

The built-in `UnifiProvider` lives entirely in `providers/unifi.py`. A new
first-party brand belongs in its own module and is wired only into
`register_builtin_usage_providers`; queue policy does not import concrete
providers. The minimum implementation is:

```python
from qbittorrent_smart_queues.providers import (
    NetworkUsageProvider,
    UsageSnapshot,
    register_usage_provider,
)


class ExampleProvider(NetworkUsageProvider):
    provider_name = "example"

    def usage_snapshot(self, now):
        return UsageSnapshot(
            cycle_usage_bytes=read_cycle_usage(),
            day_usage_bytes=read_day_usage(),
        )


register_usage_provider(ExampleProvider.provider_name, ExampleProvider)
```

Subclass `BackupInternetProvider` as well only when the brand can resolve the
active WAN role. Provider factories are validated at creation time, including
their inheritance and canonical provider identity.

### Migrating to 0.2

Version 0.2 is an intentional clean break. Update deployment configuration
before changing the image; the removed `UDM_*` names are not read at runtime.

| 0.1 variable | 0.2 variable |
| --- | --- |
| `UDM_URL` | `UNIFI_URL` |
| `UDM_API_KEY` | `UNIFI_API_KEY` |
| `UDM_USER`, `UDM_PASSWORD` | `UNIFI_USER`, `UNIFI_PASSWORD` |
| `UDM_SITE` | `UNIFI_SITE` |
| `UDM_API_BASE_PATH` | `UNIFI_API_BASE_PATH` |
| `UDM_VERIFY_TLS` | `UNIFI_VERIFY_TLS` |
| `UDM_BILLING_CYCLE_DAY` | `QBT_BILLING_CYCLE_DAY` |
| `UDM_MONTHLY_DOWNLOAD_QUOTA_BYTES` | `QBT_MONTHLY_QUOTA_BYTES` |
| `UDM_MONTHLY_DOWNLOAD_GUARDRAIL_BYTES` | `QBT_MONTHLY_GUARDRAIL_BYTES` |
| `UDM_MONTHLY_CAP_FRACTION` | `QBT_MONTHLY_CAP_FRACTION` |
| `UDM_STATS_INTERVAL` | `UNIFI_STATS_INTERVAL` |
| `UDM_HISTORY_STATS_INTERVAL` | `UNIFI_HISTORY_STATS_INTERVAL` |
| `UDM_CURRENT_STATS_INTERVAL` | `UNIFI_CURRENT_STATS_INTERVAL` |
| `UDM_DOWNLOAD_ATTRS` | `UNIFI_USAGE_ATTRS` |
| `UDM_INCLUDE_UPLOAD` | `QBT_USAGE_INCLUDE_UPLOAD` |
| `UDM_FAIL_CLOSED` | `QBT_USAGE_FAIL_CLOSED` |
| `UDM_BACKUP_INTERNET_STOP_ENABLED` | `QBT_BACKUP_INTERNET_STOP_ENABLED` |
| `UDM_BACKUP_INTERNET_FAIL_CLOSED` | `QBT_BACKUP_INTERNET_FAIL_CLOSED` |

Set `QBT_USAGE_PROVIDER=unifi` explicitly when quota enforcement is expected.
The status field is now `usage_provider`, and provider metrics use the
`qbt_guard_usage_*` prefix.

Billing-cycle and day boundaries follow UniFi's reporting timezone, discovered
from `stat/sysinfo` unless `UNIFI_STATS_TIMEZONE` overrides it.
`QBT_BILLING_CYCLE_DAY` selects the local calendar day when each monthly quota
cycle starts; for example, `17` covers the 17th through the 16th inclusive. In
the default `split-daily-hourly` mode, completed local days use UniFi daily
reports and the open local day uses hourly reports, with explicit timestamp
filtering so the open day cannot appear in both totals. Hourly values above the
WAN provider capability reported by UniFi, plus the configured safety
multiplier, are treated as counter discontinuities rather than traffic. The
invalid field is replaced with the larger adjacent valid hourly value; other
WAN fields in the same bucket remain counted. The subtracted correction is
persisted by local date so a later UniFi daily rollup cannot restore the bad
counter value. If a newly enabled counter makes an already completed daily
rollup physically impossible, the controller replays that day's retained
hourly report once and merges the new field correction into the existing state.
If the hourly data is unavailable or inconclusive, it keeps the raw daily value
so quota enforcement remains conservative.

Set `QBT_USAGE_INCLUDE_UPLOAD=true` to make the monthly and daily guardrails count
download plus upload usage. Quota byte budgets apply to the combined total when
upload counting is enabled.

The optional backup-internet guard stops every torrent before queue selection
when UniFi has failed over to a backup WAN. It reads configured WAN roles from
UniFi Network's `networkconf` response, where backup connections are marked
`failover-only`, and resolves the gateway's actual active uplink from
`stat/device`. It deliberately does not use `last_wan_status`: that historical
field can disagree with the active routed uplink. Editable UniFi display names
are recorded for observability only; decisions use WAN groups, logical
interfaces, and uplink interface identifiers read dynamically from UniFi. No
WAN name, network group, or physical port identifier is configured in Smart
Queues. On failover, the controller sets the configured stop-rate limits and
calls qBittorrent's stop-all endpoint on every poll. Normal queue selection can
resume on the first successful poll that resolves the primary uplink again.
While this guard is enabled, quota usage is also limited to WANs whose UniFi
role is not `failover-only`. Smart Queues generates the required UniFi report
fields from those roles at runtime; configured download attributes, editable
WAN names, network-group identifiers, and physical ports are not configured to
select the primary. This keeps backup-provider traffic out of the primary ISP's
monthly quota because qBittorrent is stopped for the entire backup session.

Download-rate limits are integer bytes per second. Use binary examples when
translating ISP speed into qBittorrent caps: `10485760` = `10 MiB/s`,
`8388608` = `8 MiB/s`, `2097152` = `2 MiB/s`, and `524288` = `512 KiB/s`.
Set ISP usable caps no higher than the real sustained throughput available
after router/VPN/protocol overhead.

| Variable | Default | Purpose |
| --- | --- | --- |
| `QBT_USAGE_PROVIDER` | `unifi` | Network usage adapter. Supported values are `unifi` and `none`. |
| `UNIFI_URL` | unset | UniFi Network base URL, for example `https://unifi.example`. |
| `UNIFI_API_KEY` | unset | API key authentication. |
| `UNIFI_USER`, `UNIFI_PASSWORD` | unset | Login authentication fallback. |
| `UNIFI_SITE` | `default` | UniFi Network site name. |
| `UNIFI_API_BASE_PATH` | `/proxy/network` | UniFi Network API path behind the console. |
| `UNIFI_VERIFY_TLS` | `true` | Verify the UniFi controller TLS certificate. Disable only for a controller with a deliberately trusted self-signed endpoint. |
| `UNIFI_USAGE_ATTRS` | `wan-rx_bytes,wan2-rx_bytes` | UniFi report fields used when backup-internet role filtering is disabled. |
| `QBT_MONTHLY_QUOTA_BYTES` | `2500000000000` | Monthly WAN download budget. |
| `QBT_MONTHLY_CAP_FRACTION` | `1.0` | Fraction of the monthly budget to expose to the guardrail. |
| `QBT_BILLING_CYCLE_DAY` | `1` | Local calendar day from `1` to `31` when the monthly quota cycle starts. Days beyond a shorter month's end are clamped to that month's final day. |
| `QBT_USAGE_INCLUDE_UPLOAD` | `false` | Include WAN upload bytes in the monthly and daily quota guardrails. |
| `QBT_USAGE_FAIL_CLOSED` | `false` | Pause downloads if quota data cannot be read. |
| `UNIFI_STATS_TIMEZONE` | unset | Optional IANA timezone override for UniFi usage periods. When unset, the controller discovers the timezone from UniFi `stat/sysinfo`. |
| `UNIFI_STATS_RATE_LIMIT_MULTIPLIER` | `1.25` | Safety allowance above UniFi's WAN provider capability before an hourly field is classified as an impossible counter discontinuity. |
| `UNIFI_STATS_MAX_DOWNLOAD_RATE_BYTES_PER_SEC` | `0` | Optional per-WAN-field byte/s ceiling when UniFi has no provider capability. `0` leaves fields without a discovered capability unbounded. |
| `UNIFI_USAGE_CORRECTION_STATE_PATH` | `/state/unifi-usage-corrections.json` | Persistent per-local-day corrections for impossible UniFi report buckets. |
| `QBT_BACKUP_INTERNET_STOP_ENABLED` | `false` | Stop all torrents while UniFi reports a configured failover-only WAN as the gateway's active uplink. |
| `QBT_BACKUP_INTERNET_FAIL_CLOSED` | `true` | Stop all torrents when the backup-internet guard is enabled but its UniFi role or active-uplink state cannot be read or mapped safely. |
| `QBT_ISP_USABLE_DOWNLOAD_LIMIT_BYTES_PER_SEC` | `10485760` | Hard ISP usable download cap in bytes/s. This caps smoothed quota rates, burst mode, and single-download mode. Example: `10485760` = `10 MiB/s`. |
| `QBT_UNCAPPED_DOWNLOAD_WINDOW_ENABLED` | `false` | Set qBittorrent's download limit to `0` during the configured local-time window, which qBittorrent treats as unlimited. Monthly/daily quota stop guardrails, thermal checks, storage checks, and queue selection still apply. |
| `QBT_UNCAPPED_DOWNLOAD_WINDOW_TIMEZONE` | `Asia/Kolkata` | IANA timezone used for the uncapped window. |
| `QBT_UNCAPPED_DOWNLOAD_WINDOW_START_LOCAL` | `22:00` | Local start time for uncapped downloads, inclusive. Example: `22:00` = 10 PM. |
| `QBT_UNCAPPED_DOWNLOAD_WINDOW_END_LOCAL` | `05:00` | Local end time for uncapped downloads, exclusive. Windows that cross midnight are supported. Example: `05:00` = 5 AM. |
| `QBT_UNCAPPED_DOWNLOAD_WINDOW_MAX_ACTIVE_DOWNLOADS` | `QBT_SINGLE_DOWNLOAD_NORMAL_MAX_ACTIVE_DOWNLOADS` | Active download worker limit used during the uncapped window. Parked stalled torrents add listening slots above this worker limit so they can resume immediately when seeders return. |
| `QBT_QUOTA_BURST_ENABLED` | `false` | Allow faster downloads above the smoothed quota-safe rate while daily and monthly reserves remain. |
| `QBT_ISP_USABLE_BURST_DOWNLOAD_LIMIT_BYTES_PER_SEC` | `QBT_ISP_USABLE_DOWNLOAD_LIMIT_BYTES_PER_SEC` | Burst-mode ISP usable cap in bytes/s. Example: `10485760` = `10 MiB/s`. |
| `QBT_QUOTA_BURST_MIN_MONTHLY_REMAINING_FRACTION` | `0.10` | Minimum monthly guardrail reserve required before burst mode is allowed. |
| `QBT_QUOTA_BURST_MIN_DAILY_REMAINING_FRACTION` | `0.20` | Minimum daily guardrail reserve required before burst mode is allowed. |

Optional media integrations only load when both URL(s) and an API key are set:

| Integration | URL variable(s) | API key variable(s) |
| --- | --- | --- |
| Sonarr TV queue | `QBT_TV_QUEUE_SONARR_URLS`, `SONARR_URLS`, `SONARR_URL` | `QBT_TV_QUEUE_SONARR_API_KEY`, `SONARR_API_KEY` |
| Radarr movie queue | `QBT_MOVIE_QUEUE_RADARR_URLS`, `RADARR_URLS`, `RADARR_URL` | `QBT_MOVIE_QUEUE_RADARR_API_KEY`, `RADARR_API_KEY` |
| Jellyfin watch state | `QBT_TV_WATCH_JELLYFIN_URLS`, `JELLYFIN_URLS`, `JELLYFIN_URL` | `QBT_TV_WATCH_JELLYFIN_API_KEY`, `JELLYFIN_API_KEY` |

Sonarr and Radarr queue reads are paginated so large backlogs are visible to
ordering, cleanup, and rejection filters. `QBT_TV_QUEUE_PAGE_SIZE` controls
Sonarr page size, `QBT_MOVIE_QUEUE_PAGE_SIZE` controls Radarr page size,
`QBT_ARR_QUEUE_PAGE_SIZE` is the Radarr fallback, and
`QBT_ARR_QUEUE_MAX_PAGES` defaults to `100` as a safety cap.

When Sonarr TV queue metadata is available, TV torrents are constrained by a
hard per-series order. A later season or episode for the same show cannot be
selected while an older incomplete queued item for that show remains in
qBittorrent; priority tags and Jellyfin watch boosts do not bypass this rule.

For multi-file torrents, Smart Queues can also manage qBittorrent file
priorities without parsing filenames. The first incomplete selected media file
in natural filename order is raised to maximum priority, the next configured
lookahead files are raised to high priority, and later selected media files are
returned to normal priority. Secondary media paths such as extras, samples, and
trailers are sorted behind primary media files.

| Variable | Default | Purpose |
| --- | --- | --- |
| `QBT_SINGLE_DOWNLOAD_FILE_PRIORITY_ENABLED` | `QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED` or `true` | Manage selected media-file priorities inside eligible multi-file torrents. |
| `QBT_SINGLE_DOWNLOAD_FILE_PRIORITY_CATEGORIES` | `QBT_SINGLE_DOWNLOAD_CATEGORIES` or `tv,movies,anime,priority-tv,priority-movies,priority-anime` | qBittorrent categories whose selected media files should be reprioritized. |
| `QBT_SINGLE_DOWNLOAD_FILE_PRIORITY_LOOKAHEAD_FILES` | `QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_LOOKAHEAD_EPISODES` or `2` | Number of incomplete selected media files after the first one to raise to high priority. |

Optional stale torrent maintenance:

Generic `missingFiles` cleanup removes only the qBittorrent entry and always
sends `deleteFiles=false`. A missing-files state does not prove that partial or
moved payload data is disposable. Files are deleted only by the receipt-aware
Arr and Ryokan cleanup paths after their import checks succeed. Torrents in the
Ryokan-managed anime categories are retained entirely when Ryokan cleanup is
enabled so receipt reconciliation can requeue and recheck an interrupted import.

| Variable | Default | Purpose |
| --- | --- | --- |
| `QBT_STALE_TORRENT_MAINTENANCE_ENABLED` | `true` | Track stalled incomplete torrents in the health state and run stale maintenance. |
| `QBT_STALE_TORRENT_DAYS` | `14` | Age before a continuously stalled or parked incomplete torrent is considered stale. |
| `QBT_STALE_TORRENT_TAG_PREFIX` | `stale-stalled` | Prefix used for stale stalled torrent tags, for example `stale-stalled-20260601`. |
| `QBT_STALE_TORRENT_REANNOUNCE_ENABLED` | `true` | Reannounce stale stalled torrents so they can find peers without occupying active work slots. |
| `QBT_STALE_TORRENT_PARK_RUNNING_ENABLED` | `true` | Stop running stale stalled torrents after tagging/reannouncing so other downloads can run. |
| `QBT_STALE_TORRENT_REMOVE_IMPORTED_COMPLETED` | `true` | Remove completed Sonarr/Radarr leftovers when Arr says the media is already imported and every remaining queue reason is a known terminal no-import-needed warning. |
| `QBT_STALE_TORRENT_FAIL_PERMANENT_IMPORT_FAILURES` | `true` | Remove and blocklist completed Radarr downloads with permanent corrupt/sample-detection import failures. |
| `QBT_STALE_TORRENT_ARR_TIMEOUT` | `QBT_ARR_QUEUE_TIMEOUT` or `10` | Timeout for Sonarr/Radarr queue delete calls during stale cleanup. |
| `QBT_METADATA_TIMEOUT_ARR_TIMEOUT` | `QBT_ARR_QUEUE_TIMEOUT` or `10` | Timeout for Sonarr/Radarr queue delete calls after metadata bootstrap timeouts. |
| `QBT_ARR_IMPORT_REJECTION_CLEANUP_ENABLED` | `true` | Remove Sonarr/Radarr queue records with terminal import rejections, such as already-imported media or releases that are not an upgrade, before normal download selection. |
| `QBT_ARR_IMPORT_REJECTION_FILTER_ENABLED` | `true` | Keep terminal import-rejected torrents out of Smart Queue worker selection even when cleanup is disabled, verification fails, or Arr deletion fails. |
| `QBT_ARR_IMPORT_REJECTION_VERIFY_EXISTING_FILE` | `true` | Require Sonarr/Radarr to show an existing episode or movie file before removing an already-imported or not-an-upgrade queue record. |
| `QBT_ARR_IMPORT_REJECTION_BLOCKLIST` | `false` | Whether import-rejection cleanup asks Sonarr/Radarr to blocklist the release. The default only removes the queue item and torrent from qBittorrent. |
| `QBT_ARR_IMPORT_REJECTION_ARR_TIMEOUT` | `QBT_ARR_QUEUE_TIMEOUT` or `10` | Timeout for Sonarr/Radarr verification and queue delete calls during import-rejection cleanup. |
| `QBT_RYOKAN_IMPORTED_ANIME_CLEANUP_ENABLED` | `false` | Reconcile completed Ryokan anime leftovers and delete them only after exact Ryokan source receipts and distinct size-matched library targets are complete. This supports source-retaining copy mode. |
| `QBT_RYOKAN_IMPORTED_ANIME_CATEGORIES` | `anime,priority-anime` | qBittorrent categories treated as Ryokan-managed anime downloads. |
| `QBT_RYOKAN_IMPORTED_ANIME_DOWNLOAD_ROOT` | `/downloads` | Mounted download root used to resolve every selected source safely for receipt verification. Sources may still exist in copy mode and are removed by qBittorrent only after verification. |
| `QBT_RYOKAN_IMPORTED_ANIME_MIN_COMPLETED_SECONDS` | `1800` | Minimum age after qBittorrent completion before Ryokan imported-anime cleanup can delete the torrent entry. |
| `QBT_RYOKAN_IMPORTED_ANIME_DELETE_FILES` | `QBT_DELETE_FILES` or `true` | qBittorrent `deleteFiles` value for Ryokan imported-anime cleanup after source media verification succeeds. |
| `QBT_RYOKAN_IMPORT_RECONCILER_URL` | unset | Base URL of the Ryokan import reconciler. Cleanup fails closed when it is unset or unavailable. |
| `QBT_RYOKAN_IMPORT_RECONCILER_API_KEY` | `SONARR_ANIME_API_KEY` | Shared API key sent to the import reconciler. |
| `QBT_RYOKAN_IMPORT_RECONCILER_TIMEOUT` | `10` | Timeout in seconds for receipt verification and repair. |

The qBittorrent tag `blacklist` is a built-in manual operator action. On each
successful qBittorrent connection, the controller ensures the global
`blacklist` tag exists so it is available from the qBittorrent UI tag list. On
each pass, the controller consumes torrents with this tag before normal queue
selection, finds the matching Sonarr or Radarr queue record, and calls the Arr
queue API with `removeFromClient=true`, `blocklist=true`, and
`skipRedownload=false`. That removes the current torrent, blocklists the release
in Arr, and leaves Sonarr/Radarr free to grab a different source. If no matching
Arr queue record is found, the controller deletes the torrent directly from
qBittorrent with `deleteFiles=true`. If the Arr delete call or direct qBittorrent
delete fails, it replaces the action tag with `blacklist-failed`.

Optional total-availability admission control prevents Smart Queues from
spending the full payload on a swarm that cannot supply every selected piece.
Before a new worker starts, the controller applies an `availability-probe` tag
and a small per-torrent download cap. A later pass reannounces the torrent and
collects multiple fresh qBittorrent availability samples. Any sample at or
above `1.0` admits the torrent, removes the probe cap, and tags it
`availability-verified`. Repeated positive samples below `1.0` remove and
blocklist the release through Sonarr/Radarr, with `skipRedownload=false`, so Arr
can choose a different release. Zero or unavailable telemetry is inconclusive:
the torrent is stopped and cooled down without being deleted. Force-started
torrents remain manual overrides. The controller also evaluates legacy stalled
torrents near completion, so enabling the feature drains existing incomplete
swarm debt instead of protecting only new grabs.

| Variable | Default | Purpose |
| --- | --- | --- |
| `QBT_AVAILABILITY_ADMISSION_ENABLED` | `false` | Enable bounded total-availability probes, Arr blocklisting, and qBittorrent removal for incomplete swarms. |
| `QBT_AVAILABILITY_MIN_COMPLETE` | `1.0` | Minimum qBittorrent total availability that proves all selected pieces exist in the observed swarm. |
| `QBT_AVAILABILITY_PROBE_DOWNLOAD_LIMIT_BYTES_PER_SEC` | `1048576` | Per-torrent download cap while availability is unverified. |
| `QBT_AVAILABILITY_PROBE_SAMPLES` | `6` | Maximum number of fresh availability samples collected after reannounce and start. |
| `QBT_AVAILABILITY_REQUIRED_BELOW_MINIMUM_SAMPLES` | `5` | Positive below-threshold samples required before destructive rejection. Any complete sample admits immediately. |
| `QBT_AVAILABILITY_PROBE_INTERVAL_SECONDS` | `10` | Delay between fresh qBittorrent availability samples. |
| `QBT_AVAILABILITY_PROBE_MAX_ATTEMPTS_PER_RUN` | `2` | Maximum availability-admission torrents checked in one controller run. Attempts remain sequential and bounded by the normal run-time budget. |
| `QBT_AVAILABILITY_PROBE_TRACKER_DEAD_RETRY_SECONDS` | `1800` | Retry interval for an admission-pending torrent whose last probe returned no swarm telemetry. This overrides a longer generic tracker-dead cooldown only for `availability-probe` torrents. |
| `QBT_AVAILABILITY_LEGACY_MIN_PROGRESS` | `0.95` | Progress threshold that makes an existing incomplete, below-threshold torrent eligible for probing. |
| `QBT_AVAILABILITY_LEGACY_MIN_NO_PROGRESS_SAMPLES` | `2` | Stored no-progress observations that make another existing below-threshold torrent eligible for probing. |
| `QBT_AVAILABILITY_VERIFIED_RECHECK_SECONDS` | `21600` | Minimum time after a complete availability sample before a still-stalled torrent can be probed again. The timestamp is persisted in its `availability-verified-*` tag. |
| `QBT_AVAILABILITY_ARR_TIMEOUT` | `QBT_ARR_QUEUE_TIMEOUT` or `10` | Timeout for Sonarr/Radarr removal and blocklist calls. |

Stale maintenance is intentionally conservative. It does not delete incomplete
14-day stalled torrents just because they are old; it tags, reannounces, and
parks them so they can resume later while the selector moves on to torrents that
can make progress. Destructive cleanup is limited to completed downloads where
Arr confirms that the media was already imported, completed Arr leftovers where
already-imported warnings are mixed only with known terminal no-import-needed
warnings, completed Radarr downloads that Arr marks with permanent
corrupt media/sample-detection failures, terminal Sonarr/Radarr import
rejections verified against an existing library file, and metadata bootstrap
timeouts. Ryokan imported-anime cleanup is opt-in and does not trust completion
alone: it requires strict qBittorrent completion, the configured anime category,
the download root to be mounted, every selected media source file to be missing
from that root, an exact match against Ryokan's imported-source receipts, and one
distinct library target with the same file size per selected media file. If
Ryokan marked a grab imported without satisfying that contract, the reconciler
atomically moves only that grab back to `pending` and Smart Queues requests a
qBittorrent recheck so Ryokan's normal post-processor can import the complete
set. Pending, unknown, ambiguous, or selected-file/episode-count-mismatched grabs
fail closed without mutation. Automatic requeue is allowed only when Ryokan's
distinct grabbed episode count equals the qBittorrent-selected media count. When a
metadata timeout matches a Sonarr or Radarr queue record, Smart Queues asks that
app to remove the torrent from the client, blocklist the release, and allow
redownload so the app can search for another release. If no Arr queue record
matches, it deletes the torrent directly from qBittorrent with
`deleteFiles=true`.

Import-rejection cleanup runs before normal queue selection. If Sonarr or Radarr
marks a queued torrent as already imported, not an upgrade, or not a Custom
Format upgrade for the existing episode/movie file, Smart Queues first verifies
the existing library file through that Arr instance, then removes the Arr queue record with
`removeFromClient=true`, `blocklist=false`, and `skipRedownload=false` by
default. If verification or deletion cannot be completed, the selection filter
still rejects that torrent so Smart Queues does not assign bandwidth to a release
Arr has already decided it cannot import.

Optional single-download selection tuning:

If a torrent is force-started in qBittorrent (`forcedDL`/`forced*` state), Smart
Queues treats that as a manual operator override outside the managed worker
pool. Normal queue selection continues without stopping or counting the forced
torrent, so a user-started download runs in addition to the configured useful
worker limit. Explicit safety stops such as quota, backup-internet, thermal,
storage hard-stop, and shutdown hooks still apply.

| Variable | Default | Purpose |
| --- | --- | --- |
| `QBT_SINGLE_DOWNLOAD_SELECTION_STRATEGY` | `tiered` | `tiered` keeps Arr/queue order as the primary sort key. `balanced` lets the unified score weigh queue order with health, progress, ETA, sources, availability, priority, cooldown, and storage-fit components. |
| `QBT_SINGLE_DOWNLOAD_SLOW_MIN_RATE_BYTES_PER_SEC` | `65536` | Minimum active download speed treated as productive in normal selection and used as the default recovery slow-torrent floor. |
| `QBT_SINGLE_DOWNLOAD_PREEMPT_PRODUCTIVE_ENABLED` | `false` | Allow a productive active torrent to yield when a stopped candidate has a much better unified score. |
| `QBT_SINGLE_DOWNLOAD_PREEMPT_PRODUCTIVE_SCORE_MARGIN` | `25.0` | Minimum unified-score advantage required before preempting a productive torrent. |
| `QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_ENABLED` | `false` | In category-batch mode, let a productive worker yield when its observed rate is far below the other productive workers and another safely queued same-category torrent is waiting. |
| `QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_FRACTION` | `0.25` | Yield threshold as a fraction of the other productive workers' median observed rate. `0.25` means slower than 25% of the peer median. Values are bounded to `0.01`–`0.95`. |
| `QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_TOLERANCE_FRACTION` | `0.10` | Leeway below the calculated relative threshold and acceptable-speed floor. `0.10` allows a 10% margin and avoids churn near either cutoff. Values are bounded to `0.0`–`0.95`; use `0` for exact cutoffs. |
| `QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_MIN_ACCEPTABLE_BYTES_PER_SEC` | `1048576` | A productive worker at or above this rate, after tolerance, is considered good enough even when its peers are much faster. With the defaults, rates down to about 0.9 MiB/s are retained. |
| `QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_MIN_PEERS` | `1` | Minimum number of other productive workers required before making a relative-speed comparison. The good-enough floor and tolerance keep a two-worker comparison from churning acceptable workers. |
| `QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_MIN_REFERENCE_BYTES_PER_SEC` | `1048576` | Minimum peer-median rate required before relative-speed yielding is allowed, preventing churn when the entire active pool is slow. |
| `QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_MIN_TRIAL_SECONDS` | `300` | Minimum trial time for a newly selected worker before it can yield for relative speed. Existing workers with no recorded selection time can be assessed immediately. |
| `QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_DEFER_SECONDS` | `1800` | Non-failure defer window after a relative-speed yield. The torrent becomes eligible for another trial after this window; spare slots remain available for other useful work in the meantime. |
| `QBT_SINGLE_DOWNLOAD_SELECTION_LEASE_SECONDS` | `900` | Minimum dwell lease granted when a torrent is selected. While the lease is active, a torrent that is productive or has current/recent connected peers is not preempted or replaced by a briefly higher-scoring candidate. Set to `0` to disable. |
| `QBT_SINGLE_DOWNLOAD_SELECTION_LEASE_PEER_GRACE_SECONDS` | lease seconds | How long recent connected peer contact keeps an active lease eligible after peers temporarily disappear. |
| `QBT_SINGLE_DOWNLOAD_PRODUCTIVE_CAP_FRACTION` | `0.80` | Fraction of each worker's effective cap share used as the productive-speed floor. |
| `QBT_SINGLE_DOWNLOAD_PROGRESS_CAP_FRACTION` | `0.80` | Fraction of each worker's effective cap share used as the progress byte floor. |
| `QBT_NO_PROGRESS_MISSING_FINAL_PIECE_MIN_PROGRESS` | `0.999` | Progress threshold for classifying a no-progress torrent as `missing-final-piece`. |
| `QBT_NO_PROGRESS_MISSING_FINAL_PIECE_MIN_AVAILABILITY` | `0.95` | Lower availability bound for `missing-final-piece` classification. |
| `QBT_NO_PROGRESS_MISSING_FINAL_PIECE_MAX_AVAILABILITY` | `1.0` | Upper availability bound for `missing-final-piece` classification. |
| `QBT_NO_PROGRESS_CLASS_SCORE_MAX_AGE_SECONDS` | `86400` | How long the last no-progress class influences candidate scoring. |
| `QBT_SINGLE_DOWNLOAD_MAX_ACTIVE_DOWNLOADS_PER_CATEGORY` | `0` | Optional normal-mode category worker limit. When set above `0`, the selector keeps or starts up to this many active download workers for each qBittorrent category, while parked stalled torrents remain active outside the per-category worker count. |
| `QBT_SINGLE_DOWNLOAD_MAX_TOTAL_ACTIVE_DOWNLOADS` | `0` | Optional aggregate normal-mode worker cap across all categories. `0` leaves the total governed by the per-category and effective-rate limits. Parked stalled listeners remain outside this worker count. |
| `QBT_SINGLE_DOWNLOAD_ADAPTIVE_WORKERS_ENABLED` | `true` | Dynamically size the useful worker pool between the configured minimum and worker ceiling. Underused capacity adds bounded probe slots; productive workers that meet the utilization target cause the pool to contract instead of starting unnecessary torrents. |
| `QBT_SINGLE_DOWNLOAD_MIN_ACTIVE_DOWNLOADS` | `1` | Minimum useful worker target while eligible candidates exist. The candidate count and configured worker ceilings can still lower it. |
| `QBT_SINGLE_DOWNLOAD_PROBE_SLOTS` | `2` | Additional candidate slots opened while productive throughput remains below the target. Failed probes are replaced in the same controller run when attempt and time budgets allow. |
| `QBT_SINGLE_DOWNLOAD_TARGET_UTILIZATION_FRACTION` | `0.80` | Fraction of the effective download capacity at which productive workers are considered sufficient and probe slots are closed. |
| `QBT_SINGLE_DOWNLOAD_PARK_STALLED_ENABLED` | `true` | Keep stalled/no-progress torrents active instead of pausing them, and run replacement candidates beside them. |
| `QBT_SINGLE_DOWNLOAD_PARK_STALLED_SAMPLES` | storage recovery stall samples | No-progress samples required before a non-productive running torrent is parked. qBittorrent `stalledDL`/`metaDL` torrents park immediately. |
| `QBT_SINGLE_DOWNLOAD_MAX_PARKED_STALLED` | `0` | Maximum parked stalled torrents in normal mode. `0` means no cap, so stalled torrents are not paused just because the parked set is large. |
| `QBT_SINGLE_DOWNLOAD_METADATA_BOOTSTRAP_ENABLED` | `true` | When storage-fit checks are enabled, briefly start one stopped or queued magnet whose size is unknown so qBittorrent can fetch its metadata before the controller evaluates storage headroom. Productive downloads retain priority. |
| `QBT_SINGLE_DOWNLOAD_METADATA_BOOTSTRAP_TIMEOUT_SECONDS` | `60` | Maximum time to wait for one magnet's metadata before stopping it and applying metadata cooldown. |
| `QBT_SINGLE_DOWNLOAD_METADATA_BOOTSTRAP_POLL_SECONDS` | `2` | Poll interval while waiting for qBittorrent to report metadata. |
| `QBT_SINGLE_DOWNLOAD_METADATA_BOOTSTRAP_MAX_ATTEMPTS_PER_RUN` | `1` | Maximum unknown-metadata magnets attempted during one controller run. |
| `QBT_SINGLE_DOWNLOAD_METADATA_BOOTSTRAP_DOWNLOAD_LIMIT_BYTES_PER_SEC` | `65536` | Temporary per-torrent download cap during metadata discovery. Set to `0` to leave the torrent's existing limit unchanged. |
| `QBT_SINGLE_DOWNLOAD_METADATA_BOOTSTRAP_UPLOAD_LIMIT_BYTES_PER_SEC` | `16384` | Temporary per-torrent upload cap during metadata discovery. Set to `0` to leave the torrent's existing limit unchanged. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_SECONDS` | `3600` | Base cooldown for torrents that fail a single-download attempt. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_NO_PROGRESS_SECONDS` | base cooldown | Cooldown for torrents that run but do not move enough bytes during the sample. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_METADATA_SECONDS` | min(base, 1800) | Reason-specific cooldown window for future metadata-wait health-state entries. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_TRACKER_DEAD_SECONDS` | max(base, 21600) | Cooldown for stalled torrents with no connected seeds, reported seeds, or availability. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_IMPORT_FAILED_SECONDS` | max(base, 86400) | Reason-specific cooldown window for future import-failed health-state entries. |
| `QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_MANUAL_HOLD_SECONDS` | max(base, 604800) | Reason-specific cooldown window for future manual-hold health-state entries. |
| `QBT_TRACKER_HEALTH_SCORING_ENABLED` | `true` | Read qBittorrent tracker responses for eligible candidates and include tracker health in selection scores. |
| `QBT_TRACKER_HEALTH_MAX_CANDIDATES_PER_PASS` | `50` | Maximum `/torrents/trackers` reads per controller pass. |
| `QBT_TRACKER_HEALTH_MIN_REFRESH_SECONDS` | `300` | Minimum age before refreshing a torrent's tracker health again. |
| `QBT_TRACKER_HEALTH_SCORE_MAX_AGE_SECONDS` | `21600` | Maximum tracker-health observation age used for scoring; `0` means no age limit. |
| `QBT_STATUS_HTTP_ENABLED` | `false` | Enable the in-process queue status endpoint. |
| `QBT_STATUS_HTTP_HOST` | `0.0.0.0` | Bind address for the status endpoint. |
| `QBT_STATUS_HTTP_PORT` | `8081` | Bind port for `/healthz`, `/status`, and `/metrics`. |

When the status endpoint is enabled, backup-WAN state is exposed as
`qbt_guard_backup_internet_active`,
`qbt_guard_backup_internet_state_available`, and
`qbt_guard_active_wan_info`. The last metric labels the resolved network,
network group, logical interface, uplink, and primary/backup role without
exposing WAN addresses or credentials.

Cooldown state is canonical in `QBT_TORRENT_HEALTH_STATE_PATH`, including the
reason, scope, current cooldown failure count, first-seen time, last-tried time,
and next retry time. qBittorrent tags remain visibility output only, using
`<prefix>-<reason>-<timestamp>` names such as
`quota-stalled-tracker-dead-20260601T123456Z`; tag-only cooldowns are cleaned up
but do not block selection.

Storage-fit checks need the torrent's selected file sizes, which qBittorrent
does not know for a newly added magnet until its metadata arrives. Without a
metadata bootstrap, an "add stopped" workflow can deadlock: the controller
cannot prove the torrent fits, so it never starts the torrent that must run to
learn its size. The bootstrap path breaks that cycle without bypassing the
storage reserve. A runnable metadata bootstrap takes precedence over the older
availability-probe backlog, so a newly queued magnet cannot be starved one
probe at a time. It temporarily opens one queue slot, applies the per-torrent
traffic caps above, starts the stopped or queued magnet, polls `has_metadata`,
and always attempts to stop it before restoring its previous limits. If both
targeted and global stop calls fail, the low traffic caps remain in place. A
timeout enters the existing metadata cooldown, then removes and blocklists the
associated Sonarr/Radarr queue item so unreachable magnets do not monopolize
every pass and the media app can grab another release. When no Sonarr/Radarr queue item
matches, Smart Queues deletes the timed-out torrent and its files directly from
qBittorrent. The path is disabled automatically when torrent-fit enforcement is
not active, when storage is already at/below reserve, or when qBittorrent file
preallocation is enabled. Disabling bootstrap with preallocation avoids
allocating an unknown-size payload before its storage fit can be verified.

For new magnets, configure qBittorrent to add torrents in the started state with
the stop condition set to `MetadataReceived`. qBittorrent then performs this
metadata-only transition immediately after add and leaves the torrent stopped
for Smart Queues. The controller bootstrap remains necessary for older magnets
that were added stopped before that setting was enabled.

Optional storage and thermal guards:

| Variable | Default | Purpose |
| --- | --- | --- |
| `QBT_DOWNLOAD_STORAGE_PATH` | `/downloads` | Filesystem path checked for free download headroom. |
| `QBT_DOWNLOAD_STORAGE_CAPACITY_BYTES` | `0` | Optional logical capacity for a quota-backed download root. When positive, recursively measure allocated bytes below `QBT_DOWNLOAD_STORAGE_PATH` instead of trusting pool-wide NFS free-space reporting. |
| `QBT_DOWNLOAD_STORAGE_USAGE_CACHE_SECONDS` | `60` | Minimum interval between allocated-byte scans when a logical capacity is configured. |
| `QBT_DOWNLOAD_STORAGE_MIN_FREE_BYTES` | `32212254720` | Minimum free-space reserve. |
| `QBT_DOWNLOAD_STORAGE_PRESSURE_MIN_BLOCKED` | `10` | Minimum number of storage-blocked candidates required before storage pressure mode can activate. |
| `QBT_DOWNLOAD_STORAGE_PRESSURE_BLOCKED_FRACTION` | `0.50` | Minimum fraction of all candidates blocked by storage headroom before storage pressure mode can activate. |
| `QBT_TORRENT_HEALTH_STATE_PATH` | `/state/torrent-health.json` | Persistent torrent health state file. |
| `PROMETHEUS_URL` | unset | Prometheus base URL for thermal checks. |
| `QBT_NVME_THERMAL_STOP_ENABLED` | enabled only when `PROMETHEUS_URL` is set | Enable NVMe thermal stop checks. |
| `QBT_NVME_THERMAL_QUERY` | generic node-exporter NVMe composite-temperature query | PromQL query returning temperature samples. |

NFS servers may report the backing pool's capacity even when they enforce a
smaller per-export quota. Set `QBT_DOWNLOAD_STORAGE_CAPACITY_BYTES` to that
quota in bytes so the guard derives logical free space from allocated blocks
under the download root. The filesystem-reported totals remain in debug output
for diagnosis but no longer control torrent-fit decisions.

Optional Raspberry Pi thermal coordinator:

| Variable | Default | Description |
| --- | --- | --- |
| `QBT_RPI_COOLING_ENABLED` | `false` | Enable Raspberry Pi thermal mitigation. |
| `QBT_RPI_COOLING_NODES` | `k8s-rpi1,k8s-rpi2,k8s-rpi3` | Nodes monitored for thermal mitigation. |
| `QBT_RPI_COOLING_K8S_TIMEOUT` | `5` | Timeout in seconds for each Kubernetes API request. |
| `QBT_RPI_COOLING_K8S_RETRIES` | `2` | Retries after transient Kubernetes transport or retryable HTTP failures. |
| `QBT_RPI_COOLING_K8S_RETRY_DELAY_SECONDS` | `0.5` | Delay between Kubernetes API retries. |
| `QBT_RPI_COOLING_TEMPERATURE_SAMPLE_GRACE_SECONDS` | `180` | Grace period for Prometheus to resume temperature samples after a node becomes Ready. |
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
window. Transient Kubernetes API failures are retried, and a node that has just
become Ready receives a bounded grace period for its Prometheus temperature
samples to reappear. Missing samples outside that recovery window still fail
closed. Clean shutdown is disabled by default and is intended as last-resort
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
- `/status`: JSON snapshot of the latest queue decision, loop result, selected torrents, rejection counts, and candidate counts.
- `/metrics`: Prometheus text metrics for the latest decision. The endpoint
  includes controller freshness, latest action labels, legacy single selected
  torrent gauges, per-torrent selected/parked/progress gauges for progress,
  remaining bytes, speed, ETA, availability, and seed counts, queue funnel
  counts, rejection reasons, effective transfer caps, budget bytes, and
  storage headroom.

Single-download mode keeps an active torrent only when selected bytes or
downloaded bytes move by at least `QBT_SINGLE_DOWNLOAD_MIN_PROGRESS_BYTES`
during the `QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS` sample window. Instantaneous
download speed is used to decide whether active workers are productive, but a
low speed does not stop a torrent if it is making enough progress. When
`QBT_SINGLE_DOWNLOAD_ADAPTIVE_PROGRESS_ENABLED=true`, defaulting to true, that
floor scales up for larger torrents using `QBT_SINGLE_DOWNLOAD_PROGRESS_SIZE_FRACTION`,
is capped by `QBT_SINGLE_DOWNLOAD_PROGRESS_MAX_BYTES`, and is relaxed for older
torrents using `QBT_SINGLE_DOWNLOAD_PROGRESS_AGE_RELIEF_DAYS` and
`QBT_SINGLE_DOWNLOAD_PROGRESS_AGE_RELIEF_FRACTION`.
The controller also clamps worker count, productive speed, and progress bytes to
what the current effective download cap can support. This keeps fallback, quota,
thermal, or burst caps from making every worker look stalled just because the
configured static thresholds assume a higher link speed.

Torrents force-started in qBittorrent (`forcedDL`/`forcedUP`) are treated as
manual overrides. The controller excludes them from normal selection,
stale/no-progress cooldowns, and queue-managed replacement stops. The
human-started torrent runs outside the managed worker limit while the configured
normal worker pool continues. Explicit quota, backup-internet, thermal, storage
hard-stop, and shutdown safeguards retain authority over all downloads.

By default, normal single-download mode adaptively sizes the useful worker pool.
When measured productive throughput is below the configured utilization target,
the controller adds a bounded number of probe slots; when existing productive
workers meet the target, it contracts to those workers. Failed probes are
replaced during the same run instead of waiting for the next scheduled pass.

Category-batch mode can also opt into relative-speed yielding. After the normal
progress sample, the controller compares each productive worker with the median
observed rate of the other productive workers. A worker below the configured
fraction yields only after its minimum trial time, only when the peer median is
fast enough to be meaningful, and only when another queued torrent exists in
the same category after hard safety and ordering checks. That alternative may
still be cooling down: the yielded slot remains available until useful work is
ready. A configurable good-enough speed floor and tolerance prevent
workers near 1 MiB/s from churning just because their peers are much faster.
Multiple clear outliers can yield in one pass when enough queued alternatives
exist. The yield is scheduling state rather than a failed download: it adds
no stall tag, failure count, or failure backoff. Instead, the torrent is deferred
briefly in the health-state store, leaving spare worker capacity for other useful
candidates, and becomes eligible for a later probe. For the behavior illustrated
by a few multi-MiB/s workers beside one or two KiB/s workers, enable
`QBT_SINGLE_DOWNLOAD_RELATIVE_SPEED_ENABLED=true`; the remaining defaults are a
conservative starting point.

Stalled/no-progress torrents are parked only while listener capacity remains.
Parked torrents stay active in qBittorrent so they can resume immediately if a
needed peer appears, while the controller excludes them from replacement
selection and raises qBittorrent's active download limit enough to start
replacement candidates beside them. A failed probe that cannot fit in the
bounded listener pool is stopped and given reason-specific cooldown/backoff;
this prevents repeatedly retrying the same unavailable torrent while the rest
of the queue starves. Set
`QBT_SINGLE_DOWNLOAD_MAX_ACTIVE_DOWNLOADS_PER_CATEGORY` above `0` to run a
normal-mode batch with that many active download workers per qBittorrent
category. `QBT_SINGLE_DOWNLOAD_MAX_TOTAL_ACTIVE_DOWNLOADS` can additionally cap
the useful worker count across all categories. The controller tracks qBittorrent
active slots separately from Smart Queue worker slots: `qB active download
limit = useful worker slots + parked listener slots`. Parked listeners stay
active for peer discovery but do not consume the limited useful download worker
slots or category worker counts.
Internally the selector classifies every torrent into a lifecycle state:
`candidate`, `selected-worker`, `productive`, `parked-listener`, `cooldown`,
`retryable`, or `stale`. Worker states consume a useful download slot;
parked-listener states keep qBittorrent listening for peers without consuming a
worker slot.
Priority and active-watch pools are applied only when they contain a usable
worker candidate. If every preferred torrent is parked, deferred, attempted, or
cooling down, selection falls back to the normal eligible pool rather than
reporting an empty selection pool. A productive normal download remains in the
worker set while preferred candidates use the available probe slots.
No-progress samples are also classified as `actively-progressing`,
`slow-progressing`, `missing-final-piece`, `metadata-wait`,
`no-connected-peers`, or `tracker-dead`. Listener-worthy classes stay parked and
out of normal worker competition. `tracker-dead` means there are no connected
peers, no reported seeds, and no available pieces, so it is stopped and put into
health-state cooldown rather than occupying a listener slot.

Selection decisions use the same scoring model in normal mode, uncapped windows,
preemption checks, and storage recovery. Decision logs include the chosen
torrent's `score` object with visible components for priority, queue order,
health, progress, near-complete progress, remaining bytes, ETA, sources,
availability, stopped state, cooldown, storage fit, and storage remaining.
Stopped torrents carry a score penalty so a high-progress but parked item does
not jump ahead of an active candidate that is currently able to download. In
`tiered` mode the queue key still sorts before the score; in `balanced` mode the
score is the primary ordering signal after explicit priority.
Selected torrents also receive a dwell lease. During that lease, the controller
keeps the torrent if it is still making productive progress or has current or
recent connected peer contact, even when another candidate briefly scores
higher. This prevents stop/requeue churn from dropping useful peer connections.

Before storage reaches reserve, storage pressure mode can use the storage-fit
score in normal selection when enough candidates are blocked by headroom. This
starts smaller fitting downloads earlier, frees completed data sooner, and
reduces the chance of falling into constrained recovery. When download storage
is at or below the configured reserve and torrent-fit checks are enabled, the
controller enters a constrained recovery mode instead of pausing every torrent.
It only considers torrents whose selected remaining bytes can fit in the
currently free space, selects the smallest verified remaining downloads first,
temporarily raises qBittorrent's active queue limit up to
`QBT_DOWNLOAD_STORAGE_RECOVERY_MAX_ACTIVE` downloads, defaulting to `5`, and
tracks no-progress samples for each recovery member. After
`QBT_DOWNLOAD_STORAGE_RECOVERY_STALL_SAMPLES` samples, defaulting to `2`, a
stalled member is parked: it stays active in qBittorrent so it can resume if
seeders appear, but it no longer consumes one of the active recovery worker
slots. The controller then refills open worker slots with other fitting
torrents while accounting for parked torrents in the storage headroom budget and
adding parked listeners on top of the recovery worker-slot limit in
qBittorrent's active download limit.
At most `QBT_DOWNLOAD_STORAGE_RECOVERY_MAX_PARKED_STALLED` stalled torrents are
parked, defaulting to `10`; set it to `0` for no parked-stalled count cap.
Recovery workers also need to meet
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
