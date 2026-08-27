"""UniFi Network usage provider.

All UniFi API, authentication, report parsing, correction, and WAN-role
behavior is isolated here. Queue policy depends only on the provider ABCs.
"""

from __future__ import annotations

import json
import math
import os
import re
import ssl
import urllib.request
from datetime import UTC, datetime, timedelta
from datetime import time as datetime_time
from http.cookiejar import CookieJar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from qbittorrent_smart_queues.config import (
    env_bool,
    env_float,
    env_int,
    env_str,
    split_lines_or_csv,
)
from qbittorrent_smart_queues.errors import ApiError
from qbittorrent_smart_queues.http import join_url, request_json, response_rows
from qbittorrent_smart_queues.providers.base import (
    BackupInternetProvider,
    NetworkUsageProvider,
    ProviderDiagnostics,
    UsageSnapshot,
)
from qbittorrent_smart_queues.quota import local_billing_cycle_window


def _noop(message, **context):
    pass


def _format_utc(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_utc(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).astimezone(UTC)
    except ValueError:
        return None


def _configured_billing_cycle_day():
    day = env_int("QBT_BILLING_CYCLE_DAY", 1)
    if not 1 <= day <= 31:
        raise ValueError("QBT_BILLING_CYCLE_DAY must be between 1 and 31")
    return day


def _local_calendar_starts(now, local_timezone, cycle_day=1):
    local_now = now.astimezone(local_timezone)
    local_day_start = datetime(
        local_now.year,
        local_now.month,
        local_now.day,
        tzinfo=local_timezone,
    )
    local_cycle_start, _, _ = local_billing_cycle_window(
        now,
        local_timezone,
        cycle_day,
    )
    return (
        local_cycle_start,
        local_day_start.astimezone(UTC),
        local_now.date().isoformat(),
    )


def parse_unifi_row_time(value):
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            value = float(raw)
        except ValueError:
            return _parse_utc(raw)
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    if timestamp > 10_000_000_000:
        timestamp = timestamp / 1000.0
    try:
        return datetime.fromtimestamp(timestamp, UTC)
    except (OverflowError, OSError, ValueError):
        return None


def latest_unifi_row_time(rows):
    latest = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_time = parse_unifi_row_time(row.get("time"))
        if row_time and (latest is None or row_time > latest):
            latest = row_time
    return latest


def filter_unifi_rows(rows, start, end):
    filtered = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_time = parse_unifi_row_time(row.get("time"))
        if row_time is None or start <= row_time < end:
            filtered.append(row)
    return filtered


def unifi_stats_interval_seconds(interval):
    normalized = str(interval or "").strip().lower()
    return {
        "5minutes": 300,
        "hourly": 3600,
        "daily": 86400,
        "monthly": 2_678_400,
    }.get(normalized)

def normalize_unifi_wan_identifier(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def unifi_wan_interface_aliases(interface_name, interface):
    aliases = {
        normalize_unifi_wan_identifier(interface_name),
    }
    if normalize_unifi_wan_identifier(interface_name) == "wan1":
        aliases.add("wan")
    if isinstance(interface, dict):
        for key in ("name", "ifname", "uplink_ifname"):
            alias = normalize_unifi_wan_identifier(interface.get(key))
            if alias:
                aliases.add(alias)
    return {alias for alias in aliases if alias}


def unifi_wan_stats_prefix(network_group):
    normalized = normalize_unifi_wan_identifier(network_group)
    if normalized in {"wan", "wan1"}:
        return "wan"
    if re.fullmatch(r"wan[2-9]\d*", normalized):
        return normalized
    return ""


def unifi_primary_usage_stats_attrs(network_rows, include_upload=False):
    attrs = []
    network_groups = []
    for row in network_rows:
        if (
            not isinstance(row, dict)
            or str(row.get("purpose") or "").strip().lower() != "wan"
        ):
            continue
        load_balance_type = str(
            row.get("wan_load_balance_type") or ""
        ).strip().lower()
        if load_balance_type in {"failover", "failover-only"}:
            continue
        network_group = str(row.get("wan_networkgroup") or "").strip()
        stats_prefix = unifi_wan_stats_prefix(network_group)
        if not stats_prefix:
            raise ApiError(
                "Could not map primary UniFi WAN group "
                f"{network_group!r} to a report field"
            )
        attrs.append(f"{stats_prefix}-rx_bytes")
        if include_upload:
            attrs.append(f"{stats_prefix}-tx_bytes")
        network_groups.append(network_group)
    if not attrs:
        raise ApiError(
            "UniFi has no primary WAN report fields after excluding failover-only roles"
        )
    attrs.append("time")
    return list(dict.fromkeys(attrs)), sorted(set(network_groups))


def classify_unifi_backup_internet_state(network_rows, device_rows):
    wan_networks = {}
    backup_network_groups = set()
    for row in network_rows:
        if not isinstance(row, dict) or str(row.get("purpose") or "").strip().lower() != "wan":
            continue
        network_group = normalize_unifi_wan_identifier(row.get("wan_networkgroup"))
        if network_group:
            wan_networks[network_group] = row
        load_balance_type = str(row.get("wan_load_balance_type") or "").strip().lower()
        if load_balance_type in {"failover", "failover-only"}:
            identifier = normalize_unifi_wan_identifier(row.get("wan_networkgroup"))
            if identifier:
                backup_network_groups.add(identifier)

    if not backup_network_groups:
        raise ApiError("UniFi has no WAN configured with failover-only role")

    gateways = []
    for row in device_rows:
        if not isinstance(row, dict) or not isinstance(row.get("uplink"), dict):
            continue
        interface_names = [
            key
            for key, value in row.items()
            if re.fullmatch(r"wan\d*", str(key).strip().lower()) and isinstance(value, dict)
        ]
        if interface_names:
            gateways.append((row, interface_names))
    if len(gateways) != 1:
        raise ApiError(f"Expected one UniFi gateway with WAN interfaces, found {len(gateways)}")

    gateway, interface_names = gateways[0]
    uplink = gateway.get("uplink") or {}
    active_uplink = normalize_unifi_wan_identifier(uplink.get("name") or uplink.get("ifname"))
    if uplink.get("up") is False:
        raise ApiError("UniFi gateway reports that its active uplink is down")

    interfaces = []
    for interface_name in interface_names:
        interface = gateway.get(interface_name) or {}
        aliases = unifi_wan_interface_aliases(interface_name, interface)
        matching_network = None
        for network_group, network in wan_networks.items():
            if network_group in aliases:
                matching_network = network
                aliases.add(network_group)
                break
        interfaces.append({
            "name": interface_name,
            "details": interface,
            "aliases": {alias for alias in aliases if alias},
            "network": matching_network or {},
        })

    known_interface_identifiers = set().union(
        *(interface["aliases"] for interface in interfaces)
    )
    unmatched_backup_groups = backup_network_groups - known_interface_identifiers
    if unmatched_backup_groups:
        raise ApiError(
            "UniFi failover-only WAN group does not match a gateway logical "
            "interface or uplink interface: "
            + ", ".join(sorted(unmatched_backup_groups))
        )

    active_interfaces = []
    if active_uplink:
        active_interfaces = [
            interface
            for interface in interfaces
            if active_uplink in interface["aliases"]
        ]
    if not active_interfaces:
        active_interfaces = [
            interface
            for interface in interfaces
            if interface["details"].get("is_uplink") is True
        ]
    if len(active_interfaces) != 1:
        rendered_uplink = uplink.get("name") or uplink.get("ifname") or "unknown"
        raise ApiError(
            f"Could not uniquely map active UniFi uplink {rendered_uplink!r} "
            f"to a WAN interface; matched {len(active_interfaces)}"
        )

    active_interface = active_interfaces[0]
    network = active_interface["network"]
    backup_active = bool(active_interface["aliases"] & backup_network_groups)
    return {
        "enabled": True,
        "available": True,
        "backup_active": backup_active,
        "active_role": "backup" if backup_active else "primary",
        "active_network": str(network.get("name") or active_interface["name"]),
        "active_network_group": str(
            network.get("wan_networkgroup") or active_interface["name"]
        ),
        "active_interface": str(active_interface["name"]),
        "active_uplink": str(uplink.get("name") or uplink.get("ifname") or ""),
        "role_source": "wan_load_balance_type",
    }


class UnifiUsageCorrectionStore:
    def __init__(self, path, warning=None):
        self.log_warning = warning or _noop
        self.path = path
        self.data = {"version": 1, "days": {}}
        self.load()

    def load(self):
        if not self.path:
            return
        try:
            with open(self.path, "r", encoding="utf-8") as state_file:
                payload = json.load(state_file)
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError) as exc:
            self.log_warning(f"Failed to read UniFi usage correction state: {exc}")
            return
        days = payload.get("days") if isinstance(payload, dict) else None
        if isinstance(days, dict):
            self.data = {"version": 1, "days": days}

    def save(self):
        if not self.path:
            return
        directory = os.path.dirname(self.path)
        tmp_path = f"{self.path}.tmp"
        try:
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(tmp_path, "w", encoding="utf-8") as state_file:
                json.dump(
                    self.data,
                    state_file,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                state_file.write("\n")
                state_file.flush()
                os.fsync(state_file.fileno())
            os.replace(tmp_path, self.path)
        except OSError as exc:
            self.log_warning(f"Failed to save UniFi usage correction state: {exc}")

    def corrections_for(self, local_day, attrs):
        day_state = (self.data.get("days") or {}).get(local_day) or {}
        corrections = day_state.get("corrections") or {}
        return {
            attr: max(0, int(corrections.get(attr, 0) or 0))
            for attr in attrs
        }

    def correction_bytes(self, local_day, attrs):
        return sum(self.corrections_for(local_day, attrs).values())

    def update(self, local_day, corrections, timezone_name):
        normalized = {
            str(attr): max(0, int(value))
            for attr, value in corrections.items()
            if int(value) > 0
        }
        if not normalized:
            return False
        days = self.data.setdefault("days", {})
        existing = days.get(local_day) or {}
        merged = dict(existing.get("corrections") or {})
        merged.update(normalized)
        if existing.get("corrections") == merged:
            return False
        days[local_day] = {
            **existing,
            "corrections": merged,
            "timezone": timezone_name,
            "updated_at": _format_utc(datetime.now(UTC)),
        }
        self.save()
        return True


class UnifiProvider(NetworkUsageProvider, BackupInternetProvider):
    """UniFi Network implementation of the network usage provider contract."""

    provider_name = "unifi"

    def __init__(self, *, debug=None, warning=None):
        self.log_debug = debug or _noop
        self.log_warning = warning or _noop
        self.base_url = env_str("UNIFI_URL").rstrip("/")
        self.site = env_str("UNIFI_SITE", "default")
        self.api_base_path = env_str("UNIFI_API_BASE_PATH", "/proxy/network")
        self.timeout = env_int("UNIFI_TIMEOUT", 30)
        self.verify_tls = env_bool("UNIFI_VERIFY_TLS", True)
        self.api_key = env_str("UNIFI_API_KEY")
        self.authenticated = False
        self.username = env_str(("UNIFI_USER", "UNIFI_USERNAME"))
        self.password = env_str("UNIFI_PASSWORD")
        self.csrf_token = ""
        self.latest_stats_at = None
        self.network_rows = None
        self.device_rows = None
        self.stats_timezone_name = ""
        self.stats_timezone_info = None
        self.stats_timezone_source = ""
        self.billing_cycle_day = 1
        self.billing_cycle_start = None
        self.billing_cycle_end = None
        self.billing_cycle_days = 0
        self.local_day_end = None
        self.usage_scope = "all"
        self.usage_network_groups = []
        self.usage_anomalies = []
        self.usage_corrected_bytes = 0
        self.usage_correction_store = UnifiUsageCorrectionStore(
            env_str(
                "UNIFI_USAGE_CORRECTION_STATE_PATH",
                "/state/unifi-usage-corrections.json",
            ),
            warning=self.log_warning,
        )
        self.cookie_jar = CookieJar()
        context = None
        if not self.verify_tls:
            context = ssl._create_unverified_context()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar),
            urllib.request.HTTPSHandler(context=context),
        )

    def headers(self):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["X-API-KEY"] = self.api_key
        if self.csrf_token:
            headers["X-CSRF-Token"] = self.csrf_token
        return headers

    def login(self):
        if self.authenticated:
            return
        if not self.base_url:
            raise ApiError("UNIFI_URL is required for UniFi quota data")
        if self.api_key:
            self.log_debug("Using UniFi API key authentication")
            self.authenticated = True
            return
        if not self.username or not self.password:
            raise ApiError(
                "UniFi credentials missing; set UNIFI_API_KEY or "
                "UNIFI_USER/UNIFI_PASSWORD"
            )

        payload = json.dumps({"username": self.username, "password": self.password}).encode("utf-8")
        login_paths = split_lines_or_csv(env_str("UNIFI_LOGIN_PATHS")) or [
            "/api/auth/login",
            "/api/login",
        ]
        errors = []
        for path in login_paths:
            url = join_url(self.base_url, path)
            try:
                _, response = request_json(
                    self.opener,
                    "POST",
                    url,
                    headers=self.headers(),
                    body=payload,
                    timeout=self.timeout,
                )
                self.csrf_token = response.headers.get("X-CSRF-Token", "")
                self.log_debug(f"Authenticated to UniFi with {path}")
                self.authenticated = True
                return
            except ApiError as exc:
                errors.append(str(exc))
        raise ApiError("UniFi login failed: " + " | ".join(errors))

    def stats_attrs(self):
        if env_bool(
            "QBT_BACKUP_INTERNET_STOP_ENABLED",
            False,
        ):
            if self.network_rows is None:
                network_endpoint = (
                    f"{self.api_base_path}/api/s/{self.site}/rest/networkconf"
                )
                self.network_rows = self.api_rows(
                    network_endpoint,
                    "UniFi network configuration",
                )
            attrs, network_groups = unifi_primary_usage_stats_attrs(
                self.network_rows,
                include_upload=env_bool("QBT_USAGE_INCLUDE_UPLOAD", False),
            )
            self.usage_scope = "primary"
            self.usage_network_groups = network_groups
            return attrs

        attrs = split_lines_or_csv(env_str("UNIFI_USAGE_ATTRS")) or [
            "wan-rx_bytes",
            "wan2-rx_bytes",
        ]
        if env_bool("QBT_USAGE_INCLUDE_UPLOAD", False):
            attrs.extend(["wan-tx_bytes", "wan2-tx_bytes"])
        if "time" not in attrs:
            attrs.append("time")
        self.usage_scope = "all"
        self.usage_network_groups = []
        return attrs

    def stats_rows(self, interval, start, end, attrs):
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        report_type = env_str("UNIFI_STATS_TYPE", "site")
        endpoint = f"{self.api_base_path}/api/s/{self.site}/stat/report/{interval}.{report_type}"
        url = join_url(self.base_url, endpoint)

        payload = json.dumps({"start": start_ms, "end": end_ms, "attrs": attrs}).encode("utf-8")
        data, _ = request_json(
            self.opener,
            "POST",
            url,
            headers=self.headers(),
            body=payload,
            timeout=self.timeout,
        )
        rows = response_rows(data, "UniFi stats")
        latest_row_time = latest_unifi_row_time(rows)
        if latest_row_time and (self.latest_stats_at is None or latest_row_time > self.latest_stats_at):
            self.latest_stats_at = latest_row_time
        self.log_debug(f"UniFi returned {len(rows)} {interval}.{report_type} rows")
        return rows

    def resolve_stats_timezone(self):
        configured = env_str("UNIFI_STATS_TIMEZONE")
        source = "UNIFI_STATS_TIMEZONE"
        timezone_name = configured
        if not timezone_name:
            endpoint = f"{self.api_base_path}/api/s/{self.site}/stat/sysinfo"
            sysinfo_rows = self.api_rows(endpoint, "UniFi system information")
            timezone_names = {
                str(row.get("timezone") or "").strip()
                for row in sysinfo_rows
                if isinstance(row, dict) and str(row.get("timezone") or "").strip()
            }
            if len(timezone_names) != 1:
                raise ApiError(
                    "Could not resolve one UniFi reporting timezone from stat/sysinfo"
                )
            timezone_name = timezone_names.pop()
            source = "stat/sysinfo"
        try:
            local_timezone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ApiError(
                f"UniFi reporting timezone {timezone_name!r} is not available"
            ) from exc
        self.stats_timezone_name = timezone_name
        self.stats_timezone_info = local_timezone
        self.stats_timezone_source = source
        return local_timezone

    def stats_rate_limits(self, attrs):
        download_attrs = [attr for attr in attrs if attr != "time"]
        configured_limit = max(
            0,
            env_int("UNIFI_STATS_MAX_DOWNLOAD_RATE_BYTES_PER_SEC", 0),
        )
        if configured_limit:
            return {
                attr: configured_limit
                for attr in download_attrs
                if str(attr).strip().lower().endswith("-rx_bytes")
            }

        if self.network_rows is None:
            network_endpoint = (
                f"{self.api_base_path}/api/s/{self.site}/rest/networkconf"
            )
            self.network_rows = self.api_rows(
                network_endpoint,
                "UniFi network configuration",
            )

        wan_capabilities = {}
        for row in self.network_rows:
            if (
                not isinstance(row, dict)
                or str(row.get("purpose") or "").strip().lower() != "wan"
            ):
                continue
            network_group = normalize_unifi_wan_identifier(
                row.get("wan_networkgroup")
            )
            capabilities = row.get("wan_provider_capabilities")
            if not network_group or not isinstance(capabilities, dict):
                continue
            download_kbps = capabilities.get("download_kilobits_per_second")
            upload_kbps = capabilities.get("upload_kilobits_per_second")
            try:
                download_rate = max(
                    0,
                    math.floor(float(download_kbps) * 1000 / 8),
                )
            except (TypeError, ValueError):
                download_rate = 0
            try:
                upload_rate = max(
                    0,
                    math.floor(float(upload_kbps) * 1000 / 8),
                )
            except (TypeError, ValueError):
                upload_rate = 0
            wan_capabilities[network_group] = {
                "rx": download_rate,
                "tx": upload_rate,
            }

        limits = {}
        for attr in download_attrs:
            match = re.fullmatch(
                r"(wan\d*)-(rx|tx)_bytes",
                str(attr).strip().lower(),
            )
            if not match:
                continue
            network_group = normalize_unifi_wan_identifier(match.group(1))
            direction = match.group(2)
            rate = (wan_capabilities.get(network_group) or {}).get(direction, 0)
            if rate > 0:
                limits[attr] = rate
        return limits

    def sum_download_bytes(
        self,
        rows,
        attrs,
        local_timezone=None,
        apply_saved_corrections=False,
    ):
        total = 0
        download_attrs = [attr for attr in attrs if attr != "time"]
        remaining_corrections_by_day = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            saved_corrections = {}
            if apply_saved_corrections and local_timezone is not None:
                row_time = parse_unifi_row_time(row.get("time"))
                if row_time is not None:
                    local_day = row_time.astimezone(
                        local_timezone
                    ).date().isoformat()
                    if local_day not in remaining_corrections_by_day:
                        remaining_corrections_by_day[local_day] = (
                            self.usage_correction_store.corrections_for(
                                local_day,
                                download_attrs,
                            )
                        )
                    saved_corrections = remaining_corrections_by_day[
                        local_day
                    ]
            row_total = 0
            for attr in download_attrs:
                value = row.get(attr)
                if isinstance(value, (int, float)) and value > 0:
                    attr_value = int(value)
                    applied_correction = min(
                        attr_value,
                        saved_corrections.get(attr, 0),
                    )
                    row_total += attr_value - applied_correction
                    if applied_correction:
                        saved_corrections[attr] -= applied_correction
                    self.usage_corrected_bytes += applied_correction
            total += row_total
        return total

    def corrected_download_bytes(self, rows, attrs, interval, local_day):
        download_attrs = [attr for attr in attrs if attr != "time"]
        interval_seconds = unifi_stats_interval_seconds(interval)
        rate_limits = self.stats_rate_limits(attrs) if interval_seconds else {}
        multiplier = max(
            1.0,
            env_float("UNIFI_STATS_RATE_LIMIT_MULTIPLIER", 1.25),
        )
        values_by_attr = {
            attr: [
                int(row.get(attr))
                if (
                    isinstance(row, dict)
                    and isinstance(row.get(attr), (int, float))
                    and row.get(attr) > 0
                )
                else 0
                for row in rows
            ]
            for attr in download_attrs
        }
        total = 0
        anomalies = []
        corrections = {}
        for attr, values in values_by_attr.items():
            rate_limit = rate_limits.get(attr, 0)
            bucket_limit = (
                math.floor(rate_limit * interval_seconds * multiplier)
                if rate_limit and interval_seconds
                else 0
            )
            for index, observed in enumerate(values):
                if not bucket_limit or observed <= bucket_limit:
                    total += observed
                    continue
                previous_value = next(
                    (
                        value
                        for value in reversed(values[:index])
                        if value <= bucket_limit
                    ),
                    0,
                )
                next_value = next(
                    (
                        value
                        for value in values[index + 1 :]
                        if value <= bucket_limit
                    ),
                    0,
                )
                replacement = max(previous_value, next_value)
                correction = max(0, observed - replacement)
                total += replacement
                corrections[attr] = corrections.get(attr, 0) + correction
                row = rows[index] if index < len(rows) else {}
                row_time = (
                    parse_unifi_row_time(row.get("time"))
                    if isinstance(row, dict)
                    else None
                )
                anomalies.append({
                    "attribute": attr,
                    "time": _format_utc(row_time) if row_time else None,
                    "observed_bytes": observed,
                    "replacement_bytes": replacement,
                    "bucket_limit_bytes": bucket_limit,
                    "correction_bytes": correction,
                })

        existing_correction = self.usage_correction_store.correction_bytes(
            local_day,
            download_attrs,
        )
        if corrections:
            changed = self.usage_correction_store.update(
                local_day,
                corrections,
                self.stats_timezone_name,
            )
            if changed:
                for anomaly in anomalies:
                    self.log_warning(
                        "Corrected impossible UniFi usage bucket",
                        **anomaly,
                    )
        elif existing_correction:
            total = self.sum_download_bytes(
                rows,
                attrs,
                local_timezone=self.stats_timezone_info,
                apply_saved_corrections=True,
            )

        self.usage_anomalies.extend(anomalies)
        if corrections:
            self.usage_corrected_bytes += sum(corrections.values())
        return total

    def backfill_daily_history_corrections(
        self,
        rows,
        attrs,
        interval,
        local_timezone,
    ):
        if str(interval or "").strip().lower() != "daily":
            return

        download_attrs = [attr for attr in attrs if attr != "time"]
        rate_limits = self.stats_rate_limits(attrs)
        multiplier = max(
            1.0,
            env_float("UNIFI_STATS_RATE_LIMIT_MULTIPLIER", 1.25),
        )
        daily_limits = {
            attr: math.floor(rate * 86400 * multiplier)
            for attr, rate in rate_limits.items()
            if rate > 0
        }
        suspicious_attrs_by_day = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_time = parse_unifi_row_time(row.get("time"))
            if row_time is None:
                continue
            local_date = row_time.astimezone(local_timezone).date()
            local_day = local_date.isoformat()
            saved_corrections = (
                self.usage_correction_store.corrections_for(
                    local_day,
                    download_attrs,
                )
            )
            for attr in download_attrs:
                observed = row.get(attr)
                daily_limit = daily_limits.get(attr, 0)
                if (
                    isinstance(observed, (int, float))
                    and observed > daily_limit > 0
                    and saved_corrections.get(attr, 0) <= 0
                ):
                    suspicious_attrs_by_day.setdefault(
                        local_date,
                        set(),
                    ).add(attr)

        for local_date, suspicious_attrs in sorted(
            suspicious_attrs_by_day.items()
        ):
            local_day = local_date.isoformat()
            day_start = datetime.combine(
                local_date,
                datetime_time.min,
                tzinfo=local_timezone,
            )
            day_end = day_start + timedelta(days=1)
            replay_attrs = sorted(suspicious_attrs)
            replay_attrs.append("time")
            hourly_rows = self.stats_rows(
                "hourly",
                day_start,
                day_end,
                replay_attrs,
            )
            hourly_rows = filter_unifi_rows(
                hourly_rows,
                day_start,
                day_end,
            )
            corrected_bytes_before = self.usage_corrected_bytes
            if hourly_rows:
                self.corrected_download_bytes(
                    hourly_rows,
                    replay_attrs,
                    "hourly",
                    local_day,
                )
                # The daily rollup applies and counts the persisted correction
                # below. Do not count the replay itself as corrected usage too.
                self.usage_corrected_bytes = corrected_bytes_before

            persisted = self.usage_correction_store.corrections_for(
                local_day,
                suspicious_attrs,
            )
            unresolved = sorted(
                attr
                for attr in suspicious_attrs
                if persisted.get(attr, 0) <= 0
            )
            if unresolved:
                self.log_warning(
                    "Could not backfill impossible UniFi daily usage fields; "
                    "counting their raw values",
                    local_day=local_day,
                    attributes=unresolved,
                    hourly_row_count=len(hourly_rows),
                )

    def _snapshot(self, cycle_usage_bytes, day_usage_bytes):
        return UsageSnapshot(
            cycle_usage_bytes=cycle_usage_bytes,
            day_usage_bytes=day_usage_bytes,
            billing_cycle_start=self.billing_cycle_start,
            billing_cycle_end=self.billing_cycle_end,
            billing_cycle_days=self.billing_cycle_days,
            local_day_end=self.local_day_end,
            timezone_name=self.stats_timezone_name,
            metadata={
                "usage_scope": self.usage_scope,
                "usage_network_groups": list(self.usage_network_groups),
            },
        )

    def diagnostics(self):
        return ProviderDiagnostics(
            latest_stats_at=self.latest_stats_at,
            stats_timezone=self.stats_timezone_name,
            stats_timezone_source=self.stats_timezone_source,
            billing_cycle_day=self.billing_cycle_day,
            billing_cycle_start=self.billing_cycle_start,
            billing_cycle_end=self.billing_cycle_end,
            billing_cycle_days=self.billing_cycle_days,
            usage_scope=self.usage_scope,
            usage_network_groups=tuple(self.usage_network_groups),
            usage_anomalies=tuple(self.usage_anomalies),
            usage_corrected_bytes=self.usage_corrected_bytes,
        )

    def usage_snapshot(self, now):
        self.login()
        self.usage_anomalies = []
        self.usage_corrected_bytes = 0
        local_timezone = self.resolve_stats_timezone()
        self.billing_cycle_day = _configured_billing_cycle_day()
        (
            self.billing_cycle_start,
            self.billing_cycle_end,
            self.billing_cycle_days,
        ) = local_billing_cycle_window(
            now,
            local_timezone,
            self.billing_cycle_day,
        )
        month_start, today_start, local_day = _local_calendar_starts(
            now,
            local_timezone,
            self.billing_cycle_day,
        )
        local_now = now.astimezone(local_timezone)
        self.local_day_end = (
            datetime(
                local_now.year,
                local_now.month,
                local_now.day,
                tzinfo=local_timezone,
            )
            + timedelta(days=1)
        ).astimezone(UTC)
        interval = env_str("UNIFI_STATS_INTERVAL", "split-daily-hourly")
        attrs = self.stats_attrs()

        if interval != "split-daily-hourly":
            month_rows = self.stats_rows(interval, month_start, now, attrs)
            day_rows = self.stats_rows(interval, today_start, now, attrs)
            month_rows = filter_unifi_rows(month_rows, month_start, now)
            day_rows = filter_unifi_rows(day_rows, today_start, now)
            day_raw = self.sum_download_bytes(day_rows, attrs)
            month_total = self.sum_download_bytes(
                month_rows,
                attrs,
                local_timezone=local_timezone,
                apply_saved_corrections=True,
            )
            day_total = self.corrected_download_bytes(
                day_rows,
                attrs,
                interval,
                local_day,
            )
            return self._snapshot(
                max(0, month_total - day_raw + day_total),
                day_total,
            )

        month_total = 0
        if month_start < today_start:
            history_interval = env_str("UNIFI_HISTORY_STATS_INTERVAL", "daily")
            rows = self.stats_rows(history_interval, month_start, today_start, attrs)
            rows = filter_unifi_rows(rows, month_start, today_start)
            self.backfill_daily_history_corrections(
                rows,
                attrs,
                history_interval,
                local_timezone,
            )
            month_total += self.sum_download_bytes(
                rows,
                attrs,
                local_timezone=local_timezone,
                apply_saved_corrections=True,
            )

        current_interval = env_str("UNIFI_CURRENT_STATS_INTERVAL", "hourly")
        current_rows = self.stats_rows(current_interval, today_start, now, attrs)
        current_rows = filter_unifi_rows(current_rows, today_start, now)
        if not current_rows:
            fallback_interval = env_str(
                "UNIFI_CURRENT_STATS_FALLBACK_INTERVAL",
                "daily",
            )
            current_rows = self.stats_rows(fallback_interval, today_start, now, attrs)
            current_rows = filter_unifi_rows(current_rows, today_start, now)
            current_interval = fallback_interval
        day_total = self.corrected_download_bytes(
            current_rows,
            attrs,
            current_interval,
            local_day,
        )
        month_total += day_total
        return self._snapshot(month_total, day_total)

    def api_rows(self, endpoint, label):
        self.login()
        url = join_url(self.base_url, endpoint)
        data, _ = request_json(
            self.opener,
            "GET",
            url,
            headers=self.headers(),
            timeout=self.timeout,
        )
        return response_rows(data, label)

    def backup_internet_state(self):
        network_endpoint = (
            f"{self.api_base_path}/api/s/{self.site}/rest/networkconf"
        )
        device_endpoint = (
            f"{self.api_base_path}/api/s/{self.site}/stat/device"
        )
        self.network_rows = self.api_rows(
            network_endpoint,
            "UniFi network configuration",
        )
        self.device_rows = self.api_rows(device_endpoint, "UniFi device status")
        return classify_unifi_backup_internet_state(
            self.network_rows,
            self.device_rows,
        )
