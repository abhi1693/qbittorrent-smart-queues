#!/usr/bin/env python3
import calendar
import json
import math
import os
import re
import signal
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from http.cookiejar import CookieJar


QBT_DEFAULT_URLS = []

TV_EPISODE_PATTERNS = [
    re.compile(r"(?:^|[ ._\[\(\-])s(?P<season>\d{1,3})[ ._\-]*e(?P<episode>\d{1,3})", re.IGNORECASE),
    re.compile(r"(?:^|[ ._\[\(\-])(?P<season>\d{1,2})x(?P<episode>\d{1,3})(?:\D|$)", re.IGNORECASE),
]
TV_SEASON_PATTERN = re.compile(
    r"(?:^|[ ._\[\(\-])s(?P<season>\d{1,3})(?:[ ._\]\)\-]|$)",
    re.IGNORECASE,
)
TV_SEASON_WORD_PATTERN = re.compile(
    r"(?:^|[ ._\[\(\-])season[ ._\-]*(?P<season>\d{1,3})(?:[ ._\]\)\-]|$)",
    re.IGNORECASE,
)
MEDIA_FILE_EXTENSIONS = {
    ".avi",
    ".m2ts",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".ts",
    ".webm",
    ".wmv",
}
QBT_FILE_PRIORITY_NORMAL = 1
QBT_FILE_PRIORITY_HIGH = 6
QBT_FILE_PRIORITY_MAXIMUM = 7

PROMETHEUS_DEFAULT_URL = ""
NVME_THERMAL_QUERY = (
    'max by (instance) ('
    'node_hwmon_temp_celsius{chip=~"nvme_.*"} '
    '* on(instance, chip, sensor) group_left(label) '
    'node_hwmon_sensor_label{chip=~"nvme_.*", label=~"Composite.*"}'
    ')'
)
RPI_COOLING_DEFAULT_NODES = ["k8s-rpi1", "k8s-rpi2", "k8s-rpi3"]
RPI_COOLING_CPU_QUERY = (
    'max by (nodename) ('
    'node_hwmon_temp_celsius{chip=~"thermal_thermal_zone.*"} '
    '* on(instance) group_left(nodename) '
    'node_uname_info{machine="aarch64", nodename=~"k8s-rpi[123]"}'
    ')'
)
RPI_COOLING_NVME_QUERY = (
    'max by (nodename) ('
    'node_hwmon_temp_celsius{chip=~"nvme_.*"} '
    '* on(instance, chip, sensor) group_left(label) '
    'node_hwmon_sensor_label{chip=~"nvme_.*", label="Composite"} '
    '* on(instance) group_left(nodename) '
    'node_uname_info{machine="aarch64", nodename=~"k8s-rpi[123]"}'
    ')'
)
KUBERNETES_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
KUBERNETES_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
THERMAL_ACTION_CLEAR = "clear"
THERMAL_ACTION_THROTTLE = "throttle"
THERMAL_ACTION_PAUSE = "pause"
_DECISION_SUMMARY_REPEAT_STATE = {}
_DECISION_SUMMARY_REPEAT_LOCK = threading.Lock()


class ApiError(RuntimeError):
    pass


def env_bool(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name, default):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def env_float(name, default):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def first_env(names):
    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip() != "":
            return value
    return None


def split_lines_or_csv(value):
    if not value:
        return []
    parts = []
    for line in value.replace(",", "\n").splitlines():
        item = line.strip()
        if item:
            parts.append(item)
    return parts


def split_key_value_lines(value):
    items = {}
    for item in split_lines_or_csv(value):
        if "=" not in item:
            continue
        key, item_value = item.split("=", 1)
        key = key.strip()
        item_value = item_value.strip()
        if key and item_value:
            items[key] = item_value
    return items


def human_size(value):
    try:
        size = float(value)
    except (TypeError, ValueError):
        return str(value)

    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    unit_index = 0
    while abs(size) >= 1000 and unit_index < len(units) - 1:
        size /= 1000
        unit_index += 1

    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    if abs(size) >= 100:
        return f"{size:.0f} {units[unit_index]}"
    if abs(size) >= 10:
        return f"{size:.1f} {units[unit_index]}"
    return f"{size:.2f} {units[unit_index]}"


def human_rate(value):
    return f"{human_size(value)}/s"


def human_duration(seconds):
    try:
        remaining = max(0, int(seconds))
    except (TypeError, ValueError):
        return str(seconds)
    days, remainder = divmod(remaining, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _ = divmod(remainder, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    return " ".join(parts)


def join_url(base_url, path):
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def utc_month_window(now):
    start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    last_day = calendar.monthrange(now.year, now.month)[1]
    end = datetime(now.year, now.month, last_day, 23, 59, 59, tzinfo=timezone.utc)
    return start, end


def utc_day_start(now):
    return datetime(now.year, now.month, now.day, tzinfo=timezone.utc)


def utc_day_end(now):
    return utc_day_start(now) + timedelta(days=1) - timedelta(seconds=1)


def format_utc(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def json_safe(value):
    if isinstance(value, datetime):
        return format_utc(value)
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, set):
        return [json_safe(item) for item in sorted(value)]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


LOG_LEVELS = {
    "debug": 10,
    "info": 20,
    "warning": 30,
    "error": 40,
}


def normalize_log_level(value, default="info"):
    level = str(value or default).strip().lower()
    if level == "warn":
        level = "warning"
    return level if level in LOG_LEVELS else default


def configured_log_level():
    return normalize_log_level(os.environ.get("QBT_LOG_LEVEL"), "info")


def log_level_enabled(level):
    requested = LOG_LEVELS[normalize_log_level(level)]
    configured = LOG_LEVELS[configured_log_level()]
    return requested >= configured


def configured_log_format():
    value = os.environ.get("QBT_LOG_FORMAT", "text").strip().lower()
    if value in {"json", "structured"}:
        return "json"
    return "text"


def decision_logs_enabled():
    if "QBT_DECISION_LOGS_ENABLED" in os.environ:
        return env_bool("QBT_DECISION_LOGS_ENABLED", True)
    return env_bool("QBT_STRUCTURED_DECISION_LOGS_ENABLED", True)


def configured_decision_log_level():
    return normalize_log_level(os.environ.get("QBT_DECISION_LOG_LEVEL"), "debug")


def text_log_value(value):
    safe_value = json_safe(value)
    if isinstance(safe_value, str):
        if safe_value and not re.search(r"\s|=", safe_value):
            return safe_value
        return json.dumps(safe_value)
    return json.dumps(safe_value, sort_keys=True, separators=(",", ":"))


def text_log_line(record, omit_fields=None):
    omit_fields = set(omit_fields or ())
    prefix = f"{record['timestamp']} {record['level']}"
    if record.get("event"):
        prefix = f"{prefix} {record['event']}"

    message = record.get("message") or ""
    fields = [
        f"{key}={text_log_value(value)}"
        for key, value in record.items()
        if key not in {"timestamp", "level", "event", "message"}
        and key not in omit_fields
    ]
    parts = [prefix]
    if message:
        parts.append(message)
    parts.extend(fields)
    return " ".join(parts)


def emit_log(level, message="", event="qbt_guard", text_omit_fields=None, **fields):
    level = normalize_log_level(level)
    if not log_level_enabled(level):
        return

    record = {
        "timestamp": format_utc(datetime.now(timezone.utc)),
        "level": level.upper(),
        "event": event,
        "message": message,
    }
    record.update(fields)
    if configured_log_format() == "json":
        line = json.dumps(json_safe(record), sort_keys=True, separators=(",", ":"))
    else:
        line = text_log_line(record, text_omit_fields)

    stream = sys.stderr if LOG_LEVELS[level] >= LOG_LEVELS["warning"] else sys.stdout
    stream.write(line + "\n")
    stream.flush()


def log_debug(message, **fields):
    emit_log("debug", message, **fields)


def log_info(message, **fields):
    emit_log("info", message, **fields)


def log_warning(message, **fields):
    emit_log("warning", message, **fields)


def log_error(message, **fields):
    emit_log("error", message, **fields)


def decision_log_message(event, fields):
    action = fields.get("action")
    selected = fields.get("selected_torrent")
    selected_name = selected.get("name") if isinstance(selected, dict) else ""
    reason = fields.get("reason")

    parts = []
    if action:
        parts.append(str(action))
    if selected_name:
        parts.append(f"selected={selected_name}")
    if reason:
        parts.append(f"reason={reason}")
    return " ".join(parts) or event


def emit_decision_log(event, **fields):
    if not decision_logs_enabled():
        return
    emit_log(
        configured_decision_log_level(),
        decision_log_message(event, fields),
        event=event,
        **fields,
    )


def decision_summary_repeat_seconds():
    return max(0, env_int("QBT_DECISION_SUMMARY_REPEAT_SECONDS", 900))


def log_decision_info(
    action,
    message,
    summary_key=None,
    repeat_seconds=None,
    text_omit_fields=None,
    **fields,
):
    if summary_key is not None:
        if repeat_seconds is None:
            repeat_seconds = decision_summary_repeat_seconds()
        now = time.monotonic()
        with _DECISION_SUMMARY_REPEAT_LOCK:
            state = _DECISION_SUMMARY_REPEAT_STATE.get(summary_key)
            if repeat_seconds > 0 and state is not None:
                elapsed = now - state["last_logged_at"]
                if elapsed < repeat_seconds:
                    state["suppressed_count"] += 1
                    return
            suppressed_count = state["suppressed_count"] if state else 0
            _DECISION_SUMMARY_REPEAT_STATE[summary_key] = {
                "last_logged_at": now,
                "suppressed_count": 0,
            }
        if suppressed_count:
            fields = dict(fields)
            fields["suppressed_decision_log_count"] = suppressed_count
    emit_log(
        "info",
        message,
        event="qbt_guard_decision",
        action=action,
        text_omit_fields=text_omit_fields,
        **fields,
    )


def parse_udm_row_time(value):
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            value = float(raw)
        except ValueError:
            return parse_utc(raw)
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    if timestamp > 10_000_000_000:
        timestamp = timestamp / 1000.0
    try:
        return datetime.fromtimestamp(timestamp, timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def latest_udm_row_time(rows):
    latest = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_time = parse_udm_row_time(row.get("time"))
        if row_time and (latest is None or row_time > latest):
            latest = row_time
    return latest


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
        raise ApiError(f"{method} {url} failed with HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise ApiError(f"{method} {url} failed: {exc}") from exc


def response_rows(data, label, key="data"):
    if isinstance(data, dict):
        rows = data.get(key, [])
    elif isinstance(data, list):
        rows = data
    else:
        raise ApiError(f"{label} response has unexpected shape: {type(data).__name__}")
    if not isinstance(rows, list):
        raise ApiError(f"{label} response has unexpected shape: {type(rows).__name__}")
    return rows


class NvmeThermalGuard:
    def __init__(self):
        self.prometheus_url = os.environ.get("PROMETHEUS_URL", PROMETHEUS_DEFAULT_URL).strip().rstrip("/")
        self.enabled = env_bool("QBT_NVME_THERMAL_STOP_ENABLED", bool(self.prometheus_url))
        self.query = os.environ.get("QBT_NVME_THERMAL_QUERY", NVME_THERMAL_QUERY).strip()
        self.threshold = env_float("QBT_NVME_THERMAL_STOP_CELSIUS", 80.0)
        self.timeout = env_int("QBT_NVME_THERMAL_TIMEOUT", 5)
        self.fail_closed = env_bool("QBT_NVME_THERMAL_FAIL_CLOSED", True)
        self.opener = urllib.request.build_opener()

    def check(self):
        if not self.enabled:
            return {"enabled": False, "stop": False, "reason": "NVMe thermal guard disabled", "readings": []}
        if not self.prometheus_url:
            reason = "PROMETHEUS_URL is required when NVMe thermal guard is enabled"
            if self.fail_closed:
                return {"enabled": True, "stop": True, "reason": reason, "readings": []}
            log_warning(f"{reason}; continuing because QBT_NVME_THERMAL_FAIL_CLOSED=false")
            return {"enabled": True, "stop": False, "reason": reason, "readings": []}

        url = join_url(self.prometheus_url, "/api/v1/query")
        url += "?" + urllib.parse.urlencode({"query": self.query})
        try:
            data, _ = request_json(self.opener, "GET", url, timeout=self.timeout)
            if data.get("status") != "success":
                raise ApiError(f"Prometheus query returned status {data.get('status')!r}")
            results = data.get("data", {}).get("result", [])
            readings = []
            for sample in results:
                metric = sample.get("metric") or {}
                value = sample.get("value") or []
                temperature = float(value[1])
                node_name = (
                    metric.get("nodename")
                    or metric.get("node")
                    or metric.get("instance")
                    or "<unknown>"
                )
                readings.append({"node": str(node_name), "temperature": temperature})
            if not readings:
                raise ApiError("Prometheus returned no NVMe temperature samples")
        except (ApiError, IndexError, KeyError, TypeError, ValueError) as exc:
            reason = f"NVMe thermal check failed: {exc}"
            if self.fail_closed:
                return {"enabled": True, "stop": True, "reason": reason, "readings": []}
            log_warning(f"{reason}; continuing because QBT_NVME_THERMAL_FAIL_CLOSED=false")
            return {"enabled": True, "stop": False, "reason": reason, "readings": []}

        readings.sort(key=lambda item: item["node"])
        summary = ", ".join(
            f"{item['node']}={item['temperature']:.1f}C"
            for item in readings
        )
        log_debug(f"NVMe thermal check: {summary}; stop threshold {self.threshold:.1f}C")

        hot_readings = [
            item for item in readings
            if item["temperature"] >= self.threshold
        ]
        if not hot_readings:
            return {
                "enabled": True,
                "stop": False,
                "reason": f"all NVMe temperatures below {self.threshold:.1f}C",
                "readings": readings,
            }

        hot_summary = ", ".join(
            f"{item['node']}={item['temperature']:.1f}C"
            for item in hot_readings
        )
        return {
            "enabled": True,
            "stop": True,
            "reason": f"NVMe thermal stop threshold {self.threshold:.1f}C reached: {hot_summary}",
            "readings": readings,
        }


class KubernetesNodeClient:
    def __init__(self):
        api_host = os.environ.get("KUBERNETES_SERVICE_HOST", "").strip()
        api_port = os.environ.get("KUBERNETES_SERVICE_PORT", "443").strip() or "443"
        self.api_base = f"https://{api_host}:{api_port}" if api_host else ""
        self.token_path = os.environ.get("KUBERNETES_TOKEN_PATH", KUBERNETES_TOKEN_PATH)
        self.ca_path = os.environ.get("KUBERNETES_CA_PATH", KUBERNETES_CA_PATH)
        self.timeout = env_int("QBT_RPI_COOLING_K8S_TIMEOUT", 5)
        self.opener = urllib.request.build_opener()

    def read_token(self):
        with open(self.token_path, "r", encoding="utf-8") as token_file:
            return token_file.read().strip()

    def fetch_node(self, node_name):
        quoted_name = urllib.parse.quote(node_name, safe="")
        return self.fetch_path(f"/api/v1/nodes/{quoted_name}", f"Kubernetes node {node_name}")

    def fetch_path(self, path, description):
        return self.request_path("GET", path, description)

    def request_path(self, method, path, description, headers=None, body=None):
        if not self.api_base:
            raise ApiError("Kubernetes service host is unavailable")
        request = urllib.request.Request(
            join_url(self.api_base, path),
            data=body,
            method=method,
            headers=headers or {},
        )
        request.add_header("Authorization", f"Bearer {self.read_token()}")
        request.add_header("Accept", "application/json")
        context = ssl.create_default_context(cafile=self.ca_path)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout, context=context) as response:
                payload = response.read()
                if not payload:
                    return {}
                return json.loads(payload.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise ApiError(f"{description} failed with HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise ApiError(f"{description} failed: {exc}") from exc

    def node_ready(self, node_name):
        node = self.fetch_node(node_name)
        for condition in node.get("status", {}).get("conditions", []):
            if condition.get("type") == "Ready":
                return condition.get("status") == "True"
        return False

    def ready_map(self, node_names):
        return {node_name: self.node_ready(node_name) for node_name in node_names}

    def list_pods(self, namespace, label_selector=""):
        quoted_namespace = urllib.parse.quote(namespace, safe="")
        path = f"/api/v1/namespaces/{quoted_namespace}/pods"
        if label_selector:
            path += "?" + urllib.parse.urlencode({"labelSelector": label_selector})
        return self.fetch_path(path, f"Kubernetes pods in namespace {namespace}").get("items", [])

    def fetch_pvc(self, namespace, name):
        quoted_namespace = urllib.parse.quote(namespace, safe="")
        quoted_name = urllib.parse.quote(name, safe="")
        return self.fetch_path(
            f"/api/v1/namespaces/{quoted_namespace}/persistentvolumeclaims/{quoted_name}",
            f"PersistentVolumeClaim {namespace}/{name}",
        )

    def fetch_pv(self, name):
        quoted_name = urllib.parse.quote(name, safe="")
        return self.fetch_path(
            f"/api/v1/persistentvolumes/{quoted_name}",
            f"PersistentVolume {name}",
        )

    def fetch_longhorn_volume(self, namespace, name):
        quoted_namespace = urllib.parse.quote(namespace, safe="")
        quoted_name = urllib.parse.quote(name, safe="")
        return self.fetch_path(
            f"/apis/longhorn.io/v1beta2/namespaces/{quoted_namespace}/volumes/{quoted_name}",
            f"Longhorn volume {namespace}/{name}",
        )

    def fetch_longhorn_share_manager(self, namespace, name):
        quoted_namespace = urllib.parse.quote(namespace, safe="")
        quoted_name = urllib.parse.quote(name, safe="")
        return self.fetch_path(
            f"/apis/longhorn.io/v1beta2/namespaces/{quoted_namespace}/sharemanagers/{quoted_name}",
            f"Longhorn share manager {namespace}/{name}",
        )

    def list_longhorn_replicas(self, namespace):
        quoted_namespace = urllib.parse.quote(namespace, safe="")
        return self.fetch_path(
            f"/apis/longhorn.io/v1beta2/namespaces/{quoted_namespace}/replicas",
            f"Longhorn replicas in namespace {namespace}",
        ).get("items", [])

    def fetch_cronjob(self, namespace, name):
        quoted_namespace = urllib.parse.quote(namespace, safe="")
        quoted_name = urllib.parse.quote(name, safe="")
        return self.fetch_path(
            f"/apis/batch/v1/namespaces/{quoted_namespace}/cronjobs/{quoted_name}",
            f"CronJob {namespace}/{name}",
        )

    def set_cronjob_suspended(self, namespace, name, suspended):
        current = self.fetch_cronjob(namespace, name)
        if bool((current.get("spec") or {}).get("suspend", False)) == bool(suspended):
            return False
        quoted_namespace = urllib.parse.quote(namespace, safe="")
        quoted_name = urllib.parse.quote(name, safe="")
        body = json.dumps({"spec": {"suspend": bool(suspended)}}).encode("utf-8")
        self.request_path(
            "PATCH",
            f"/apis/batch/v1/namespaces/{quoted_namespace}/cronjobs/{quoted_name}",
            f"CronJob {namespace}/{name}",
            headers={"Content-Type": "application/merge-patch+json"},
            body=body,
        )
        return True


def parse_namespaced_names(value):
    targets = []
    for item in split_lines_or_csv(value):
        if "/" not in item:
            log_warning("Ignoring invalid namespaced target", target=item)
            continue
        namespace, name = item.split("/", 1)
        namespace = namespace.strip()
        name = name.strip()
        if namespace and name:
            targets.append((namespace, name))
    return targets


class BatchWorkSuspender:
    def __init__(self, kubernetes):
        self.enabled = env_bool("QBT_RPI_COOLING_BATCH_SUSPEND_ENABLED", False)
        self.targets = parse_namespaced_names(os.environ.get("QBT_RPI_COOLING_BATCH_SUSPEND_TARGETS"))
        self.fail_closed = env_bool("QBT_RPI_COOLING_BATCH_SUSPEND_FAIL_CLOSED", False)
        self.kubernetes = kubernetes

    def reconcile(self, suspended):
        if not self.enabled:
            return {"enabled": False, "changed": [], "errors": []}
        changed = []
        errors = []
        for namespace, name in self.targets:
            try:
                if self.kubernetes.set_cronjob_suspended(namespace, name, suspended):
                    changed.append({"namespace": namespace, "name": name, "suspend": bool(suspended)})
                    log_info(
                        "Updated thermal batch-work suspension",
                        namespace=namespace,
                        name=name,
                        suspend=bool(suspended),
                    )
            except ApiError as exc:
                error = {"namespace": namespace, "name": name, "error": str(exc)}
                errors.append(error)
                log_warning(
                    "Failed to update thermal batch-work suspension",
                    namespace=namespace,
                    name=name,
                    suspend=bool(suspended),
                    error=str(exc),
                )
        if errors and self.fail_closed:
            raise ApiError(f"failed to update thermal batch-work suspension for {len(errors)} CronJob(s)")
        return {"enabled": True, "changed": changed, "errors": errors}


class LonghornReplicaSafetyCheck:
    def __init__(self, kubernetes):
        self.enabled = env_bool("QBT_RPI_COOLING_LONGHORN_REPLICA_CHECK_ENABLED", False)
        self.namespace = os.environ.get("QBT_RPI_COOLING_LONGHORN_NAMESPACE", "longhorn-system").strip()
        self.fail_closed = env_bool("QBT_RPI_COOLING_LONGHORN_FAIL_CLOSED", True)
        self.protected_volume_regex = os.environ.get(
            "QBT_RPI_COOLING_LONGHORN_PROTECTED_VOLUME_REGEX",
            "",
        ).strip()
        self.protected_volume_pattern = (
            re.compile(self.protected_volume_regex)
            if self.protected_volume_regex
            else None
        )
        self.kubernetes = kubernetes

    def volume_is_protected(self, volume_name):
        if not self.protected_volume_pattern:
            return True
        return bool(self.protected_volume_pattern.search(volume_name or ""))

    def evaluate(self, node_name):
        if not self.enabled:
            return {"enabled": False, "safe": True, "reason": "Longhorn replica check disabled"}
        if not self.namespace:
            return {"enabled": True, "safe": True, "reason": "Longhorn namespace is not configured"}

        try:
            replicas = self.kubernetes.list_longhorn_replicas(self.namespace)
        except ApiError as exc:
            if self.fail_closed:
                return {
                    "enabled": True,
                    "safe": False,
                    "reason": f"Longhorn replica check failed: {exc}",
                }
            log_warning(
                f"Longhorn replica check failed: {exc}; continuing because "
                "QBT_RPI_COOLING_LONGHORN_FAIL_CLOSED=false",
            )
            return {"enabled": True, "safe": True, "reason": f"Longhorn replica check failed: {exc}"}

        protected_replicas = []
        for replica in replicas:
            spec = replica.get("spec") or {}
            volume_name = spec.get("volumeName") or ""
            if not spec.get("active", True) or not self.volume_is_protected(volume_name):
                continue
            protected_replicas.append(replica)

        active_replicas_by_volume = {}
        for replica in protected_replicas:
            volume_name = (replica.get("spec") or {}).get("volumeName") or ""
            if volume_name:
                active_replicas_by_volume.setdefault(volume_name, []).append(replica)

        blocked = []
        for volume_name, volume_replicas in active_replicas_by_volume.items():
            target_replicas = [
                replica
                for replica in volume_replicas
                if (replica.get("spec") or {}).get("nodeID") == node_name
            ]
            if target_replicas and len(volume_replicas) <= 1:
                replica = target_replicas[0]
                spec = replica.get("spec") or {}
                status = replica.get("status") or {}
                blocked.append(
                    {
                        "volume": volume_name,
                        "replica": replica.get("metadata", {}).get("name"),
                        "state": status.get("currentState") or spec.get("desireState"),
                        "failed_at": status.get("failedAt") or spec.get("failedAt") or "",
                        "last_healthy_at": status.get("lastHealthyAt") or spec.get("lastHealthyAt") or "",
                    }
                )

        if blocked:
            volume_summary = ", ".join(item["volume"] for item in blocked[:5])
            if len(blocked) > 5:
                volume_summary += f", +{len(blocked) - 5} more"
            return {
                "enabled": True,
                "safe": False,
                "reason": f"node hosts sole active Longhorn replica(s): {volume_summary}",
                "blocked_replicas": blocked,
            }

        return {
            "enabled": True,
            "safe": True,
            "reason": "no sole active Longhorn replicas on candidate node",
        }


def pod_pvc_names(pod):
    names = []
    for volume in (pod.get("spec") or {}).get("volumes") or []:
        claim = volume.get("persistentVolumeClaim") or {}
        claim_name = claim.get("claimName")
        if claim_name:
            names.append(str(claim_name))
    return names


def pv_longhorn_volume_name(pv):
    spec = pv.get("spec") or {}
    csi = spec.get("csi") or {}
    handle = csi.get("volumeHandle")
    if handle:
        return str(handle)
    return (pv.get("metadata") or {}).get("name") or ""


def add_nonempty_node(nodes, node_name):
    if node_name:
        nodes.add(str(node_name))


def longhorn_running_replica_nodes(replicas, volume_name):
    nodes = set()
    for replica in replicas:
        spec = replica.get("spec") or {}
        status = replica.get("status") or {}
        if spec.get("volumeName") != volume_name:
            continue
        current_state = status.get("currentState") or spec.get("currentState") or spec.get("desireState")
        if str(current_state or "").lower() != "running":
            continue
        if spec.get("active") is False:
            continue
        add_nonempty_node(nodes, spec.get("nodeID") or status.get("ownerID"))
    return nodes


class QbtThermalTopology:
    def __init__(self, kubernetes):
        self.kubernetes = kubernetes
        self.enabled = env_bool("QBT_RPI_COOLING_QBT_TOPOLOGY_ENABLED", False)
        self.fail_closed = env_bool("QBT_RPI_COOLING_QBT_TOPOLOGY_FAIL_CLOSED", False)
        self.namespace = os.environ.get("QBT_RPI_COOLING_QBT_NAMESPACE", "media").strip() or "media"
        self.selector = os.environ.get(
            "QBT_RPI_COOLING_QBT_SELECTOR",
            "app.kubernetes.io/instance=qbittorrent,app.kubernetes.io/name=qbittorrent",
        ).strip()
        self.longhorn_namespace = os.environ.get(
            "QBT_RPI_COOLING_LONGHORN_NAMESPACE",
            "longhorn-system",
        ).strip()
        self.static_nodes = set(split_lines_or_csv(os.environ.get("QBT_RPI_COOLING_QBT_AFFECTED_NODES")))

    def affected_nodes(self):
        if self.static_nodes:
            return {
                "enabled": self.enabled,
                "source": "static",
                "nodes": sorted(self.static_nodes),
                "volumes": [],
                "reason": "static qBittorrent affected nodes configured",
            }
        if not self.enabled:
            return {
                "enabled": False,
                "source": "disabled",
                "nodes": [],
                "volumes": [],
                "reason": "qBittorrent topology discovery disabled",
            }

        nodes = set()
        volumes = []
        try:
            pods = self.kubernetes.list_pods(self.namespace, self.selector)
            replicas = self.kubernetes.list_longhorn_replicas(self.longhorn_namespace)
            for pod in pods:
                add_nonempty_node(nodes, (pod.get("spec") or {}).get("nodeName"))
                for claim_name in pod_pvc_names(pod):
                    pvc = self.kubernetes.fetch_pvc(self.namespace, claim_name)
                    volume_name = (pvc.get("spec") or {}).get("volumeName")
                    if not volume_name:
                        continue
                    pv = self.kubernetes.fetch_pv(volume_name)
                    longhorn_volume_name = pv_longhorn_volume_name(pv)
                    if not longhorn_volume_name:
                        continue
                    volume_nodes = set()
                    longhorn_volume = self.kubernetes.fetch_longhorn_volume(
                        self.longhorn_namespace,
                        longhorn_volume_name,
                    )
                    volume_spec = longhorn_volume.get("spec") or {}
                    volume_status = longhorn_volume.get("status") or {}
                    for node_name in (
                        volume_spec.get("nodeID"),
                        volume_status.get("currentNodeID"),
                        volume_status.get("ownerID"),
                    ):
                        add_nonempty_node(volume_nodes, node_name)

                    access_mode = str(volume_spec.get("accessMode") or "").lower()
                    if access_mode == "rwx" or volume_status.get("shareState"):
                        try:
                            share_manager = self.kubernetes.fetch_longhorn_share_manager(
                                self.longhorn_namespace,
                                longhorn_volume_name,
                            )
                        except ApiError:
                            share_manager = {}
                        share_status = share_manager.get("status") or {}
                        add_nonempty_node(volume_nodes, share_status.get("ownerID"))

                    volume_nodes.update(longhorn_running_replica_nodes(replicas, longhorn_volume_name))
                    nodes.update(volume_nodes)
                    volumes.append({
                        "claim": claim_name,
                        "volume": longhorn_volume_name,
                        "nodes": sorted(volume_nodes),
                    })
        except ApiError as exc:
            if self.fail_closed:
                log_warning(
                    "qBittorrent thermal topology discovery failed; using all cooling nodes",
                    reason=str(exc),
                )
                return {
                    "enabled": True,
                    "source": "error-fail-closed",
                    "nodes": [],
                    "volumes": volumes,
                    "reason": str(exc),
                }
            log_warning(
                "qBittorrent thermal topology discovery failed; leaving qBittorrent unmanaged for this cycle",
                reason=str(exc),
            )
            return {
                "enabled": True,
                "source": "error-fail-open",
                "nodes": [],
                "volumes": volumes,
                "reason": str(exc),
            }

        return {
            "enabled": True,
            "source": "discovered",
            "nodes": sorted(nodes),
            "volumes": volumes,
            "reason": "qBittorrent affected nodes discovered",
        }


class RpiCoolingStateStore:
    def __init__(self, path):
        self.path = path

    def load(self):
        if not self.path:
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as state_file:
                payload = json.load(state_file)
        except FileNotFoundError:
            return {}
        except (OSError, json.JSONDecodeError) as exc:
            log_warning(f"Failed to read RPi cooling state: {exc}")
            return {}
        return payload if isinstance(payload, dict) else {}

    def save(self, state):
        if not self.path:
            return
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as state_file:
            json.dump(json_safe(state), state_file, sort_keys=True, separators=(",", ":"))
            state_file.write("\n")
            state_file.flush()
            os.fsync(state_file.fileno())
        os.replace(tmp_path, self.path)
        if directory:
            try:
                directory_fd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except OSError as exc:
                log_warning(f"Failed to fsync RPi cooling state directory: {exc}")

    def clear(self):
        if not self.path:
            return
        try:
            os.remove(self.path)
        except FileNotFoundError:
            return
        except OSError as exc:
            log_warning(f"Failed to clear RPi cooling state: {exc}")


class RpiThermalCoolingManager:
    def __init__(self):
        self.enabled = env_bool("QBT_RPI_COOLING_ENABLED", False)
        self.prometheus_url = os.environ.get("PROMETHEUS_URL", PROMETHEUS_DEFAULT_URL).strip().rstrip("/")
        self.nodes = split_lines_or_csv(os.environ.get("QBT_RPI_COOLING_NODES")) or list(RPI_COOLING_DEFAULT_NODES)
        self.cpu_query = os.environ.get("QBT_RPI_COOLING_CPU_QUERY", RPI_COOLING_CPU_QUERY).strip()
        self.nvme_query = os.environ.get("QBT_RPI_COOLING_NVME_QUERY", RPI_COOLING_NVME_QUERY).strip()
        self.cpu_throttle_threshold = env_float("QBT_RPI_COOLING_CPU_THROTTLE_CELSIUS", 70.0)
        self.nvme_throttle_threshold = env_float("QBT_RPI_COOLING_NVME_THROTTLE_CELSIUS", 65.0)
        self.cpu_pause_threshold = env_float("QBT_RPI_COOLING_CPU_PAUSE_CELSIUS", 74.0)
        self.nvme_pause_threshold = env_float("QBT_RPI_COOLING_NVME_PAUSE_CELSIUS", 68.0)
        self.cpu_resume_threshold = env_float("QBT_RPI_COOLING_CPU_RESUME_CELSIUS", 65.0)
        self.nvme_resume_threshold = env_float("QBT_RPI_COOLING_NVME_RESUME_CELSIUS", 60.0)
        self.resume_hold_seconds = env_int("QBT_RPI_COOLING_RESUME_HOLD_SECONDS", 900)
        self.shutdown_enabled = env_bool("QBT_RPI_COOLING_SHUTDOWN_ENABLED", False)
        self.last_resort_shutdown_enabled = env_bool("QBT_RPI_COOLING_LAST_RESORT_SHUTDOWN_ENABLED", False)
        self.cpu_threshold = env_float("QBT_RPI_COOLING_CPU_SHUTDOWN_CELSIUS", 85.0)
        self.nvme_threshold = env_float("QBT_RPI_COOLING_NVME_SHUTDOWN_CELSIUS", 80.0)
        self.last_resort_min_active_seconds = env_int("QBT_RPI_COOLING_LAST_RESORT_MIN_ACTIVE_SECONDS", 1800)
        self.timeout = env_int("QBT_RPI_COOLING_PROMETHEUS_TIMEOUT", 5)
        self.shutdown_timeout_seconds = env_int("QBT_RPI_COOLING_SHUTDOWN_TIMEOUT_SECONDS", 300)
        self.cooldown_seconds = env_int("QBT_RPI_COOLING_COOLDOWN_SECONDS", 1200)
        self.require_all_ready = env_bool("QBT_RPI_COOLING_REQUIRE_ALL_NODES_READY", True)
        self.require_all_temperatures = env_bool("QBT_RPI_COOLING_REQUIRE_ALL_TEMPERATURES", True)
        self.shutdown_urls = split_key_value_lines(os.environ.get("QBT_RPI_COOLING_SHUTDOWN_URLS"))
        self.shutdown_url_template = os.environ.get(
            "QBT_RPI_COOLING_SHUTDOWN_URL_TEMPLATE",
            "http://rpi-shutdown-{node}:8000/shutdown",
        ).strip()
        self.power_off_urls = split_key_value_lines(os.environ.get("QBT_RPI_COOLING_POWER_OFF_URLS"))
        self.power_on_urls = split_key_value_lines(os.environ.get("QBT_RPI_COOLING_POWER_ON_URLS"))
        self.shutdown_request_timeout = env_int("QBT_RPI_COOLING_SHUTDOWN_REQUEST_TIMEOUT", 10)
        self.power_request_timeout = env_int("QBT_RPI_COOLING_POWER_REQUEST_TIMEOUT", 10)
        self.state = RpiCoolingStateStore(
            os.environ.get("QBT_RPI_COOLING_STATE_PATH", "/state/rpi-cooling.json").strip()
        )
        self.opener = urllib.request.build_opener()
        self.kubernetes = KubernetesNodeClient()
        self.longhorn_replicas = LonghornReplicaSafetyCheck(self.kubernetes)
        self.batch_work = BatchWorkSuspender(self.kubernetes)
        self.qbt_topology = QbtThermalTopology(self.kubernetes)

    def shutdown_url(self, node_name):
        return self.shutdown_urls.get(node_name) or self.shutdown_url_template.format(node=node_name)

    def power_url(self, action, node_name):
        urls = self.power_on_urls if action == "on" else self.power_off_urls
        return urls.get(node_name, "")

    def request_plain_http(self, method, url, timeout):
        request = urllib.request.Request(url, method=method)
        try:
            with self.opener.open(request, timeout=timeout) as response:
                response.read()
                return response.status
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise ApiError(f"{method} {url} failed with HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise ApiError(f"{method} {url} failed: {exc}") from exc

    def request_power(self, action, node_name):
        url = self.power_url(action, node_name)
        if not url:
            log_warning(
                "RPi cooling power action URL is not configured",
                node=node_name,
                action=action,
            )
            return False
        self.request_plain_http("POST", url, self.power_request_timeout)
        log_info("Requested RPi power action", node=node_name, action=action)
        return True

    def prometheus_temperature_readings(self, query, label):
        if not self.prometheus_url:
            raise ApiError("PROMETHEUS_URL is required when RPi cooling is enabled")
        url = join_url(self.prometheus_url, "/api/v1/query")
        url += "?" + urllib.parse.urlencode({"query": query})
        data, _ = request_json(self.opener, "GET", url, timeout=self.timeout)
        if data.get("status") != "success":
            raise ApiError(f"Prometheus {label} query returned status {data.get('status')!r}")
        readings = {}
        for sample in data.get("data", {}).get("result", []):
            metric = sample.get("metric") or {}
            value = sample.get("value") or []
            node_name = (
                metric.get("nodename")
                or metric.get("node")
                or metric.get("instance")
                or ""
            )
            if not node_name:
                continue
            readings[str(node_name)] = float(value[1])
        if not readings:
            raise ApiError(f"Prometheus returned no RPi {label} temperature samples")
        return readings

    def temperature_snapshot(self):
        cpu = self.prometheus_temperature_readings(self.cpu_query, "CPU")
        nvme = self.prometheus_temperature_readings(self.nvme_query, "NVMe")
        if self.require_all_temperatures:
            missing = [
                node_name
                for node_name in self.nodes
                if node_name not in cpu or node_name not in nvme
            ]
            if missing:
                raise ApiError(f"missing RPi temperature samples for {', '.join(missing)}")
        return {
            node_name: {
                "cpu": cpu.get(node_name),
                "nvme": nvme.get(node_name),
            }
            for node_name in self.nodes
        }

    def hot_candidate_for(self, temperatures, cpu_threshold, nvme_threshold):
        candidates = []
        for node_name, readings in temperatures.items():
            cpu_temp = readings.get("cpu")
            nvme_temp = readings.get("nvme")
            if cpu_temp is not None and cpu_temp >= cpu_threshold:
                candidates.append((cpu_temp - cpu_threshold, node_name, "CPU", cpu_temp, cpu_threshold))
            if nvme_temp is not None and nvme_temp >= nvme_threshold:
                candidates.append((nvme_temp - nvme_threshold, node_name, "NVMe", nvme_temp, nvme_threshold))
        if not candidates:
            return None
        _, node_name, kind, temperature, threshold = max(candidates, key=lambda item: (item[0], item[3]))
        return {
            "node": node_name,
            "kind": kind,
            "temperature": temperature,
            "threshold": threshold,
        }

    def hot_candidate(self, temperatures):
        return self.hot_candidate_for(temperatures, self.cpu_threshold, self.nvme_threshold)

    def thermal_action_candidate(self, temperatures):
        pause_candidate = self.hot_candidate_for(
            temperatures,
            self.cpu_pause_threshold,
            self.nvme_pause_threshold,
        )
        if pause_candidate:
            return THERMAL_ACTION_PAUSE, pause_candidate
        throttle_candidate = self.hot_candidate_for(
            temperatures,
            self.cpu_throttle_threshold,
            self.nvme_throttle_threshold,
        )
        if throttle_candidate:
            return THERMAL_ACTION_THROTTLE, throttle_candidate
        return THERMAL_ACTION_CLEAR, None

    def qbt_action_for_candidate(self, action, candidate):
        if action not in {THERMAL_ACTION_THROTTLE, THERMAL_ACTION_PAUSE} or not candidate:
            return "", {}
        topology = self.qbt_topology.affected_nodes()
        if topology.get("source") == "disabled":
            return action, topology
        affected_nodes = set(topology.get("nodes") or [])
        if topology.get("source") == "error-fail-closed":
            affected_nodes = set(self.nodes)
        if candidate.get("node") in affected_nodes:
            return action, topology
        return "", topology

    def all_temperatures_below_resume(self, temperatures):
        for readings in temperatures.values():
            cpu_temp = readings.get("cpu")
            nvme_temp = readings.get("nvme")
            if cpu_temp is not None and cpu_temp >= self.cpu_resume_threshold:
                return False
            if nvme_temp is not None and nvme_temp >= self.nvme_resume_threshold:
                return False
        return True

    def cooling_state_from_candidate(self, candidate, now, phase):
        return {
            "node": candidate["node"],
            "phase": phase,
            "started_at": format_utc(now),
            "reason": (
                f"{candidate['kind']} temperature {candidate['temperature']:.1f}C "
                f"reached threshold {candidate['threshold']:.1f}C"
            ),
            "temperature_kind": candidate["kind"],
            "temperature_celsius": candidate["temperature"],
            "threshold_celsius": candidate["threshold"],
        }

    def thermal_state_from_candidate(self, action, candidate, now):
        state = self.cooling_state_from_candidate(candidate, now, action)
        state["thermal_action"] = action
        qbt_action, topology = self.qbt_action_for_candidate(action, candidate)
        state["qbt_action"] = qbt_action
        state["qbt_topology"] = topology
        state["last_active_at"] = format_utc(now)
        state["shutdown_eligible_after"] = format_utc(now + timedelta(seconds=self.last_resort_min_active_seconds))
        return state

    def reconcile_batch_work(self, suspended):
        return self.batch_work.reconcile(suspended)

    def thermal_state_reconciled(self, active, now, ready):
        node_name = active.get("node")
        phase = active.get("phase")
        if phase not in {THERMAL_ACTION_THROTTLE, THERMAL_ACTION_PAUSE}:
            return None

        temperatures = self.temperature_snapshot()
        action, candidate = self.thermal_action_candidate(temperatures)
        shutdown_candidate = self.hot_candidate(temperatures)
        batch = self.reconcile_batch_work(True)

        if action == THERMAL_ACTION_CLEAR and self.all_temperatures_below_resume(temperatures):
            clear_started_at = parse_utc(active.get("clear_started_at"))
            if clear_started_at is None:
                active["clear_started_at"] = format_utc(now)
                active["temperatures"] = temperatures
                self.state.save(active)
                log_info(
                    "RPi thermal mitigation clear window started",
                    node=node_name,
                    resume_hold_seconds=self.resume_hold_seconds,
                )
                return {
                    "enabled": True,
                    "action": phase,
                    "active": active,
                    "candidate": {"node": node_name},
                    "temperatures": temperatures,
                    "ready": ready,
                    "batch": batch,
                    "reason": active.get("reason") or "",
                }
            clear_elapsed = max(0, int((now - clear_started_at).total_seconds()))
            if clear_elapsed >= self.resume_hold_seconds:
                self.state.clear()
                batch = self.reconcile_batch_work(False)
                log_info("RPi thermal mitigation cleared after resume hold", elapsed_seconds=clear_elapsed)
                return {
                    "enabled": True,
                    "action": THERMAL_ACTION_CLEAR,
                    "temperatures": temperatures,
                    "ready": ready,
                    "batch": batch,
                }
            active["temperatures"] = temperatures
            self.state.save(active)
            return {
                "enabled": True,
                "action": phase,
                "active": active,
                "candidate": {"node": node_name},
                "temperatures": temperatures,
                "ready": ready,
                "batch": batch,
                "reason": active.get("reason") or "",
            }

        if candidate:
            if action != phase:
                original_started_at = active.get("started_at")
                active.update(self.thermal_state_from_candidate(action, candidate, now))
                if original_started_at:
                    active["started_at"] = original_started_at
                log_warning(
                    "RPi thermal mitigation changed state",
                    node=candidate["node"],
                    action=action,
                    kind=candidate["kind"],
                    temperature_celsius=round(candidate["temperature"], 1),
                    threshold_celsius=round(candidate["threshold"], 1),
                )
            else:
                active["last_active_at"] = format_utc(now)
                active["clear_started_at"] = ""
                active["temperatures"] = temperatures
                active["reason"] = (
                    f"{candidate['kind']} temperature {candidate['temperature']:.1f}C "
                    f"reached threshold {candidate['threshold']:.1f}C"
                )
                active["temperature_kind"] = candidate["kind"]
                active["temperature_celsius"] = candidate["temperature"]
                active["threshold_celsius"] = candidate["threshold"]
                qbt_action, topology = self.qbt_action_for_candidate(action, candidate)
                active["qbt_action"] = qbt_action
                active["qbt_topology"] = topology
            self.state.save(active)

        if (
            shutdown_candidate
            and (self.shutdown_enabled or self.last_resort_shutdown_enabled)
            and ready.get(shutdown_candidate["node"]) is True
        ):
            started_at = parse_utc(active.get("started_at")) or now
            active_elapsed = max(0, int((now - started_at).total_seconds()))
            if self.shutdown_enabled or active_elapsed >= self.last_resort_min_active_seconds:
                longhorn_safety = self.longhorn_replicas.evaluate(shutdown_candidate["node"])
                if not longhorn_safety.get("safe", True):
                    log_warning(
                        "RPi last-resort shutdown skipped by Longhorn safety check",
                        candidate=shutdown_candidate,
                        reason=longhorn_safety.get("reason"),
                    )
                else:
                    self.request_shutdown(shutdown_candidate, now, existing_state=active)
                    return {
                        "enabled": True,
                        "action": "shutdown_requested",
                        "candidate": shutdown_candidate,
                        "ready": ready,
                        "temperatures": temperatures,
                        "longhorn": longhorn_safety,
                        "batch": batch,
                    }

        return {
            "enabled": True,
            "action": active.get("phase") or action,
            "active": active,
            "candidate": candidate or {"node": node_name},
            "temperatures": temperatures,
            "ready": ready,
            "batch": batch,
            "reason": active.get("reason") or "",
        }

    def active_state_reconciled(self, now, ready):
        active = self.state.load()
        node_name = active.get("node")
        phase = active.get("phase")
        if not node_name or node_name not in self.nodes:
            if active:
                log_warning("Clearing invalid RPi cooling state", state=active)
                self.state.clear()
            return False

        node_ready = ready.get(node_name)
        started_at = parse_utc(active.get("started_at")) or now
        elapsed_seconds = max(0, int((now - started_at).total_seconds()))

        if phase in {"draining", "drain_aborted"}:
            log_warning(
                "Clearing legacy RPi cooling drain state; drain is disabled",
                node=node_name,
                phase=phase,
            )
            self.state.clear()
            return True

        thermal_reconcile = self.thermal_state_reconciled(active, now, ready)
        if thermal_reconcile is not None:
            return thermal_reconcile

        if phase == "shutdown_requested":
            if node_ready is False:
                power_off_requested = self.request_power("off", node_name)
                active["phase"] = "cooling"
                active["cooling_started_at"] = format_utc(now)
                active["power_off_requested"] = power_off_requested
                self.state.save(active)
                log_info(
                    "RPi cooling shutdown completed; cooling window started",
                    node=node_name,
                    cooldown_seconds=self.cooldown_seconds,
                )
            elif elapsed_seconds >= self.shutdown_timeout_seconds:
                log_warning(
                    "RPi cooling shutdown still pending; keeping lock active",
                    node=node_name,
                    elapsed_seconds=elapsed_seconds,
                    timeout_seconds=self.shutdown_timeout_seconds,
                )
            return True

        if phase == "cooling":
            if node_ready is True:
                self.state.clear()
                log_info("RPi cooling completed; node is Ready and lock is released", node=node_name)
            else:
                cooling_started_at = parse_utc(active.get("cooling_started_at")) or started_at
                cooling_elapsed = max(0, int((now - cooling_started_at).total_seconds()))
                if cooling_elapsed >= self.cooldown_seconds:
                    power_on_requested = self.request_power("on", node_name)
                    active["phase"] = "booting"
                    active["boot_started_at"] = format_utc(now)
                    active["power_on_requested"] = power_on_requested
                    self.state.save(active)
                    log_info(
                        "RPi cooling cooldown elapsed; boot window started",
                        node=node_name,
                        elapsed_seconds=cooling_elapsed,
                    )
            return True

        if phase == "booting":
            if node_ready is True:
                self.state.clear()
                log_info("RPi cooling completed; node is Ready and lock is released", node=node_name)
            else:
                boot_started_at = parse_utc(active.get("boot_started_at")) or started_at
                boot_elapsed = max(0, int((now - boot_started_at).total_seconds()))
                log_warning(
                    "RPi cooling boot still pending; keeping lock active",
                    node=node_name,
                    elapsed_seconds=boot_elapsed,
                )
            return True

        log_warning("Clearing unknown RPi cooling phase", node=node_name, phase=phase)
        self.state.clear()
        return False

    def request_shutdown(self, candidate, now, existing_state=None):
        node_name = candidate["node"]
        url = self.shutdown_url(node_name)
        state = dict(existing_state or self.cooling_state_from_candidate(candidate, now, "shutdown_requested"))
        original_started_at = state.get("started_at")
        state.update(self.cooling_state_from_candidate(candidate, now, "shutdown_requested"))
        if original_started_at:
            state["started_at"] = original_started_at
        state["phase"] = "shutdown_requested"
        state["shutdown_requested_at"] = format_utc(now)
        self.state.save(state)
        try:
            request_json(self.opener, "POST", url, body=b"", timeout=self.shutdown_request_timeout)
        except Exception:
            self.state.clear()
            raise
        log_warning(
            "Requested RPi clean shutdown for thermal cooling",
            node=node_name,
            kind=candidate["kind"],
            temperature_celsius=round(candidate["temperature"], 1),
            threshold_celsius=round(candidate["threshold"], 1),
        )

    def reconcile(self):
        if not self.enabled:
            return {"enabled": False, "action": "disabled"}
        if not self.nodes:
            return {"enabled": True, "action": "skipped", "reason": "no RPi cooling nodes configured"}

        now = datetime.now(timezone.utc)
        ready = self.kubernetes.ready_map(self.nodes)
        active_reconcile = self.active_state_reconciled(now, ready)
        if active_reconcile:
            if isinstance(active_reconcile, dict):
                return active_reconcile
            active = self.state.load()
            return {
                "enabled": True,
                "action": "active",
                "ready": ready,
                "active": active,
                "candidate": {"node": active.get("node")} if active.get("node") else {},
                "reason": active.get("reason") or "",
            }

        if self.require_all_ready and not all(ready.values()):
            log_debug("RPi cooling skipped because not all nodes are Ready", ready=ready)
            return {"enabled": True, "action": "skipped", "reason": "not all nodes are Ready", "ready": ready}

        temperatures = self.temperature_snapshot()
        action, candidate = self.thermal_action_candidate(temperatures)
        if not candidate:
            batch = self.reconcile_batch_work(False)
            return {
                "enabled": True,
                "action": THERMAL_ACTION_CLEAR,
                "temperatures": temperatures,
                "ready": ready,
                "batch": batch,
            }

        batch = self.reconcile_batch_work(True)
        state = self.thermal_state_from_candidate(action, candidate, now)
        state["temperatures"] = temperatures
        self.state.save(state)
        log_warning(
            "RPi thermal mitigation started",
            node=candidate["node"],
            action=action,
            kind=candidate["kind"],
            temperature_celsius=round(candidate["temperature"], 1),
            threshold_celsius=round(candidate["threshold"], 1),
        )

        shutdown_candidate = self.hot_candidate(temperatures)
        if not shutdown_candidate:
            return {
                "enabled": True,
                "action": action,
                "candidate": candidate,
                "active": state,
                "temperatures": temperatures,
                "ready": ready,
                "batch": batch,
            }

        if not (self.shutdown_enabled or self.last_resort_shutdown_enabled):
            return {
                "enabled": True,
                "action": action,
                "candidate": candidate,
                "active": state,
                "temperatures": temperatures,
                "ready": ready,
                "batch": batch,
            }

        if ready.get(shutdown_candidate["node"]) is not True:
            log_warning("RPi cooling candidate is not Ready; shutdown skipped", candidate=shutdown_candidate, ready=ready)
            return {
                "enabled": True,
                "action": action,
                "reason": "shutdown candidate node is not Ready",
                "candidate": candidate,
                "active": state,
                "temperatures": temperatures,
                "ready": ready,
                "batch": batch,
            }

        if self.last_resort_shutdown_enabled and not self.shutdown_enabled:
            return {
                "enabled": True,
                "action": action,
                "candidate": candidate,
                "active": state,
                "temperatures": temperatures,
                "ready": ready,
                "batch": batch,
            }

        longhorn_safety = self.longhorn_replicas.evaluate(shutdown_candidate["node"])
        if not longhorn_safety.get("safe", True):
            log_warning(
                "RPi cooling candidate failed Longhorn replica safety check; shutdown skipped",
                candidate=shutdown_candidate,
                reason=longhorn_safety.get("reason"),
            )
            return {
                "enabled": True,
                "action": action,
                "reason": longhorn_safety.get("reason") or "Longhorn replica safety check failed",
                "candidate": candidate,
                "shutdown_candidate": shutdown_candidate,
                "active": state,
                "temperatures": temperatures,
                "ready": ready,
                "longhorn": longhorn_safety,
                "batch": batch,
            }

        self.request_shutdown(shutdown_candidate, now, existing_state=state)
        return {
            "enabled": True,
            "action": "shutdown_requested",
            "candidate": shutdown_candidate,
            "ready": ready,
            "temperatures": temperatures,
            "longhorn": longhorn_safety,
            "batch": batch,
        }


class DownloadStorageGuard:
    def __init__(self):
        self.enabled = env_bool("QBT_DOWNLOAD_STORAGE_GUARD_ENABLED", True)
        self.path = os.environ.get("QBT_DOWNLOAD_STORAGE_PATH", "/downloads").strip() or "/downloads"
        self.min_free_bytes = env_int("QBT_DOWNLOAD_STORAGE_MIN_FREE_BYTES", 30 * 1024 * 1024 * 1024)
        self.min_free_fraction = env_float("QBT_DOWNLOAD_STORAGE_MIN_FREE_FRACTION", 0.10)
        self.require_torrent_fit = env_bool("QBT_DOWNLOAD_STORAGE_REQUIRE_TORRENT_FIT", True)
        self.fail_closed = env_bool("QBT_DOWNLOAD_STORAGE_FAIL_CLOSED", True)

    def snapshot(self):
        if not self.enabled:
            return {"enabled": False, "stop": False, "reason": "download storage guard disabled"}

        try:
            stat = os.statvfs(self.path)
        except OSError as exc:
            reason = f"download storage check failed for {self.path}: {exc}"
            if self.fail_closed:
                return {"enabled": True, "stop": True, "reason": reason}
            log_warning(
                f"{reason}; continuing because QBT_DOWNLOAD_STORAGE_FAIL_CLOSED=false",
            )
            return {"enabled": True, "stop": False, "reason": reason}

        block_size = stat.f_frsize or stat.f_bsize
        total_bytes = max(0, stat.f_blocks * block_size)
        free_bytes = max(0, stat.f_bavail * block_size)
        reserve_bytes = max(
            max(0, self.min_free_bytes),
            math.floor(total_bytes * max(0.0, self.min_free_fraction)),
        )
        headroom_bytes = max(0, free_bytes - reserve_bytes)
        if free_bytes <= reserve_bytes:
            reason = (
                f"download storage free space {human_size(free_bytes)} is at or below "
                f"reserve {human_size(reserve_bytes)} on {self.path}"
            )
            return {
                "enabled": True,
                "stop": True,
                "reason": reason,
                "path": self.path,
                "total_bytes": total_bytes,
                "free_bytes": free_bytes,
                "reserve_bytes": reserve_bytes,
                "headroom_bytes": headroom_bytes,
            }

        return {
            "enabled": True,
            "stop": False,
            "reason": "download storage has free headroom",
            "path": self.path,
            "total_bytes": total_bytes,
            "free_bytes": free_bytes,
            "reserve_bytes": reserve_bytes,
            "headroom_bytes": headroom_bytes,
        }

    def check(self):
        state = self.snapshot()
        if not state.get("enabled"):
            return state
        if state.get("total_bytes") is None:
            return state
        log_debug(
            f"Download storage check: {state['path']} has "
            f"{human_size(state['free_bytes'])} free of {human_size(state['total_bytes'])}; "
            f"reserve {human_size(state['reserve_bytes'])}, "
            f"torrent headroom {human_size(state['headroom_bytes'])}"
        )
        return state


class UdmClient:
    def __init__(self):
        self.base_url = os.environ.get("UDM_URL", "").strip().rstrip("/")
        self.site = os.environ.get("UDM_SITE", "default")
        self.api_base_path = os.environ.get("UDM_API_BASE_PATH", "/proxy/network").strip()
        self.timeout = env_int("UDM_TIMEOUT", 30)
        self.verify_tls = env_bool("UDM_VERIFY_TLS", False)
        self.api_key = os.environ.get("UDM_API_KEY", "").strip()
        self.authenticated = False
        self.username = (
            os.environ.get("UDM_USER")
            or os.environ.get("UDM_USERNAME")
            or os.environ.get("UNIFI_USER")
            or os.environ.get("UNIFI_USERNAME")
            or ""
        ).strip()
        self.password = (os.environ.get("UDM_PASSWORD") or os.environ.get("UNIFI_PASSWORD") or "").strip()
        self.csrf_token = ""
        self.latest_stats_at = None
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
            raise ApiError("UDM_URL is required for UDM quota data")
        if self.api_key:
            log_debug("Using UDM API key authentication")
            self.authenticated = True
            return
        if not self.username or not self.password:
            raise ApiError("UDM credentials missing; set UDM_API_KEY or UDM_USER/UDM_PASSWORD")

        payload = json.dumps({"username": self.username, "password": self.password}).encode("utf-8")
        login_paths = split_lines_or_csv(os.environ.get("UDM_LOGIN_PATHS")) or [
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
                log_debug(f"Authenticated to UDM with {path}")
                self.authenticated = True
                return
            except ApiError as exc:
                errors.append(str(exc))
        raise ApiError("UDM login failed: " + " | ".join(errors))

    def stats_attrs(self):
        attrs = split_lines_or_csv(os.environ.get("UDM_DOWNLOAD_ATTRS")) or ["wan-rx_bytes", "wan2-rx_bytes"]
        if env_bool("UDM_INCLUDE_UPLOAD", False):
            attrs.extend(["wan-tx_bytes", "wan2-tx_bytes"])
        if "time" not in attrs:
            attrs.append("time")
        return attrs

    def stats_rows(self, interval, start, end, attrs):
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        report_type = os.environ.get("UDM_STATS_TYPE", "site").strip()
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
        rows = response_rows(data, "UDM stats")
        latest_row_time = latest_udm_row_time(rows)
        if latest_row_time and (self.latest_stats_at is None or latest_row_time > self.latest_stats_at):
            self.latest_stats_at = latest_row_time
        log_debug(f"UDM returned {len(rows)} {interval}.{report_type} rows")
        return rows

    def sum_download_bytes(self, rows, attrs):
        total = 0
        download_attrs = [attr for attr in attrs if attr != "time"]
        for row in rows:
            if not isinstance(row, dict):
                continue
            for attr in download_attrs:
                value = row.get(attr)
                if isinstance(value, (int, float)) and value > 0:
                    total += int(value)
        return total

    def download_usage_snapshot(self, now):
        self.login()
        month_start, _ = utc_month_window(now)
        today_start = utc_day_start(now)
        interval = os.environ.get("UDM_STATS_INTERVAL", "split-daily-hourly").strip()
        attrs = self.stats_attrs()

        if interval != "split-daily-hourly":
            month_rows = self.stats_rows(interval, month_start, now, attrs)
            day_rows = self.stats_rows(interval, today_start, now, attrs)
            return self.sum_download_bytes(month_rows, attrs), self.sum_download_bytes(day_rows, attrs)

        month_total = 0
        if month_start < today_start:
            history_interval = os.environ.get("UDM_HISTORY_STATS_INTERVAL", "daily").strip()
            rows = self.stats_rows(history_interval, month_start, today_start, attrs)
            month_total += self.sum_download_bytes(rows, attrs)

        current_interval = os.environ.get("UDM_CURRENT_STATS_INTERVAL", "hourly").strip()
        current_rows = self.stats_rows(current_interval, today_start, now, attrs)
        if not current_rows:
            fallback_interval = os.environ.get("UDM_CURRENT_STATS_FALLBACK_INTERVAL", "daily").strip()
            current_rows = self.stats_rows(fallback_interval, today_start, now, attrs)
        day_total = self.sum_download_bytes(current_rows, attrs)
        month_total += day_total
        return month_total, day_total

    def month_to_date_download_bytes(self, now):
        month_total, _ = self.download_usage_snapshot(now)
        return month_total


class QbtClient:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")
        self.timeout = env_int("QBT_TIMEOUT", 30)
        self.request_attempts = env_int("QBT_REQUEST_ATTEMPTS", 3)
        self.retry_delay = env_float("QBT_REQUEST_RETRY_DELAY", 2.0)
        self.username = (os.environ.get("QBT_USER") or os.environ.get("QBT_USERNAME") or "").strip()
        self.password = os.environ.get("QBT_PASSWORD", "")
        self.cookie_jar = CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookie_jar))

    def request(self, method, path, form=None):
        body = None
        headers = {}
        if form is not None:
            body = urllib.parse.urlencode(form).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        url = join_url(self.base_url, path)
        last_error = None
        for attempt in range(1, self.request_attempts + 1):
            request = urllib.request.Request(url, data=body, method=method, headers=headers)
            try:
                with self.opener.open(request, timeout=self.timeout) as response:
                    return response.read()
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
                last_error = exc
                if attempt < self.request_attempts:
                    time.sleep(self.retry_delay)
        raise ApiError(f"{method} {path} failed after {self.request_attempts} attempts: {last_error}")

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

    def stop_all(self):
        try:
            self.request("POST", "/api/v2/torrents/stop", {"hashes": "all"})
        except ApiError:
            self.request("POST", "/api/v2/torrents/pause", {"hashes": "all"})

    def torrents_info(self, filter_name=None):
        path = "/api/v2/torrents/info"
        if filter_name:
            path += "?" + urllib.parse.urlencode({"filter": filter_name})
        payload = self.request("GET", path)
        return json.loads(payload.decode("utf-8"))

    def transfer_info(self):
        payload = self.request("GET", "/api/v2/transfer/info")
        return json.loads(payload.decode("utf-8"))

    def torrent_files(self, item_hash):
        if not item_hash:
            return []
        path = "/api/v2/torrents/files?" + urllib.parse.urlencode({"hash": item_hash})
        payload = self.request("GET", path)
        return json.loads(payload.decode("utf-8"))

    def set_file_priority(self, item_hash, file_ids, priority):
        if not item_hash or not file_ids:
            return
        self.request(
            "POST",
            "/api/v2/torrents/filePrio",
            {
                "hash": item_hash,
                "id": "|".join(str(file_id) for file_id in file_ids),
                "priority": str(int(priority)),
            },
        )

    def start_hashes(self, hashes):
        if not hashes:
            return
        form = {"hashes": "|".join(hashes)}
        try:
            self.request("POST", "/api/v2/torrents/start", form)
        except ApiError:
            self.request("POST", "/api/v2/torrents/resume", form)

    def stop_hashes(self, hashes):
        if not hashes:
            return
        form = {"hashes": "|".join(hashes)}
        try:
            self.request("POST", "/api/v2/torrents/stop", form)
        except ApiError:
            self.request("POST", "/api/v2/torrents/pause", form)

    def top_priority(self, hashes):
        if not hashes:
            return
        self.request("POST", "/api/v2/torrents/topPrio", {"hashes": "|".join(hashes)})

    def delete_hashes(self, hashes, delete_files):
        if not hashes:
            return
        self.request(
            "POST",
            "/api/v2/torrents/delete",
            {
                "hashes": "|".join(hashes),
                "deleteFiles": str(bool(delete_files)).lower(),
            },
        )

    def reannounce_hashes(self, hashes):
        if not hashes:
            return
        self.request("POST", "/api/v2/torrents/reannounce", {"hashes": "|".join(hashes)})

    def add_tags(self, hashes, tags):
        if not hashes or not tags:
            return
        self.request(
            "POST",
            "/api/v2/torrents/addTags",
            {"hashes": "|".join(hashes), "tags": ",".join(tags)},
        )

    def remove_tags(self, hashes, tags):
        if not hashes or not tags:
            return
        self.request(
            "POST",
            "/api/v2/torrents/removeTags",
            {"hashes": "|".join(hashes), "tags": ",".join(tags)},
        )

    def all_tags(self):
        payload = self.request("GET", "/api/v2/torrents/tags")
        return json.loads(payload.decode("utf-8"))

    def delete_tags(self, tags):
        if not tags:
            return
        self.request(
            "POST",
            "/api/v2/torrents/deleteTags",
            {"tags": ",".join(tags)},
        )


def qbt_urls():
    return split_lines_or_csv(os.environ.get("QBT_URLS")) or QBT_DEFAULT_URLS


def reachable_qbt_clients():
    urls = qbt_urls()
    clients = []
    for url in urls:
        client = QbtClient(url)
        try:
            client.login()
            client.request("GET", "/api/v2/app/version")
            clients.append(client)
            log_debug("Connected to qBittorrent service")
        except ApiError as exc:
            log_warning(f"Skipping unavailable qBittorrent service: {exc}")
    return clients


def apply_fail_closed():
    clients = reachable_qbt_clients()
    if not clients:
        log_error("No qBittorrent clients reachable while failing closed")
        return False
    stop_limit = env_int("QBT_STOP_DOWNLOAD_LIMIT_BYTES_PER_SEC", 1)
    stop_upload_limit = env_int("QBT_STOP_UPLOAD_LIMIT_BYTES_PER_SEC", 1)
    for client in clients:
        client.set_download_limit(stop_limit)
        client.set_upload_limit(stop_upload_limit)
        client.stop_all()
        log_decision_info(
            "pause_all",
            "Paused all torrents because UDM quota data is unavailable",
            reason="UDM quota data is unavailable",
        )
    return True


def qbt_limit_decision_summary_key(action, pause_torrents, download_limit, upload_limit, decision_context):
    context = decision_context or {}
    rpi_cooling_state = context.get("rpi_cooling") or {}
    rpi_action = rpi_cooling_qbt_action(rpi_cooling_state)
    if not rpi_action:
        return None
    candidate = rpi_cooling_state.get("candidate") or {}
    active = rpi_cooling_state.get("active") or {}
    node_name = candidate.get("node") or active.get("node") or ""
    phase = active.get("phase") or rpi_cooling_state.get("action") or ""
    return (
        "rpi_cooling_qbt_limits",
        action,
        rpi_action,
        phase,
        node_name,
        bool(pause_torrents),
        int_or_none(download_limit),
        int_or_none(upload_limit),
    )


def rpi_cooling_decision_fields(decision_context):
    context = decision_context or {}
    rpi_cooling_state = context.get("rpi_cooling") or {}
    if not rpi_cooling_qbt_action(rpi_cooling_state):
        return {}

    candidate = rpi_cooling_state.get("candidate") or {}
    active = rpi_cooling_state.get("active") or {}
    fields = {
        "thermal_action": rpi_cooling_qbt_action(rpi_cooling_state),
        "thermal_node": candidate.get("node") or active.get("node") or "",
        "thermal_sensor": candidate.get("kind") or active.get("temperature_kind") or "",
        "temperature_celsius": candidate.get("temperature") or active.get("temperature_celsius"),
        "threshold_celsius": candidate.get("threshold") or active.get("threshold_celsius"),
    }
    return {key: value for key, value in fields.items() if value not in {"", None}}


def thermal_qbt_limit_message(pause_torrents, download_limit, upload_limit, decision_context, reason):
    fields = rpi_cooling_decision_fields(decision_context)
    if not fields:
        if pause_torrents:
            return f"Paused all torrents; {reason}", {}
        return (
            f"Throttled qBittorrent to {human_rate(download_limit)} down "
            f"and {human_rate(upload_limit)} up; {reason}"
        ), {}

    node = fields.get("thermal_node", "unknown-node")
    sensor = fields.get("thermal_sensor", "temperature")
    try:
        temperature = f"{float(fields['temperature_celsius']):.1f}C"
    except (KeyError, TypeError, ValueError):
        temperature = "unknown"
    try:
        threshold = f"{float(fields['threshold_celsius']):.1f}C"
    except (KeyError, TypeError, ValueError):
        threshold = "configured threshold"

    if pause_torrents:
        message = (
            f"Thermal pause: paused qBittorrent for {node}; "
            f"{sensor} {temperature} >= {threshold}"
        )
    else:
        message = (
            f"Thermal throttle: limited qBittorrent to "
            f"{human_rate(download_limit)} down / {human_rate(upload_limit)} up for {node}; "
            f"{sensor} {temperature} >= {threshold}"
        )
    return message, fields


def apply_qbt_limits(clients, reason, pause_torrents, download_limit, upload_limit, decision_context=None):
    action = "pause_all" if pause_torrents else "throttle"
    summary_key = qbt_limit_decision_summary_key(
        action,
        pause_torrents,
        download_limit,
        upload_limit,
        decision_context,
    )
    for client in clients:
        client.set_download_limit(download_limit)
        client.set_upload_limit(upload_limit)
        emit_decision_log(
            "qbt_guard_stop",
            **decision_base_context(decision_context, client),
            action=action,
            reason=reason,
            effective_cap={
                "download_limit_bytes_per_sec": download_limit,
                "upload_limit_bytes_per_sec": upload_limit,
            },
        )
        if pause_torrents:
            client.stop_all()
            message, thermal_fields = thermal_qbt_limit_message(
                True,
                download_limit,
                upload_limit,
                decision_context,
                reason,
            )
            log_decision_info(
                "pause_all",
                message,
                summary_key=summary_key,
                text_omit_fields={"action", "reason", *thermal_fields.keys()},
                reason=reason,
                **thermal_fields,
            )
        else:
            message, thermal_fields = thermal_qbt_limit_message(
                False,
                download_limit,
                upload_limit,
                decision_context,
                reason,
            )
            log_decision_info(
                "throttle",
                message,
                summary_key=summary_key,
                text_omit_fields={"action", "reason", *thermal_fields.keys()},
                reason=reason,
                **thermal_fields,
            )


def apply_stop_limits(clients, reason, pause_torrents, decision_context=None):
    stop_limit = env_int("QBT_STOP_DOWNLOAD_LIMIT_BYTES_PER_SEC", 1)
    stop_upload_limit = env_int("QBT_STOP_UPLOAD_LIMIT_BYTES_PER_SEC", 1)
    apply_qbt_limits(clients, reason, pause_torrents, stop_limit, stop_upload_limit, decision_context)


def apply_thermal_throttle_limits(clients, reason, decision_context=None):
    download_limit = env_int("QBT_RPI_COOLING_THROTTLE_DOWNLOAD_LIMIT_BYTES_PER_SEC", 2 * 1024 * 1024)
    upload_limit = env_int("QBT_RPI_COOLING_THROTTLE_UPLOAD_LIMIT_BYTES_PER_SEC", 128 * 1024)
    apply_qbt_limits(clients, reason, False, download_limit, upload_limit, decision_context)


def full_guard_thermal_state():
    if not env_bool("QBT_FULL_GUARD_THERMAL_CHECK_ENABLED", True):
        return {
            "enabled": False,
            "stop": False,
            "reason": "full guard thermal check disabled",
            "readings": [],
        }
    return NvmeThermalGuard().check()


def apply_full_guard_thermal_stop(clients, thermal_state=None, decision_context=None):
    if not clients:
        return False
    if thermal_state is None:
        thermal_state = full_guard_thermal_state()
    if not thermal_state.get("stop"):
        return False
    context = dict(decision_context or {})
    context["thermal"] = thermal_decision_summary(thermal_state)
    apply_stop_limits(clients, thermal_state["reason"], pause_torrents=True, decision_context=context)
    cleanup_qbt_clients(clients)
    return True


def rpi_cooling_stop_reason(rpi_cooling_state):
    if not rpi_cooling_state or not rpi_cooling_state.get("enabled", True):
        return ""
    candidate = rpi_cooling_state.get("candidate") or {}
    active = rpi_cooling_state.get("active") or {}
    node_name = candidate.get("node")
    if not node_name:
        node_name = active.get("node")
    longhorn = rpi_cooling_state.get("longhorn") or {}
    if longhorn and not longhorn.get("safe", True):
        reason = longhorn.get("reason") or rpi_cooling_state.get("reason") or "Longhorn replica safety check failed"
        if node_name:
            return f"RPi thermal cooling blocked for {node_name}: {reason}"
        return f"RPi thermal cooling blocked: {reason}"
    if rpi_cooling_state.get("action") == "error":
        reason = rpi_cooling_state.get("reason") or "unknown error"
        return f"RPi thermal cooling check failed: {reason}"
    if rpi_cooling_state.get("action") == "active" and active.get("phase") in {"shutdown_requested", "cooling", "booting"} and node_name:
        return f"RPi thermal cooling active for {node_name}"
    if rpi_cooling_state.get("action") == "shutdown_requested" and node_name:
        return f"RPi thermal cooling shutdown requested for {node_name}"
    thermal_action = rpi_cooling_qbt_action(rpi_cooling_state)
    if thermal_action in {THERMAL_ACTION_THROTTLE, THERMAL_ACTION_PAUSE}:
        reason = rpi_cooling_state.get("reason") or active.get("reason") or "RPi thermal mitigation active"
        if node_name:
            return f"RPi thermal mitigation {thermal_action} active for {node_name}: {reason}"
        return f"RPi thermal mitigation {thermal_action} active: {reason}"
    return ""


def rpi_cooling_qbt_action(rpi_cooling_state):
    if not rpi_cooling_state or not rpi_cooling_state.get("enabled", True):
        return ""
    qbt_action = rpi_cooling_state.get("qbt_action")
    active = rpi_cooling_state.get("active") or {}
    if not qbt_action:
        qbt_action = active.get("qbt_action")
    if qbt_action in {THERMAL_ACTION_THROTTLE, THERMAL_ACTION_PAUSE}:
        return qbt_action
    if qbt_action == "":
        return ""
    action = rpi_cooling_state.get("action")
    phase = active.get("phase")
    if action in {THERMAL_ACTION_THROTTLE, THERMAL_ACTION_PAUSE}:
        return action
    if action == "active" and phase in {THERMAL_ACTION_THROTTLE, THERMAL_ACTION_PAUSE}:
        return phase
    return ""


def apply_rpi_cooling_stop(clients, rpi_cooling_state, decision_context=None):
    if not clients:
        return False
    reason = rpi_cooling_stop_reason(rpi_cooling_state)
    if not reason:
        return False
    context = dict(decision_context or {})
    context["rpi_cooling"] = rpi_cooling_state
    qbt_action = rpi_cooling_qbt_action(rpi_cooling_state)
    if qbt_action == THERMAL_ACTION_THROTTLE:
        apply_thermal_throttle_limits(clients, reason, decision_context=context)
    else:
        apply_stop_limits(clients, reason, pause_torrents=True, decision_context=context)
    cleanup_qbt_clients(clients)
    return True


def apply_rpi_thermal_cooling():
    try:
        return RpiThermalCoolingManager().reconcile()
    except Exception as exc:
        log_error(f"RPi thermal cooling check failed: {exc}")
        return {"enabled": True, "action": "error", "reason": str(exc)}


def human_download_limit(value):
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return str(value)
    if limit <= 0:
        return "unlimited"
    return human_rate(limit)


def quota_rate_state(
    now,
    usage_bytes,
    day_usage_bytes,
    cap_bytes,
    daily_cap_bytes,
    headroom,
    max_download_limit,
):
    if usage_bytes >= cap_bytes:
        return {"stop_reason": "monthly UDM quota guardrail reached"}
    if day_usage_bytes >= daily_cap_bytes:
        return {"stop_reason": "daily UDM quota guardrail reached"}

    day_end = utc_day_end(now)
    day_seconds_remaining = max(1, int((day_end - now).total_seconds()))
    daily_remaining_bytes = daily_cap_bytes - day_usage_bytes

    _, month_end = utc_month_window(now)
    month_seconds_remaining = max(1, int((month_end - now).total_seconds()))
    monthly_remaining_bytes = cap_bytes - usage_bytes
    monthly_limit = math.floor((monthly_remaining_bytes / month_seconds_remaining) * headroom)
    daily_limit = math.floor((daily_remaining_bytes / day_seconds_remaining) * headroom)
    aggregate_limit = min(monthly_limit, daily_limit)
    if max_download_limit > 0:
        aggregate_limit = min(aggregate_limit, max_download_limit)

    return {
        "stop_reason": "",
        "monthly_remaining_bytes": monthly_remaining_bytes,
        "daily_remaining_bytes": daily_remaining_bytes,
        "month_seconds_remaining": month_seconds_remaining,
        "day_seconds_remaining": day_seconds_remaining,
        "monthly_limit": monthly_limit,
        "daily_limit": daily_limit,
        "aggregate_limit": aggregate_limit,
        "smart_download_limit": max(1, aggregate_limit),
    }


def torrent_hash(torrent):
    value = torrent.get("hash") or torrent.get("infohash_v1") or torrent.get("infohash_v2")
    return str(value).strip() if value else ""


def torrent_name(torrent):
    return torrent.get("name") or torrent_hash(torrent) or "<unknown>"


def dedupe_torrents(torrent_lists):
    torrents = []
    seen_hashes = set()
    for torrent_list in torrent_lists:
        for torrent in torrent_list:
            item_hash = torrent_hash(torrent)
            if not item_hash or item_hash in seen_hashes:
                continue
            seen_hashes.add(item_hash)
            torrents.append(torrent)
    return torrents


def optional_filtered_torrents(client, filter_name):
    try:
        return client.torrents_info(filter_name)
    except ApiError as exc:
        log_warning(
            f"Failed to list qBittorrent torrents with {filter_name!r} "
            f"filter: {exc}",
        )
        return []


def error_state_torrents(client):
    return dedupe_torrents([
        optional_filtered_torrents(client, "errored"),
        optional_filtered_torrents(client, "error"),
    ])


def single_download_torrents(client):
    return dedupe_torrents([
        client.torrents_info(),
        error_state_torrents(client),
    ])


def torrent_amount_left(torrent):
    try:
        return max(0, int(torrent.get("amount_left") or 0))
    except (TypeError, ValueError):
        return 0


def torrent_downloaded_bytes(torrent):
    for key in ("downloaded", "downloaded_session", "completed"):
        try:
            value = int(torrent.get(key))
        except (TypeError, ValueError):
            continue
        if value >= 0:
            return value
    return None


def torrent_progress(torrent):
    try:
        return float(torrent.get("progress") or 0)
    except (TypeError, ValueError):
        return 0.0


def torrent_download_speed(torrent):
    try:
        return max(0, int(torrent.get("dlspeed") or 0))
    except (TypeError, ValueError):
        return 0


def torrent_eta_seconds(torrent):
    try:
        eta = int(torrent.get("eta"))
        if eta >= 0:
            return eta
    except (TypeError, ValueError):
        pass

    speed = torrent_download_speed(torrent)
    if speed <= 0:
        return None
    return math.ceil(torrent_amount_left(torrent) / speed)


def torrent_state(torrent):
    return str(torrent.get("state") or "").strip()


def torrent_tags(torrent):
    raw_tags = str(torrent.get("tags") or "")
    return {
        tag.strip()
        for tag in raw_tags.split(",")
        if tag.strip()
    }


def torrent_category(torrent):
    return str(torrent.get("category") or "").strip()


def torrent_int(torrent, key):
    try:
        return max(0, int(torrent.get(key) or 0))
    except (TypeError, ValueError):
        return 0


def torrent_float(torrent, key):
    try:
        return max(0.0, float(torrent.get(key) or 0))
    except (TypeError, ValueError):
        return 0.0


def torrent_connected_seeds(torrent):
    return torrent_int(torrent, "num_seeds")


def torrent_reported_seeds(torrent):
    return torrent_int(torrent, "num_complete")


def torrent_availability(torrent):
    return torrent_float(torrent, "availability")


def torrent_decision_summary(torrent):
    if not torrent:
        return None
    return {
        "hash": torrent_hash(torrent),
        "name": torrent_name(torrent),
        "category": torrent_category(torrent),
        "state": torrent_state(torrent),
        "progress": torrent_progress(torrent),
        "amount_left_bytes": torrent_amount_left(torrent),
        "downloaded_bytes": torrent_downloaded_bytes(torrent),
        "download_speed_bytes_per_sec": torrent_download_speed(torrent),
        "connected_seeds": torrent_connected_seeds(torrent),
        "reported_seeds": torrent_reported_seeds(torrent),
        "availability": torrent_availability(torrent),
        "tags": sorted(torrent_tags(torrent)),
    }


def storage_decision_summary(storage_state):
    if not storage_state:
        return None
    keys = (
        "enabled",
        "stop",
        "reason",
        "path",
        "total_bytes",
        "free_bytes",
        "reserve_bytes",
        "headroom_bytes",
    )
    return {key: storage_state.get(key) for key in keys if key in storage_state}


def thermal_decision_summary(thermal_state):
    if not thermal_state:
        return None
    readings = thermal_state.get("readings") or []
    max_temperature = None
    for reading in readings:
        try:
            temperature = float(reading.get("temperature"))
        except (AttributeError, TypeError, ValueError):
            continue
        if max_temperature is None or temperature > max_temperature:
            max_temperature = temperature
    return {
        "enabled": thermal_state.get("enabled", True),
        "stop": bool(thermal_state.get("stop")),
        "reason": thermal_state.get("reason", ""),
        "max_temperature_celsius": max_temperature,
        "readings": readings,
    }


def udm_decision_summary(udm_client, now, error=None):
    latest_stats_at = getattr(udm_client, "latest_stats_at", None) if udm_client else None
    age_seconds = None
    if latest_stats_at:
        age_seconds = max(0, int((now - latest_stats_at).total_seconds()))
    return {
        "available": error is None,
        "error": str(error) if error else "",
        "latest_stats_at": format_utc(latest_stats_at) if latest_stats_at else None,
        "stats_age_seconds": age_seconds,
    }


def decision_base_context(decision_context, client, storage_state=None):
    context = dict(decision_context or {})
    if storage_state is not None:
        context["storage"] = storage_decision_summary(storage_state)
    return context


def normalized_set(items):
    return {
        item.strip().lower()
        for item in items
        if item.strip()
    }


def normalize_tv_sort_text(value):
    text = str(value or "")
    text = re.sub(r"(?i)\b(?:www\.)?uindex\.org\b", " ", text)
    text = re.sub(r"[\[\]\(\)]", " ", text)
    text = re.sub(r"[._/\\-]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def normalize_download_id(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").strip().lower())


def torrent_download_ids(torrent):
    ids = set()
    for key in ("hash", "infohash_v1", "infohash_v2"):
        value = normalize_download_id(torrent.get(key))
        if value:
            ids.add(value)
    return ids


def parse_tv_episode_order(value):
    text = str(value or "")
    for pattern in TV_EPISODE_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        return {
            "series": normalize_tv_sort_text(text[:match.start()]),
            "season": int(match.group("season")),
            "episode": int(match.group("episode")),
            "season_pack": False,
        }

    match = TV_SEASON_PATTERN.search(text) or TV_SEASON_WORD_PATTERN.search(text)
    if match:
        return {
            "series": normalize_tv_sort_text(text[:match.start()]),
            "season": int(match.group("season")),
            "episode": 0,
            "season_pack": True,
        }

    return None


def queue_record_values(record, keys):
    values = []
    for key in keys:
        value = record.get(key)
        if value:
            values.append(value)

    tracked_download = record.get("trackedDownload")
    if isinstance(tracked_download, dict):
        download_item = tracked_download.get("downloadItem")
        if isinstance(download_item, dict):
            for key in keys:
                value = download_item.get(key)
                if value:
                    values.append(value)
    return values


def queue_record_download_ids(record):
    values = queue_record_values(
        record,
        ("downloadId", "download_id", "hash", "infoHash", "torrentHash"),
    )
    return {
        normalize_download_id(value)
        for value in values
        if normalize_download_id(value)
    }


def queue_record_titles(record):
    titles = []
    for key in ("title", "sourceTitle", "downloadClient", "downloadClientId"):
        value = record.get(key)
        if value:
            titles.append(str(value))

    tracked_download = record.get("trackedDownload")
    if isinstance(tracked_download, dict):
        for key in ("title", "sourceTitle"):
            value = tracked_download.get(key)
            if value:
                titles.append(str(value))
    return titles


def queue_record_movie_titles(record):
    titles = queue_record_titles(record)

    movie = record.get("movie")
    if isinstance(movie, dict):
        for key in ("sortTitle", "title", "originalTitle", "cleanTitle", "titleSlug"):
            value = movie.get(key)
            if value:
                titles.append(str(value))

        alternate_titles = movie.get("alternateTitles")
        if isinstance(alternate_titles, list):
            for item in alternate_titles:
                if not isinstance(item, dict):
                    continue
                for key in ("title", "sourceTitle"):
                    value = item.get(key)
                    if value:
                        titles.append(str(value))

    parsed = record.get("parsedMovieInfo")
    if isinstance(parsed, dict):
        raw_titles = parsed.get("movieTitles")
        if isinstance(raw_titles, list):
            titles.extend(str(item) for item in raw_titles if item)
        for key in ("primaryMovieTitle", "originalTitle", "releaseTitle", "simpleReleaseTitle"):
            value = parsed.get(key)
            if value:
                titles.append(str(value))

    return titles


def queue_record_series_title(record):
    series = record.get("series")
    if isinstance(series, dict):
        for key in ("sortTitle", "title"):
            value = series.get(key)
            if value:
                return str(value)

    episode_order = parse_tv_episode_order(record.get("sourceTitle") or record.get("title") or "")
    if episode_order:
        return episode_order["series"]
    return ""


def queue_record_movie_title(record):
    movie = record.get("movie")
    if isinstance(movie, dict):
        for key in ("sortTitle", "title", "originalTitle", "cleanTitle"):
            value = movie.get(key)
            if value:
                return str(value)

    parsed = record.get("parsedMovieInfo")
    if isinstance(parsed, dict):
        raw_titles = parsed.get("movieTitles")
        if isinstance(raw_titles, list):
            for value in raw_titles:
                if value:
                    return str(value)
        for key in ("primaryMovieTitle", "originalTitle", "simpleReleaseTitle"):
            value = parsed.get(key)
            if value:
                return str(value)

    for key in ("sourceTitle", "title"):
        value = record.get(key)
        if value:
            return str(value)

    return ""


def queue_record_episode_order(record):
    season = None
    episodes = []

    episode = record.get("episode")
    if isinstance(episode, dict):
        episodes.append(episode)
    raw_episodes = record.get("episodes")
    if isinstance(raw_episodes, list):
        episodes.extend(item for item in raw_episodes if isinstance(item, dict))

    for item in episodes:
        try:
            item_season = int(item.get("seasonNumber"))
            item_episode = int(item.get("episodeNumber"))
        except (TypeError, ValueError):
            continue
        if season is None or (item_season, item_episode) < (season, episode_number):
            season = item_season
            episode_number = item_episode

    if season is not None:
        return season, episode_number, len(episodes) > 1

    parsed = parse_tv_episode_order(record.get("sourceTitle") or record.get("title") or "")
    if parsed:
        return parsed["season"], parsed["episode"], parsed["season_pack"]
    return None


class SonarrQueueMetadata:
    def __init__(self):
        self.enabled = env_bool("QBT_TV_QUEUE_SONARR_ENABLED", True)
        self.timeout = env_int("QBT_TV_QUEUE_TIMEOUT", 10)
        self.by_download_id = {}
        self.by_title = {}
        if self.enabled:
            self.load()

    def configs(self):
        api_key = (
            os.environ.get("QBT_TV_QUEUE_SONARR_API_KEY")
            or os.environ.get("SONARR_API_KEY")
            or ""
        ).strip()
        urls = [
            url.rstrip("/")
            for url in split_lines_or_csv(
                first_env(["QBT_TV_QUEUE_SONARR_URLS", "SONARR_URLS", "SONARR_URL"])
            )
            if url.rstrip("/")
        ]
        if not api_key or not urls:
            return []
        return [("sonarr", url, api_key) for url in urls]

    def load(self):
        configs = self.configs()
        if not configs:
            log_debug("Sonarr queue enrichment disabled; API key and URL(s) are not both set")
            return

        for label, base_url, api_key in configs:
            try:
                self.load_queue(label, base_url, api_key)
            except ApiError as exc:
                log_warning(f"Failed to read {label} queue from {base_url}: {exc}")

    def load_queue(self, label, base_url, api_key):
        opener = urllib.request.build_opener()
        params = urllib.parse.urlencode({
            "page": "1",
            "pageSize": os.environ.get("QBT_TV_QUEUE_PAGE_SIZE", "1000"),
            "includeUnknownSeriesItems": "true",
            "includeSeries": "true",
            "includeEpisode": "true",
        })
        url = join_url(base_url, "/api/v3/queue") + "?" + params
        data, _ = request_json(
            opener,
            "GET",
            url,
            headers={"Accept": "application/json", "X-Api-Key": api_key},
            timeout=self.timeout,
        )
        records = response_rows(data, f"{label} queue", key="records")

        loaded = 0
        for position, record in enumerate(records):
            if not isinstance(record, dict):
                continue
            metadata = self.record_metadata(record, position, label)
            if not metadata:
                continue
            for download_id in queue_record_download_ids(record):
                self.by_download_id[download_id] = metadata
            for title in queue_record_titles(record):
                normalized_title = normalize_tv_sort_text(title)
                if normalized_title:
                    self.by_title[normalized_title] = metadata
            loaded += 1
        log_debug(f"Loaded {loaded} {label} queue record(s) for TV ordering")

    def record_metadata(self, record, position, source):
        series = normalize_tv_sort_text(queue_record_series_title(record))
        episode_order = queue_record_episode_order(record)
        if not series or not episode_order:
            return None

        season, episode, season_pack = episode_order
        return {
            "series": series,
            "season": season,
            "episode": episode,
            "season_pack": season_pack,
            "queue_position": position,
            "source": source,
        }

    def torrent_metadata(self, torrent):
        for download_id in torrent_download_ids(torrent):
            metadata = self.by_download_id.get(download_id)
            if metadata:
                return metadata

        normalized_name = normalize_tv_sort_text(torrent_name(torrent))
        metadata = self.by_title.get(normalized_name)
        if metadata:
            return metadata
        return None


def jellyfin_episode_watch_order(item):
    if not isinstance(item, dict):
        return None
    if item.get("Type") and str(item.get("Type")).lower() != "episode":
        return None

    series = (
        item.get("SeriesName")
        or item.get("Series")
        or item.get("SeriesTitle")
        or ""
    )
    season = int_or_none(
        item.get("ParentIndexNumber")
        or item.get("SeasonNumber")
        or item.get("Season")
    )
    episode = int_or_none(
        item.get("IndexNumber")
        or item.get("EpisodeNumber")
        or item.get("Episode")
    )
    series = normalize_tv_sort_text(series)
    if not series or season is None:
        return None
    return {
        "series": series,
        "season": season,
        "episode": episode or 0,
    }


class JellyfinWatchMetadata:
    def __init__(self):
        self.enabled = env_bool("QBT_TV_WATCH_JELLYFIN_ENABLED", True)
        self.timeout = env_int(
            "QBT_TV_WATCH_TIMEOUT",
            env_int("QBT_ARR_QUEUE_TIMEOUT", env_int("QBT_TV_QUEUE_TIMEOUT", 10)),
        )
        self.active_within_seconds = max(
            1,
            env_int("QBT_TV_WATCH_ACTIVE_WITHIN_SECONDS", 7200),
        )
        self.by_series_season = {}
        if self.enabled:
            self.load()

    def configs(self):
        api_key = (
            os.environ.get("QBT_TV_WATCH_JELLYFIN_API_KEY")
            or os.environ.get("JELLYFIN_API_KEY")
            or ""
        ).strip()
        urls = [
            url.rstrip("/")
            for url in split_lines_or_csv(
                first_env(["QBT_TV_WATCH_JELLYFIN_URLS", "JELLYFIN_URLS", "JELLYFIN_URL"])
            )
            if url.rstrip("/")
        ]
        if not api_key or not urls:
            return []
        return [("jellyfin", url, api_key) for url in urls]

    def load(self):
        configs = self.configs()
        if not configs:
            log_debug("Jellyfin watch enrichment disabled; API key and URL(s) are not both set")
            return

        for label, base_url, api_key in configs:
            try:
                self.load_sessions(label, base_url, api_key)
            except ApiError as exc:
                log_warning(f"Failed to read {label} active sessions: {exc}")

    def load_sessions(self, label, base_url, api_key):
        opener = urllib.request.build_opener()
        params = urllib.parse.urlencode({
            "ActiveWithinSeconds": str(self.active_within_seconds),
        })
        url = join_url(base_url, "/Sessions") + "?" + params
        try:
            data, _ = request_json(
                opener,
                "GET",
                url,
                headers={"Accept": "application/json", "X-Emby-Token": api_key},
                timeout=self.timeout,
            )
        except ApiError as exc:
            raise ApiError("GET /Sessions failed") from exc
        sessions = response_rows(data, f"{label} sessions")

        loaded = 0
        for position, session in enumerate(sessions):
            if not isinstance(session, dict):
                continue
            item = session.get("NowPlayingItem")
            watch_order = jellyfin_episode_watch_order(item)
            if not watch_order:
                continue
            activity_at = (
                parse_utc(session.get("LastActivityDate"))
                or parse_utc(session.get("LastPlaybackCheckIn"))
                or parse_utc((item.get("UserData") or {}).get("LastPlayedDate") if isinstance(item, dict) else None)
            )
            self.record_watch(
                watch_order["series"],
                watch_order["season"],
                watch_order["episode"],
                position,
                activity_at,
                "jellyfin-active-session",
            )
            loaded += 1
        log_debug(f"Loaded {loaded} active {label} TV watch session(s)")

    def record_watch(self, series, season, episode, position, activity_at, source):
        key = (series, int(season))
        rank = (
            0 if activity_at else 1,
            -int(activity_at.timestamp()) if activity_at else 0,
            int(position),
        )
        existing = self.by_series_season.get(key)
        if existing and existing["rank"] <= rank:
            return
        self.by_series_season[key] = {
            "series": series,
            "season": int(season),
            "episode": int(episode or 0),
            "position": int(position),
            "activity_at": format_utc(activity_at) if activity_at else None,
            "source": source,
            "rank": rank,
        }

    def torrent_watch_priority(self, torrent, order):
        if not self.enabled or not order:
            return None
        single_episode_order = tv_order_single_episode_torrent_order(torrent, order)
        if not single_episode_order:
            return None

        series = order.get("series")
        season, candidate_episode = single_episode_order
        if not series:
            return None
        watch = self.by_series_season.get((series, season))
        if not watch:
            return None
        watched_episode = int(watch.get("episode") or 0)
        if candidate_episode <= watched_episode:
            return None

        return {
            "series": watch["series"],
            "season": watch["season"],
            "episode": watch["episode"],
            "next_episode": watch["episode"] + 1,
            "target_episode": candidate_episode,
            "activity_at": watch.get("activity_at"),
            "source": watch.get("source", ""),
            "rank": watch["rank"],
        }


class RadarrQueueMetadata:
    def __init__(self):
        self.enabled = env_bool("QBT_MOVIE_QUEUE_RADARR_ENABLED", True)
        self.timeout = env_int(
            "QBT_MOVIE_QUEUE_TIMEOUT",
            env_int("QBT_ARR_QUEUE_TIMEOUT", env_int("QBT_TV_QUEUE_TIMEOUT", 10)),
        )
        self.by_download_id = {}
        self.by_title = {}
        if self.enabled:
            self.load()

    def configs(self):
        api_key = (
            os.environ.get("QBT_MOVIE_QUEUE_RADARR_API_KEY")
            or os.environ.get("RADARR_API_KEY")
            or ""
        ).strip()
        urls = [
            url.rstrip("/")
            for url in split_lines_or_csv(
                first_env(["QBT_MOVIE_QUEUE_RADARR_URLS", "RADARR_URLS", "RADARR_URL"])
            )
            if url.rstrip("/")
        ]
        if not api_key or not urls:
            return []
        return [("radarr", url, api_key) for url in urls]

    def load(self):
        configs = self.configs()
        if not configs:
            log_debug("Radarr queue enrichment disabled; API key and URL(s) are not both set")
            return

        for label, base_url, api_key in configs:
            try:
                self.load_queue(label, base_url, api_key)
            except ApiError as exc:
                log_warning(f"Failed to read {label} queue from {base_url}: {exc}")

    def load_queue(self, label, base_url, api_key):
        opener = urllib.request.build_opener()
        params = urllib.parse.urlencode({
            "page": "1",
            "pageSize": (
                os.environ.get("QBT_MOVIE_QUEUE_PAGE_SIZE")
                or os.environ.get("QBT_ARR_QUEUE_PAGE_SIZE")
                or os.environ.get("QBT_TV_QUEUE_PAGE_SIZE", "1000")
            ),
            "includeUnknownMovieItems": "true",
            "includeMovie": "true",
        })
        url = join_url(base_url, "/api/v3/queue") + "?" + params
        data, _ = request_json(
            opener,
            "GET",
            url,
            headers={"Accept": "application/json", "X-Api-Key": api_key},
            timeout=self.timeout,
        )
        records = response_rows(data, f"{label} queue", key="records")

        loaded = 0
        for position, record in enumerate(records):
            if not isinstance(record, dict):
                continue
            metadata = self.record_metadata(record, position, label)
            if not metadata:
                continue
            for download_id in queue_record_download_ids(record):
                self.by_download_id[download_id] = metadata
            for title in queue_record_movie_titles(record):
                normalized_title = normalize_tv_sort_text(title)
                if normalized_title:
                    self.by_title[normalized_title] = metadata
            loaded += 1
        log_debug(f"Loaded {loaded} {label} queue record(s) for movie ordering")

    def record_metadata(self, record, position, source):
        title = normalize_tv_sort_text(queue_record_movie_title(record))
        if not title:
            return None

        movie = record.get("movie") if isinstance(record.get("movie"), dict) else {}
        movie_id = int_or_none(record.get("movieId") or movie.get("id"))
        year = int_or_none(movie.get("year"))
        return {
            "title": title,
            "movie_id": movie_id,
            "year": year,
            "queue_position": position,
            "source": source,
        }

    def torrent_metadata(self, torrent):
        for download_id in torrent_download_ids(torrent):
            metadata = self.by_download_id.get(download_id)
            if metadata:
                return metadata

        normalized_name = normalize_tv_sort_text(torrent_name(torrent))
        metadata = self.by_title.get(normalized_name)
        if metadata:
            return metadata
        return None


def tv_torrent_order(torrent, tv_order_categories, sonarr_queue):
    category = torrent_category(torrent).lower()
    if category not in tv_order_categories:
        return None

    metadata = sonarr_queue.torrent_metadata(torrent) if sonarr_queue else None
    if metadata:
        return dict(metadata)

    episode_order = parse_tv_episode_order(torrent_name(torrent))
    if not episode_order:
        return None

    series = episode_order["series"] or normalize_tv_sort_text(torrent_name(torrent))
    return {
        "series": series,
        "season": episode_order["season"],
        "episode": episode_order["episode"],
        "season_pack": episode_order["season_pack"],
        "queue_position": 999999,
        "source": "torrent-name",
    }


def tv_order_is_full_season_pack(torrent, order):
    if not order or not order.get("season_pack"):
        return False
    parsed = parse_tv_episode_order(torrent_name(torrent))
    if parsed and parsed.get("season_pack"):
        return True
    return int_or_none(order.get("episode")) == 0


def tv_order_single_episode_torrent_order(torrent, order):
    if not order:
        return None
    parsed = parse_tv_episode_order(torrent_name(torrent))
    if parsed:
        episode = int_or_none(parsed.get("episode")) or 0
        season = int_or_none(parsed.get("season"))
        if not parsed.get("season_pack") and season is not None and episode > 0:
            return season, episode
        return None

    episode = int_or_none(order.get("episode")) or 0
    season = int_or_none(order.get("season"))
    if not order.get("season_pack") and season is not None and episode > 0:
        return season, episode
    return None


def tv_order_is_single_episode_torrent(torrent, order):
    return tv_order_single_episode_torrent_order(torrent, order) is not None


def tv_order_sequence(order):
    return (
        int(order.get("season") or 0),
        int(order.get("episode") or 0),
    )


def tv_order_label(order):
    season, episode = tv_order_sequence(order)
    if order.get("season_pack") or episode <= 0:
        return f"S{season:02d}"
    return f"S{season:02d}E{episode:02d}"


def tv_order_is_incomplete(torrent):
    return torrent_progress(torrent) < 1.0


def build_tv_order_state(torrents, tv_order_categories, sonarr_queue, watch_metadata=None):
    orders = {}
    series_ranks = {}
    series_heads = {}
    watch_priorities = {}

    for torrent in torrents:
        item_hash = torrent_hash(torrent)
        order = tv_torrent_order(torrent, tv_order_categories, sonarr_queue)
        if not item_hash or not order:
            continue
        orders[item_hash] = order
        watch_priority = (
            watch_metadata.torrent_watch_priority(torrent, order)
            if watch_metadata
            else None
        )
        if watch_priority:
            watch_priorities[item_hash] = watch_priority

        series = order["series"]
        rank = (
            int(order.get("queue_position", 999999)),
            int(order["season"]),
            int(order["episode"]),
            series,
        )
        if series not in series_ranks or rank < series_ranks[series]:
            series_ranks[series] = rank
        head_rank = (
            tv_order_sequence(order),
            int(order.get("queue_position", 999999)),
            torrent_name(torrent).lower(),
            item_hash,
        )
        if tv_order_is_incomplete(torrent) and (
            series not in series_heads or head_rank < series_heads[series]["rank"]
        ):
            series_heads[series] = {
                "hash": item_hash,
                "name": torrent_name(torrent),
                "order": order,
                "rank": head_rank,
            }

    return {
        "orders": orders,
        "series_ranks": series_ranks,
        "series_heads": series_heads,
        "watch_priorities": watch_priorities,
    }


def tv_queue_order_block_reason(torrent, tv_order_categories, tv_order_state):
    category = torrent_category(torrent).lower()
    if category not in tv_order_categories:
        return ""

    item_hash = torrent_hash(torrent)
    order = tv_order_state.get("orders", {}).get(item_hash)
    if not order:
        return ""

    head = tv_order_state.get("series_heads", {}).get(order["series"])
    if not head or head.get("hash") == item_hash:
        return ""

    head_order = head.get("order") or {}
    if tv_order_sequence(order) <= tv_order_sequence(head_order):
        return ""

    return (
        f"waiting for older queued TV item in {order['series']}: "
        f"{tv_order_label(head_order)} {head.get('name') or head.get('hash')} "
        f"before {tv_order_label(order)}"
    )


def tv_episode_order_key(torrent, tv_order_categories, tv_order_state):
    category = torrent_category(torrent).lower()
    if category not in tv_order_categories:
        return (2, (999999, 9999, 9999, ""), 9999, 9999, torrent_name(torrent).lower())

    order = tv_order_state.get("orders", {}).get(torrent_hash(torrent))
    if not order:
        return (1, (999999, 9999, 9999, normalize_tv_sort_text(torrent_name(torrent))), 9999, 9999, torrent_name(torrent).lower())

    series_rank = tv_order_state.get("series_ranks", {}).get(
        order["series"],
        (999999, int(order["season"]), int(order["episode"]), order["series"]),
    )
    watch_priority = tv_order_state.get("watch_priorities", {}).get(torrent_hash(torrent))
    return (
        0,
        0 if watch_priority else 1,
        watch_priority.get("rank", (999999, 999999, 999999)) if watch_priority else (),
        series_rank,
        int(order["season"]),
        int(order["episode"]),
        torrent_name(torrent).lower(),
    )


def movie_torrent_order(torrent, movie_order_categories, radarr_queue):
    category = torrent_category(torrent).lower()
    if category not in movie_order_categories:
        return None

    metadata = radarr_queue.torrent_metadata(torrent) if radarr_queue else None
    if metadata:
        return dict(metadata)

    return {
        "title": normalize_tv_sort_text(torrent_name(torrent)),
        "movie_id": None,
        "year": None,
        "queue_position": 999999,
        "source": "torrent-name",
    }


def build_movie_order_state(torrents, movie_order_categories, radarr_queue):
    orders = {}

    for torrent in torrents:
        item_hash = torrent_hash(torrent)
        order = movie_torrent_order(torrent, movie_order_categories, radarr_queue)
        if not item_hash or not order:
            continue
        orders[item_hash] = order

    return {"orders": orders}


def movie_queue_order_key(torrent, movie_order_categories, movie_order_state):
    category = torrent_category(torrent).lower()
    if category not in movie_order_categories:
        return (2, 999999, "", torrent_name(torrent).lower())

    order = movie_order_state.get("orders", {}).get(torrent_hash(torrent))
    if not order:
        return (1, 999999, normalize_tv_sort_text(torrent_name(torrent)), torrent_name(torrent).lower())

    return (
        0,
        int(order.get("queue_position", 999999)),
        int(order.get("year") or 0),
        order.get("title", ""),
        torrent_name(torrent).lower(),
    )


def media_queue_order_key(
    torrent,
    tv_order_categories,
    tv_order_state,
    movie_order_categories,
    movie_order_state,
):
    category = torrent_category(torrent).lower()
    if category in tv_order_categories:
        return (0, tv_episode_order_key(torrent, tv_order_categories, tv_order_state))
    if category in movie_order_categories:
        return (1, movie_queue_order_key(torrent, movie_order_categories, movie_order_state))
    return (2, torrent_name(torrent).lower())


def file_path(file_item):
    return str(file_item.get("name") or file_item.get("path") or "")


def file_index(file_item, fallback):
    for key in ("index", "id"):
        try:
            return int(file_item[key])
        except (KeyError, TypeError, ValueError):
            pass
    return fallback


def file_priority(file_item):
    try:
        return int(file_item.get("priority"))
    except (TypeError, ValueError):
        return QBT_FILE_PRIORITY_NORMAL


def file_size(file_item):
    for key in ("size", "total_size"):
        try:
            return max(0, int(file_item.get(key) or 0))
        except (TypeError, ValueError):
            continue
    return 0


def file_progress(file_item):
    try:
        return max(0.0, min(1.0, float(file_item.get("progress") or 0)))
    except (TypeError, ValueError):
        return 0.0


def selected_file_remaining_state(files):
    if not files:
        return None

    selected_count = 0
    selected_size = 0
    remaining_bytes = 0
    for file_item in files:
        if file_priority(file_item) <= 0:
            continue
        size = file_size(file_item)
        selected_count += 1
        selected_size += size
        remaining_bytes += math.ceil(size * (1.0 - file_progress(file_item)))

    if selected_count <= 0:
        return {
            "remaining_bytes": 0,
            "selected_count": 0,
            "selected_size": 0,
            "present_bytes": 0,
        }

    return {
        "remaining_bytes": max(0, int(remaining_bytes)),
        "selected_count": selected_count,
        "selected_size": selected_size,
        "present_bytes": max(0, selected_size - remaining_bytes),
    }


def is_media_file(file_item):
    path = file_path(file_item).lower()
    return any(path.endswith(extension) for extension in MEDIA_FILE_EXTENSIONS)


def apply_tv_episode_file_priorities(
    client,
    torrent,
    tv_order_categories,
    enabled,
    lookahead_episodes,
    watch_priority=None,
):
    item_hash = torrent_hash(torrent)
    if not enabled or not item_hash or torrent_category(torrent).lower() not in tv_order_categories:
        return

    try:
        files = client.torrent_files(item_hash)
    except ApiError as exc:
        log_warning(
            f"Failed to read qBittorrent file list for TV ordering "
            f"{torrent_name(torrent)}; {exc}",
        )
        return

    episode_files = []
    for fallback, file_item in enumerate(files):
        if not is_media_file(file_item) or file_priority(file_item) <= 0:
            continue
        episode_order = parse_tv_episode_order(file_path(file_item))
        if not episode_order:
            continue
        episode_files.append((
            (
                episode_order["season"],
                episode_order["episode"],
                normalize_tv_sort_text(file_path(file_item)),
            ),
            file_index(file_item, fallback),
            file_item,
        ))

    incomplete_episode_files = [
        item for item in episode_files
        if file_progress(item[2]) < 1.0
    ]
    if not incomplete_episode_files:
        return

    incomplete_episode_files.sort(key=lambda item: item[0])
    ordered_episode_keys = []
    for episode_key, _, _ in incomplete_episode_files:
        short_key = episode_key[:2]
        if short_key not in ordered_episode_keys:
            ordered_episode_keys.append(short_key)

    high_candidate_keys = ordered_episode_keys
    if watch_priority:
        watched_season = int_or_none(watch_priority.get("season"))
        watched_episode = int_or_none(watch_priority.get("episode")) or 0
        watch_ordered_keys = [
            episode_key for episode_key in ordered_episode_keys
            if episode_key[0] == watched_season and episode_key[1] > watched_episode
        ]
        if watch_ordered_keys:
            high_candidate_keys = watch_ordered_keys
            ordered_episode_keys = watch_ordered_keys + [
                episode_key for episode_key in ordered_episode_keys
                if episode_key not in watch_ordered_keys
            ]

    maximum_keys = set(ordered_episode_keys[:1])
    high_keys = set(high_candidate_keys[1:1 + max(0, lookahead_episodes)])
    maximum_ids = [
        file_id for episode_key, file_id, _ in episode_files
        if episode_key[:2] in maximum_keys
    ]
    high_ids = [
        file_id for episode_key, file_id, _ in episode_files
        if episode_key[:2] in high_keys
    ]
    raised_ids = set(maximum_ids + high_ids)
    normal_ids = [
        file_id for _, file_id, file_item in episode_files
        if file_id not in raised_ids and file_priority(file_item) != QBT_FILE_PRIORITY_NORMAL
    ]

    if normal_ids:
        client.set_file_priority(item_hash, normal_ids, QBT_FILE_PRIORITY_NORMAL)
    if high_ids:
        client.set_file_priority(item_hash, high_ids, QBT_FILE_PRIORITY_HIGH)
    if maximum_ids:
        client.set_file_priority(item_hash, maximum_ids, QBT_FILE_PRIORITY_MAXIMUM)

    season, episode = ordered_episode_keys[0]
    if watch_priority:
        watched_episode = int(watch_priority.get("episode") or 0)
        log_debug(
            f"Prioritized watched TV episode files: "
            f"{torrent_name(torrent)} S{season:02d}E{episode:02d} first "
            f"after watched S{int(watch_priority['season']):02d}E{watched_episode:02d}"
        )
    else:
        log_debug(
            f"Prioritized TV episode files: "
            f"{torrent_name(torrent)} S{season:02d}E{episode:02d} first"
        )


def torrent_priority_reason(torrent, priority_tags, priority_categories):
    matching_tags = sorted(
        tag
        for tag in torrent_tags(torrent)
        if tag.lower() in priority_tags
    )
    if matching_tags:
        return "tag " + ",".join(matching_tags)

    category = torrent_category(torrent)
    if category and category.lower() in priority_categories:
        return "category " + category

    return ""


def priority_log_suffix(torrent, priority_tags, priority_categories):
    reason = torrent_priority_reason(torrent, priority_tags, priority_categories)
    if reason:
        return f"; priority via {reason}"
    return ""


def watch_priority_log_suffix(torrent, tv_order_state):
    watch_priority = tv_order_state.get("watch_priorities", {}).get(torrent_hash(torrent))
    if not watch_priority:
        return ""
    target_episode = int(
        watch_priority.get("target_episode")
        or watch_priority.get("next_episode")
        or 0
    )
    return (
        f"; watched TV target S{int(watch_priority['season']):02d}"
        f"E{target_episode:02d}"
    )


def cleanup_qbt_client(client):
    delete_files = env_bool("QBT_DELETE_FILES", True)
    error_torrents = error_state_torrents(client)
    to_delete = [
        torrent for torrent in error_torrents
        if torrent_state(torrent) == "missingFiles" and torrent_hash(torrent)
    ]
    to_start = [
        torrent for torrent in error_torrents
        if torrent_state(torrent) != "missingFiles" and torrent_hash(torrent)
    ]

    if to_delete:
        log_info(f"Deleting {len(to_delete)} missing-files torrent(s):")
        for torrent in to_delete:
            log_info(f"- {torrent_name(torrent)}")
        client.delete_hashes([torrent_hash(torrent) for torrent in to_delete], delete_files)
    else:
        log_debug(f"No missing-files torrents need cleanup")

    if to_start:
        log_debug(
            f"Leaving {len(to_start)} recoverable errored torrent(s) for "
            f"the single-download selector"
        )


def cleanup_qbt_clients(clients):
    for client in clients:
        cleanup_qbt_client(client)
        cleanup_stall_tags(client)


def is_single_download_candidate(torrent, min_progress, max_remaining_bytes, categories):
    return single_download_reject_reason(torrent, min_progress, max_remaining_bytes, categories) == ""


def single_download_reject_reason(torrent, min_progress, max_remaining_bytes, categories):
    if not torrent_hash(torrent):
        return "missing_hash"
    if categories and torrent_category(torrent) not in categories:
        return "category_not_allowed"
    progress = torrent_progress(torrent)
    if progress < min_progress:
        return "below_min_progress"
    if progress >= 1.0:
        return "complete"
    if max_remaining_bytes > 0 and torrent_amount_left(torrent) > max_remaining_bytes:
        return "too_much_remaining"
    return ""


def is_stopped_torrent(torrent):
    state = torrent_state(torrent).lower()
    return state.startswith("stopped") or state.startswith("paused")


def is_stalled_torrent(torrent):
    return torrent_state(torrent) in {"metaDL", "stalledDL"} and torrent_download_speed(torrent) <= 0


def is_running_torrent(torrent):
    if is_stopped_torrent(torrent):
        return False
    return torrent_state(torrent) in {
        "allocating",
        "checkingDL",
        "downloading",
        "forcedDL",
        "metaDL",
        "stalledDL",
        "queuedDL",
    }


def is_productive_torrent(torrent):
    if is_stopped_torrent(torrent) or is_stalled_torrent(torrent):
        return False
    return torrent_download_speed(torrent) > 0


def torrent_progress_reason(before, after, min_download_delta_bytes):
    before_left = torrent_amount_left(before)
    after_left = torrent_amount_left(after)
    left_delta = before_left - after_left
    if left_delta > 0:
        return f"amount left decreased by {human_size(left_delta)}"

    before_downloaded = torrent_downloaded_bytes(before)
    after_downloaded = torrent_downloaded_bytes(after)
    if before_downloaded is not None and after_downloaded is not None:
        downloaded_delta = after_downloaded - before_downloaded
        if downloaded_delta >= max(1, int(min_download_delta_bytes)):
            return f"downloaded bytes increased by {human_size(downloaded_delta)}"

    before_speed = torrent_download_speed(before)
    after_speed = torrent_download_speed(after)
    if before_speed > 0 and after_speed > 0:
        return (
            f"download speed remained nonzero "
            f"({human_rate(before_speed)} -> {human_rate(after_speed)})"
        )

    return ""


def torrent_progress_delta_bytes(before, after):
    before_left = torrent_amount_left(before)
    after_left = torrent_amount_left(after)
    left_delta = before_left - after_left

    downloaded_delta = 0
    before_downloaded = torrent_downloaded_bytes(before)
    after_downloaded = torrent_downloaded_bytes(after)
    if before_downloaded is not None and after_downloaded is not None:
        downloaded_delta = after_downloaded - before_downloaded

    return max(0, left_delta, downloaded_delta)


def slow_torrent_reason(torrent, allowed_download_limit, min_rate_fraction, min_rate_bytes, max_eta_seconds):
    if not is_productive_torrent(torrent):
        return ""

    speed = torrent_download_speed(torrent)
    if speed <= 0:
        return ""

    eta = torrent_eta_seconds(torrent)
    if eta is None or eta < max_eta_seconds:
        return ""

    rate_floor = max(1, math.floor(max(1, allowed_download_limit) * min_rate_fraction))
    rate_floor = max(rate_floor, max(1, int(min_rate_bytes)))
    if speed >= rate_floor:
        return ""

    return (
        f"download speed {human_rate(speed)} is below "
        f"{human_rate(rate_floor)} minimum for allowed rate "
        f"{human_rate(allowed_download_limit)}, ETA {human_duration(eta)}"
    )


def candidate_health_class(torrent, healthy_min_seeds, healthy_min_availability):
    reported_sources = max(torrent_connected_seeds(torrent), torrent_reported_seeds(torrent))
    availability = torrent_availability(torrent)
    if availability >= healthy_min_availability or reported_sources >= healthy_min_seeds:
        return 0
    if availability >= 1.0 or reported_sources > 0:
        return 1
    return 2


class TorrentHealthStore:
    def __init__(self):
        self.enabled = env_bool("QBT_TORRENT_HEALTH_SCORING_ENABLED", True)
        self.path = os.environ.get("QBT_TORRENT_HEALTH_STATE_PATH", "/state/torrent-health.json").strip()
        self.ewma_alpha = max(0.01, min(1.0, env_float("QBT_TORRENT_HEALTH_EWMA_ALPHA", 0.35)))
        self.stale_days = max(1, env_int("QBT_TORRENT_HEALTH_STALE_DAYS", 90))
        self.data = {"version": 1, "torrents": {}}
        self.dirty = False
        if self.enabled:
            self.load()

    def load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError) as exc:
            log_warning(f"Failed to read qBittorrent health state at {self.path}: {exc}")
            return

        torrents = data.get("torrents")
        if isinstance(torrents, dict):
            self.data = {"version": 1, "torrents": torrents}

    def save(self):
        if not self.enabled or not self.dirty:
            return

        try:
            directory = os.path.dirname(self.path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            payload = dict(self.data)
            payload["updated_at"] = format_utc(datetime.now(timezone.utc))
            tmp_path = f"{self.path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(tmp_path, self.path)
            self.dirty = False
        except OSError as exc:
            log_warning(f"Failed to write qBittorrent health state at {self.path}: {exc}")

    def entry(self, item_hash):
        if not self.enabled or not item_hash:
            return None
        torrents = self.data.setdefault("torrents", {})
        return torrents.setdefault(item_hash, {})

    def observe_torrents(self, torrents, now):
        if not self.enabled:
            return

        seen_hashes = set()
        for torrent in torrents:
            item_hash = torrent_hash(torrent)
            if not item_hash:
                continue
            seen_hashes.add(item_hash)
            entry = self.entry(item_hash)
            entry["name"] = torrent_name(torrent)
            entry["category"] = torrent_category(torrent)
            entry.setdefault("first_seen_at", format_utc(now))
            entry["last_seen_at"] = format_utc(now)
            entry["last_speed_bytes_per_sec"] = torrent_download_speed(torrent)
            entry["last_amount_left_bytes"] = torrent_amount_left(torrent)
            entry["last_progress"] = torrent_progress(torrent)
            entry["last_connected_seeds"] = torrent_connected_seeds(torrent)
            entry["last_reported_seeds"] = torrent_reported_seeds(torrent)
            entry["last_availability"] = torrent_availability(torrent)
            entry["predicted_completion_seconds"] = self.predicted_completion_seconds(torrent, entry)
            self.dirty = True

        self.prune_stale(now)
        self.save()

    def prune_stale(self, now):
        cutoff = now - timedelta(days=self.stale_days)
        torrents = self.data.setdefault("torrents", {})
        stale_hashes = []
        for item_hash, entry in torrents.items():
            last_seen = parse_utc(entry.get("last_seen_at"))
            if last_seen and last_seen < cutoff:
                stale_hashes.append(item_hash)
        for item_hash in stale_hashes:
            torrents.pop(item_hash, None)
        if stale_hashes:
            self.dirty = True
            log_info(f"Pruned {len(stale_hashes)} stale qBittorrent health record(s)")

    def predicted_completion_seconds(self, torrent, entry):
        amount_left = torrent_amount_left(torrent)
        if amount_left <= 0:
            return 0

        speed = max(
            torrent_download_speed(torrent),
            int(entry.get("ewma_speed_bytes_per_sec") or 0),
        )
        if speed <= 0:
            return None
        return math.ceil(amount_left / speed)

    def record_attempt(self, torrent, now):
        entry = self.entry(torrent_hash(torrent))
        if entry is None:
            return
        entry["name"] = torrent_name(torrent)
        entry["category"] = torrent_category(torrent)
        entry["last_attempt_at"] = format_utc(now)
        entry["attempts"] = int(entry.get("attempts") or 0) + 1
        self.dirty = True
        self.save()

    def record_productive(self, before, after, now, sample_seconds):
        entry = self.entry(torrent_hash(after) or torrent_hash(before))
        if entry is None:
            return

        sample_seconds = max(1, int(sample_seconds))
        delta_bytes = torrent_progress_delta_bytes(before, after)
        observed_speed = max(
            torrent_download_speed(after),
            math.floor(delta_bytes / sample_seconds),
        )
        if observed_speed > 0:
            previous_speed = float(entry.get("ewma_speed_bytes_per_sec") or 0)
            if previous_speed <= 0:
                ewma_speed = observed_speed
            else:
                ewma_speed = (self.ewma_alpha * observed_speed) + ((1.0 - self.ewma_alpha) * previous_speed)
            entry["ewma_speed_bytes_per_sec"] = max(1, int(ewma_speed))

        entry["last_productive_at"] = format_utc(now)
        entry["successful_attempts"] = int(entry.get("successful_attempts") or 0) + 1
        entry["consecutive_failures"] = 0
        entry["last_failure_reason"] = ""
        entry["last_amount_left_bytes"] = torrent_amount_left(after)
        entry["last_speed_bytes_per_sec"] = torrent_download_speed(after)
        entry["last_connected_seeds"] = torrent_connected_seeds(after)
        entry["last_reported_seeds"] = torrent_reported_seeds(after)
        entry["last_availability"] = torrent_availability(after)
        entry["predicted_completion_seconds"] = self.predicted_completion_seconds(after, entry)
        self.dirty = True
        self.save()

    def record_failure(self, torrent, now, reason):
        entry = self.entry(torrent_hash(torrent))
        if entry is None:
            return

        entry["name"] = torrent_name(torrent)
        entry["category"] = torrent_category(torrent)
        entry["last_failure_at"] = format_utc(now)
        entry["last_failure_reason"] = str(reason or "not productive")
        entry["failed_attempts"] = int(entry.get("failed_attempts") or 0) + 1
        entry["consecutive_failures"] = int(entry.get("consecutive_failures") or 0) + 1
        previous_speed = float(entry.get("ewma_speed_bytes_per_sec") or 0)
        if previous_speed > 0:
            entry["ewma_speed_bytes_per_sec"] = max(0, int(previous_speed * (1.0 - self.ewma_alpha)))
        entry["last_amount_left_bytes"] = torrent_amount_left(torrent)
        entry["last_speed_bytes_per_sec"] = torrent_download_speed(torrent)
        entry["last_connected_seeds"] = torrent_connected_seeds(torrent)
        entry["last_reported_seeds"] = torrent_reported_seeds(torrent)
        entry["last_availability"] = torrent_availability(torrent)
        entry["predicted_completion_seconds"] = self.predicted_completion_seconds(torrent, entry)
        self.dirty = True
        self.save()

    def score(self, torrent, now):
        if not self.enabled:
            return 0.0

        entry = self.entry(torrent_hash(torrent))
        if entry is None:
            return 0.0

        score = 0.0
        sources = max(
            torrent_connected_seeds(torrent),
            torrent_reported_seeds(torrent),
            int(entry.get("last_connected_seeds") or 0),
            int(entry.get("last_reported_seeds") or 0),
        )
        availability = max(torrent_availability(torrent), float(entry.get("last_availability") or 0.0))
        score += min(25.0, sources * 3.0)
        score += min(20.0, availability * 10.0)

        ewma_speed = float(entry.get("ewma_speed_bytes_per_sec") or 0.0)
        if ewma_speed > 0:
            score += min(35.0, math.log2((ewma_speed / 65_536.0) + 1.0) * 8.0)

        predicted = entry.get("predicted_completion_seconds")
        try:
            predicted = int(predicted)
        except (TypeError, ValueError):
            predicted = None
        if predicted is not None and predicted > 0:
            if predicted <= 86_400:
                score += 15.0
            elif predicted <= 172_800:
                score += 8.0
            elif predicted >= 604_800:
                score -= 8.0

        last_productive_at = parse_utc(entry.get("last_productive_at"))
        if last_productive_at:
            age_seconds = max(0, int((now - last_productive_at).total_seconds()))
            if age_seconds <= 86_400:
                score += 12.0
            elif age_seconds <= 7 * 86_400:
                score += 5.0

        consecutive_failures = int(entry.get("consecutive_failures") or 0)
        failed_attempts = int(entry.get("failed_attempts") or 0)
        score -= min(45.0, consecutive_failures * 14.0)
        score -= min(20.0, max(0, failed_attempts - int(entry.get("successful_attempts") or 0)) * 2.0)

        return max(-100.0, min(100.0, score))

    def summary(self, torrent, now):
        if not self.enabled:
            return ""
        entry = self.entry(torrent_hash(torrent))
        if entry is None:
            return ""

        score = self.score(torrent, now)
        speed = int(entry.get("ewma_speed_bytes_per_sec") or 0)
        failures = int(entry.get("consecutive_failures") or 0)
        predicted = entry.get("predicted_completion_seconds")
        predicted_text = "unknown"
        try:
            predicted_value = int(predicted)
            if predicted_value > 0:
                predicted_text = human_duration(predicted_value)
            elif predicted_value == 0:
                predicted_text = "done"
        except (TypeError, ValueError):
            pass

        return (
            f"; health score {score:.1f}, ewma {human_rate(speed)}, "
            f"ETA {predicted_text}, consecutive failures {failures}"
        )


def candidate_sort_key(
    torrent,
    priority_tags,
    priority_categories,
    tv_order_categories,
    tv_order_state,
    movie_order_categories,
    movie_order_state,
    healthy_min_seeds,
    healthy_min_availability,
    health_store,
    now,
):
    is_priority = bool(torrent_priority_reason(torrent, priority_tags, priority_categories))
    reported_sources = max(torrent_connected_seeds(torrent), torrent_reported_seeds(torrent))
    return (
        0 if is_priority else 1,
        media_queue_order_key(
            torrent,
            tv_order_categories,
            tv_order_state,
            movie_order_categories,
            movie_order_state,
        ),
        -health_store.score(torrent, now),
        candidate_health_class(torrent, healthy_min_seeds, healthy_min_availability),
        -min(torrent_availability(torrent), 100.0),
        -reported_sources,
        -torrent_connected_seeds(torrent),
        -torrent_progress(torrent),
        torrent_amount_left(torrent),
        torrent_name(torrent).lower(),
    )


def selected_storage_remaining_state(client, torrent):
    item_hash = torrent_hash(torrent)
    fallback = {
        "remaining_bytes": torrent_amount_left(torrent),
        "selected_count": None,
        "selected_size": None,
        "present_bytes": None,
        "source": "torrent amount_left fallback",
    }
    if not item_hash:
        return fallback

    try:
        file_state = selected_file_remaining_state(client.torrent_files(item_hash))
    except ApiError as exc:
        log_warning(
            f"Failed to read qBittorrent file list for storage fit "
            f"{torrent_name(torrent)}; {exc}; "
            "using torrent amount_left",
        )
        return fallback

    if file_state is None:
        return fallback

    file_state["source"] = "selected files"
    return file_state


def storage_torrent_block_reason(client, torrent, storage_guard, storage_state):
    if not storage_guard or not storage_guard.require_torrent_fit:
        return ""
    if not storage_state or not storage_state.get("enabled") or storage_state.get("stop"):
        return ""

    remaining_state = selected_storage_remaining_state(client, torrent)
    remaining_bytes = int(remaining_state.get("remaining_bytes") or 0)
    if remaining_bytes <= 0:
        return ""

    headroom_bytes = max(0, int(storage_state.get("headroom_bytes") or 0))
    if remaining_bytes <= headroom_bytes:
        return ""

    if remaining_state.get("selected_count") is not None:
        return (
            f"{human_size(remaining_bytes)} selected-file bytes left "
            f"({remaining_state['selected_count']} selected file(s), "
            f"{human_size(remaining_state.get('present_bytes') or 0)} already present) "
            f"exceeds download storage headroom {human_size(headroom_bytes)}"
        )

    return (
        f"{human_size(remaining_bytes)} left exceeds download storage "
        f"headroom {human_size(headroom_bytes)} "
        f"({remaining_state.get('source', 'torrent amount_left fallback')})"
    )


def stall_cooldown_tag(prefix, now):
    return f"{prefix}-{now.strftime('%Y%m%dT%H%M%SZ')}"


def parse_stall_cooldown_tag(tag, prefix):
    marker = f"{prefix}-"
    if not tag.startswith(marker):
        return None
    try:
        return datetime.strptime(tag[len(marker):], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def stall_cooldown_tags(torrent, prefix, now, cooldown_seconds):
    active_tags = []
    expired_tags = []
    cutoff = now - timedelta(seconds=max(0, cooldown_seconds))
    for tag in torrent_tags(torrent):
        tag_time = parse_stall_cooldown_tag(tag, prefix)
        if tag_time is None:
            continue
        if cooldown_seconds > 0 and tag_time > cutoff:
            active_tags.append(tag)
        else:
            expired_tags.append(tag)
    return active_tags, expired_tags


def clear_expired_stall_tags(client, torrent, prefix, now, cooldown_seconds):
    active_tags, expired_tags = stall_cooldown_tags(torrent, prefix, now, cooldown_seconds)
    if expired_tags:
        try:
            client.remove_tags([torrent_hash(torrent)], expired_tags)
            log_info(
                f"Cleared expired quota-stall cooldown tag(s) from "
                f"{torrent_name(torrent)}"
            )
        except ApiError as exc:
            log_warning(
                f"Failed to clear expired quota-stall cooldown tag(s) from "
                f"{torrent_name(torrent)}: {exc}",
            )
    return active_tags


def add_stall_cooldown_tag(client, torrent, prefix, now, cooldown_seconds):
    if cooldown_seconds <= 0 or not prefix or not torrent_hash(torrent):
        return
    tag = stall_cooldown_tag(prefix, now)
    try:
        client.add_tags([torrent_hash(torrent)], [tag])
        log_info(
            f"Marked {torrent_name(torrent)} with quota-stall cooldown "
            f"for {cooldown_seconds}s"
        )
    except ApiError as exc:
        log_warning(
            f"Failed to mark {torrent_name(torrent)} with quota-stall cooldown: "
            f"{exc}",
        )


def cleanup_stall_tags(client):
    stall_tag_prefix = os.environ.get("QBT_SINGLE_DOWNLOAD_STALL_TAG_PREFIX", "quota-stalled").strip()
    if not stall_tag_prefix:
        return

    now = datetime.now(timezone.utc)
    stall_cooldown_seconds = env_int("QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_SECONDS", 3600)
    active_stall_tags = set()
    expired_tag_assignments = []

    try:
        torrents = single_download_torrents(client)
    except ApiError as exc:
        log_warning(
            "Failed to list qBittorrent torrents for quota-stall tag cleanup: "
            f"{exc}",
        )
        return

    for torrent in torrents:
        item_hash = torrent_hash(torrent)
        active_tags, expired_tags = stall_cooldown_tags(
            torrent,
            stall_tag_prefix,
            now,
            stall_cooldown_seconds,
        )
        active_stall_tags.update(active_tags)
        if item_hash and expired_tags:
            expired_tag_assignments.append((torrent, expired_tags))

    removed_assignments = 0
    affected_torrents = 0
    for torrent, expired_tags in expired_tag_assignments:
        try:
            client.remove_tags([torrent_hash(torrent)], expired_tags)
            removed_assignments += len(expired_tags)
            affected_torrents += 1
        except ApiError as exc:
            log_warning(
                f"Failed to clear expired quota-stall cooldown tag(s) from "
                f"{torrent_name(torrent)}: {exc}",
            )

    if removed_assignments:
        log_info(
            f"Cleared {removed_assignments} expired quota-stall cooldown tag "
            f"assignment(s) from {affected_torrents} torrent(s)"
        )

    try:
        all_tags = client.all_tags()
    except ApiError as exc:
        log_warning(
            "Failed to list qBittorrent tags for unused quota-stall tag cleanup: "
            f"{exc}",
        )
        return

    unused_stall_tags = sorted(
        tag for tag in all_tags
        if parse_stall_cooldown_tag(tag, stall_tag_prefix) is not None
        and tag not in active_stall_tags
    )
    if not unused_stall_tags:
        log_debug(f"No unused quota-stall tags need cleanup")
        return

    try:
        client.delete_tags(unused_stall_tags)
        log_info(f"Deleted {len(unused_stall_tags)} unused quota-stall tag(s)")
    except ApiError as exc:
        log_warning(
            f"Failed to delete unused quota-stall tag(s): {exc}",
        )


def apply_single_download(
    clients,
    usage_bytes,
    monthly_limit_bytes,
    download_limit,
    limit_reason,
    storage_guard=None,
    download_limit_ceiling=None,
    decision_context=None,
):
    min_progress = env_float("QBT_SINGLE_DOWNLOAD_MIN_PROGRESS", 0.0)
    max_remaining_bytes = env_int("QBT_SINGLE_DOWNLOAD_MAX_REMAINING_BYTES", 0)
    configured_download_limit = env_int("QBT_SINGLE_DOWNLOAD_DOWNLOAD_LIMIT_BYTES_PER_SEC", 10_485_760)
    if download_limit_ceiling is not None:
        configured_download_limit = max(1, int(download_limit_ceiling))
    requested_download_limit = int(download_limit)
    if requested_download_limit <= 0:
        download_limit = 0
        slow_reference_limit = max(1, configured_download_limit)
    else:
        download_limit = max(1, min(requested_download_limit, configured_download_limit))
        slow_reference_limit = download_limit
    upload_limit = env_int("QBT_SINGLE_DOWNLOAD_UPLOAD_LIMIT_BYTES_PER_SEC", 524_288)
    effective_cap = {
        "requested_download_limit_bytes_per_sec": requested_download_limit,
        "download_limit_bytes_per_sec": download_limit,
        "upload_limit_bytes_per_sec": upload_limit,
        "configured_download_ceiling_bytes_per_sec": configured_download_limit,
        "slow_reference_limit_bytes_per_sec": slow_reference_limit,
        "reason": limit_reason,
    }
    run_decision_context = dict(decision_context or {})
    budget = dict(run_decision_context.get("budget") or {})
    budget.setdefault("monthly_usage_bytes", usage_bytes)
    budget.setdefault("monthly_guardrail_bytes", monthly_limit_bytes)
    budget.setdefault("monthly_remaining_bytes", max(0, monthly_limit_bytes - usage_bytes))
    run_decision_context["budget"] = budget
    run_decision_context["effective_cap"] = effective_cap
    stall_check_seconds = env_int("QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS", 60)
    min_progress_bytes = env_int("QBT_SINGLE_DOWNLOAD_MIN_PROGRESS_BYTES", 1_048_576)
    max_attempts = max(0, env_int("QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN", 0))
    attempt_limit_label = str(max_attempts) if max_attempts > 0 else "time-budget"
    max_run_seconds = max(1, env_int("QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS", 900))
    stall_cooldown_seconds = env_int("QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_SECONDS", 3600)
    stall_tag_prefix = os.environ.get("QBT_SINGLE_DOWNLOAD_STALL_TAG_PREFIX", "quota-stalled").strip()
    slow_min_rate_fraction = env_float("QBT_SINGLE_DOWNLOAD_SLOW_MIN_RATE_FRACTION", 0.10)
    slow_min_rate_bytes = env_int("QBT_SINGLE_DOWNLOAD_SLOW_MIN_RATE_BYTES_PER_SEC", 65_536)
    slow_max_eta_seconds = env_int("QBT_SINGLE_DOWNLOAD_SLOW_MAX_ETA_SECONDS", 172_800)
    healthy_min_seeds = env_int("QBT_SINGLE_DOWNLOAD_HEALTHY_MIN_SEEDS", 3)
    healthy_min_availability = env_float("QBT_SINGLE_DOWNLOAD_HEALTHY_MIN_AVAILABILITY", 1.05)
    tv_file_priority_enabled = env_bool("QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED", True)
    tv_file_priority_lookahead = env_int("QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_LOOKAHEAD_EPISODES", 2)
    categories = {
        item.strip()
        for item in split_lines_or_csv(os.environ.get("QBT_SINGLE_DOWNLOAD_CATEGORIES", ""))
        if item.strip()
    }
    tv_order_categories = normalized_set(
        split_lines_or_csv(os.environ.get("QBT_SINGLE_DOWNLOAD_TV_ORDER_CATEGORIES", "tv,priority-tv"))
    )
    movie_order_categories = normalized_set(
        split_lines_or_csv(
            os.environ.get("QBT_SINGLE_DOWNLOAD_MOVIE_ORDER_CATEGORIES", "movies,priority-movies")
        )
    )
    priority_tags = normalized_set(
        split_lines_or_csv(os.environ.get("QBT_SINGLE_DOWNLOAD_PRIORITY_TAGS", "priority"))
    )
    priority_categories = normalized_set(
        split_lines_or_csv(
            os.environ.get(
                "QBT_SINGLE_DOWNLOAD_PRIORITY_CATEGORIES",
                "priority-tv,priority-movies,priority-anime",
            )
        )
    )
    health_store = TorrentHealthStore()
    sonarr_queue = SonarrQueueMetadata()
    jellyfin_watch = JellyfinWatchMetadata()
    radarr_queue = RadarrQueueMetadata()

    for client in clients:
        client.set_download_limit(download_limit)
        client.set_upload_limit(upload_limit)
        storage_state = None
        if storage_guard:
            storage_state = storage_guard.check()
        emit_decision_log(
            "qbt_guard_run",
            **decision_base_context(run_decision_context, client, storage_state),
            action="start_client",
        )
        if storage_guard:
            if storage_state.get("stop"):
                emit_decision_log(
                    "qbt_guard_decision",
                    **decision_base_context(run_decision_context, client, storage_state),
                    action="stop_for_storage",
                    reason=storage_state["reason"],
                    rejected_counts={"storage_stop": 1},
                    selected_torrent=None,
                )
                client.stop_all()
                log_decision_info(
                    "stop_for_storage",
                    f"Paused all torrents; {storage_state['reason']}",
                    reason=storage_state["reason"],
                )
                continue
        attempted_hashes = set()
        attempt = 0
        deadline = time.monotonic() + max_run_seconds

        while True:
            if max_attempts > 0 and attempt >= max_attempts:
                client.stop_all()
                emit_decision_log(
                    "qbt_guard_decision",
                    **decision_base_context(run_decision_context, client, storage_state),
                    action="stop_attempt_limit",
                    reason="max attempts reached",
                    rejected_counts={"attempt_limit": 1},
                    selected_torrent=None,
                )
                log_decision_info(
                    "stop_attempt_limit",
                    f"No torrent became active after {max_attempts} attempt(s) "
                    "the next scheduled run will continue the cycle",
                    reason="max attempts reached",
                )
                break
            if time.monotonic() >= deadline:
                client.stop_all()
                emit_decision_log(
                    "qbt_guard_decision",
                    **decision_base_context(run_decision_context, client, storage_state),
                    action="stop_run_budget",
                    reason="run time budget expired",
                    rejected_counts={"run_budget_expired": 1},
                    selected_torrent=None,
                )
                log_decision_info(
                    "stop_run_budget",
                    f"No torrent became active before the {human_duration(max_run_seconds)} "
                    f"single-download run budget expired; "
                    "the next scheduled run will continue the cycle",
                    reason="run time budget expired",
                )
                break

            now = datetime.now(timezone.utc)
            torrents = single_download_torrents(client)
            health_store.observe_torrents(torrents, now)
            if storage_guard:
                storage_state = storage_guard.snapshot()
                if storage_state.get("stop"):
                    client.stop_all()
                    emit_decision_log(
                        "qbt_guard_decision",
                        **decision_base_context(run_decision_context, client, storage_state),
                        action="stop_for_storage",
                        reason=storage_state["reason"],
                        rejected_counts={"storage_stop": 1},
                        selected_torrent=None,
                    )
                    log_decision_info(
                        "stop_for_storage",
                        f"Paused all torrents; {storage_state['reason']}",
                        reason=storage_state["reason"],
                    )
                    break
            else:
                storage_state = None

            rejected_counts = Counter()
            eligible_torrents = []
            for torrent in torrents:
                reject_reason = single_download_reject_reason(
                    torrent,
                    min_progress,
                    max_remaining_bytes,
                    categories,
                )
                if reject_reason:
                    rejected_counts[reject_reason] += 1
                else:
                    eligible_torrents.append(torrent)

            tv_order_state = build_tv_order_state(
                torrents,
                tv_order_categories,
                sonarr_queue,
                jellyfin_watch,
            )
            movie_order_state = build_movie_order_state(torrents, movie_order_categories, radarr_queue)
            all_candidates = sorted(
                eligible_torrents,
                key=lambda torrent: candidate_sort_key(
                    torrent,
                    priority_tags,
                    priority_categories,
                    tv_order_categories,
                    tv_order_state,
                    movie_order_categories,
                    movie_order_state,
                    healthy_min_seeds,
                    healthy_min_availability,
                    health_store,
                    now,
                ),
            )
            candidates = []
            storage_blocked_count = 0
            storage_blocked_examples = []
            tv_order_blocked_count = 0
            tv_order_blocked_examples = []
            for torrent in all_candidates:
                storage_reason = storage_torrent_block_reason(client, torrent, storage_guard, storage_state)
                if storage_reason:
                    rejected_counts["storage_headroom"] += 1
                    storage_blocked_count += 1
                    if len(storage_blocked_examples) < 3:
                        storage_blocked_examples.append(f"{torrent_name(torrent)}: {storage_reason}")
                    continue
                tv_order_reason = tv_queue_order_block_reason(
                    torrent,
                    tv_order_categories,
                    tv_order_state,
                )
                if tv_order_reason:
                    rejected_counts["tv_queue_order_blocked"] += 1
                    tv_order_blocked_count += 1
                    if len(tv_order_blocked_examples) < 3:
                        tv_order_blocked_examples.append(f"{torrent_name(torrent)}: {tv_order_reason}")
                    continue
                candidates.append(torrent)

            available_candidates = []
            cooldown_count = 0
            for torrent in candidates:
                candidate_hash = torrent_hash(torrent)
                if candidate_hash in attempted_hashes:
                    rejected_counts["attempted_this_run"] += 1
                    continue
                active_tags = []
                if stall_tag_prefix:
                    active_tags = clear_expired_stall_tags(
                        client,
                        torrent,
                        stall_tag_prefix,
                        now,
                        stall_cooldown_seconds,
                    )
                if active_tags:
                    rejected_counts["cooldown"] += 1
                    cooldown_count += 1
                    log_info(
                        f"Skipping torrent in quota-stall cooldown "
                        f"{torrent_name(torrent)}"
                    )
                    continue
                available_candidates.append(torrent)

            watch_priority_candidates = [
                torrent for torrent in available_candidates
                if torrent_hash(torrent) in tv_order_state.get("watch_priorities", {})
            ]
            priority_candidates = [
                torrent for torrent in available_candidates
                if torrent_priority_reason(torrent, priority_tags, priority_categories)
            ]
            selection_candidates = watch_priority_candidates or priority_candidates or available_candidates
            if watch_priority_candidates:
                rejected_counts["deferred_by_watch_activity"] += (
                    len(available_candidates) - len(watch_priority_candidates)
                )
            elif priority_candidates:
                rejected_counts["deferred_by_priority"] += len(available_candidates) - len(priority_candidates)

            productive_candidates = []
            slow_candidates = []
            for torrent in selection_candidates:
                if not is_productive_torrent(torrent):
                    if is_running_torrent(torrent) and torrent_download_speed(torrent) <= 0:
                        rejected_counts["not_productive_zero_speed"] += 1
                    else:
                        rejected_counts["not_productive"] += 1
                    continue
                slow_reason = slow_torrent_reason(
                    torrent,
                    slow_reference_limit,
                    slow_min_rate_fraction,
                    slow_min_rate_bytes,
                    slow_max_eta_seconds,
                )
                if slow_reason:
                    rejected_counts["too_slow"] += 1
                    slow_candidates.append((torrent, slow_reason))
                else:
                    productive_candidates.append(torrent)
            candidate_counts = {
                "total": len(torrents),
                "eligible": len(all_candidates),
                "after_storage": len(candidates),
                "tv_queue_order_blocked": tv_order_blocked_count,
                "available": len(available_candidates),
                "selection_pool": len(selection_candidates),
                "priority": len(priority_candidates),
                "watch_priority": len(watch_priority_candidates),
                "watched_tv_episode_torrents": len(tv_order_state.get("watch_priorities", {})),
                "productive": len(productive_candidates),
                "slow": len(slow_candidates),
            }

            if productive_candidates:
                keep = productive_candidates[0]
                keep_hash = torrent_hash(keep)
                stop_hashes = [
                    torrent_hash(torrent) for torrent in torrents
                    if torrent_hash(torrent) and torrent_hash(torrent) != keep_hash
                ]
                client.stop_hashes(stop_hashes)
                apply_tv_episode_file_priorities(
                    client,
                    keep,
                    tv_order_categories,
                    tv_file_priority_enabled,
                    tv_file_priority_lookahead,
                    tv_order_state.get("watch_priorities", {}).get(keep_hash),
                )
                client.top_priority([keep_hash])
                emit_decision_log(
                    "qbt_guard_decision",
                    **decision_base_context(run_decision_context, client, storage_state),
                    action="keep_productive",
                    selected_torrent=torrent_decision_summary(keep),
                    rejected_counts=dict(rejected_counts),
                    candidate_counts=candidate_counts,
                )
                log_decision_info(
                    "keep_productive",
                    f"Keeping active: "
                    f"{torrent_name(keep)} "
                    f"({torrent_progress(keep) * 100:.2f}% complete, "
                    f"{human_size(torrent_amount_left(keep))} left, "
                    f"{human_rate(torrent_download_speed(keep))} down); "
                    f"limit {human_download_limit(download_limit)}"
                    f"{priority_log_suffix(keep, priority_tags, priority_categories)}"
                    f"{watch_priority_log_suffix(keep, tv_order_state)}",
                    selected=torrent_name(keep),
                )
                if stall_check_seconds <= 0:
                    break

                time.sleep(stall_check_seconds)
                refreshed = {
                    torrent_hash(torrent): torrent
                    for torrent in single_download_torrents(client)
                }
                keep_refreshed = refreshed.get(keep_hash)
                if storage_guard:
                    storage_state = storage_guard.snapshot()
                    if storage_state.get("stop"):
                        client.stop_hashes([keep_hash])
                        emit_decision_log(
                            "qbt_guard_decision",
                            **decision_base_context(run_decision_context, client, storage_state),
                            action="stop_kept_for_storage",
                            reason=storage_state["reason"],
                            selected_torrent=torrent_decision_summary(keep_refreshed or keep),
                            rejected_counts={"storage_stop": 1},
                            candidate_counts=candidate_counts,
                        )
                        log_decision_info(
                            "stop_kept_for_storage",
                            f"Stopped kept torrent after storage check: "
                            f"{torrent_name(keep_refreshed or keep)}; "
                            f"{storage_state['reason']}",
                            selected=torrent_name(keep_refreshed or keep),
                            reason=storage_state["reason"],
                        )
                        break
                if keep_refreshed:
                    slow_reason = slow_torrent_reason(
                        keep_refreshed,
                        slow_reference_limit,
                        slow_min_rate_fraction,
                        slow_min_rate_bytes,
                        slow_max_eta_seconds,
                    )
                    if slow_reason:
                        client.stop_hashes([keep_hash])
                        attempted_hashes.add(keep_hash)
                        health_store.record_failure(keep_refreshed, datetime.now(timezone.utc), slow_reason)
                        add_stall_cooldown_tag(
                            client,
                            keep_refreshed,
                            stall_tag_prefix,
                            now,
                            stall_cooldown_seconds,
                        )
                        emit_decision_log(
                            "qbt_guard_decision",
                            **decision_base_context(run_decision_context, client, storage_state),
                            action="stop_kept_too_slow",
                            reason=slow_reason,
                            selected_torrent=torrent_decision_summary(keep_refreshed),
                            rejected_counts={"too_slow_after_wait": 1},
                            candidate_counts=candidate_counts,
                        )
                        log_decision_info(
                            "stop_kept_too_slow",
                            f"Stopped kept torrent because it is too slow after "
                            f"{stall_check_seconds}s: "
                            f"{torrent_name(keep_refreshed)}; {slow_reason}",
                            selected=torrent_name(keep_refreshed),
                            reason=slow_reason,
                        )
                        continue

                progress_reason = ""
                if keep_refreshed:
                    progress_reason = torrent_progress_reason(
                        keep,
                        keep_refreshed,
                        min_progress_bytes,
                    )
                if progress_reason:
                    emit_decision_log(
                        "qbt_guard_decision",
                        **decision_base_context(run_decision_context, client, storage_state),
                        action="confirm_kept_productive",
                        reason=progress_reason,
                        selected_torrent=torrent_decision_summary(keep_refreshed),
                        rejected_counts=dict(rejected_counts),
                        candidate_counts=candidate_counts,
                    )
                    log_debug(
                        f"Kept torrent is active after "
                        f"{stall_check_seconds}s: "
                        f"{torrent_name(keep_refreshed)}; {progress_reason}"
                    )
                    health_store.record_productive(
                        keep,
                        keep_refreshed,
                        datetime.now(timezone.utc),
                        stall_check_seconds,
                    )
                    break

                client.stop_hashes([keep_hash])
                attempted_hashes.add(keep_hash)
                health_store.record_failure(
                    keep_refreshed or keep,
                    datetime.now(timezone.utc),
                    "did not make progress",
                )
                add_stall_cooldown_tag(
                    client,
                    keep_refreshed or keep,
                    stall_tag_prefix,
                    now,
                    stall_cooldown_seconds,
                )
                emit_decision_log(
                    "qbt_guard_decision",
                    **decision_base_context(run_decision_context, client, storage_state),
                    action="stop_kept_no_progress",
                    reason="did not make progress",
                    selected_torrent=torrent_decision_summary(keep_refreshed or keep),
                    rejected_counts={"no_progress_after_wait": 1},
                    candidate_counts=candidate_counts,
                )
                log_decision_info(
                    "stop_kept_no_progress",
                    f"Stopped kept torrent because it did not make progress after "
                    f"{stall_check_seconds}s: "
                    f"{torrent_name(keep_refreshed or keep)}",
                    selected=torrent_name(keep_refreshed or keep),
                    reason="did not make progress",
                )
                continue

            if slow_candidates:
                slow_hashes = [torrent_hash(torrent) for torrent, _ in slow_candidates]
                client.stop_hashes(slow_hashes)
                emit_decision_log(
                    "qbt_guard_decision",
                    **decision_base_context(run_decision_context, client, storage_state),
                    action="stop_slow_candidates",
                    reason="slow candidates below productivity threshold",
                    rejected_counts=dict(rejected_counts),
                    candidate_counts=candidate_counts,
                    selected_torrent=None,
                    rejected_torrents=[
                        {
                            "torrent": torrent_decision_summary(torrent),
                            "reason": slow_reason,
                        }
                        for torrent, slow_reason in slow_candidates[:5]
                    ],
                )
                for torrent, slow_reason in slow_candidates:
                    attempted_hashes.add(torrent_hash(torrent))
                    health_store.record_failure(torrent, now, slow_reason)
                    add_stall_cooldown_tag(
                        client,
                        torrent,
                        stall_tag_prefix,
                        now,
                        stall_cooldown_seconds,
                    )
                    log_info(
                        f"Stopped slow torrent: "
                        f"{torrent_name(torrent)}; {slow_reason}"
                    )
                log_decision_info(
                    "stop_slow_candidates",
                    f"Stopped {len(slow_candidates)} slow torrent(s) "
                    "while trying the next eligible candidate",
                    count=len(slow_candidates),
                )
                continue

            stalled_candidates = [
                torrent for torrent in selection_candidates
                if is_running_torrent(torrent) and is_stalled_torrent(torrent)
            ]
            if stalled_candidates:
                rejected_counts["stalled_zero_speed"] += len(stalled_candidates)
                candidate_counts["stalled"] = len(stalled_candidates)
                stalled_hashes = [torrent_hash(torrent) for torrent in stalled_candidates]
                client.stop_hashes(stalled_hashes)
                emit_decision_log(
                    "qbt_guard_decision",
                    **decision_base_context(run_decision_context, client, storage_state),
                    action="stop_stalled_candidates",
                    reason="stalled without download speed",
                    rejected_counts=dict(rejected_counts),
                    candidate_counts=candidate_counts,
                    selected_torrent=None,
                    rejected_torrents=[
                        torrent_decision_summary(torrent)
                        for torrent in stalled_candidates[:5]
                    ],
                )
                for torrent in stalled_candidates:
                    attempted_hashes.add(torrent_hash(torrent))
                    health_store.record_failure(torrent, now, "stalled without download speed")
                    add_stall_cooldown_tag(
                        client,
                        torrent,
                        stall_tag_prefix,
                        now,
                        stall_cooldown_seconds,
                    )
                log_decision_info(
                    "stop_stalled_candidates",
                    f"Stopped {len(stalled_candidates)} stalled torrent(s) "
                    "while trying the next eligible candidate",
                    count=len(stalled_candidates),
                )
                continue

            if not available_candidates:
                client.stop_all()
                no_available_reason = "no eligible candidates"
                if candidates:
                    no_available_reason = "all candidates cooling down or already attempted"
                elif storage_blocked_count:
                    no_available_reason = "candidates blocked by storage headroom"
                elif tv_order_blocked_count:
                    no_available_reason = "later TV candidates blocked by older queued items"
                elif all_candidates:
                    no_available_reason = "all candidates already attempted"
                emit_decision_log(
                    "qbt_guard_decision",
                    **decision_base_context(run_decision_context, client, storage_state),
                    action="stop_no_available_candidates",
                    reason=no_available_reason,
                    rejected_counts=dict(rejected_counts),
                    candidate_counts=candidate_counts,
                    selected_torrent=None,
                    storage_blocked_examples=storage_blocked_examples,
                    tv_order_blocked_examples=tv_order_blocked_examples,
                )
                if candidates:
                    log_decision_info(
                        "stop_no_available_candidates",
                        "No torrents available for single-download policy; "
                        f"{cooldown_count} candidate(s) are cooling down "
                        "or were already tried in this run",
                        reason=no_available_reason,
                        cooldown_count=cooldown_count,
                    )
                elif storage_blocked_count:
                    log_decision_info(
                        "stop_no_available_candidates",
                        "No torrents available for single-download policy; "
                        f"{storage_blocked_count} candidate(s) do not fit "
                        "download storage headroom",
                        reason=no_available_reason,
                        storage_blocked_count=storage_blocked_count,
                    )
                    for example in storage_blocked_examples:
                        log_info(f"- {example}")
                elif tv_order_blocked_count:
                    log_decision_info(
                        "stop_no_available_candidates",
                        "No torrents available for single-download policy; "
                        f"{tv_order_blocked_count} later TV candidate(s) are waiting "
                        "for older queued Sonarr item(s)",
                        reason=no_available_reason,
                        tv_queue_order_blocked_count=tv_order_blocked_count,
                    )
                    for example in tv_order_blocked_examples:
                        log_info(f"- {example}")
                elif all_candidates:
                    log_decision_info(
                        "stop_no_available_candidates",
                        "No torrents available for single-download policy; "
                        "all candidate(s) were already tried in this run",
                        reason=no_available_reason,
                    )
                else:
                    log_decision_info(
                        "stop_no_available_candidates",
                        "No torrents eligible for single-download policy",
                        reason=no_available_reason,
                    )
                break

            selected = selection_candidates[0]
            selected_hash = torrent_hash(selected)
            attempted_hashes.add(selected_hash)
            health_store.record_attempt(selected, now)
            attempt += 1
            stop_hashes = [
                torrent_hash(torrent) for torrent in torrents
                if torrent_hash(torrent) and torrent_hash(torrent) != selected_hash
            ]
            client.stop_hashes(stop_hashes)
            apply_tv_episode_file_priorities(
                client,
                selected,
                tv_order_categories,
                tv_file_priority_enabled,
                tv_file_priority_lookahead,
                tv_order_state.get("watch_priorities", {}).get(selected_hash),
            )
            client.top_priority([selected_hash])
            try:
                client.reannounce_hashes([selected_hash])
            except ApiError as exc:
                log_warning(
                    f"Failed to reannounce selected torrent: {exc}",
                )
            client.start_hashes([selected_hash])
            emit_decision_log(
                "qbt_guard_decision",
                **decision_base_context(run_decision_context, client, storage_state),
                action="try_candidate",
                selected_torrent=torrent_decision_summary(selected),
                rejected_counts=dict(rejected_counts),
                candidate_counts=candidate_counts,
                attempt=attempt,
                attempt_limit=max_attempts,
            )
            log_decision_info(
                "try_candidate",
                f"Trying torrent {attempt}/{attempt_limit_label}: "
                f"{torrent_name(selected)} "
                f"({torrent_progress(selected) * 100:.2f}% complete, "
                f"{human_size(torrent_amount_left(selected))} left); "
                f"limit {human_download_limit(download_limit)}"
                f"{priority_log_suffix(selected, priority_tags, priority_categories)}"
                f"{watch_priority_log_suffix(selected, tv_order_state)}"
                f"{health_store.summary(selected, now)}",
                selected=torrent_name(selected),
                attempt=attempt,
                attempt_limit=max_attempts,
            )

            if stall_check_seconds <= 0:
                break

            time.sleep(stall_check_seconds)
            refreshed = {
                torrent_hash(torrent): torrent
                for torrent in single_download_torrents(client)
            }
            selected_refreshed = refreshed.get(selected_hash)
            if storage_guard:
                storage_state = storage_guard.snapshot()
                if storage_state.get("stop"):
                    client.stop_hashes([selected_hash])
                    emit_decision_log(
                        "qbt_guard_decision",
                        **decision_base_context(run_decision_context, client, storage_state),
                        action="stop_selected_for_storage",
                        reason=storage_state["reason"],
                        selected_torrent=torrent_decision_summary(selected_refreshed or selected),
                        rejected_counts={"storage_stop": 1},
                        candidate_counts=candidate_counts,
                    )
                    log_decision_info(
                        "stop_selected_for_storage",
                        f"Stopped selected torrent after storage check: "
                        f"{torrent_name(selected_refreshed or selected)}; "
                        f"{storage_state['reason']}",
                        selected=torrent_name(selected_refreshed or selected),
                        reason=storage_state["reason"],
                    )
                    break
            if selected_refreshed:
                slow_reason = slow_torrent_reason(
                    selected_refreshed,
                    slow_reference_limit,
                    slow_min_rate_fraction,
                    slow_min_rate_bytes,
                    slow_max_eta_seconds,
                )
                if slow_reason:
                    client.stop_hashes([selected_hash])
                    health_store.record_failure(selected_refreshed, datetime.now(timezone.utc), slow_reason)
                    add_stall_cooldown_tag(
                        client,
                        selected_refreshed,
                        stall_tag_prefix,
                        now,
                        stall_cooldown_seconds,
                    )
                    emit_decision_log(
                        "qbt_guard_decision",
                        **decision_base_context(run_decision_context, client, storage_state),
                        action="stop_selected_too_slow",
                        reason=slow_reason,
                        selected_torrent=torrent_decision_summary(selected_refreshed),
                        rejected_counts={"too_slow_after_wait": 1},
                        candidate_counts=candidate_counts,
                    )
                    log_decision_info(
                        "stop_selected_too_slow",
                        f"Stopped selected torrent because it is too slow after "
                        f"{stall_check_seconds}s: "
                        f"{torrent_name(selected_refreshed)}; {slow_reason}",
                        selected=torrent_name(selected_refreshed),
                        reason=slow_reason,
                    )
                    continue

            progress_reason = ""
            if selected_refreshed:
                progress_reason = torrent_progress_reason(
                    selected,
                    selected_refreshed,
                    min_progress_bytes,
                )
            if progress_reason:
                emit_decision_log(
                    "qbt_guard_decision",
                    **decision_base_context(run_decision_context, client, storage_state),
                    action="confirm_selected_productive",
                    reason=progress_reason,
                    selected_torrent=torrent_decision_summary(selected_refreshed),
                    rejected_counts=dict(rejected_counts),
                    candidate_counts=candidate_counts,
                )
                log_decision_info(
                    "confirm_selected_productive",
                    f"Selected torrent is active after "
                    f"{stall_check_seconds}s: "
                    f"{torrent_name(selected_refreshed)}; {progress_reason}",
                    selected=torrent_name(selected_refreshed),
                    reason=progress_reason,
                )
                health_store.record_productive(
                    selected,
                    selected_refreshed,
                    datetime.now(timezone.utc),
                    stall_check_seconds,
                )
                break

            client.stop_hashes([selected_hash])
            health_store.record_failure(
                selected_refreshed or selected,
                datetime.now(timezone.utc),
                "did not make progress",
            )
            add_stall_cooldown_tag(
                client,
                selected_refreshed or selected,
                stall_tag_prefix,
                now,
                stall_cooldown_seconds,
            )
            emit_decision_log(
                "qbt_guard_decision",
                **decision_base_context(run_decision_context, client, storage_state),
                action="stop_selected_no_progress",
                reason="did not make progress",
                selected_torrent=torrent_decision_summary(selected_refreshed or selected),
                rejected_counts={"no_progress_after_wait": 1},
                candidate_counts=candidate_counts,
            )
            log_decision_info(
                "stop_selected_no_progress",
                f"Stopped torrent because it did not make progress after "
                f"{stall_check_seconds}s: {torrent_name(selected_refreshed or selected)}",
                selected=torrent_name(selected_refreshed or selected),
                reason="did not make progress",
            )


def run_once():
    now = datetime.now(timezone.utc)
    monthly_quota = env_int("UDM_MONTHLY_DOWNLOAD_QUOTA_BYTES", 2_500_000_000_000)
    cap_fraction = env_float("UDM_MONTHLY_CAP_FRACTION", 1.0)
    cap_bytes = env_int(
        "UDM_MONTHLY_DOWNLOAD_GUARDRAIL_BYTES",
        math.floor(monthly_quota * cap_fraction),
    )
    days_in_month = calendar.monthrange(now.year, now.month)[1]
    daily_cap_bytes = max(1, math.floor(cap_bytes / days_in_month))
    headroom = env_float("QBT_RATE_HEADROOM_FRACTION", 0.95)
    max_download_limit = env_int("QBT_MAX_AGGREGATE_DOWNLOAD_LIMIT_BYTES_PER_SEC", 10_485_760)
    fallback_download_limit = env_int(
        "QBT_FALLBACK_AGGREGATE_DOWNLOAD_LIMIT_BYTES_PER_SEC",
        max_download_limit,
    )

    rpi_cooling_state = apply_rpi_thermal_cooling()
    storage_guard = DownloadStorageGuard()
    try:
        udm_client = UdmClient()
        usage_bytes, day_usage_bytes = udm_client.download_usage_snapshot(now)
    except ApiError as exc:
        log_warning(f"Failed to read UDM month-to-date WAN usage: {exc}")
        if env_bool("UDM_FAIL_CLOSED", False):
            if not apply_fail_closed():
                return 0
            return 1
        clients = reachable_qbt_clients()
        thermal_state = full_guard_thermal_state()
        fallback_context = {
            "budget": {
                "monthly_usage_bytes": 0,
                "monthly_guardrail_bytes": monthly_quota,
                "monthly_remaining_bytes": monthly_quota,
                "quota_source": "fallback",
            },
            "udm": udm_decision_summary(None, now, error=exc),
            "thermal": thermal_decision_summary(thermal_state),
            "rpi_cooling": rpi_cooling_state,
        }
        if apply_rpi_cooling_stop(clients, rpi_cooling_state, fallback_context):
            return 0
        if apply_full_guard_thermal_stop(clients, thermal_state, fallback_context):
            return 0
        apply_single_download(
            clients,
            0,
            monthly_quota,
            fallback_download_limit,
            "UDM quota data unavailable fallback",
            storage_guard,
            decision_context=fallback_context,
        )
        cleanup_qbt_clients(clients)
        return 0

    usage_percent = (usage_bytes / cap_bytes) * 100 if cap_bytes else 0
    day_usage_percent = (day_usage_bytes / daily_cap_bytes) * 100 if daily_cap_bytes else 0
    log_debug(
        "UDM month-to-date WAN download usage: "
        f"{human_size(usage_bytes)} of {human_size(cap_bytes)} monthly guardrail "
        f"({usage_percent:.2f}%)"
    )
    log_debug(
        "UDM day-to-date WAN download usage: "
        f"{human_size(day_usage_bytes)} of {human_size(daily_cap_bytes)} daily guardrail "
        f"({day_usage_percent:.2f}%; {human_size(cap_bytes)} / {days_in_month} days)"
    )

    clients = reachable_qbt_clients()
    if not clients:
        emit_decision_log(
            "qbt_guard_decision",
            action="no_reachable_clients",
            client_count=0,
            budget={
                "monthly_usage_bytes": usage_bytes,
                "monthly_guardrail_bytes": cap_bytes,
                "monthly_remaining_bytes": max(0, cap_bytes - usage_bytes),
                "day_usage_bytes": day_usage_bytes,
                "daily_guardrail_bytes": daily_cap_bytes,
                "daily_remaining_bytes": max(0, daily_cap_bytes - day_usage_bytes),
            },
            udm=udm_decision_summary(udm_client, now),
        )
        log_info("No qBittorrent clients reachable; leaving quota state unchanged")
        return 0

    thermal_state = full_guard_thermal_state()
    base_decision_context = {
        "budget": {
            "monthly_usage_bytes": usage_bytes,
            "monthly_guardrail_bytes": cap_bytes,
            "monthly_remaining_bytes": max(0, cap_bytes - usage_bytes),
            "day_usage_bytes": day_usage_bytes,
            "daily_guardrail_bytes": daily_cap_bytes,
            "daily_remaining_bytes": max(0, daily_cap_bytes - day_usage_bytes),
            "days_in_month": days_in_month,
            "rate_headroom_fraction": headroom,
        },
        "udm": udm_decision_summary(udm_client, now),
        "thermal": thermal_decision_summary(thermal_state),
        "rpi_cooling": rpi_cooling_state,
    }

    if apply_rpi_cooling_stop(clients, rpi_cooling_state, base_decision_context):
        return 0

    if apply_full_guard_thermal_stop(clients, thermal_state, base_decision_context):
        return 0

    quota_state = quota_rate_state(
        now,
        usage_bytes,
        day_usage_bytes,
        cap_bytes,
        daily_cap_bytes,
        headroom,
        max_download_limit,
    )
    base_decision_context["budget"].update({
        "monthly_limit_bytes_per_sec": quota_state.get("monthly_limit"),
        "daily_limit_bytes_per_sec": quota_state.get("daily_limit"),
        "smart_download_limit_bytes_per_sec": quota_state.get("smart_download_limit"),
        "max_download_limit_bytes_per_sec": max_download_limit,
    })
    if quota_state["stop_reason"]:
        apply_stop_limits(
            clients,
            quota_state["stop_reason"],
            pause_torrents=True,
            decision_context=base_decision_context,
        )
        cleanup_qbt_clients(clients)
        return 0

    smart_download_limit = quota_state["smart_download_limit"]

    apply_single_download(
        clients,
        usage_bytes,
        cap_bytes,
        smart_download_limit,
        "monthly and daily quota guard",
        storage_guard,
        decision_context=base_decision_context,
    )
    cleanup_qbt_clients(clients)

    return 0


def install_loop_signal_handlers(stop_event):
    def handle_signal(signum, frame):
        log_info(f"Received signal {signum}; stopping qBittorrent guard loop")
        stop_event.set()

    for signal_name in ("SIGTERM", "SIGINT"):
        signum = getattr(signal, signal_name, None)
        if signum is not None:
            signal.signal(signum, handle_signal)


def loop_sleep_seconds(result, elapsed_seconds, poll_seconds, error_poll_seconds):
    target = error_poll_seconds if result else poll_seconds
    if env_bool("QBT_GUARD_POLL_FIXED_RATE", True):
        return max(0.0, float(target) - max(0.0, float(elapsed_seconds)))
    return float(target)


def run_loop():
    poll_seconds = max(1, env_int("QBT_GUARD_POLL_SECONDS", 60))
    error_poll_seconds = max(1, env_int("QBT_GUARD_ERROR_POLL_SECONDS", poll_seconds))
    stop_event = threading.Event()
    install_loop_signal_handlers(stop_event)
    log_info(
        "Starting continuous qBittorrent guard loop: "
        f"poll={poll_seconds}s, error_poll={error_poll_seconds}s"
    )
    log_info("Configured qBittorrent service endpoint(s)", qbt_urls=qbt_urls())

    while not stop_event.is_set():
        started = time.monotonic()
        result = 0
        try:
            result = int(run_once() or 0)
        except Exception as exc:
            result = 1
            log_error(f"Unhandled qBittorrent guard loop error: {exc}")

        if result and env_bool("QBT_GUARD_LOOP_EXIT_ON_ERROR", False):
            return result
        if stop_event.is_set():
            break

        elapsed_seconds = time.monotonic() - started
        sleep_seconds = loop_sleep_seconds(
            result,
            elapsed_seconds,
            poll_seconds,
            error_poll_seconds,
        )
        emit_decision_log(
            "qbt_guard_loop",
            action="sleep",
            result=result,
            elapsed_seconds=round(elapsed_seconds, 3),
            sleep_seconds=round(sleep_seconds, 3),
        )
        if sleep_seconds > 0 and stop_event.wait(sleep_seconds):
            break

    log_info("Continuous qBittorrent guard loop stopped")
    return 0


def main():
    return run_loop()


if __name__ == "__main__":
    sys.exit(main())
