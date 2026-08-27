"""Brand-neutral contracts for network usage sources.

Providers own vendor API details. The controller consumes a stable snapshot and
optional backup-internet state, so adding another router brand does not require
editing queue policy code.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

from qbittorrent_smart_queues.errors import ApiError


@dataclass(frozen=True)
class UsageSnapshot:
    """Provider-neutral quota inputs and period metadata."""

    cycle_usage_bytes: int
    day_usage_bytes: int
    billing_cycle_start: datetime | None = None
    billing_cycle_end: datetime | None = None
    billing_cycle_days: int = 0
    local_day_end: datetime | None = None
    timezone_name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.cycle_usage_bytes < 0 or self.day_usage_bytes < 0:
            raise ValueError("usage byte counts cannot be negative")


@dataclass(frozen=True)
class ProviderDiagnostics:
    """Optional provider health details exposed through a stable schema."""

    latest_stats_at: datetime | None = None
    stats_timezone: str = ""
    stats_timezone_source: str = ""
    billing_cycle_day: int = 1
    billing_cycle_start: datetime | None = None
    billing_cycle_end: datetime | None = None
    billing_cycle_days: int = 0
    usage_scope: str = "all"
    usage_network_groups: tuple[str, ...] = ()
    usage_anomalies: tuple[dict[str, Any], ...] = ()
    usage_corrected_bytes: int = 0


class NetworkUsageProvider(ABC):
    """Base class for router and network usage adapters."""

    provider_name: ClassVar[str]

    @abstractmethod
    def usage_snapshot(self, now: datetime) -> UsageSnapshot:
        """Return current billing-cycle and local-day usage."""

    def diagnostics(self) -> ProviderDiagnostics:
        """Return optional health metadata using the shared provider schema."""

        return ProviderDiagnostics()

    def decision_summary(
        self,
        now: datetime,
        *,
        error: Exception | None = None,
        backup_state: dict[str, Any] | None = None,
        backup_error: Exception | None = None,
    ) -> dict[str, Any]:
        """Return common, credential-safe provider diagnostics."""

        diagnostics = self.diagnostics()
        latest_stats_at = diagnostics.latest_stats_at
        age_seconds = None
        if latest_stats_at:
            age_seconds = max(0, int((now - latest_stats_at).total_seconds()))
        summary = {
            "provider": self.provider_name,
            "available": error is None,
            "error": str(error) if error else "",
            "latest_stats_at": self._format_utc(latest_stats_at),
            "stats_age_seconds": age_seconds,
            "stats_timezone": diagnostics.stats_timezone,
            "stats_timezone_source": diagnostics.stats_timezone_source,
            "billing_cycle_day": diagnostics.billing_cycle_day,
            "billing_cycle_start": self._format_utc(
                diagnostics.billing_cycle_start
            ),
            "billing_cycle_end": self._format_utc(
                diagnostics.billing_cycle_end
            ),
            "billing_cycle_days": diagnostics.billing_cycle_days,
            "usage_scope": diagnostics.usage_scope,
            "usage_network_groups": list(diagnostics.usage_network_groups),
        }
        usage_anomalies = diagnostics.usage_anomalies
        summary["usage_anomaly_count"] = len(usage_anomalies)
        summary["usage_corrected_bytes"] = diagnostics.usage_corrected_bytes
        if usage_anomalies:
            summary["usage_anomalies"] = list(usage_anomalies)
        if backup_state is not None:
            summary["backup_internet"] = dict(backup_state)
            if backup_error:
                summary["backup_internet"]["available"] = False
                summary["backup_internet"]["error"] = str(backup_error)
        return summary

    @staticmethod
    def _format_utc(value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.astimezone(UTC).isoformat().replace(
            "+00:00",
            "Z",
        )


class BackupInternetProvider(ABC):
    """Optional capability for resolving the active WAN role."""

    @abstractmethod
    def backup_internet_state(self) -> dict[str, Any]:
        """Return active-uplink state."""


ProviderFactory = Callable[[], NetworkUsageProvider]


class UsageProviderRegistry:
    """Small explicit registry used as the extension seam for new brands."""

    def __init__(self):
        self._factories: dict[str, ProviderFactory] = {}
        self._canonical_names: dict[str, str] = {}

    @staticmethod
    def normalize_name(name: str) -> str:
        return str(name or "").strip().lower().replace("_", "-")

    def register(
        self,
        name: str,
        factory: ProviderFactory,
        *,
        aliases: Iterable[str] = (),
    ) -> None:
        canonical = self.normalize_name(name)
        if not canonical:
            raise ValueError("provider name cannot be empty")
        if not callable(factory):
            raise TypeError("provider factory must be callable")
        for candidate in (canonical, *aliases):
            normalized = self.normalize_name(candidate)
            if not normalized:
                continue
            existing = self._canonical_names.get(normalized)
            if existing and existing != canonical:
                raise ValueError(
                    f"provider name {normalized!r} is already registered for {existing!r}"
                )
            self._factories[normalized] = factory
            self._canonical_names[normalized] = canonical

    def create(self, name: str) -> NetworkUsageProvider:
        normalized = self.normalize_name(name)
        try:
            factory = self._factories[normalized]
        except KeyError as exc:
            available = ", ".join(self.available()) or "none"
            raise ApiError(
                f"Unknown network usage provider {name!r}; available: {available}"
            ) from exc
        provider = factory()
        if not isinstance(provider, NetworkUsageProvider):
            raise TypeError(
                f"provider factory for {normalized!r} returned an incompatible object"
            )
        canonical = self._canonical_names[normalized]
        provider_name = self.normalize_name(provider.provider_name)
        if provider_name != canonical:
            raise TypeError(
                f"provider factory for {canonical!r} returned provider "
                f"{provider_name or 'without a name'!r}"
            )
        return provider

    def canonical_name(self, name: str) -> str:
        return self._canonical_names.get(self.normalize_name(name), "")

    def available(self) -> tuple[str, ...]:
        return tuple(sorted(set(self._canonical_names.values())))
