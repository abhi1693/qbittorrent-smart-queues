"""Provider-neutral quota controller configuration."""

from __future__ import annotations

import calendar
import math
from dataclasses import dataclass
from datetime import UTC, datetime

from qbittorrent_smart_queues.config import env_bool, env_float, env_int


def local_billing_cycle_window(now, local_timezone, cycle_day):
    """Return UTC bounds and local-calendar length for the active quota cycle."""

    if not 1 <= cycle_day <= 31:
        raise ValueError("billing cycle day must be between 1 and 31")

    local_now = now.astimezone(local_timezone)

    def local_anchor(year, month):
        anchor_day = min(cycle_day, calendar.monthrange(year, month)[1])
        return datetime(year, month, anchor_day, tzinfo=local_timezone)

    current_anchor = local_anchor(local_now.year, local_now.month)
    if local_now >= current_anchor:
        start = current_anchor
        if local_now.month == 12:
            end = local_anchor(local_now.year + 1, 1)
        else:
            end = local_anchor(local_now.year, local_now.month + 1)
    else:
        end = current_anchor
        if local_now.month == 1:
            start = local_anchor(local_now.year - 1, 12)
        else:
            start = local_anchor(local_now.year, local_now.month - 1)

    return (
        start.astimezone(UTC),
        end.astimezone(UTC),
        (end.date() - start.date()).days,
    )


@dataclass(frozen=True)
class QuotaSettings:
    backup_internet_stop_enabled: bool
    backup_internet_fail_closed: bool
    usage_fail_closed: bool
    monthly_quota_bytes: int
    monthly_guardrail_bytes: int
    billing_cycle_day: int
    rate_headroom_fraction: float
    max_download_limit: int
    fallback_download_limit: int
    burst_enabled: bool
    burst_download_limit: int
    burst_min_monthly_remaining_fraction: float
    burst_min_daily_remaining_fraction: float

    @classmethod
    def from_env(cls) -> QuotaSettings:
        monthly_quota = env_int("QBT_MONTHLY_QUOTA_BYTES", 2_500_000_000_000)
        cap_fraction = env_float("QBT_MONTHLY_CAP_FRACTION", 1.0)
        guardrail = env_int(
            "QBT_MONTHLY_GUARDRAIL_BYTES",
            math.floor(monthly_quota * cap_fraction),
        )
        max_download_limit = env_int(
            "QBT_ISP_USABLE_DOWNLOAD_LIMIT_BYTES_PER_SEC",
            10_485_760,
        )
        billing_day = env_int("QBT_BILLING_CYCLE_DAY", 1)
        if not 1 <= billing_day <= 31:
            raise ValueError("QBT_BILLING_CYCLE_DAY must be between 1 and 31")
        return cls(
            backup_internet_stop_enabled=env_bool(
                "QBT_BACKUP_INTERNET_STOP_ENABLED",
                False,
            ),
            backup_internet_fail_closed=env_bool(
                "QBT_BACKUP_INTERNET_FAIL_CLOSED",
                True,
            ),
            usage_fail_closed=env_bool("QBT_USAGE_FAIL_CLOSED", False),
            monthly_quota_bytes=max(1, monthly_quota),
            monthly_guardrail_bytes=max(1, guardrail),
            billing_cycle_day=billing_day,
            rate_headroom_fraction=env_float("QBT_RATE_HEADROOM_FRACTION", 0.95),
            max_download_limit=max_download_limit,
            fallback_download_limit=env_int(
                "QBT_FALLBACK_AGGREGATE_DOWNLOAD_LIMIT_BYTES_PER_SEC",
                max_download_limit,
            ),
            burst_enabled=env_bool("QBT_QUOTA_BURST_ENABLED", False),
            burst_download_limit=env_int(
                "QBT_ISP_USABLE_BURST_DOWNLOAD_LIMIT_BYTES_PER_SEC",
                max_download_limit,
            ),
            burst_min_monthly_remaining_fraction=env_float(
                "QBT_QUOTA_BURST_MIN_MONTHLY_REMAINING_FRACTION",
                0.10,
            ),
            burst_min_daily_remaining_fraction=env_float(
                "QBT_QUOTA_BURST_MIN_DAILY_REMAINING_FRACTION",
                0.20,
            ),
        )
