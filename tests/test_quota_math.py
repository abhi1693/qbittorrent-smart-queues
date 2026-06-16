import importlib
import os
import unittest
from datetime import datetime, timezone
from unittest import mock


class QuotaMathTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_rate_state_uses_tighter_daily_budget(self):
        now = datetime(2026, 6, 30, 23, 59, 58, tzinfo=timezone.utc)

        state = self.guard.quota_rate_state(
            now,
            usage_bytes=900,
            day_usage_bytes=50,
            cap_bytes=1000,
            daily_cap_bytes=100,
            headroom=0.5,
            max_download_limit=1000,
        )

        self.assertEqual("", state["stop_reason"])
        self.assertEqual(50, state["monthly_limit"])
        self.assertEqual(25, state["daily_limit"])
        self.assertEqual(25, state["aggregate_limit"])
        self.assertEqual(25, state["smart_download_limit"])

    def test_rate_state_caps_to_configured_download_limit(self):
        now = datetime(2026, 6, 30, 23, 59, 58, tzinfo=timezone.utc)

        state = self.guard.quota_rate_state(
            now,
            usage_bytes=900,
            day_usage_bytes=50,
            cap_bytes=1000,
            daily_cap_bytes=100,
            headroom=0.5,
            max_download_limit=10,
        )

        self.assertEqual(10, state["aggregate_limit"])
        self.assertEqual(10, state["smart_download_limit"])

    def test_rate_state_can_burst_above_smoothed_quota_rate(self):
        now = datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)

        state = self.guard.quota_rate_state(
            now,
            usage_bytes=100,
            day_usage_bytes=10,
            cap_bytes=1000,
            daily_cap_bytes=100,
            headroom=0.5,
            max_download_limit=500,
            burst_enabled=True,
            burst_download_limit=250,
            burst_min_monthly_remaining_fraction=0.10,
            burst_min_daily_remaining_fraction=0.20,
        )

        self.assertTrue(state["burst_active"])
        self.assertEqual(250, state["aggregate_limit"])
        self.assertEqual(250, state["smart_download_limit"])

    def test_rate_state_does_not_burst_when_daily_reserve_is_low(self):
        now = datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)

        state = self.guard.quota_rate_state(
            now,
            usage_bytes=100,
            day_usage_bytes=85,
            cap_bytes=1000,
            daily_cap_bytes=100,
            headroom=0.5,
            max_download_limit=500,
            burst_enabled=True,
            burst_download_limit=250,
            burst_min_monthly_remaining_fraction=0.10,
            burst_min_daily_remaining_fraction=0.20,
        )

        self.assertFalse(state["burst_active"])
        self.assertLess(state["smart_download_limit"], 250)

    def test_rate_state_uses_unlimited_download_limit_during_uncapped_window(self):
        now = datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)

        state = self.guard.quota_rate_state(
            now,
            usage_bytes=100,
            day_usage_bytes=10,
            cap_bytes=1000,
            daily_cap_bytes=100,
            headroom=0.5,
            max_download_limit=500,
            uncapped_downloads_active=True,
        )

        self.assertEqual("", state["stop_reason"])
        self.assertEqual(0, state["smart_download_limit"])
        self.assertTrue(state["uncapped_downloads_active"])

    def test_uncapped_window_is_active_across_midnight_in_ist(self):
        with mock.patch.dict(
            os.environ,
            {
                "QBT_UNCAPPED_DOWNLOAD_WINDOW_ENABLED": "true",
                "QBT_UNCAPPED_DOWNLOAD_WINDOW_TIMEZONE": "Asia/Kolkata",
                "QBT_UNCAPPED_DOWNLOAD_WINDOW_START_LOCAL": "22:00",
                "QBT_UNCAPPED_DOWNLOAD_WINDOW_END_LOCAL": "05:00",
            },
            clear=False,
        ):
            evening = datetime(2026, 6, 16, 17, 0, tzinfo=timezone.utc)
            early_morning = datetime(2026, 6, 16, 23, 0, tzinfo=timezone.utc)
            daytime = datetime(2026, 6, 16, 8, 0, tzinfo=timezone.utc)

            self.assertTrue(self.guard.uncapped_download_window_state(evening)["active"])
            self.assertTrue(self.guard.uncapped_download_window_state(early_morning)["active"])
            self.assertFalse(self.guard.uncapped_download_window_state(daytime)["active"])

    def test_isp_usable_cap_aliases_prefer_clear_names(self):
        with mock.patch.dict(
            os.environ,
            {
                "QBT_ISP_USABLE_DOWNLOAD_LIMIT_BYTES_PER_SEC": "123",
                "QBT_MAX_AGGREGATE_DOWNLOAD_LIMIT_BYTES_PER_SEC": "999",
                "QBT_ISP_USABLE_BURST_DOWNLOAD_LIMIT_BYTES_PER_SEC": "234",
                "QBT_QUOTA_BURST_DOWNLOAD_LIMIT_BYTES_PER_SEC": "888",
            },
            clear=False,
        ):
            self.assertEqual(
                123,
                self.guard.env_int_first(
                    [
                        "QBT_ISP_USABLE_DOWNLOAD_LIMIT_BYTES_PER_SEC",
                        "QBT_MAX_AGGREGATE_DOWNLOAD_LIMIT_BYTES_PER_SEC",
                    ],
                    10,
                ),
            )
            self.assertEqual(
                234,
                self.guard.env_int_first(
                    [
                        "QBT_ISP_USABLE_BURST_DOWNLOAD_LIMIT_BYTES_PER_SEC",
                        "QBT_QUOTA_BURST_DOWNLOAD_LIMIT_BYTES_PER_SEC",
                    ],
                    10,
                ),
            )

    def test_rate_state_reports_monthly_guardrail_before_daily_guardrail(self):
        now = datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)

        state = self.guard.quota_rate_state(
            now,
            usage_bytes=1000,
            day_usage_bytes=100,
            cap_bytes=1000,
            daily_cap_bytes=100,
            headroom=0.95,
            max_download_limit=1000,
        )

        self.assertEqual("monthly UDM quota guardrail reached", state["stop_reason"])

    def test_rate_state_reports_daily_guardrail(self):
        now = datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)

        state = self.guard.quota_rate_state(
            now,
            usage_bytes=500,
            day_usage_bytes=100,
            cap_bytes=1000,
            daily_cap_bytes=100,
            headroom=0.95,
            max_download_limit=1000,
        )

        self.assertEqual("daily UDM quota guardrail reached", state["stop_reason"])


if __name__ == "__main__":
    unittest.main()
