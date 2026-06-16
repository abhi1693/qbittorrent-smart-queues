import importlib
import unittest
from datetime import datetime, timezone


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
