import importlib
import unittest
from datetime import datetime, timezone
from unittest import mock


class CooldownParsingTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_stall_cooldown_tag_round_trip(self):
        now = datetime(2026, 6, 1, 12, 34, 56, tzinfo=timezone.utc)

        tag = self.guard.stall_cooldown_tag("quota-stalled", now)

        self.assertEqual("quota-stalled-20260601T123456Z", tag)
        self.assertEqual(now, self.guard.parse_stall_cooldown_tag(tag, "quota-stalled"))

    def test_parse_stall_cooldown_tag_rejects_wrong_prefix_and_bad_date(self):
        self.assertIsNone(
            self.guard.parse_stall_cooldown_tag("other-20260601T123456Z", "quota-stalled")
        )
        self.assertIsNone(
            self.guard.parse_stall_cooldown_tag("quota-stalled-not-a-date", "quota-stalled")
        )

    def test_stall_cooldown_tags_splits_active_and_expired(self):
        now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        torrent = {
            "tags": ",".join([
                "quota-stalled-20260601T113001Z",
                "quota-stalled-20260601T105959Z",
                "manual",
            ]),
        }

        active, expired = self.guard.stall_cooldown_tags(
            torrent,
            "quota-stalled",
            now,
            cooldown_seconds=3600,
        )

        self.assertEqual(["quota-stalled-20260601T113001Z"], active)
        self.assertEqual(["quota-stalled-20260601T105959Z"], expired)

    def test_reasoned_cooldown_tag_round_trip(self):
        now = datetime(2026, 6, 1, 12, 34, 56, tzinfo=timezone.utc)

        tag = self.guard.stall_cooldown_tag("quota-stalled", now, "tracker-dead")
        details = self.guard.parse_stall_cooldown_tag_details(tag, "quota-stalled")

        self.assertEqual("quota-stalled-tracker-dead-20260601T123456Z", tag)
        self.assertEqual(now, self.guard.parse_stall_cooldown_tag(tag, "quota-stalled"))
        self.assertEqual("tracker-dead", details["reason"])
        self.assertEqual(now, details["time"])

    def test_reasoned_cooldown_tags_use_reason_specific_windows(self):
        now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        torrent = {
            "tags": ",".join([
                "quota-stalled-no-progress-20260601T105959Z",
                "quota-stalled-tracker-dead-20260601T070001Z",
                "quota-stalled-tracker-dead-20260601T055959Z",
                "quota-stalled-manual-hold-20260529T120001Z",
            ]),
        }

        active, expired = self.guard.stall_cooldown_tags(
            torrent,
            "quota-stalled",
            now,
            cooldown_seconds=3600,
        )

        self.assertEqual([
            "quota-stalled-manual-hold-20260529T120001Z",
            "quota-stalled-tracker-dead-20260601T070001Z",
        ], sorted(active))
        self.assertEqual([
            "quota-stalled-no-progress-20260601T105959Z",
            "quota-stalled-tracker-dead-20260601T055959Z",
        ], sorted(expired))

    def test_reason_specific_cooldown_window_can_be_overridden(self):
        with mock.patch.dict(
            "os.environ",
            {"QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_TRACKER_DEAD_SECONDS": "120"},
            clear=False,
        ):
            self.assertEqual(
                120,
                self.guard.stall_cooldown_seconds_for_reason(3600, "tracker-dead"),
            )

    def test_tracker_dead_reason_requires_stalled_torrent_without_sources(self):
        self.assertEqual(
            "tracker-dead",
            self.guard.tracker_dead_cooldown_reason({
                "state": "stalledDL",
                "dlspeed": 0,
                "num_seeds": 0,
                "num_complete": 0,
                "availability": 0,
            }),
        )
        self.assertEqual(
            "no-progress",
            self.guard.tracker_dead_cooldown_reason({
                "state": "stalledDL",
                "dlspeed": 0,
                "num_seeds": 1,
                "num_complete": 0,
                "availability": 0,
            }),
        )


if __name__ == "__main__":
    unittest.main()
