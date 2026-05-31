import importlib
import unittest
from datetime import datetime, timezone


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


if __name__ == "__main__":
    unittest.main()
