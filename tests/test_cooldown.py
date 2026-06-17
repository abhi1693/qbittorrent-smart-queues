import importlib
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
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

    def test_no_progress_backoff_extends_cooldown_and_decays_on_progress(self):
        now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        torrent = {
            "hash": "abc123",
            "name": "Backoff.Show.S01E01",
            "category": "tv",
            "state": "stalledDL",
            "amount_left": 1000,
            "downloaded": 0,
            "progress": 0.5,
            "tags": "quota-stalled-no-progress-20260601T103000Z",
        }
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.dict(
            os.environ,
            {
                "QBT_TORRENT_HEALTH_STATE_PATH": os.path.join(tmpdir, "health.json"),
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
                "QBT_SINGLE_DOWNLOAD_STALL_BACKOFF_ENABLED": "true",
                "QBT_SINGLE_DOWNLOAD_STALL_BACKOFF_MULTIPLIER": "2",
                "QBT_SINGLE_DOWNLOAD_STALL_BACKOFF_MAX_SECONDS": "86400",
                "QBT_SINGLE_DOWNLOAD_STALL_BACKOFF_DECAY_STEPS": "1",
            },
            clear=False,
        ):
            store = self.guard.TorrentHealthStore()

            store.record_failure(torrent, now, "did not make progress", cooldown_reason="no-progress")
            self.assertEqual(1, store.no_progress_backoff_level(torrent))
            self.assertEqual(3600, store.cooldown_seconds_for_torrent(torrent, 3600, "no-progress"))

            store.record_failure(
                torrent,
                now + timedelta(minutes=5),
                "did not make progress",
                cooldown_reason="no-progress",
            )
            self.assertEqual(2, store.no_progress_backoff_level(torrent))
            self.assertEqual(7200, store.cooldown_seconds_for_torrent(torrent, 3600, "no-progress"))
            self.assertIn("backoff level 2", store.summary(torrent, now))

            active, expired = self.guard.stall_cooldown_tags(
                torrent,
                "quota-stalled",
                now,
                3600,
                health_store=store,
            )
            self.assertEqual(["quota-stalled-no-progress-20260601T103000Z"], active)
            self.assertEqual([], expired)

            after = dict(torrent)
            after["amount_left"] = 0
            after["downloaded"] = 1000
            after["progress"] = 1.0
            store.record_productive(torrent, after, now + timedelta(minutes=10), sample_seconds=60)
            self.assertEqual(1, store.no_progress_backoff_level(torrent))
            self.assertEqual(3600, store.cooldown_seconds_for_torrent(torrent, 3600, "no-progress"))

    def test_record_failure_writes_canonical_cooldown_state(self):
        now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        torrent = {
            "hash": "abc123",
            "name": "Canonical.Cooldown.S01E01",
            "category": "tv",
            "state": "stalledDL",
            "amount_left": 1000,
            "downloaded": 0,
            "progress": 0.5,
            "tags": "",
        }
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.dict(
            os.environ,
            {
                "QBT_TORRENT_HEALTH_STATE_PATH": os.path.join(tmpdir, "health.json"),
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
                "QBT_SINGLE_DOWNLOAD_STALL_COOLDOWN_SECONDS": "120",
            },
            clear=False,
        ):
            store = self.guard.TorrentHealthStore()
            store.record_failure(torrent, now, "did not make progress", cooldown_reason="no-progress")

            state = store.active_cooldown_state(torrent, now, scope="normal")
            self.assertEqual("no-progress", state["reason"])
            self.assertEqual("normal", state["scope"])
            self.assertEqual(1, state["failure_count"])
            self.assertEqual(self.guard.format_utc(now), state["first_seen_at"])
            self.assertEqual(self.guard.format_utc(now), state["last_tried_at"])
            self.assertEqual(self.guard.format_utc(now + timedelta(seconds=120)), state["next_retry_at"])
            self.assertEqual(120, state["cooldown_seconds"])

            after = dict(torrent, amount_left=0, downloaded=1000, progress=1.0)
            store.record_productive(torrent, after, now + timedelta(seconds=30), sample_seconds=30)
            self.assertEqual({}, store.active_cooldown_state(torrent, now + timedelta(seconds=30), scope="normal"))

    def test_storage_cooldown_is_not_normal_cooldown_state(self):
        now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        torrent = {"hash": "abc123", "name": "Storage.Cooldown", "amount_left": 1000}
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.dict(
            os.environ,
            {
                "QBT_TORRENT_HEALTH_STATE_PATH": os.path.join(tmpdir, "health.json"),
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
            },
            clear=False,
        ):
            store = self.guard.TorrentHealthStore()
            store.record_failure(
                torrent,
                now,
                "did not make progress",
                cooldown_reason="no-progress",
                cooldown_scope="storage",
            )

            self.assertEqual({}, store.active_cooldown_state(torrent, now, scope="normal"))
            self.assertEqual("storage", store.active_cooldown_state(torrent, now, scope="storage")["scope"])

    def test_no_progress_backoff_is_capped(self):
        now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        torrent = {"hash": "abc123", "name": "Backoff.Show.S01E02", "amount_left": 1000}
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.dict(
            os.environ,
            {
                "QBT_TORRENT_HEALTH_STATE_PATH": os.path.join(tmpdir, "health.json"),
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
                "QBT_SINGLE_DOWNLOAD_STALL_BACKOFF_MULTIPLIER": "3",
                "QBT_SINGLE_DOWNLOAD_STALL_BACKOFF_MAX_SECONDS": "10000",
            },
            clear=False,
        ):
            store = self.guard.TorrentHealthStore()
            for offset in range(5):
                store.record_failure(
                    torrent,
                    now + timedelta(minutes=offset),
                    "did not make progress",
                    cooldown_reason="no-progress",
                )

            self.assertEqual(5, store.no_progress_backoff_level(torrent))
            self.assertEqual(10000, store.cooldown_seconds_for_torrent(torrent, 3600, "no-progress"))


if __name__ == "__main__":
    unittest.main()
