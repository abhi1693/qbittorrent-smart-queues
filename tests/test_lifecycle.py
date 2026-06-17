import importlib
import unittest
from datetime import datetime, timezone


class FakeHealthStore:
    def __init__(self, samples=0, entries=None, stale_age_seconds=0):
        self.samples = samples
        self.entries = entries or {}
        self.stale_age_seconds = stale_age_seconds

    def storage_recovery_no_progress_samples(self, torrent):
        return self.samples

    def stale_stalled_age_seconds(self, torrent, now):
        return self.stale_age_seconds

    def entry(self, item_hash):
        return self.entries.get(item_hash)


class TorrentLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")
        self.now = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)

    def torrent(self, **overrides):
        item = {
            "hash": "abc123",
            "name": "Example.Movie.2026.1080p",
            "state": "stoppedDL",
            "dlspeed": 0,
            "tags": "",
        }
        item.update(overrides)
        return item

    def test_candidate_is_default_lifecycle_for_stopped_torrent(self):
        lifecycle = self.guard.torrent_lifecycle(self.torrent())

        self.assertEqual(self.guard.TORRENT_LIFECYCLE_CANDIDATE, lifecycle.state)
        self.assertTrue(lifecycle.selectable)
        self.assertTrue(lifecycle.retryable)
        self.assertFalse(lifecycle.worker_slot)
        self.assertFalse(lifecycle.listener_slot)

    def test_running_torrent_below_floor_is_selected_worker(self):
        lifecycle = self.guard.torrent_lifecycle(self.torrent(state="downloading", dlspeed=1024))

        self.assertEqual(self.guard.TORRENT_LIFECYCLE_SELECTED_WORKER, lifecycle.state)
        self.assertTrue(lifecycle.worker_slot)
        self.assertFalse(lifecycle.listener_slot)

    def test_productive_torrent_uses_worker_slot(self):
        lifecycle = self.guard.torrent_lifecycle(self.torrent(state="downloading", dlspeed=65_536))

        self.assertEqual(self.guard.TORRENT_LIFECYCLE_PRODUCTIVE, lifecycle.state)
        self.assertTrue(lifecycle.worker_slot)
        self.assertFalse(lifecycle.listener_slot)

    def test_stalled_torrent_is_parked_listener(self):
        lifecycle = self.guard.torrent_lifecycle(self.torrent(state="stalledDL", dlspeed=0))

        self.assertEqual(self.guard.TORRENT_LIFECYCLE_PARKED_LISTENER, lifecycle.state)
        self.assertFalse(lifecycle.worker_slot)
        self.assertTrue(lifecycle.listener_slot)

    def test_no_progress_samples_make_running_torrent_parked_listener(self):
        lifecycle = self.guard.torrent_lifecycle(
            self.torrent(state="downloading", dlspeed=0),
            FakeHealthStore(samples=2),
            self.now,
            required_park_samples=2,
        )

        self.assertEqual(self.guard.TORRENT_LIFECYCLE_PARKED_LISTENER, lifecycle.state)
        self.assertTrue(lifecycle.listener_slot)

    def test_cooldown_lifecycle_is_not_selectable(self):
        lifecycle = self.guard.torrent_lifecycle(
            self.torrent(),
            active_cooldown_tags=["quota-stalled-no-progress-20260601T120000Z"],
            active_cooldown_prefix="quota-stalled",
        )

        self.assertEqual(self.guard.TORRENT_LIFECYCLE_COOLDOWN, lifecycle.state)
        self.assertFalse(lifecycle.selectable)
        self.assertFalse(lifecycle.retryable)

    def test_expired_failure_lifecycle_is_retryable(self):
        lifecycle = self.guard.torrent_lifecycle(
            self.torrent(),
            FakeHealthStore(entries={"abc123": {"failed_attempts": 1}}),
            self.now,
        )

        self.assertEqual(self.guard.TORRENT_LIFECYCLE_RETRYABLE, lifecycle.state)
        self.assertTrue(lifecycle.selectable)
        self.assertTrue(lifecycle.retryable)

    def test_stale_lifecycle_overrides_retryable_candidate(self):
        lifecycle = self.guard.torrent_lifecycle(
            self.torrent(),
            FakeHealthStore(stale_age_seconds=900),
            self.now,
            stale_after_seconds=600,
        )

        self.assertEqual(self.guard.TORRENT_LIFECYCLE_STALE, lifecycle.state)
        self.assertFalse(lifecycle.selectable)
        self.assertFalse(lifecycle.retryable)


if __name__ == "__main__":
    unittest.main()
