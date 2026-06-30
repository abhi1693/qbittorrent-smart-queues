import importlib
import unittest
from datetime import datetime, timezone
from unittest import mock


class FakeHealthStore:
    def __init__(self, scores=None, cooldown_hashes=None, no_progress_hashes=None):
        self.scores = scores or {}
        self.cooldown_hashes = set(cooldown_hashes or [])
        self.no_progress_hashes = set(no_progress_hashes or [])
        self.observed_torrents = []
        self.progress_classifications = []

    def observe_torrents(self, torrents, now):
        self.observed_torrents.append((list(torrents), now))

    def observe_tracker_health(self, client, torrents, now, max_candidates, min_refresh_seconds):
        return len(torrents[:max_candidates]) if max_candidates > 0 else 0

    def score(self, torrent, now):
        return self.scores.get(torrent.get("hash"), 0.0)

    def recent_progress_class(self, torrent, now):
        return ""

    def entry(self, item_hash):
        return None

    def active_cooldown_state(self, torrent, now, scope="normal"):
        if torrent.get("hash") not in self.cooldown_hashes:
            return {}
        return {
            "reason": "no-progress",
            "remaining_seconds": 300,
            "next_retry_at": "2026-06-01T12:05:00Z",
        }

    def storage_recovery_no_progress_samples(self, torrent):
        return 2 if torrent.get("hash") in self.no_progress_hashes else 0

    def record_progress_classification(self, torrent, now, classification):
        self.progress_classifications.append((torrent, now, classification))


class FakeClient:
    def __init__(self, files=None):
        self.files = files or {}
        self.removed_tags = []

    def torrent_files(self, item_hash):
        return [dict(item) for item in self.files.get(item_hash, [])]

    def remove_tags(self, hashes, tags):
        self.removed_tags.append((list(hashes), list(tags)))


class PolicyEngineTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")
        self.now = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)

    def torrent(self, item_hash, **overrides):
        torrent = {
            "hash": item_hash,
            "name": f"{item_hash}.Movie.1080p",
            "category": "movies",
            "state": "stoppedDL",
            "dlspeed": 0,
            "amount_left": 1000,
            "downloaded": 100,
            "progress": 0.5,
            "availability": 1.0,
            "num_seeds": 1,
            "num_complete": 1,
            "tags": "",
        }
        torrent.update(overrides)
        return torrent

    def engine(
        self,
        torrents,
        health_store=None,
        attempted_hashes=None,
        client=None,
        storage_guard=None,
        storage_state=None,
    ):
        return self.guard.SmartQueuePolicyEngine(
            client=client or FakeClient(),
            torrents=torrents,
            health_store=health_store or FakeHealthStore(),
            now=self.now,
            storage_guard=storage_guard,
            storage_state=storage_state,
            storage_constrained_mode=False,
            min_progress=0.0,
            max_remaining_bytes=0,
            categories=set(),
            tracker_health_max_candidates=50,
            tracker_health_min_refresh_seconds=300,
            tv_order_categories=set(),
            movie_order_categories=set(),
            sonarr_queue=None,
            jellyfin_watch=None,
            radarr_queue=None,
            priority_tags={"priority"},
            priority_categories=set(),
            healthy_min_seeds=3,
            healthy_min_availability=1.05,
            selection_strategy_name="balanced",
            attempted_hashes=attempted_hashes or set(),
            stall_tag_prefix="quota-stalled",
            stall_cooldown_seconds=3600,
            storage_recovery_max_active=5,
            storage_recovery_stall_samples=2,
            storage_recovery_max_parked_stalled=10,
            storage_recovery_min_rate_bytes=65_536,
            park_stalled_downloads_enabled=True,
            park_stalled_samples=2,
            max_parked_stalled_downloads=0,
            normal_worker_limit=1,
            productive_rate_floor_bytes=65_536,
            normal_progress_min_bytes=lambda torrent: 1,
        )

    def test_policy_stages_are_independently_testable(self):
        priority = self.torrent("priority", tags="priority")
        normal = self.torrent("normal")
        complete = self.torrent("complete", progress=1.0)
        health_store = FakeHealthStore(scores={"priority": 10.0, "normal": 5.0})
        engine = self.engine([normal, complete, priority], health_store=health_store)

        observation = engine.observe()
        self.assertEqual(self.guard.SMART_QUEUE_POLICY_STAGES, engine.stages)
        self.assertEqual(1, len(health_store.observed_torrents))

        classification = engine.classify(observation)
        self.assertEqual(["normal", "priority"], [
            torrent["hash"] for torrent in classification.eligible_torrents
        ])
        self.assertEqual(1, classification.rejected_counts["complete"])
        self.assertEqual(2, classification.tracker_health_observed)

        filters = engine.filter(observation, classification)
        self.assertEqual(2, len(filters.available_candidates))

        scoring = engine.score(observation, classification, filters)
        self.assertEqual(["priority"], [
            torrent["hash"] for torrent in scoring.priority_candidates
        ])
        self.assertEqual(["priority"], [
            torrent["hash"] for torrent in scoring.selection_candidates
        ])

        allocation = engine.allocate_slots(observation, scoring)
        self.assertEqual(1, allocation.slot_plan.worker_slots)
        self.assertEqual(0, allocation.slot_plan.parked_listener_slots)

        action = engine.act(observation, filters, scoring)
        self.assertEqual("try_candidate", action.action)

        result = engine.record(observation, classification, filters, scoring, allocation, action)
        self.assertEqual(7, result.candidate_counts["policy_stage_count"])
        self.assertEqual("try_candidate", result.action_plan.action)
        self.assertNotIn("policy_action", result.candidate_counts)
        self.assertEqual(1, result.candidate_counts["selection_pool"])
        self.assertEqual(1, result.rejected_counts["deferred_by_priority"])

    def test_filter_stage_keeps_cooldown_out_of_available_candidates(self):
        cooling = self.torrent("cooling")
        ready = self.torrent("ready")
        health_store = FakeHealthStore(cooldown_hashes={"cooling"})
        engine = self.engine([cooling, ready], health_store=health_store)

        observation = engine.observe()
        classification = engine.classify(observation)
        filters = engine.filter(observation, classification)

        self.assertEqual(["ready"], [
            torrent["hash"] for torrent in filters.available_candidates
        ])
        self.assertEqual(1, filters.cooldown_count)
        self.assertEqual(1, classification.rejected_counts["cooldown"])
        self.assertEqual(1, classification.rejected_counts["cooldown_no_progress"])

    def test_over_cap_parked_listeners_are_not_selected_as_workers(self):
        parked = self.torrent("parked", state="stalledDL")
        deferred = self.torrent("deferred", state="stalledDL")
        ready = self.torrent("ready")
        health_store = FakeHealthStore(no_progress_hashes={"parked", "deferred"})
        engine = self.engine([parked, deferred, ready], health_store=health_store)
        engine.max_parked_stalled_downloads = 1

        observation = engine.observe()
        classification = engine.classify(observation)
        filters = engine.filter(observation, classification)
        scoring = engine.score(observation, classification, filters)

        parked_hashes = [
            torrent["hash"] for torrent in scoring.normal_parked_stalled_torrents
        ]
        self.assertEqual(1, len(parked_hashes))
        self.assertIn(parked_hashes[0], {"parked", "deferred"})
        self.assertEqual(1, scoring.normal_parked_stalled_deferred_count)
        self.assertEqual(["ready"], [
            torrent["hash"] for torrent in scoring.selection_candidates
        ])
        self.assertEqual(1, classification.rejected_counts["deferred_parked_stalled"])

    def test_storage_pressure_sorts_fitting_candidates_by_storage_score(self):
        class StorageGuard:
            require_torrent_fit = True

        storage_state = {
            "enabled": True,
            "stop": False,
            "reason": "pressure",
            "free_bytes": 20_000,
            "reserve_bytes": 1_000,
            "headroom_bytes": 10_000,
        }
        small = self.torrent(
            "small",
            name="Small.Movie.1080p",
            amount_left=2_000,
            progress=0.95,
            availability=3.0,
            num_seeds=10,
            num_complete=10,
        )
        large = self.torrent(
            "large",
            name="Large.Movie.1080p",
            amount_left=8_000,
            progress=0.10,
            availability=2.0,
            num_seeds=5,
            num_complete=5,
        )
        blocked = self.torrent("blocked", name="Blocked.Movie.1080p", amount_left=40_000)
        client = FakeClient(
            files={
                "small": [{"name": "small.mkv", "size": 10_000, "progress": 0.8, "priority": 1}],
                "large": [{"name": "large.mkv", "size": 10_000, "progress": 0.2, "priority": 1}],
                "blocked": [{"name": "blocked.mkv", "size": 50_000, "progress": 0.0, "priority": 1}],
            }
        )
        engine = self.engine(
            [blocked, large, small],
            client=client,
            storage_guard=StorageGuard(),
            storage_state=storage_state,
        )

        with mock.patch.dict(
            "os.environ",
            {
                "QBT_DOWNLOAD_STORAGE_PRESSURE_MIN_BLOCKED": "1",
                "QBT_DOWNLOAD_STORAGE_PRESSURE_BLOCKED_FRACTION": "0.25",
            },
            clear=False,
        ):
            observation = engine.observe()
            classification = engine.classify(observation)
            filters = engine.filter(observation, classification)

        self.assertTrue(filters.storage_pressure_mode)
        self.assertEqual(["small", "large"], [
            torrent["hash"] for torrent in filters.candidates
        ])
        self.assertEqual(1, filters.storage_blocked_count)
        self.assertEqual(1, classification.rejected_counts["storage_headroom"])


if __name__ == "__main__":
    unittest.main()
