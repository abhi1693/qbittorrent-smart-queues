import importlib
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock


class FakeHealthStore:
    def __init__(self, scores):
        self.scores = scores

    def score(self, torrent, now):
        return self.scores.get(torrent.get("hash"), 0.0)


class FakeStorageClient:
    def __init__(self, files):
        self.files = files

    def torrent_files(self, item_hash):
        return [dict(item) for item in self.files.get(item_hash, [])]


class FakeStorageGuard:
    require_torrent_fit = True


class CandidateSortingTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")
        self.now = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)

    def torrent(self, item_hash, **overrides):
        torrent = {
            "hash": item_hash,
            "name": "Same.Name.1080p",
            "category": "movies",
            "tags": "",
            "amount_left": 1000,
            "progress": 0.5,
            "availability": 1.0,
            "num_seeds": 1,
            "num_complete": 1,
        }
        torrent.update(overrides)
        return torrent

    def sort(self, torrents, health_store, priority_tags=None, priority_categories=None):
        return sorted(
            torrents,
            key=lambda torrent: self.guard.candidate_sort_key(
                torrent,
                priority_tags or set(),
                priority_categories or set(),
                set(),
                {},
                set(),
                {},
                3,
                1.05,
                health_store,
                self.now,
            ),
        )

    def sort_balanced(self, torrents, health_store):
        return sorted(
            torrents,
            key=lambda torrent: self.guard.candidate_sort_key(
                torrent,
                set(),
                set(),
                set(),
                {},
                set(),
                {},
                3,
                1.05,
                health_store,
                self.now,
                "balanced",
            ),
        )

    def sort_with_movie_queue(self, torrents, health_store, movie_state):
        return sorted(
            torrents,
            key=lambda torrent: self.guard.candidate_sort_key(
                torrent,
                set(),
                set(),
                set(),
                {},
                {"movies"},
                movie_state,
                3,
                1.05,
                health_store,
                self.now,
            ),
        )

    def sort_balanced_with_movie_queue(self, torrents, health_store, movie_state):
        return sorted(
            torrents,
            key=lambda torrent: self.guard.candidate_sort_key(
                torrent,
                set(),
                set(),
                set(),
                {},
                {"movies"},
                movie_state,
                3,
                1.05,
                health_store,
                self.now,
                "balanced",
            ),
        )

    def test_priority_tag_sorts_before_healthier_nonpriority_candidate(self):
        priority = self.torrent(
            "priority",
            tags="priority",
            availability=0.1,
            num_seeds=0,
            num_complete=0,
        )
        ordinary = self.torrent(
            "ordinary",
            availability=10.0,
            num_seeds=20,
            num_complete=20,
        )
        store = FakeHealthStore({"priority": -50.0, "ordinary": 100.0})

        ordered = self.sort([ordinary, priority], store, priority_tags={"priority"})

        self.assertEqual(["priority", "ordinary"], [item["hash"] for item in ordered])

    def test_health_score_breaks_otherwise_equal_candidates(self):
        weak = self.torrent("weak")
        strong = self.torrent("strong")
        store = FakeHealthStore({"weak": 0.0, "strong": 75.0})

        ordered = self.sort([weak, strong], store)

        self.assertEqual(["strong", "weak"], [item["hash"] for item in ordered])

    def test_tracker_health_breaks_candidates_with_equal_score(self):
        unhealthy = self.torrent("unhealthy", availability=0.25, num_seeds=0, num_complete=0)
        healthy = self.torrent("healthy", availability=1.5, num_seeds=0, num_complete=0)
        store = FakeHealthStore({})

        ordered = self.sort([unhealthy, healthy], store)

        self.assertEqual(["healthy", "unhealthy"], [item["hash"] for item in ordered])

    def test_persisted_tracker_response_health_breaks_equal_candidates(self):
        dead = self.torrent("dead", availability=1.0, num_seeds=0, num_complete=0)
        healthy = self.torrent("healthy", availability=1.0, num_seeds=0, num_complete=0)
        with tempfile.TemporaryDirectory() as tmpdir:
            env = {
                "QBT_TORRENT_HEALTH_STATE_PATH": f"{tmpdir}/health.json",
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
                "QBT_TRACKER_HEALTH_SCORING_ENABLED": "true",
            }
            with mock.patch.dict("os.environ", env, clear=False):
                store = self.guard.TorrentHealthStore()
                store.record_tracker_health(dead, [
                    {"url": "udp://dead.example/announce", "status": 4, "num_peers": 0, "num_seeds": 0},
                ], self.now)
                store.record_tracker_health(healthy, [
                    {
                        "url": "udp://ok.example/announce",
                        "status": 2,
                        "num_peers": 18,
                        "num_seeds": 8,
                        "num_leeches": 10,
                    },
                ], self.now)

                ordered = self.sort([dead, healthy], store)

        self.assertEqual(["healthy", "dead"], [item["hash"] for item in ordered])

    def test_tracker_health_observation_is_bounded_and_cached(self):
        class TrackerClient:
            def __init__(self):
                self.calls = []

            def torrent_trackers(self, item_hash):
                self.calls.append(item_hash)
                return [{"url": f"udp://{item_hash}.example/announce", "status": 2, "num_peers": 3}]

        torrents = [self.torrent("one"), self.torrent("two"), self.torrent("three")]
        client = TrackerClient()
        with tempfile.TemporaryDirectory() as tmpdir:
            env = {
                "QBT_TORRENT_HEALTH_STATE_PATH": f"{tmpdir}/health.json",
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
                "QBT_TRACKER_HEALTH_SCORING_ENABLED": "true",
            }
            with mock.patch.dict("os.environ", env, clear=False):
                store = self.guard.TorrentHealthStore()

                observed = store.observe_tracker_health(
                    client,
                    torrents,
                    self.now,
                    max_torrents=2,
                    min_refresh_seconds=300,
                )
                observed_again = store.observe_tracker_health(
                    client,
                    torrents,
                    self.now,
                    max_torrents=2,
                    min_refresh_seconds=300,
                )
                observed_third_time = store.observe_tracker_health(
                    client,
                    torrents,
                    self.now,
                    max_torrents=2,
                    min_refresh_seconds=300,
                )

        self.assertEqual(2, observed)
        self.assertEqual(1, observed_again)
        self.assertEqual(0, observed_third_time)
        self.assertEqual(["one", "two", "three"], client.calls)

    def test_balanced_strategy_prefers_near_complete_candidate(self):
        early = self.torrent("early", progress=0.25, amount_left=80 * 1024 * 1024 * 1024)
        almost_done = self.torrent("almost-done", progress=0.94, amount_left=5 * 1024 * 1024 * 1024)
        store = FakeHealthStore({"early": 40.0, "almost-done": 0.0})

        ordered = self.sort_balanced([early, almost_done], store)

        self.assertEqual(["almost-done", "early"], [item["hash"] for item in ordered])

    def test_balanced_strategy_still_keeps_priority_tier_first(self):
        priority = self.torrent("priority", tags="priority", progress=0.05, amount_left=90 * 1024 * 1024 * 1024)
        almost_done = self.torrent("almost-done", progress=0.99, amount_left=1024 * 1024 * 1024)
        store = FakeHealthStore({"priority": -50.0, "almost-done": 100.0})

        ordered = sorted(
            [almost_done, priority],
            key=lambda torrent: self.guard.candidate_sort_key(
                torrent,
                {"priority"},
                set(),
                set(),
                {},
                set(),
                {},
                3,
                1.05,
                store,
                self.now,
                "balanced",
            ),
        )

        self.assertEqual(["priority", "almost-done"], [item["hash"] for item in ordered])

    def test_candidate_score_exposes_unified_components(self):
        gib = 1024 * 1024 * 1024
        torrent = self.torrent(
            "score",
            category="movies",
            tags="priority",
            progress=0.92,
            amount_left=512 * 1024 * 1024,
            eta=3600,
            availability=2.0,
            num_seeds=3,
            num_complete=7,
        )
        movie_state = {
            "orders": {
                "score": {
                    "title": "score movie",
                    "movie_id": 1,
                    "year": 2026,
                    "queue_position": 3,
                    "source": "radarr",
                },
            },
        }
        storage_state = {
            "enabled": True,
            "stop": True,
            "reason": "reserve reached",
            "free_bytes": 2 * gib,
            "reserve_bytes": 3 * gib,
            "headroom_bytes": 0,
        }

        score = self.guard.candidate_score(
            torrent,
            {"priority"},
            set(),
            set(),
            {},
            {"movies"},
            movie_state,
            3,
            1.05,
            FakeHealthStore({"score": 12.5}),
            self.now,
            strategy="balanced",
            storage_client=FakeStorageClient({
                "score": [
                    {"name": "score.mkv", "size": gib, "progress": 0.5, "priority": 1},
                ],
            }),
            storage_guard=FakeStorageGuard(),
            storage_state=storage_state,
            active_cooldown_tags={"quota-stalled-no-progress-29990101T000000Z"},
            active_cooldown_prefix="quota-stalled",
        )
        score_dict = score.as_dict()
        components = score_dict["components"]

        for name in (
            "availability",
            "content_total",
            "cooldown",
            "eta",
            "health",
            "near_complete",
            "priority",
            "progress",
            "queue_order",
            "remaining",
            "sources",
            "storage_fit",
            "storage_remaining",
        ):
            self.assertIn(name, components)
        self.assertEqual(1000.0, components["priority"])
        self.assertEqual(-1000.0, components["cooldown"])
        self.assertEqual(100.0, components["storage_fit"])
        self.assertLess(components["queue_order"], 0)
        self.assertTrue(score_dict["storage_fits"])
        self.assertEqual(512 * 1024 * 1024, score_dict["storage_remaining_bytes"])
        expected_total = sum(
            score.components[name]
            for name in (
                "content_total",
                "priority",
                "queue_order",
                "cooldown",
                "storage_fit",
                "storage_remaining",
            )
        )
        self.assertAlmostEqual(expected_total, score.total)

    def test_preemption_uses_balanced_score_margin(self):
        current = self.torrent(
            "current",
            state="downloading",
            dlspeed=900_000,
            progress=0.25,
            amount_left=60 * 1024 * 1024 * 1024,
        )
        challenger = self.torrent(
            "challenger",
            state="stoppedDL",
            progress=0.95,
            amount_left=4 * 1024 * 1024 * 1024,
        )
        store = FakeHealthStore({"current": 20.0, "challenger": 0.0})

        should_preempt = self.guard.should_preempt_productive_torrent(
            current,
            challenger,
            store,
            self.now,
            20.0,
        )

        self.assertTrue(should_preempt)

    def test_radarr_movie_queue_position_breaks_before_health_score(self):
        first_in_radarr = self.torrent("first")
        healthier_later = self.torrent("later")
        movie_state = {
            "orders": {
                "first": {
                    "title": "first movie",
                    "movie_id": 1,
                    "year": 2026,
                    "queue_position": 0,
                    "source": "radarr",
                },
                "later": {
                    "title": "later movie",
                    "movie_id": 2,
                    "year": 2026,
                    "queue_position": 5,
                    "source": "radarr",
                },
            }
        }
        store = FakeHealthStore({"first": -25.0, "later": 100.0})

        ordered = self.sort_with_movie_queue([healthier_later, first_in_radarr], store, movie_state)

        self.assertEqual(["first", "later"], [item["hash"] for item in ordered])

    def test_balanced_strategy_uses_health_before_movie_queue_position(self):
        first_in_radarr = self.torrent("first", progress=0.05, amount_left=90 * 1024 * 1024 * 1024)
        healthier_later = self.torrent("later", progress=0.75, amount_left=2 * 1024 * 1024 * 1024)
        movie_state = {
            "orders": {
                "first": {
                    "title": "first movie",
                    "movie_id": 1,
                    "year": 2026,
                    "queue_position": 0,
                    "source": "radarr",
                },
                "later": {
                    "title": "later movie",
                    "movie_id": 2,
                    "year": 2026,
                    "queue_position": 5,
                    "source": "radarr",
                },
            }
        }
        store = FakeHealthStore({"first": -25.0, "later": 100.0})

        ordered = self.sort_balanced_with_movie_queue([healthier_later, first_in_radarr], store, movie_state)

        self.assertEqual(["later", "first"], [item["hash"] for item in ordered])


if __name__ == "__main__":
    unittest.main()
