import importlib
import unittest
from datetime import datetime, timezone


class FakeHealthStore:
    def __init__(self, scores):
        self.scores = scores

    def score(self, torrent, now):
        return self.scores.get(torrent.get("hash"), 0.0)


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


if __name__ == "__main__":
    unittest.main()
