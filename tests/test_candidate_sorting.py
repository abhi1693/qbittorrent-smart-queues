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


if __name__ == "__main__":
    unittest.main()
