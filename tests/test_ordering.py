import contextlib
import importlib
import io
import os
import unittest


class FakeWatchMetadata:
    def __init__(self, priorities):
        self.priorities = priorities
        self.enabled = True

    def torrent_watch_priority(self, torrent, order):
        key = (order.get("series"), order.get("season"))
        if not self.guard.tv_order_is_full_season_pack(torrent, order):
            return None
        return self.priorities.get(key)


class FakeFilePriorityClient:
    def __init__(self, files):
        self.files = files
        self.calls = []

    def torrent_files(self, item_hash):
        return self.files

    def set_file_priority(self, item_hash, file_ids, priority):
        self.calls.append((item_hash, list(file_ids), priority))


class TvOrderingTests(unittest.TestCase):
    def setUp(self):
        os.environ["QBT_TV_QUEUE_SONARR_ENABLED"] = "false"
        os.environ["QBT_MOVIE_QUEUE_RADARR_ENABLED"] = "false"
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_groups_series_before_next_series(self):
        queue = self.guard.SonarrQueueMetadata()
        torrents = [
            {"hash": "a1", "name": "Alpha.S01E01.1080p", "category": "tv", "amount_left": 10, "progress": 0.0},
            {"hash": "b1", "name": "Beta.S01E01.1080p", "category": "tv", "amount_left": 10, "progress": 0.0},
            {"hash": "a2", "name": "Alpha.S01E02.1080p", "category": "tv", "amount_left": 10, "progress": 0.0},
            {"hash": "b2", "name": "Beta.S01E02.1080p", "category": "tv", "amount_left": 10, "progress": 0.0},
        ]

        state = self.guard.build_tv_order_state(torrents, {"tv"}, queue)
        ordered = sorted(torrents, key=lambda item: self.guard.tv_episode_order_key(item, {"tv"}, state))

        self.assertEqual(["a1", "a2", "b1", "b2"], [item["hash"] for item in ordered])

    def test_sonarr_queue_position_selects_series(self):
        queue = self.guard.SonarrQueueMetadata()
        queue.by_download_id["b1"] = {
            "series": "beta",
            "season": 1,
            "episode": 1,
            "season_pack": False,
            "queue_position": 0,
            "source": "sonarr",
        }
        queue.by_download_id["b2"] = {
            "series": "beta",
            "season": 1,
            "episode": 2,
            "season_pack": False,
            "queue_position": 1,
            "source": "sonarr",
        }
        queue.by_download_id["a1"] = {
            "series": "alpha",
            "season": 1,
            "episode": 1,
            "season_pack": False,
            "queue_position": 5,
            "source": "sonarr",
        }
        queue.by_download_id["a2"] = {
            "series": "alpha",
            "season": 1,
            "episode": 2,
            "season_pack": False,
            "queue_position": 6,
            "source": "sonarr",
        }
        torrents = [
            {"hash": "a1", "name": "Alpha.S01E01.1080p", "category": "tv"},
            {"hash": "b1", "name": "Beta.S01E01.1080p", "category": "tv"},
            {"hash": "a2", "name": "Alpha.S01E02.1080p", "category": "tv"},
            {"hash": "b2", "name": "Beta.S01E02.1080p", "category": "tv"},
        ]

        state = self.guard.build_tv_order_state(torrents, {"tv"}, queue)
        ordered = sorted(torrents, key=lambda item: self.guard.tv_episode_order_key(item, {"tv"}, state))

        self.assertEqual(["b1", "b2", "a1", "a2"], [item["hash"] for item in ordered])

    def test_radarr_queue_position_orders_movies(self):
        queue = self.guard.RadarrQueueMetadata()
        queue.by_download_id["b1"] = {
            "title": "beta movie",
            "movie_id": 200,
            "year": 2026,
            "queue_position": 0,
            "source": "radarr",
        }
        queue.by_download_id["a1"] = {
            "title": "alpha movie",
            "movie_id": 100,
            "year": 2026,
            "queue_position": 8,
            "source": "radarr",
        }
        torrents = [
            {"hash": "a1", "name": "Alpha.Movie.2026.1080p", "category": "movies"},
            {"hash": "b1", "name": "Beta.Movie.2026.1080p", "category": "movies"},
        ]

        state = self.guard.build_movie_order_state(torrents, {"movies"}, queue)
        ordered = sorted(torrents, key=lambda item: self.guard.movie_queue_order_key(item, {"movies"}, state))

        self.assertEqual(["b1", "a1"], [item["hash"] for item in ordered])

    def test_jellyfin_watch_priority_boosts_matching_season_pack_only(self):
        queue = self.guard.SonarrQueueMetadata()
        watch = FakeWatchMetadata({
            ("beta", 1): {
                "series": "beta",
                "season": 1,
                "episode": 3,
                "next_episode": 4,
                "rank": (0, -1000, 0),
                "source": "jellyfin-active-session",
            },
        })
        watch.guard = self.guard
        torrents = [
            {"hash": "a", "name": "Alpha.S01.1080p", "category": "tv"},
            {"hash": "b", "name": "Beta.S01.1080p", "category": "tv"},
            {"hash": "b-single", "name": "Beta.S01E04.1080p", "category": "tv"},
        ]

        state = self.guard.build_tv_order_state(torrents, {"tv"}, queue, watch)
        ordered = sorted(torrents, key=lambda item: self.guard.tv_episode_order_key(item, {"tv"}, state))

        self.assertEqual(["b", "a", "b-single"], [item["hash"] for item in ordered])
        self.assertIn("b", state["watch_priorities"])
        self.assertNotIn("b-single", state["watch_priorities"])

    def test_watched_season_pack_file_priority_starts_at_next_episode(self):
        client = FakeFilePriorityClient([
            {"index": 1, "name": "Beta.S01E01.mkv", "priority": 1, "progress": 0.0},
            {"index": 2, "name": "Beta.S01E02.mkv", "priority": 1, "progress": 0.0},
            {"index": 3, "name": "Beta.S01E03.mkv", "priority": 1, "progress": 0.0},
            {"index": 4, "name": "Beta.S01E04.mkv", "priority": 1, "progress": 0.0},
            {"index": 5, "name": "Beta.S01E05.mkv", "priority": 1, "progress": 0.0},
        ])

        with contextlib.redirect_stdout(io.StringIO()):
            self.guard.apply_tv_episode_file_priorities(
                client,
                {"hash": "season-pack", "name": "Beta.S01.1080p", "category": "tv"},
                {"tv"},
                True,
                2,
                {
                    "series": "beta",
                    "season": 1,
                    "episode": 3,
                    "next_episode": 4,
                    "rank": (0, -1000, 0),
                },
            )

        self.assertIn(("season-pack", [5], self.guard.QBT_FILE_PRIORITY_HIGH), client.calls)
        self.assertIn(("season-pack", [4], self.guard.QBT_FILE_PRIORITY_MAXIMUM), client.calls)


if __name__ == "__main__":
    unittest.main()
