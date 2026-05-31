import importlib
import os
import unittest


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


if __name__ == "__main__":
    unittest.main()
