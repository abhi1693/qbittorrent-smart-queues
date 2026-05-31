import contextlib
import importlib
import io
import unittest
from unittest import mock


class TvParsingTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_parse_sxxexx_episode_order(self):
        parsed = self.guard.parse_tv_episode_order("Alpha.Show.S02E03.1080p.WEB")

        self.assertEqual({
            "series": "alpha show",
            "season": 2,
            "episode": 3,
            "season_pack": False,
        }, parsed)

    def test_parse_2x_episode_order_and_normalizes_indexer_noise(self):
        parsed = self.guard.parse_tv_episode_order("www.UIndex.org - Beta Show 2x07 HDTV")

        self.assertEqual({
            "series": "beta show",
            "season": 2,
            "episode": 7,
            "season_pack": False,
        }, parsed)

    def test_parse_season_pack(self):
        parsed = self.guard.parse_tv_episode_order("Gamma.Show.S04.2160p.BluRay")

        self.assertEqual({
            "series": "gamma show",
            "season": 4,
            "episode": 0,
            "season_pack": True,
        }, parsed)

    def test_queue_record_episode_order_uses_earliest_episode(self):
        record = {
            "episodes": [
                {"seasonNumber": 2, "episodeNumber": 4},
                {"seasonNumber": 1, "episodeNumber": 10},
            ],
        }

        self.assertEqual((1, 10, True), self.guard.queue_record_episode_order(record))

    def test_queue_record_series_falls_back_to_source_title(self):
        record = {"sourceTitle": "Delta.Show.S01E01.1080p"}

        self.assertEqual("delta show", self.guard.queue_record_series_title(record))

    def test_sonarr_queue_load_accepts_raw_list_response(self):
        queue = self.guard.SonarrQueueMetadata.__new__(self.guard.SonarrQueueMetadata)
        queue.timeout = 10
        queue.by_download_id = {}
        queue.by_title = {}
        record = {
            "downloadId": "ABC-123",
            "sourceTitle": "Epsilon.Show.S01E02.1080p",
            "series": {"title": "Epsilon Show"},
            "episode": {"seasonNumber": 1, "episodeNumber": 2},
        }

        with mock.patch.object(self.guard, "request_json", return_value=([record], object())):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                queue.load_queue("sonarr", "http://sonarr.test", "api-key")

        self.assertEqual("epsilon show", queue.by_download_id["abc123"]["series"])
        self.assertEqual(2, queue.by_download_id["abc123"]["episode"])


if __name__ == "__main__":
    unittest.main()
