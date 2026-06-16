import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock


class FakeQbtClient:
    def __init__(self):
        self.deleted = []
        self.added_tags = []
        self.reannounced = []
        self.stopped = []

    def delete_hashes(self, hashes, delete_files):
        self.deleted.append((list(hashes), delete_files))

    def add_tags(self, hashes, tags):
        self.added_tags.append((list(hashes), list(tags)))

    def reannounce_hashes(self, hashes):
        self.reannounced.append(list(hashes))

    def stop_hashes(self, hashes):
        self.stopped.append(list(hashes))


class StaticQueue:
    def __init__(self, metadata, configs=None):
        self.metadata = metadata
        self._configs = configs or [("sonarr", "http://arr.test", "api-key")]

    def torrent_metadata(self, torrent):
        return self.metadata

    def configs(self):
        return list(self._configs)


class StaleTorrentMaintenanceTests(unittest.TestCase):
    def setUp(self):
        from qbittorrent_smart_queues import guard

        self.guard = guard

    def test_completed_sonarr_already_imported_torrent_is_removed_from_queue(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "abc123",
            "name": "Already Imported Show S01",
            "state": "stoppedUP",
            "progress": 1,
            "amount_left": 0,
            "category": "tv",
        }
        sonarr = StaticQueue({
            "queue_id": 42,
            "source": "sonarr",
            "series_id": 7,
            "season": 1,
            "episode": 1,
            "season_pack": False,
            "episode_ids": [1001],
            "status_messages": ["Episode file already imported"],
            "status_text": "warning importBlocked Episode file already imported",
        })
        radarr = StaticQueue(None, configs=[])

        def fake_request_json(opener, method, url, **kwargs):
            if method == "GET" and "/api/v3/episode/1001" in url:
                return {
                    "id": 1001,
                    "seriesId": 7,
                    "seasonNumber": 1,
                    "episodeNumber": 1,
                    "episodeFileId": 5001,
                    "episodeFile": {"id": 5001, "path": "/tv/Already Imported Show/Season 01/S01E01.mkv"},
                }, object()
            if method == "DELETE" and "/api/v3/queue/42?" in url:
                return {}, object()
            raise AssertionError(f"unexpected request {method} {url}")

        with mock.patch.object(self.guard, "request_json", side_effect=fake_request_json) as request_json:
            self.guard.cleanup_arr_managed_completed_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
                delete_files=True,
            )

        self.assertEqual(2, request_json.call_count)
        urls = [call.args[2] for call in request_json.call_args_list]
        self.assertTrue(any("/api/v3/episode/1001" in url for url in urls))
        delete_url = urls[-1]
        self.assertIn("/api/v3/queue/42?", delete_url)
        self.assertIn("removeFromClient=true", delete_url)
        self.assertIn("blocklist=false", delete_url)
        self.assertEqual([], client.deleted)

    def test_completed_sonarr_already_imported_torrent_is_kept_without_verified_episode_file(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "abc123",
            "name": "Already Imported Show S01",
            "state": "stoppedUP",
            "progress": 1,
            "amount_left": 0,
            "category": "tv",
        }
        sonarr = StaticQueue({
            "queue_id": 42,
            "source": "sonarr",
            "series_id": 7,
            "season": 1,
            "episode": 1,
            "season_pack": False,
            "episode_ids": [1001],
            "status_messages": ["Episode file already imported"],
            "status_text": "warning importBlocked Episode file already imported",
        })
        radarr = StaticQueue(None, configs=[])

        def fake_request_json(opener, method, url, **kwargs):
            if method == "GET" and "/api/v3/episode/1001" in url:
                return {
                    "id": 1001,
                    "seriesId": 7,
                    "seasonNumber": 1,
                    "episodeNumber": 1,
                    "episodeFileId": 0,
                }, object()
            raise AssertionError(f"unexpected request {method} {url}")

        with mock.patch.object(self.guard, "request_json", side_effect=fake_request_json) as request_json:
            self.guard.cleanup_arr_managed_completed_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
                delete_files=True,
            )

        self.assertEqual(1, request_json.call_count)
        self.assertEqual([], client.deleted)

    def test_completed_radarr_already_imported_torrent_is_removed_after_movie_file_verification(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "feed123",
            "name": "Already Imported Movie 2026",
            "state": "stoppedUP",
            "progress": 1,
            "amount_left": 0,
            "category": "movies",
        }
        sonarr = StaticQueue(None, configs=[])
        radarr = StaticQueue(
            {
                "queue_id": 88,
                "source": "radarr",
                "title": "already imported movie",
                "movie_id": 9001,
                "year": 2026,
                "status_messages": ["Movie file already imported"],
                "status_text": "warning importBlocked Movie file already imported",
            },
            configs=[("radarr", "http://radarr.test", "radarr-key")],
        )

        def fake_request_json(opener, method, url, **kwargs):
            if method == "GET" and "/api/v3/movie/9001" in url:
                return {
                    "id": 9001,
                    "title": "Already Imported Movie",
                    "year": 2026,
                    "movieFileId": 7001,
                    "movieFile": {"id": 7001, "path": "/movies/Already Imported Movie (2026)/movie.mkv"},
                }, object()
            if method == "DELETE" and "/api/v3/queue/88?" in url:
                return {}, object()
            raise AssertionError(f"unexpected request {method} {url}")

        with mock.patch.object(self.guard, "request_json", side_effect=fake_request_json) as request_json:
            self.guard.cleanup_arr_managed_completed_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
                delete_files=True,
            )

        self.assertEqual(2, request_json.call_count)
        urls = [call.args[2] for call in request_json.call_args_list]
        self.assertTrue(any("/api/v3/movie/9001" in url for url in urls))
        delete_url = urls[-1]
        self.assertIn("/api/v3/queue/88?", delete_url)
        self.assertIn("removeFromClient=true", delete_url)
        self.assertIn("blocklist=false", delete_url)
        self.assertEqual([], client.deleted)

    def test_completed_radarr_corrupt_download_is_blocklisted(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "def456",
            "name": "Taken 2008",
            "state": "stoppedUP",
            "progress": 1,
            "amount_left": 0,
            "category": "movies",
        }
        sonarr = StaticQueue(None, configs=[])
        radarr = StaticQueue(
            {
                "queue_id": 77,
                "source": "radarr",
                "status_messages": ["Unable to determine if file is a sample"],
                "status_text": "warning importPending Unable to determine if file is a sample",
            },
            configs=[("radarr", "http://radarr.test", "radarr-key")],
        )

        with mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
            self.guard.cleanup_arr_managed_completed_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
                delete_files=True,
            )

        request_json.assert_called_once()
        self.assertIn("/api/v3/queue/77?", request_json.call_args.args[2])
        self.assertIn("removeFromClient=true", request_json.call_args.args[2])
        self.assertIn("blocklist=true", request_json.call_args.args[2])
        self.assertEqual([], client.deleted)

    def test_long_stalled_torrent_is_tagged_reannounced_and_parked(self):
        now = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)
        torrent = {
            "hash": "feedbeef",
            "name": "feedbeef",
            "state": "stalledDL",
            "progress": 0,
            "amount_left": 1024,
            "dlspeed": 0,
            "num_seeds": 0,
            "num_complete": 0,
            "availability": 0,
            "tags": "",
        }
        client = FakeQbtClient()

        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "torrent-health.json")
            with open(state_path, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "version": 1,
                        "torrents": {
                            "feedbeef": {
                                "name": "feedbeef",
                                "last_seen_at": "2026-06-16T00:00:00Z",
                                "stale_stalled_first_seen_at": "2026-06-01T00:00:00Z",
                            }
                        },
                    },
                    handle,
                )

            env = {
                "QBT_TORRENT_HEALTH_STATE_PATH": state_path,
                "QBT_STALE_TORRENT_DAYS": "14",
                "QBT_STALE_TORRENT_TAG_PREFIX": "stale-stalled",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                health = self.guard.TorrentHealthStore()
                self.guard.maintain_stale_stalled_torrents(client, [torrent], health, now)

        self.assertEqual([(["feedbeef"], ["stale-stalled-20260601"])], client.added_tags)
        self.assertEqual([["feedbeef"]], client.reannounced)
        self.assertEqual([["feedbeef"]], client.stopped)

    def test_recent_stalled_torrent_is_observed_but_not_maintained(self):
        now = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)
        torrent = {
            "hash": "abc999",
            "name": "abc999",
            "state": "stalledDL",
            "progress": 0,
            "amount_left": 1024,
            "dlspeed": 0,
            "num_seeds": 0,
            "num_complete": 0,
            "availability": 0,
            "tags": "",
        }
        client = FakeQbtClient()

        with tempfile.TemporaryDirectory() as tmpdir:
            env = {
                "QBT_TORRENT_HEALTH_STATE_PATH": os.path.join(tmpdir, "torrent-health.json"),
                "QBT_STALE_TORRENT_DAYS": "14",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                health = self.guard.TorrentHealthStore()
                self.guard.maintain_stale_stalled_torrents(client, [torrent], health, now)

        self.assertEqual([], client.added_tags)
        self.assertEqual([], client.reannounced)
        self.assertEqual([], client.stopped)


if __name__ == "__main__":
    unittest.main()
