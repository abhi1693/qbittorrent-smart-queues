import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock


class FakeQbtClient:
    def __init__(self, files_by_hash=None):
        self.deleted = []
        self.added_tags = []
        self.removed_tags = []
        self.reannounced = []
        self.stopped = []
        self.files_by_hash = dict(files_by_hash or {})

    def delete_hashes(self, hashes, delete_files):
        self.deleted.append((list(hashes), delete_files))

    def torrent_files(self, item_hash):
        return list(self.files_by_hash.get(item_hash, []))

    def add_tags(self, hashes, tags):
        self.added_tags.append((list(hashes), list(tags)))

    def remove_tags(self, hashes, tags):
        self.removed_tags.append((list(hashes), list(tags)))

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

    def ryokan_cleanup_env(self, download_root):
        return {
            "QBT_RYOKAN_IMPORTED_ANIME_CLEANUP_ENABLED": "true",
            "QBT_RYOKAN_IMPORTED_ANIME_DOWNLOAD_ROOT": download_root,
            "QBT_RYOKAN_IMPORTED_ANIME_MIN_COMPLETED_SECONDS": "0",
            "QBT_RYOKAN_IMPORTED_ANIME_DELETE_FILES": "true",
        }

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
        self.assertIn("skipRedownload=false", delete_url)
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

    def test_completed_sonarr_already_imported_torrent_with_terminal_warning_is_removed(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "sherlock",
            "name": "Sherlock S01 1080p NF WEB-DL DD5 1 H 264-playWEB",
            "state": "stoppedUP",
            "progress": 1,
            "amount_left": 0,
            "category": "tv",
        }
        sonarr = StaticQueue({
            "queue_id": 420,
            "source": "sonarr",
            "series_id": 47,
            "season": 1,
            "episode": None,
            "season_pack": True,
            "episode_ids": [1001, 1002, 1003],
            "status_reasons": [
                "Episode file already imported at 8/11/2026 9:51:22AM",
                "Single episode file contains all episodes in seasons. Review file name or manually import",
            ],
            "status_text": "warning importPending Episode file already imported Single episode file contains all episodes in seasons",
        })
        radarr = StaticQueue(None, configs=[])

        def fake_request_json(opener, method, url, **kwargs):
            for episode_id, episode_number in ((1001, 1), (1002, 2), (1003, 3)):
                if method == "GET" and f"/api/v3/episode/{episode_id}" in url:
                    return {
                        "id": episode_id,
                        "seriesId": 47,
                        "seasonNumber": 1,
                        "episodeNumber": episode_number,
                        "episodeFileId": 5000 + episode_id,
                        "episodeFile": {
                            "id": 5000 + episode_id,
                            "path": f"/tv/Sherlock/Season 01/S01E{episode_number:02d}.mkv",
                        },
                    }, object()
            if method == "DELETE" and "/api/v3/queue/420?" in url:
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

        self.assertEqual(4, request_json.call_count)
        urls = [call.args[2] for call in request_json.call_args_list]
        self.assertTrue(any("/api/v3/episode/1001" in url for url in urls))
        self.assertTrue(any("/api/v3/episode/1002" in url for url in urls))
        self.assertTrue(any("/api/v3/episode/1003" in url for url in urls))
        delete_url = urls[-1]
        self.assertIn("/api/v3/queue/420?", delete_url)
        self.assertIn("removeFromClient=true", delete_url)
        self.assertEqual([], client.deleted)

    def test_completed_sonarr_already_imported_torrent_with_unknown_warning_is_kept(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "mixed-warning",
            "name": "Mixed Warning Show S01",
            "state": "stoppedUP",
            "progress": 1,
            "amount_left": 0,
            "category": "tv",
        }
        sonarr = StaticQueue({
            "queue_id": 421,
            "source": "sonarr",
            "series_id": 47,
            "season": 1,
            "episode": 1,
            "season_pack": False,
            "episode_ids": [1001],
            "status_reasons": [
                "Episode file already imported at 8/11/2026 9:51:22AM",
                "Unexpected import warning that still needs operator review",
            ],
        })
        radarr = StaticQueue(None, configs=[])

        with mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
            self.guard.cleanup_arr_managed_completed_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
                delete_files=True,
            )

        request_json.assert_not_called()
        self.assertEqual([], client.deleted)

    def test_sonarr_queue_metadata_merge_verifies_all_same_download_episode_records(self):
        first = {
            "queue_id": 1,
            "source": "sonarr",
            "series": "sherlock",
            "series_id": 47,
            "season": 1,
            "episode": 1,
            "season_pack": False,
            "episode_ids": [1001],
            "status_reasons": ["Episode file already imported"],
            "status_messages": ["Sherlock S01", "Episode file already imported"],
            "status_text": "warning importPending Episode file already imported",
            "queue_position": 10,
        }
        second = {
            "queue_id": 2,
            "source": "sonarr",
            "series": "sherlock",
            "series_id": 47,
            "season": 1,
            "episode": 2,
            "season_pack": False,
            "episode_ids": [1002],
            "status_reasons": ["Single episode file contains all episodes in seasons. Review file name or manually import"],
            "status_messages": ["Sherlock S01", "Single episode file contains all episodes in seasons. Review file name or manually import"],
            "status_text": "warning importPending Single episode file contains all episodes in seasons",
            "queue_position": 11,
        }

        merged = self.guard.merge_sonarr_queue_metadata(first, second)

        self.assertEqual([1001, 1002], merged["episode_ids"])
        self.assertTrue(merged["season_pack"])
        self.assertIsNone(merged["episode"])
        self.assertFalse(merged.get("episode_scope_ambiguous", False))
        self.assertEqual(10, merged["queue_position"])

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
        self.assertIn("skipRedownload=false", delete_url)
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
        self.assertIn("skipRedownload=false", request_json.call_args.args[2])
        self.assertEqual([], client.deleted)

    def test_completed_ryokan_anime_torrent_is_deleted_when_selected_media_sources_are_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeQbtClient({
                "animegone": [
                    {"name": "Show S01E01.mkv", "priority": 7, "progress": 1, "size": 100},
                ],
            })
            torrent = {
                "hash": "animegone",
                "name": "Show S01E01",
                "state": "stoppedUP",
                "progress": 1,
                "amount_left": 0,
                "category": "anime",
            }

            with mock.patch.dict(os.environ, self.ryokan_cleanup_env(tmpdir), clear=False):
                result = self.guard.process_ryokan_imported_anime_torrents(
                    client,
                    [torrent],
                    now=datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc),
                )

        self.assertEqual(
            {
                "attempted": 1,
                "succeeded": 1,
                "failed": 0,
                "verification_failed": 0,
                "skipped": 0,
            },
            result,
        )
        self.assertEqual([(["animegone"], True)], client.deleted)

    def test_completed_ryokan_anime_batch_is_kept_when_any_selected_media_source_remains(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_dir = os.path.join(tmpdir, "Batch")
            os.mkdir(batch_dir)
            with open(os.path.join(batch_dir, "S01E01.mkv"), "wb") as handle:
                handle.write(b"media")

            client = FakeQbtClient({
                "animepartial": [
                    {"name": "S01E01.mkv", "priority": 7, "progress": 1, "size": 100},
                    {"name": "S01E02.mkv", "priority": 7, "progress": 1, "size": 100},
                ],
            })
            torrent = {
                "hash": "animepartial",
                "name": "Batch",
                "state": "stoppedUP",
                "progress": 1,
                "amount_left": 0,
                "category": "anime",
                "content_path": batch_dir,
            }

            with mock.patch.dict(os.environ, self.ryokan_cleanup_env(tmpdir), clear=False):
                result = self.guard.process_ryokan_imported_anime_torrents(
                    client,
                    [torrent],
                    now=datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc),
                )

        self.assertEqual(
            {
                "attempted": 0,
                "succeeded": 0,
                "failed": 0,
                "verification_failed": 1,
                "skipped": 0,
            },
            result,
        )
        self.assertEqual([], client.deleted)

    def test_ryokan_cleanup_ignores_metadata_only_zero_remaining_torrent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeQbtClient({
                "metadataonly": [
                    {"name": "Show S01E01.mkv", "priority": 7, "progress": 0, "size": 0},
                ],
            })
            torrent = {
                "hash": "metadataonly",
                "name": "metadataonly",
                "state": "stoppedDL",
                "progress": 0,
                "amount_left": 0,
                "category": "anime",
            }

            with mock.patch.dict(os.environ, self.ryokan_cleanup_env(tmpdir), clear=False):
                result = self.guard.process_ryokan_imported_anime_torrents(
                    client,
                    [torrent],
                    now=datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc),
                )

        self.assertEqual(
            {
                "attempted": 0,
                "succeeded": 0,
                "failed": 0,
                "verification_failed": 0,
                "skipped": 0,
            },
            result,
        )
        self.assertEqual([], client.deleted)

    def test_ryokan_cleanup_ignores_non_anime_category(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            client = FakeQbtClient({
                "tvhash": [
                    {"name": "Show S01E01.mkv", "priority": 7, "progress": 1, "size": 100},
                ],
            })
            torrent = {
                "hash": "tvhash",
                "name": "Show S01E01",
                "state": "stoppedUP",
                "progress": 1,
                "amount_left": 0,
                "category": "tv",
            }

            with mock.patch.dict(os.environ, self.ryokan_cleanup_env(tmpdir), clear=False):
                result = self.guard.process_ryokan_imported_anime_torrents(
                    client,
                    [torrent],
                    now=datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc),
                )

        self.assertEqual(
            {
                "attempted": 0,
                "succeeded": 0,
                "failed": 0,
                "verification_failed": 0,
                "skipped": 0,
            },
            result,
        )
        self.assertEqual([], client.deleted)

    def test_completed_sonarr_not_upgrade_torrent_is_removed_after_episode_verification(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "noupgrade-tv",
            "name": "Not Upgrade Show S01E01",
            "state": "stoppedUP",
            "progress": 1,
            "amount_left": 0,
            "category": "tv",
        }
        sonarr = StaticQueue({
            "queue_id": 52,
            "source": "sonarr",
            "series_id": 7,
            "season": 1,
            "episode": 1,
            "season_pack": False,
            "episode_ids": [1001],
            "status_reasons": [
                "Not an upgrade for existing episode file(s). Existing quality: WEBDL-2160p. New Quality WEBDL-1080p."
            ],
            "status_messages": [
                "release title that should not be treated as a reason",
                "Not an upgrade for existing episode file(s). Existing quality: WEBDL-2160p. New Quality WEBDL-1080p.",
            ],
            "status_text": "warning importPending Not an upgrade for existing episode file(s)",
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
                    "episodeFile": {"id": 5001, "path": "/tv/Not Upgrade Show/Season 01/S01E01.mkv"},
                }, object()
            if method == "DELETE" and "/api/v3/queue/52?" in url:
                return {}, object()
            raise AssertionError(f"unexpected request {method} {url}")

        with mock.patch.object(self.guard, "request_json", side_effect=fake_request_json) as request_json:
            result = self.guard.process_arr_import_rejected_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
            )

        self.assertEqual(
            {"attempted": 1, "succeeded": 1, "failed": 0, "verification_failed": 0},
            result,
        )
        urls = [call.args[2] for call in request_json.call_args_list]
        self.assertTrue(any("/api/v3/episode/1001" in url for url in urls))
        delete_url = urls[-1]
        self.assertIn("/api/v3/queue/52?", delete_url)
        self.assertIn("removeFromClient=true", delete_url)
        self.assertIn("blocklist=false", delete_url)
        self.assertIn("skipRedownload=false", delete_url)
        self.assertEqual([], client.deleted)

    def test_completed_radarr_not_upgrade_torrent_is_removed_after_movie_verification(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "noupgrade-movie",
            "name": "Not Upgrade Movie 2026",
            "state": "stoppedUP",
            "progress": 1,
            "amount_left": 0,
            "category": "movies",
        }
        sonarr = StaticQueue(None, configs=[])
        radarr = StaticQueue(
            {
                "queue_id": 89,
                "source": "radarr",
                "title": "not upgrade movie",
                "movie_id": 9001,
                "year": 2026,
                "status_reasons": [
                    "Not an upgrade for existing movie file. Existing quality: Remux-2160p. New Quality Bluray-1080p."
                ],
                "status_messages": [
                    "Not Upgrade Movie 2026",
                    "Not an upgrade for existing movie file. Existing quality: Remux-2160p. New Quality Bluray-1080p.",
                ],
                "status_text": "warning importPending Not an upgrade for existing movie file",
            },
            configs=[("radarr", "http://radarr.test", "radarr-key")],
        )

        def fake_request_json(opener, method, url, **kwargs):
            if method == "GET" and "/api/v3/movie/9001" in url:
                return {
                    "id": 9001,
                    "title": "Not Upgrade Movie",
                    "year": 2026,
                    "movieFileId": 7001,
                    "movieFile": {"id": 7001, "path": "/movies/Not Upgrade Movie (2026)/movie.mkv"},
                }, object()
            if method == "DELETE" and "/api/v3/queue/89?" in url:
                return {}, object()
            raise AssertionError(f"unexpected request {method} {url}")

        with mock.patch.object(self.guard, "request_json", side_effect=fake_request_json) as request_json:
            result = self.guard.process_arr_import_rejected_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
            )

        self.assertEqual(
            {"attempted": 1, "succeeded": 1, "failed": 0, "verification_failed": 0},
            result,
        )
        urls = [call.args[2] for call in request_json.call_args_list]
        self.assertTrue(any("/api/v3/movie/9001" in url for url in urls))
        delete_url = urls[-1]
        self.assertIn("/api/v3/queue/89?", delete_url)
        self.assertIn("removeFromClient=true", delete_url)
        self.assertIn("blocklist=false", delete_url)
        self.assertIn("skipRedownload=false", delete_url)
        self.assertEqual([], client.deleted)

    def test_not_upgrade_cleanup_keeps_torrent_without_verified_existing_file(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "verifyfail",
            "name": "Verify Failure Movie 2026",
            "state": "stoppedUP",
            "progress": 1,
            "amount_left": 0,
            "category": "movies",
        }
        sonarr = StaticQueue(None, configs=[])
        radarr = StaticQueue(
            {
                "queue_id": 90,
                "source": "radarr",
                "title": "verify failure movie",
                "movie_id": 9002,
                "year": 2026,
                "status_reasons": ["Not an upgrade for existing movie file."],
                "status_text": "warning importPending Not an upgrade for existing movie file",
            },
            configs=[("radarr", "http://radarr.test", "radarr-key")],
        )

        def fake_request_json(opener, method, url, **kwargs):
            if method == "GET" and "/api/v3/movie/9002" in url:
                return {"id": 9002, "title": "Verify Failure Movie", "year": 2026}, object()
            raise AssertionError(f"unexpected request {method} {url}")

        with mock.patch.object(self.guard, "request_json", side_effect=fake_request_json) as request_json:
            result = self.guard.process_arr_import_rejected_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
            )

        self.assertEqual(
            {"attempted": 1, "succeeded": 0, "failed": 0, "verification_failed": 1},
            result,
        )
        request_json.assert_called_once()
        self.assertEqual([], client.deleted)

    def test_manual_blacklist_tag_blocklists_sonarr_queue_record(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "abc123",
            "name": "Tagged Show S01E01",
            "state": "stoppedDL",
            "progress": 0.2,
            "amount_left": 1024,
            "category": "tv",
            "tags": "blacklist,priority",
        }
        sonarr = StaticQueue({
            "queue_id": 42,
            "source": "sonarr",
            "series": "tagged show",
            "season": 1,
            "episode": 1,
        })
        radarr = StaticQueue(None, configs=[])

        with mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
            result = self.guard.process_manual_blacklist_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
            )

        self.assertEqual({"attempted": 1, "succeeded": 1, "failed": 0, "no_arr_match": 0}, result)
        request_json.assert_called_once()
        delete_url = request_json.call_args.args[2]
        self.assertIn("/api/v3/queue/42?", delete_url)
        self.assertIn("removeFromClient=true", delete_url)
        self.assertIn("blocklist=true", delete_url)
        self.assertIn("skipRedownload=false", delete_url)
        self.assertEqual([(["abc123"], ["blacklist"])], client.removed_tags)
        self.assertEqual([], client.added_tags)

    def test_manual_blacklist_tag_prefers_radarr_for_movie_category(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "feed123",
            "name": "Tagged Movie 2026",
            "state": "stoppedDL",
            "progress": 0.2,
            "amount_left": 1024,
            "category": "movies",
            "tags": "blacklist",
        }
        sonarr = StaticQueue(
            {"queue_id": 11, "source": "sonarr"},
            configs=[("sonarr", "http://sonarr.test", "sonarr-key")],
        )
        radarr = StaticQueue(
            {"queue_id": 88, "source": "radarr"},
            configs=[("radarr", "http://radarr.test", "radarr-key")],
        )

        with mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
            result = self.guard.process_manual_blacklist_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
            )

        self.assertEqual({"attempted": 1, "succeeded": 1, "failed": 0, "no_arr_match": 0}, result)
        delete_url = request_json.call_args.args[2]
        self.assertIn("http://radarr.test/api/v3/queue/88?", delete_url)
        self.assertIn("blocklist=true", delete_url)

    def test_manual_blacklist_tag_deletes_torrent_when_no_arr_queue_record_matches(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "nomatch",
            "name": "Manual Only Torrent",
            "state": "stoppedDL",
            "progress": 0,
            "amount_left": 1024,
            "category": "anime",
            "tags": "Blacklist",
        }
        sonarr = StaticQueue(None, configs=[])
        radarr = StaticQueue(None, configs=[])

        with mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
            result = self.guard.process_manual_blacklist_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
            )

        self.assertEqual({"attempted": 1, "succeeded": 1, "failed": 0, "no_arr_match": 1}, result)
        request_json.assert_not_called()
        self.assertEqual([(["nomatch"], True)], client.deleted)
        self.assertEqual([], client.removed_tags)
        self.assertEqual([], client.added_tags)

    def test_manual_blacklist_tag_marks_failure_when_direct_delete_fails(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "deletefail",
            "name": "Manual Delete Failure",
            "state": "stoppedDL",
            "progress": 0,
            "amount_left": 1024,
            "category": "anime",
            "tags": "Blacklist",
        }
        sonarr = StaticQueue(None, configs=[])
        radarr = StaticQueue(None, configs=[])

        with (
            mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json,
            mock.patch.object(client, "delete_hashes", side_effect=self.guard.ApiError("qB delete failed")),
        ):
            result = self.guard.process_manual_blacklist_torrents(
                client,
                [torrent],
                sonarr,
                radarr,
            )

        self.assertEqual({"attempted": 1, "succeeded": 0, "failed": 1, "no_arr_match": 1}, result)
        request_json.assert_not_called()
        self.assertEqual([(["deletefail"], ["Blacklist"])], client.removed_tags)
        self.assertEqual([(["deletefail"], ["blacklist-failed"])], client.added_tags)

    def test_metadata_timeout_blocklists_sonarr_queue_record(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "meta123",
            "name": "Metadata Timeout Show S01E01",
            "state": "stoppedDL",
            "progress": 0,
            "amount_left": 0,
            "category": "tv",
        }
        sonarr = StaticQueue({
            "queue_id": 123,
            "source": "sonarr",
            "series": "metadata timeout show",
            "season": 1,
            "episode": 1,
        })
        radarr = StaticQueue(None, configs=[])

        with mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
            result = self.guard.process_metadata_timeout_torrent(
                client,
                torrent,
                sonarr,
                radarr,
            )

        self.assertEqual({"attempted": 1, "succeeded": 1, "failed": 0, "no_arr_match": 0}, result)
        request_json.assert_called_once()
        delete_url = request_json.call_args.args[2]
        self.assertIn("/api/v3/queue/123?", delete_url)
        self.assertIn("removeFromClient=true", delete_url)
        self.assertIn("blocklist=true", delete_url)
        self.assertIn("skipRedownload=false", delete_url)
        self.assertEqual([], client.deleted)
        self.assertEqual([], client.added_tags)

    def test_metadata_timeout_deletes_torrent_when_no_arr_queue_record_matches(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "nometaarr",
            "name": "Metadata Timeout Manual",
            "state": "stoppedDL",
            "progress": 0,
            "amount_left": 0,
            "category": "anime",
        }
        sonarr = StaticQueue(None, configs=[])
        radarr = StaticQueue(None, configs=[])

        with mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
            result = self.guard.process_metadata_timeout_torrent(
                client,
                torrent,
                sonarr,
                radarr,
            )

        self.assertEqual({"attempted": 1, "succeeded": 1, "failed": 0, "no_arr_match": 1}, result)
        request_json.assert_not_called()
        self.assertEqual([(["nometaarr"], True)], client.deleted)
        self.assertEqual([], client.added_tags)

    def test_metadata_timeout_marks_failure_when_arr_delete_fails(self):
        client = FakeQbtClient()
        torrent = {
            "hash": "metafail",
            "name": "Metadata Timeout Failure",
            "state": "stoppedDL",
            "progress": 0,
            "amount_left": 0,
            "category": "movies",
        }
        sonarr = StaticQueue(None, configs=[])
        radarr = StaticQueue(
            {"queue_id": 88, "source": "radarr", "title": "metadata timeout failure"},
            configs=[("radarr", "http://radarr.test", "radarr-key")],
        )

        with mock.patch.object(
            self.guard,
            "request_json",
            side_effect=self.guard.ApiError("radarr unavailable"),
        ):
            result = self.guard.process_metadata_timeout_torrent(
                client,
                torrent,
                sonarr,
                radarr,
            )

        self.assertEqual({"attempted": 1, "succeeded": 0, "failed": 1, "no_arr_match": 0}, result)
        self.assertEqual([], client.deleted)
        self.assertEqual([(["metafail"], ["metadata-timeout-failed"])], client.added_tags)

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
