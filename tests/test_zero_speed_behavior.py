import contextlib
import importlib
import io
import json
import unittest
from unittest import mock


class FakeQbtClient:
    base_url = "http://qbittorrent.test"

    def __init__(self, torrents, files=None):
        self.torrents = torrents
        self.files = files or {}
        self.download_limits = []
        self.upload_limits = []
        self.started = []
        self.stopped = []
        self.stop_all_calls = 0
        self.top_priority_calls = []
        self.reannounced = []
        self.added_tags = []
        self.queue_limits = []

    def set_download_limit(self, limit):
        self.download_limits.append(limit)

    def set_upload_limit(self, limit):
        self.upload_limits.append(limit)

    def set_active_queue_limits(self, max_active_downloads, max_active_torrents=None):
        self.queue_limits.append((max_active_downloads, max_active_torrents))

    def stop_all(self):
        self.stop_all_calls += 1

    def torrents_info(self, filter_name=None):
        if filter_name:
            return []
        return [dict(torrent) for torrent in self.torrents]

    def torrent_files(self, item_hash):
        return [dict(item) for item in self.files.get(item_hash, [])]

    def stop_hashes(self, hashes):
        self.stopped.append(list(hashes))

    def top_priority(self, hashes):
        self.top_priority_calls.append(list(hashes))

    def reannounce_hashes(self, hashes):
        self.reannounced.append(list(hashes))

    def start_hashes(self, hashes):
        self.started.append(list(hashes))

    def add_tags(self, hashes, tags):
        self.added_tags.append((list(hashes), list(tags)))

    def remove_tags(self, hashes, tags):
        pass


class FakeStorageGuard:
    require_torrent_fit = False

    def state(self):
        return {
            "enabled": True,
            "stop": False,
            "reason": "enough space",
            "path": "/downloads",
            "total_bytes": 10000,
            "free_bytes": 5000,
            "reserve_bytes": 1000,
            "headroom_bytes": 4000,
        }

    def check(self):
        return self.state()

    def snapshot(self):
        return self.state()


class ConstrainedStorageGuard:
    require_torrent_fit = True

    def state(self):
        return {
            "enabled": True,
            "stop": True,
            "reason": "download storage free space is at or below reserve",
            "path": "/downloads",
            "total_bytes": 10000,
            "free_bytes": 1024 * 1024 * 1024,
            "reserve_bytes": 30 * 1024 * 1024 * 1024,
            "headroom_bytes": 0,
        }

    def check(self):
        return self.state()

    def snapshot(self):
        return self.state()


class ZeroSpeedBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_downloading_and_forced_torrents_with_zero_speed_are_not_productive(self):
        for state in ("downloading", "forcedDL"):
            with self.subTest(state=state):
                torrent = {"state": state, "dlspeed": 0}

                self.assertFalse(self.guard.is_productive_torrent(torrent))

        self.assertTrue(self.guard.is_productive_torrent({"state": "downloading", "dlspeed": 1}))

    def test_progress_reason_requires_left_delta_speed_or_downloaded_delta(self):
        before = {"amount_left": 1000, "downloaded": 100, "dlspeed": 0}

        self.assertEqual(
            "",
            self.guard.torrent_progress_reason(
                before,
                {"amount_left": 1000, "downloaded": 100, "dlspeed": 0},
                min_download_delta_bytes=100,
            ),
        )
        self.assertIn(
            "amount left decreased",
            self.guard.torrent_progress_reason(
                before,
                {"amount_left": 999, "downloaded": 100, "dlspeed": 0},
                min_download_delta_bytes=100,
            ),
        )
        self.assertIn(
            "downloaded bytes increased",
            self.guard.torrent_progress_reason(
                before,
                {"amount_left": 1000, "downloaded": 250, "dlspeed": 0},
                min_download_delta_bytes=100,
            ),
        )
        self.assertIn(
            "download speed remained nonzero",
            self.guard.torrent_progress_reason(
                {"amount_left": 1000, "downloaded": 100, "dlspeed": 1},
                {"amount_left": 1000, "downloaded": 100, "dlspeed": 2},
                min_download_delta_bytes=100,
            ),
        )

    def test_apply_single_download_stops_zero_speed_torrent_after_wait_without_bytes(self):
        client = FakeQbtClient([
            {
                "hash": "zero",
                "name": "Zero.Speed.S01E01",
                "category": "tv",
                "state": "downloading",
                "dlspeed": 0,
                "amount_left": 1000,
                "downloaded": 100,
                "progress": 0.5,
                "tags": "",
            },
        ])
        env = {
            "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
            "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "60",
            "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
            "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
            "QBT_TORRENT_HEALTH_SCORING_ENABLED": "false",
            "QBT_TV_QUEUE_SONARR_ENABLED": "false",
            "QBT_LOG_FORMAT": "json",
            "QBT_DECISION_LOG_LEVEL": "info",
        }

        with mock.patch.dict("os.environ", env, clear=False), mock.patch.object(self.guard.time, "sleep"):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.guard.apply_single_download(
                    [client],
                    usage_bytes=0,
                    monthly_limit_bytes=1000,
                    download_limit=1024,
                    limit_reason="unit test",
                    storage_guard=FakeStorageGuard(),
                    decision_context={
                        "udm": {"stats_age_seconds": 42},
                        "thermal": {"stop": False, "max_temperature_celsius": 55.5},
                    },
                )

        decision_logs = [
            json.loads(line)
            for line in stdout.getvalue().splitlines()
            if line.startswith("{")
        ]
        decision_events = [
            item for item in decision_logs
            if item.get("event") == "qbt_guard_decision"
        ]
        try_event = next(item for item in decision_events if item.get("action") == "try_candidate")
        stop_event = next(
            item for item in decision_events
            if item.get("action") == "stop_selected_no_progress"
        )

        self.assertEqual("zero", try_event["selected_torrent"]["hash"])
        self.assertEqual(1024, try_event["effective_cap"]["download_limit_bytes_per_sec"])
        self.assertEqual(0, try_event["budget"]["monthly_usage_bytes"])
        self.assertEqual(42, try_event["udm"]["stats_age_seconds"])
        self.assertEqual(4000, try_event["storage"]["headroom_bytes"])
        self.assertFalse(try_event["thermal"]["stop"])
        self.assertNotIn("client", try_event)
        self.assertNotIn("client", stop_event)
        self.assertEqual(1, try_event["rejected_counts"]["not_productive_zero_speed"])
        self.assertEqual("zero", stop_event["selected_torrent"]["hash"])
        self.assertEqual(1, stop_event["rejected_counts"]["no_progress_after_wait"])
        self.assertIn(["zero"], client.started)
        self.assertTrue(any("zero" in hashes for hashes in client.stopped))
        self.assertEqual(1, client.stop_all_calls)
        self.assertTrue(
            any(
                hashes == ["zero"] and tags[0].startswith("quota-stalled-")
                for hashes, tags in client.added_tags
            )
        )

    def test_storage_constrained_mode_selects_smallest_verified_remaining_download(self):
        client = FakeQbtClient(
            [
                {
                    "hash": "large",
                    "name": "Large.Left",
                    "category": "tv",
                    "state": "stoppedDL",
                    "dlspeed": 0,
                    "amount_left": 500 * 1024 * 1024,
                    "downloaded": 0,
                    "progress": 0.5,
                    "availability": 2.0,
                    "num_seeds": 10,
                    "num_complete": 10,
                    "tags": "",
                },
                {
                    "hash": "small",
                    "name": "Small.Left",
                    "category": "tv",
                    "state": "stoppedDL",
                    "dlspeed": 0,
                    "amount_left": 4 * 1024 * 1024,
                    "downloaded": 0,
                    "progress": 0.99,
                    "availability": 1.0,
                    "num_seeds": 1,
                    "num_complete": 1,
                    "tags": "",
                },
            ],
            files={
                "large": [
                    {"name": "large.mkv", "size": 1000 * 1024 * 1024, "progress": 0.5, "priority": 1},
                ],
                "small": [
                    {"name": "small.mkv", "size": 400 * 1024 * 1024, "progress": 0.99, "priority": 1},
                ],
            },
        )
        env = {
            "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
            "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
            "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
            "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
            "QBT_TORRENT_HEALTH_SCORING_ENABLED": "false",
            "QBT_TV_QUEUE_SONARR_ENABLED": "false",
            "QBT_LOG_FORMAT": "json",
            "QBT_DECISION_LOG_LEVEL": "info",
        }

        with mock.patch.dict("os.environ", env, clear=False):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.guard.apply_single_download(
                    [client],
                    usage_bytes=0,
                    monthly_limit_bytes=1000,
                    download_limit=1024,
                    limit_reason="unit test",
                    storage_guard=ConstrainedStorageGuard(),
                )

        decision_logs = [
            json.loads(line)
            for line in stdout.getvalue().splitlines()
            if line.startswith("{")
        ]
        try_event = next(
            item for item in decision_logs
            if item.get("event") == "qbt_guard_decision" and item.get("action") == "storage_recovery_batch"
        )

        self.assertEqual([["small", "large"]], client.started)
        self.assertEqual([(5, None)], client.queue_limits)
        self.assertEqual("small", try_event["selected_torrent"]["hash"])
        self.assertEqual(["small", "large"], [
            item["hash"] for item in try_event["selected_torrents"]
        ])
        self.assertTrue(try_event["candidate_counts"]["storage_constrained"])
        self.assertEqual(0, try_event["rejected_counts"].get("deferred_by_storage_recovery_batch", 0))

    def test_storage_constrained_mode_ignores_legacy_quota_cooldown_tags(self):
        client = FakeQbtClient(
            [
                {
                    "hash": "small",
                    "name": "Small.Left",
                    "category": "tv",
                    "state": "stoppedDL",
                    "dlspeed": 0,
                    "amount_left": 4 * 1024 * 1024,
                    "downloaded": 0,
                    "progress": 0.99,
                    "availability": 1.0,
                    "num_seeds": 1,
                    "num_complete": 1,
                    "tags": "quota-stalled-29990101T000000Z",
                },
            ],
            files={
                "small": [
                    {"name": "small.mkv", "size": 400 * 1024 * 1024, "progress": 0.99, "priority": 1},
                ],
            },
        )
        env = {
            "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
            "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
            "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
            "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
            "QBT_TORRENT_HEALTH_SCORING_ENABLED": "false",
            "QBT_TV_QUEUE_SONARR_ENABLED": "false",
            "QBT_LOG_FORMAT": "json",
            "QBT_DECISION_LOG_LEVEL": "info",
        }

        with mock.patch.dict("os.environ", env, clear=False):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.guard.apply_single_download(
                    [client],
                    usage_bytes=0,
                    monthly_limit_bytes=1000,
                    download_limit=1024,
                    limit_reason="unit test",
                    storage_guard=ConstrainedStorageGuard(),
                )

        self.assertEqual([["small"]], client.started)

    def test_storage_constrained_mode_retries_storage_stalled_cooldown_tags(self):
        client = FakeQbtClient(
            [
                {
                    "hash": "small",
                    "name": "Small.Left",
                    "category": "tv",
                    "state": "stalledDL",
                    "dlspeed": 0,
                    "amount_left": 4 * 1024 * 1024,
                    "downloaded": 0,
                    "progress": 0.99,
                    "availability": 1.0,
                    "num_seeds": 1,
                    "num_complete": 1,
                    "tags": "storage-stalled-29990101T000000Z",
                },
            ],
            files={
                "small": [
                    {"name": "small.mkv", "size": 400 * 1024 * 1024, "progress": 0.99, "priority": 1},
                ],
            },
        )
        env = {
            "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
            "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
            "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
            "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
            "QBT_TORRENT_HEALTH_SCORING_ENABLED": "false",
            "QBT_TV_QUEUE_SONARR_ENABLED": "false",
            "QBT_LOG_FORMAT": "json",
            "QBT_DECISION_LOG_LEVEL": "info",
        }

        with mock.patch.dict("os.environ", env, clear=False):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.guard.apply_single_download(
                    [client],
                    usage_bytes=0,
                    monthly_limit_bytes=1000,
                    download_limit=1024,
                    limit_reason="unit test",
                    storage_guard=ConstrainedStorageGuard(),
                )

        self.assertEqual([["small"]], client.started)
        self.assertEqual([], client.added_tags)

    def test_storage_constrained_mode_caps_recovery_batch_at_five(self):
        torrents = []
        files = {}
        for index in range(6):
            item_hash = f"small-{index}"
            torrents.append(
                {
                    "hash": item_hash,
                    "name": f"Small.Left.{index}",
                    "category": "tv",
                    "state": "stoppedDL",
                    "dlspeed": 0,
                    "amount_left": (index + 1) * 1024 * 1024,
                    "downloaded": 0,
                    "progress": 0.99,
                    "availability": 1.0,
                    "num_seeds": 1,
                    "num_complete": 1,
                    "tags": "",
                }
            )
            files[item_hash] = [
                {
                    "name": f"small-{index}.mkv",
                    "size": 100 * 1024 * 1024,
                    "progress": 0.99 - (index * 0.001),
                    "priority": 1,
                },
            ]
        client = FakeQbtClient(torrents, files=files)
        env = {
            "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
            "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
            "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
            "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
            "QBT_TORRENT_HEALTH_SCORING_ENABLED": "false",
            "QBT_TV_QUEUE_SONARR_ENABLED": "false",
            "QBT_LOG_FORMAT": "json",
            "QBT_DECISION_LOG_LEVEL": "info",
        }

        with mock.patch.dict("os.environ", env, clear=False):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.guard.apply_single_download(
                    [client],
                    usage_bytes=0,
                    monthly_limit_bytes=1000,
                    download_limit=1024,
                    limit_reason="unit test",
                    storage_guard=ConstrainedStorageGuard(),
                )

        self.assertEqual([["small-0", "small-1", "small-2", "small-3", "small-4"]], client.started)

    def test_apply_single_download_preempts_productive_for_better_balanced_candidate(self):
        client = FakeQbtClient([
            {
                "hash": "current",
                "name": "Current.Movie.1080p",
                "category": "movies",
                "state": "downloading",
                "dlspeed": 900_000,
                "amount_left": 60 * 1024 * 1024 * 1024,
                "downloaded": 100,
                "progress": 0.25,
                "availability": 1.0,
                "num_seeds": 1,
                "num_complete": 1,
                "tags": "",
            },
            {
                "hash": "challenger",
                "name": "Challenger.Movie.1080p",
                "category": "movies",
                "state": "stoppedDL",
                "dlspeed": 0,
                "amount_left": 4 * 1024 * 1024 * 1024,
                "downloaded": 100,
                "progress": 0.95,
                "availability": 1.0,
                "num_seeds": 1,
                "num_complete": 1,
                "tags": "",
            },
        ])
        env = {
            "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
            "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
            "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
            "QBT_SINGLE_DOWNLOAD_SELECTION_STRATEGY": "balanced",
            "QBT_SINGLE_DOWNLOAD_PREEMPT_PRODUCTIVE_ENABLED": "true",
            "QBT_SINGLE_DOWNLOAD_PREEMPT_PRODUCTIVE_SCORE_MARGIN": "20",
            "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
            "QBT_TORRENT_HEALTH_SCORING_ENABLED": "false",
            "QBT_TV_QUEUE_SONARR_ENABLED": "false",
            "QBT_TV_WATCH_JELLYFIN_ENABLED": "false",
            "QBT_MOVIE_QUEUE_RADARR_ENABLED": "false",
            "QBT_LOG_FORMAT": "json",
            "QBT_DECISION_LOG_LEVEL": "info",
        }

        with mock.patch.dict("os.environ", env, clear=False):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.guard.apply_single_download(
                    [client],
                    usage_bytes=0,
                    monthly_limit_bytes=1000,
                    download_limit=1024,
                    limit_reason="unit test",
                    storage_guard=FakeStorageGuard(),
                )

        decision_logs = [
            json.loads(line)
            for line in stdout.getvalue().splitlines()
            if line.startswith("{")
        ]
        decision_events = [
            item for item in decision_logs
            if item.get("event") == "qbt_guard_decision"
        ]
        preempt_event = next(item for item in decision_events if item.get("action") == "preempt_productive")
        try_event = next(item for item in decision_events if item.get("action") == "try_candidate")

        self.assertEqual("challenger", preempt_event["selected_torrent"]["hash"])
        self.assertEqual(1, preempt_event["rejected_counts"]["preempted_productive"])
        self.assertEqual("challenger", try_event["selected_torrent"]["hash"])
        self.assertIn(["current"], client.stopped)
        self.assertIn(["challenger"], client.started)
        self.assertNotIn(["current"], client.started)

    def test_tv_queue_order_blocks_priority_later_episode(self):
        client = FakeQbtClient([
            {
                "hash": "old",
                "name": "Alpha.S01E02.1080p",
                "category": "tv",
                "state": "stoppedDL",
                "dlspeed": 0,
                "amount_left": 1000,
                "downloaded": 100,
                "progress": 0.5,
                "tags": "",
            },
            {
                "hash": "later",
                "name": "Alpha.S01E03.1080p",
                "category": "tv",
                "state": "stoppedDL",
                "dlspeed": 0,
                "amount_left": 1000,
                "downloaded": 100,
                "progress": 0.5,
                "tags": "priority",
            },
        ])
        env = {
            "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
            "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
            "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
            "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
            "QBT_TORRENT_HEALTH_SCORING_ENABLED": "false",
            "QBT_TV_QUEUE_SONARR_ENABLED": "false",
            "QBT_TV_WATCH_JELLYFIN_ENABLED": "false",
            "QBT_MOVIE_QUEUE_RADARR_ENABLED": "false",
            "QBT_LOG_FORMAT": "json",
            "QBT_DECISION_LOG_LEVEL": "info",
        }

        with mock.patch.dict("os.environ", env, clear=False):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.guard.apply_single_download(
                    [client],
                    usage_bytes=0,
                    monthly_limit_bytes=1000,
                    download_limit=1024,
                    limit_reason="unit test",
                    storage_guard=FakeStorageGuard(),
                )

        decision_logs = [
            json.loads(line)
            for line in stdout.getvalue().splitlines()
            if line.startswith("{")
        ]
        try_event = next(
            item for item in decision_logs
            if item.get("event") == "qbt_guard_decision"
            and item.get("action") == "try_candidate"
        )

        self.assertEqual("old", try_event["selected_torrent"]["hash"])
        self.assertEqual(1, try_event["rejected_counts"]["tv_queue_order_blocked"])
        self.assertIn(["old"], client.started)
        self.assertTrue(any("later" in hashes for hashes in client.stopped))


if __name__ == "__main__":
    unittest.main()
