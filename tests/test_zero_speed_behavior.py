import contextlib
import importlib
import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
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

        self.assertTrue(self.guard.is_productive_torrent({"state": "downloading", "dlspeed": 65_536}))

    def test_productive_torrents_must_meet_slow_rate_floor(self):
        env = {"QBT_SINGLE_DOWNLOAD_SLOW_MIN_RATE_BYTES_PER_SEC": "65536"}

        with mock.patch.dict("os.environ", env, clear=False):
            self.assertFalse(self.guard.is_productive_torrent({"state": "downloading", "dlspeed": 1024}))
            self.assertTrue(self.guard.is_productive_torrent({"state": "downloading", "dlspeed": 65_536}))

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
                {"amount_left": 850, "downloaded": 100, "dlspeed": 0},
                min_download_delta_bytes=100,
            ),
        )
        self.assertEqual(
            "",
            self.guard.torrent_progress_reason(
                before,
                {"amount_left": 950, "downloaded": 100, "dlspeed": 0},
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
        self.assertEqual(
            "",
            self.guard.torrent_progress_reason(
                {"amount_left": 1000, "downloaded": 100, "dlspeed": 1},
                {"amount_left": 1000, "downloaded": 100, "dlspeed": 2},
                min_download_delta_bytes=100,
            ),
        )

    def test_progress_classification_distinguishes_no_progress_modes(self):
        before = {"amount_left": 1000, "downloaded": 100, "dlspeed": 0, "progress": 0.5}

        active = self.guard.torrent_progress_classification(
            before,
            {"amount_left": 800, "downloaded": 300, "dlspeed": 0, "progress": 0.6},
            min_download_delta_bytes=100,
            sample_seconds=60,
        )
        self.assertEqual(self.guard.PROGRESS_CLASS_ACTIVELY_PROGRESSING, active.state)
        self.assertFalse(active.park_as_listener)

        slow = self.guard.torrent_progress_classification(
            before,
            {
                "amount_left": 950,
                "downloaded": 150,
                "dlspeed": 1024,
                "progress": 0.55,
                "num_seeds": 1,
            },
            min_download_delta_bytes=100,
            sample_seconds=60,
        )
        self.assertEqual(self.guard.PROGRESS_CLASS_SLOW_PROGRESSING, slow.state)
        self.assertTrue(slow.park_as_listener)

        missing_piece = self.guard.torrent_progress_classification(
            {
                "state": "stalledDL",
                "amount_left": 10,
                "downloaded": 990,
                "dlspeed": 0,
                "progress": 0.999,
            },
            {
                "state": "stalledDL",
                "amount_left": 10,
                "downloaded": 990,
                "dlspeed": 0,
                "progress": 0.999,
                "availability": 0.999,
                "num_seeds": 0,
                "num_complete": 0,
            },
            min_download_delta_bytes=100,
            sample_seconds=60,
        )
        self.assertEqual(self.guard.PROGRESS_CLASS_MISSING_FINAL_PIECE, missing_piece.state)
        self.assertTrue(missing_piece.park_as_listener)

        metadata = self.guard.torrent_progress_classification(
            None,
            {"state": "metaDL", "dlspeed": 0, "progress": 0.0},
            min_download_delta_bytes=100,
        )
        self.assertEqual(self.guard.PROGRESS_CLASS_METADATA_WAIT, metadata.state)
        self.assertEqual("metadata", metadata.cooldown_reason)
        self.assertTrue(metadata.park_as_listener)

        no_peers = self.guard.torrent_progress_classification(
            None,
            {
                "state": "stalledDL",
                "dlspeed": 0,
                "progress": 0.16,
                "availability": 1.0,
                "num_seeds": 0,
                "num_complete": 3,
            },
            min_download_delta_bytes=100,
        )
        self.assertEqual(self.guard.PROGRESS_CLASS_NO_CONNECTED_PEERS, no_peers.state)
        self.assertTrue(no_peers.park_as_listener)

        tracker_dead = self.guard.torrent_progress_classification(
            None,
            {
                "state": "stalledDL",
                "dlspeed": 0,
                "progress": 0.16,
                "availability": 0,
                "num_seeds": 0,
                "num_complete": 0,
            },
            min_download_delta_bytes=100,
        )
        self.assertEqual(self.guard.PROGRESS_CLASS_TRACKER_DEAD, tracker_dead.state)
        self.assertEqual("tracker-dead", tracker_dead.cooldown_reason)
        self.assertFalse(tracker_dead.park_as_listener)

    def test_adaptive_progress_threshold_scales_with_size_and_age(self):
        now = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)
        floor = 1024 * 1024

        small = {
            "amount_left": 500 * 1024 * 1024,
            "size": 500 * 1024 * 1024,
            "added_on": int(now.timestamp()),
        }
        large = {
            "amount_left": 100 * 1024 * 1024 * 1024,
            "size": 100 * 1024 * 1024 * 1024,
            "added_on": int(now.timestamp()),
        }
        old_large = dict(large)
        old_large["added_on"] = int(now.timestamp()) - (30 * 86_400)

        self.assertEqual(
            floor,
            self.guard.adaptive_progress_min_bytes(
                small,
                floor,
                size_fraction=0.0002,
                max_bytes=64 * 1024 * 1024,
                age_relief_days=30,
                age_relief_fraction=0.75,
                now=now,
            ),
        )
        self.assertEqual(
            21_474_837,
            self.guard.adaptive_progress_min_bytes(
                large,
                floor,
                size_fraction=0.0002,
                max_bytes=64 * 1024 * 1024,
                age_relief_days=30,
                age_relief_fraction=0.75,
                now=now,
            ),
        )
        self.assertEqual(
            5_368_710,
            self.guard.adaptive_progress_min_bytes(
                old_large,
                floor,
                size_fraction=0.0002,
                max_bytes=64 * 1024 * 1024,
                age_relief_days=30,
                age_relief_fraction=0.75,
                now=now,
            ),
        )

    def test_apply_single_download_parks_zero_speed_torrent_after_wait_without_bytes(self):
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
        park_event = next(
            item for item in decision_events
            if item.get("action") == "park_selected_no_progress"
        )

        self.assertEqual("zero", try_event["selected_torrent"]["hash"])
        self.assertEqual(1024, try_event["effective_cap"]["download_limit_bytes_per_sec"])
        self.assertEqual(0, try_event["budget"]["monthly_usage_bytes"])
        self.assertEqual(42, try_event["udm"]["stats_age_seconds"])
        self.assertEqual(4000, try_event["storage"]["headroom_bytes"])
        self.assertFalse(try_event["thermal"]["stop"])
        self.assertNotIn("client", try_event)
        self.assertNotIn("client", park_event)
        self.assertEqual(1, try_event["rejected_counts"]["not_productive_zero_speed"])
        self.assertEqual("zero", park_event["selected_torrent"]["hash"])
        self.assertEqual(1, park_event["rejected_counts"]["no_progress_after_wait"])
        self.assertIn(["zero"], client.started)
        self.assertFalse(any("zero" in hashes for hashes in client.stopped))
        self.assertEqual(0, client.stop_all_calls)
        self.assertEqual([], client.added_tags)

    def test_missing_final_piece_is_parked_as_listener_after_no_progress(self):
        class MissingPieceFakeQbtClient(FakeQbtClient):
            def start_hashes(self, hashes):
                super().start_hashes(hashes)
                for torrent in self.torrents:
                    if torrent["hash"] in hashes:
                        torrent["state"] = "stalledDL"
                        torrent["progress"] = 0.999
                        torrent["availability"] = 0.999
                        torrent["num_seeds"] = 0
                        torrent["num_complete"] = 0

        client = MissingPieceFakeQbtClient([
            {
                "hash": "final-piece",
                "name": "Marvels.Agents.of.S.H.I.E.L.D.S01.1080p",
                "category": "tv",
                "state": "stoppedDL",
                "dlspeed": 0,
                "amount_left": 10 * 1024 * 1024,
                "downloaded": 1000,
                "progress": 0.999,
                "availability": 0.999,
                "num_seeds": 0,
                "num_complete": 0,
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
                )

        decision_events = [
            json.loads(line)
            for line in stdout.getvalue().splitlines()
            if line.startswith("{") and json.loads(line).get("event") == "qbt_guard_decision"
        ]
        park_event = next(item for item in decision_events if item.get("action") == "park_selected_no_progress")

        self.assertEqual("final-piece", park_event["selected_torrent"]["hash"])
        self.assertIn("missing final pieces", park_event["reason"])
        self.assertEqual(1, park_event["rejected_counts"]["progress_missing_final_piece"])
        self.assertFalse(any("final-piece" in hashes for hashes in client.stopped))
        self.assertEqual([], client.added_tags)

    def test_tracker_dead_no_progress_is_stopped_even_when_parking_enabled(self):
        class TrackerDeadFakeQbtClient(FakeQbtClient):
            def start_hashes(self, hashes):
                super().start_hashes(hashes)
                for torrent in self.torrents:
                    if torrent["hash"] in hashes:
                        torrent["state"] = "stalledDL"
                        torrent["progress"] = 0.16
                        torrent["availability"] = 0
                        torrent["num_seeds"] = 0
                        torrent["num_complete"] = 0

        client = TrackerDeadFakeQbtClient([
            {
                "hash": "dead",
                "name": "Dead.At.Sixteen.Percent.1080p",
                "category": "tv",
                "state": "stoppedDL",
                "dlspeed": 0,
                "amount_left": 80 * 1024 * 1024 * 1024,
                "downloaded": 1000,
                "progress": 0.16,
                "availability": 0,
                "num_seeds": 0,
                "num_complete": 0,
                "tags": "",
            },
        ])
        env = {
            "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
            "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "60",
            "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
            "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
            "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
            "QBT_TV_QUEUE_SONARR_ENABLED": "false",
            "QBT_LOG_FORMAT": "json",
            "QBT_DECISION_LOG_LEVEL": "info",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            env["QBT_TORRENT_HEALTH_STATE_PATH"] = f"{tmpdir}/torrent-health.json"
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
                    )

        decision_events = [
            json.loads(line)
            for line in stdout.getvalue().splitlines()
            if line.startswith("{") and json.loads(line).get("event") == "qbt_guard_decision"
        ]
        stop_event = next(item for item in decision_events if item.get("action") == "stop_selected_no_progress")

        self.assertEqual("dead", stop_event["selected_torrent"]["hash"])
        self.assertIn("no connected peers", stop_event["reason"])
        self.assertEqual(1, stop_event["rejected_counts"]["progress_tracker_dead"])
        self.assertIn(["dead"], client.stopped)
        self.assertTrue(any("dead" in hashes for hashes, _tags in client.added_tags))

    def test_apply_single_download_parks_stalled_torrent_and_runs_replacement(self):
        client = FakeQbtClient([
            {
                "hash": "stalled",
                "name": "Stalled.S01E01",
                "category": "tv",
                "state": "stalledDL",
                "dlspeed": 0,
                "amount_left": 1000,
                "downloaded": 100,
                "progress": 0.5,
                "tags": "",
            },
            {
                "hash": "next",
                "name": "Next.S01E02",
                "category": "tv",
                "state": "stoppedDL",
                "dlspeed": 0,
                "amount_left": 2000,
                "downloaded": 0,
                "progress": 0.25,
                "tags": "",
            },
        ])
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
                    storage_guard=FakeStorageGuard(),
                    decision_context={},
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

        self.assertEqual("next", try_event["selected_torrent"]["hash"])
        self.assertEqual(1, try_event["candidate_counts"]["parked_stalled"])
        self.assertEqual(1, try_event["candidate_counts"]["smart_queue_worker_slots"])
        self.assertEqual(1, try_event["candidate_counts"]["smart_queue_parked_listener_slots"])
        self.assertEqual(2, try_event["candidate_counts"]["qbt_active_download_limit"])
        self.assertIn(["next"], client.started)
        self.assertFalse(any("stalled" in hashes for hashes in client.stopped))
        self.assertIn((2, None), client.queue_limits)
        self.assertEqual([], client.added_tags)

    def test_uncapped_window_raises_normal_active_download_limit(self):
        client = FakeQbtClient([
            {
                "hash": "one",
                "name": "One.S01E01",
                "category": "tv",
                "state": "stoppedDL",
                "dlspeed": 0,
                "amount_left": 1000,
                "downloaded": 0,
                "progress": 0.0,
                "tags": "",
            },
        ])
        env = {
            "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
            "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
            "QBT_SINGLE_DOWNLOAD_NORMAL_MAX_ACTIVE_DOWNLOADS": "1",
            "QBT_UNCAPPED_DOWNLOAD_WINDOW_MAX_ACTIVE_DOWNLOADS": "5",
            "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
            "QBT_TORRENT_HEALTH_SCORING_ENABLED": "false",
            "QBT_TV_QUEUE_SONARR_ENABLED": "false",
        }

        with mock.patch.dict("os.environ", env, clear=False):
            self.guard.apply_single_download(
                [client],
                usage_bytes=0,
                monthly_limit_bytes=1000,
                download_limit=0,
                limit_reason="unit test uncapped",
                storage_guard=FakeStorageGuard(),
                decision_context={
                    "budget": {
                        "uncapped_download_window_active": True,
                    },
                },
            )

        self.assertIn((5, None), client.queue_limits)

    def test_uncapped_window_allows_parked_stalled_slots_above_worker_limit(self):
        client = FakeQbtClient([
            {
                "hash": "stalled",
                "name": "Stalled.S01E01",
                "category": "tv",
                "state": "stalledDL",
                "dlspeed": 0,
                "amount_left": 1000,
                "downloaded": 100,
                "progress": 0.5,
                "tags": "",
            },
            {
                "hash": "next",
                "name": "Next.S01E02",
                "category": "tv",
                "state": "stoppedDL",
                "dlspeed": 0,
                "amount_left": 2000,
                "downloaded": 0,
                "progress": 0.25,
                "tags": "",
            },
        ])
        env = {
            "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
            "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
            "QBT_SINGLE_DOWNLOAD_NORMAL_MAX_ACTIVE_DOWNLOADS": "1",
            "QBT_UNCAPPED_DOWNLOAD_WINDOW_MAX_ACTIVE_DOWNLOADS": "5",
            "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
            "QBT_TORRENT_HEALTH_SCORING_ENABLED": "false",
            "QBT_TV_QUEUE_SONARR_ENABLED": "false",
        }

        with mock.patch.dict("os.environ", env, clear=False):
            self.guard.apply_single_download(
                [client],
                usage_bytes=0,
                monthly_limit_bytes=1000,
                download_limit=0,
                limit_reason="unit test uncapped",
                storage_guard=FakeStorageGuard(),
                decision_context={
                    "budget": {
                        "uncapped_download_window_active": True,
                    },
                },
            )

        self.assertIn(["next"], client.started)
        self.assertFalse(any("stalled" in hashes for hashes in client.stopped))
        self.assertIn((5, None), client.queue_limits)
        self.assertIn((6, None), client.queue_limits)

    def test_apply_single_download_keeps_low_speed_torrent_with_real_progress(self):
        class ProgressingFakeQbtClient(FakeQbtClient):
            def start_hashes(self, hashes):
                super().start_hashes(hashes)
                for torrent in self.torrents:
                    if torrent["hash"] in hashes:
                        torrent["state"] = "downloading"
                        torrent["dlspeed"] = 50_000
                        torrent["amount_left"] -= 2 * 1024 * 1024
                        torrent["downloaded"] += 2 * 1024 * 1024

        client = ProgressingFakeQbtClient([
            {
                "hash": "slow-progress",
                "name": "Slow.But.Progressing.S01E01",
                "category": "tv",
                "state": "stoppedDL",
                "dlspeed": 0,
                "amount_left": 10 * 1024 * 1024,
                "downloaded": 0,
                "progress": 0.5,
                "availability": 1.0,
                "num_seeds": 1,
                "num_complete": 1,
                "tags": "",
            },
        ])
        env = {
            "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
            "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "60",
            "QBT_SINGLE_DOWNLOAD_MIN_PROGRESS_BYTES": str(1024 * 1024),
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
                    download_limit=10_485_760,
                    limit_reason="unit test",
                    storage_guard=FakeStorageGuard(),
                )

        decision_events = [
            json.loads(line)
            for line in stdout.getvalue().splitlines()
            if line.startswith("{") and json.loads(line).get("event") == "qbt_guard_decision"
        ]
        decision_actions = [item["action"] for item in decision_events]
        self.assertIn("confirm_selected_productive", decision_actions)
        self.assertNotIn("stop_selected_too_slow", decision_actions)
        self.assertFalse(any("slow-progress" in hashes for hashes in client.stopped))
        self.assertFalse(client.added_tags)

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
        selected_score = try_event["selected_torrent"]["score"]
        self.assertTrue(selected_score["storage_fits"])
        self.assertIn("storage_fit", selected_score["components"])
        self.assertIn("storage_remaining", selected_score["components"])
        self.assertIn("queue_order", selected_score["components"])
        self.assertEqual(["small", "large"], [
            item["hash"] for item in try_event["selected_torrents"]
        ])
        self.assertTrue(try_event["candidate_counts"]["storage_constrained"])
        self.assertEqual(5, try_event["candidate_counts"]["smart_queue_worker_slots"])
        self.assertEqual(0, try_event["candidate_counts"]["smart_queue_parked_listener_slots"])
        self.assertEqual(5, try_event["candidate_counts"]["qbt_active_download_limit"])
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

    def test_storage_constrained_mode_parks_stalled_batch_members_and_refills_slots(self):
        torrents = []
        files = {}
        for index in range(7):
            item_hash = f"small-{index}"
            state = "stalledDL" if index < 6 else "stoppedDL"
            torrents.append(
                {
                    "hash": item_hash,
                    "name": f"Small.Left.{index}",
                    "category": "tv",
                    "state": state,
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
                    "progress": 0.99,
                    "priority": 1,
                },
            ]
        client = FakeQbtClient(torrents, files=files)
        with tempfile.TemporaryDirectory() as tmpdir:
            env = {
                "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
                "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "60",
                "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
                "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
                "QBT_TORRENT_HEALTH_STATE_PATH": f"{tmpdir}/torrent-health.json",
                "QBT_DOWNLOAD_STORAGE_RECOVERY_STALL_SAMPLES": "1",
                "QBT_DOWNLOAD_STORAGE_RECOVERY_MAX_PARKED_STALLED": "0",
                "QBT_TV_QUEUE_SONARR_ENABLED": "false",
                "QBT_LOG_FORMAT": "json",
                "QBT_DECISION_LOG_LEVEL": "info",
            }

            with mock.patch.dict("os.environ", env, clear=False), mock.patch.object(self.guard.time, "sleep"):
                health_store = self.guard.TorrentHealthStore()
                now = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)
                for torrent in torrents[:6]:
                    health_store.record_storage_recovery_no_progress(torrent, now)
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
        recovery_event = [
            item for item in decision_logs
            if item.get("event") == "qbt_guard_decision"
            and item.get("action") == "storage_recovery_batch"
            and "candidate_counts" in item
        ][-1]

        self.assertIn(["small-6"], client.started)
        self.assertEqual(6, recovery_event["candidate_counts"]["storage_recovery_parked_stalled"])
        self.assertEqual(["small-0", "small-1", "small-2", "small-3", "small-4", "small-5"], [
            item["hash"] for item in recovery_event["selected_torrents"][:6]
        ])
        self.assertEqual("small-6", recovery_event["selected_torrents"][6]["hash"])
        self.assertEqual(5, recovery_event["candidate_counts"]["smart_queue_worker_slots"])
        self.assertEqual(6, recovery_event["candidate_counts"]["smart_queue_parked_listener_slots"])
        self.assertEqual(11, recovery_event["candidate_counts"]["qbt_active_download_limit"])
        self.assertFalse(
            any(
                stopped_hash in {"small-0", "small-1", "small-2", "small-3", "small-4", "small-5"}
                for stop_call in client.stopped
                for stopped_hash in stop_call
            )
        )
        self.assertIn((11, None), client.queue_limits)

    def test_storage_constrained_mode_replaces_too_slow_recovery_worker(self):
        class SlowStartFakeQbtClient(FakeQbtClient):
            def start_hashes(self, hashes):
                super().start_hashes(hashes)
                for torrent in self.torrents:
                    if torrent["hash"] in hashes:
                        torrent["state"] = "downloading"
                        torrent["dlspeed"] = 1024

        torrents = [
            {
                "hash": "slow",
                "name": "Slow.Recovery.Worker",
                "category": "movies",
                "state": "stoppedDL",
                "dlspeed": 0,
                "amount_left": 1024 * 1024,
                "downloaded": 0,
                "progress": 0.99,
                "availability": 1.0,
                "num_seeds": 1,
                "num_complete": 1,
                "tags": "",
            },
            {
                "hash": "replacement",
                "name": "Replacement.Worker",
                "category": "movies",
                "state": "stoppedDL",
                "dlspeed": 0,
                "amount_left": 2 * 1024 * 1024,
                "downloaded": 0,
                "progress": 0.99,
                "availability": 1.0,
                "num_seeds": 2,
                "num_complete": 2,
                "tags": "",
            },
        ]
        files = {
            item["hash"]: [
                {
                    "name": f"{item['hash']}.mkv",
                    "size": 100 * 1024 * 1024,
                    "progress": 0.99,
                    "priority": 1,
                }
            ]
            for item in torrents
        }
        client = SlowStartFakeQbtClient(torrents, files=files)
        with tempfile.TemporaryDirectory() as tmpdir:
            env = {
                "QBT_DOWNLOAD_STORAGE_RECOVERY_MAX_ACTIVE": "1",
                "QBT_DOWNLOAD_STORAGE_RECOVERY_STALL_SAMPLES": "1",
                "QBT_DOWNLOAD_STORAGE_RECOVERY_MIN_RATE_BYTES_PER_SEC": "65536",
                "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
                "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "60",
                "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
                "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
                "QBT_TORRENT_HEALTH_STATE_PATH": f"{tmpdir}/torrent-health.json",
                "QBT_TV_QUEUE_SONARR_ENABLED": "false",
                "QBT_LOG_FORMAT": "json",
                "QBT_DECISION_LOG_LEVEL": "info",
            }

            with mock.patch.dict("os.environ", env, clear=False), mock.patch.object(self.guard.time, "sleep"):
                self.guard.apply_single_download(
                    [client],
                    usage_bytes=0,
                    monthly_limit_bytes=1000,
                    download_limit=1024,
                    limit_reason="unit test",
                    storage_guard=ConstrainedStorageGuard(),
                )
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
        recovery_event = [
            item for item in decision_logs
            if item.get("event") == "qbt_guard_decision"
            and item.get("action") == "storage_recovery_batch"
            and "candidate_counts" in item
        ][-1]

        self.assertIn(["replacement"], client.started)
        self.assertEqual(1, client.started.count(["slow"]))
        self.assertTrue(any("slow" in stop_call for stop_call in client.stopped))
        self.assertEqual(1, recovery_event["candidate_counts"]["storage_recovery_slow_excluded"])

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
        self.assertIn("score", preempt_event["selected_torrent"])
        self.assertIn("content_total", preempt_event["selected_torrent"]["score"]["components"])
        self.assertIn("queue_order", preempt_event["selected_torrent"]["score"]["components"])
        self.assertIn("candidate score", preempt_event["reason"])
        self.assertIn("score", preempt_event["rejected_torrents"][0]["torrent"])
        self.assertEqual(1, preempt_event["rejected_counts"]["preempted_productive"])
        self.assertEqual("challenger", try_event["selected_torrent"]["hash"])
        self.assertIn(["current"], client.stopped)
        self.assertIn(["challenger"], client.started)
        self.assertNotIn(["current"], client.started)

    def test_tag_only_cooldown_does_not_block_normal_selection(self):
        torrent = {
            "hash": "tagged",
            "name": "Tagged.Cooldown.Movie.1080p",
            "category": "movies",
            "state": "stoppedDL",
            "dlspeed": 0,
            "amount_left": 4 * 1024 * 1024 * 1024,
            "downloaded": 100,
            "progress": 0.5,
            "availability": 1.0,
            "num_seeds": 1,
            "num_complete": 1,
            "tags": "quota-stalled-no-progress-29990101T000000Z",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            env = {
                "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
                "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
                "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
                "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
                "QBT_TORRENT_HEALTH_STATE_PATH": f"{tmpdir}/torrent-health.json",
                "QBT_TV_QUEUE_SONARR_ENABLED": "false",
                "QBT_TV_WATCH_JELLYFIN_ENABLED": "false",
                "QBT_MOVIE_QUEUE_RADARR_ENABLED": "false",
                "QBT_LOG_FORMAT": "json",
                "QBT_DECISION_LOG_LEVEL": "info",
            }
            client = FakeQbtClient([torrent])
            with mock.patch.dict("os.environ", env, clear=False):
                self.guard.apply_single_download(
                    [client],
                    usage_bytes=0,
                    monthly_limit_bytes=1000,
                    download_limit=1024,
                    limit_reason="unit test",
                    storage_guard=FakeStorageGuard(),
                )

        self.assertIn(["tagged"], client.started)
        self.assertEqual(0, client.stop_all_calls)

    def test_health_cooldown_blocks_normal_selection_without_tag(self):
        torrent = {
            "hash": "cooling",
            "name": "Canonical.Cooldown.Movie.1080p",
            "category": "movies",
            "state": "stoppedDL",
            "dlspeed": 0,
            "amount_left": 4 * 1024 * 1024 * 1024,
            "downloaded": 100,
            "progress": 0.5,
            "availability": 1.0,
            "num_seeds": 1,
            "num_complete": 1,
            "tags": "",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            env = {
                "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
                "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
                "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
                "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
                "QBT_TORRENT_HEALTH_STATE_PATH": f"{tmpdir}/torrent-health.json",
                "QBT_TV_QUEUE_SONARR_ENABLED": "false",
                "QBT_TV_WATCH_JELLYFIN_ENABLED": "false",
                "QBT_MOVIE_QUEUE_RADARR_ENABLED": "false",
                "QBT_LOG_FORMAT": "json",
                "QBT_DECISION_LOG_LEVEL": "info",
            }
            with mock.patch.dict("os.environ", env, clear=False):
                health_store = self.guard.TorrentHealthStore()
                health_store.record_failure(
                    torrent,
                    datetime.now(timezone.utc),
                    "did not make progress",
                    cooldown_reason="no-progress",
                )
                client = FakeQbtClient([torrent])

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

        decision_events = [
            json.loads(line)
            for line in stdout.getvalue().splitlines()
            if line.startswith("{") and json.loads(line).get("event") == "qbt_guard_decision"
        ]
        no_available = next(
            item for item in decision_events
            if item.get("action") == "stop_no_available_candidates"
        )

        self.assertEqual([], client.started)
        self.assertEqual(1, client.stop_all_calls)
        self.assertEqual(1, no_available["rejected_counts"]["cooldown"])
        self.assertEqual(1, no_available["rejected_counts"]["cooldown_no_progress"])

    def test_selection_lease_blocks_productive_preemption_with_connected_peer(self):
        current = {
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
        }
        challenger = {
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
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            env = {
                "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
                "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
                "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
                "QBT_SINGLE_DOWNLOAD_SELECTION_STRATEGY": "balanced",
                "QBT_SINGLE_DOWNLOAD_PREEMPT_PRODUCTIVE_ENABLED": "true",
                "QBT_SINGLE_DOWNLOAD_PREEMPT_PRODUCTIVE_SCORE_MARGIN": "20",
                "QBT_SINGLE_DOWNLOAD_SELECTION_LEASE_SECONDS": "900",
                "QBT_SINGLE_DOWNLOAD_SELECTION_LEASE_PEER_GRACE_SECONDS": "900",
                "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
                "QBT_TORRENT_HEALTH_STATE_PATH": f"{tmpdir}/torrent-health.json",
                "QBT_TV_QUEUE_SONARR_ENABLED": "false",
                "QBT_TV_WATCH_JELLYFIN_ENABLED": "false",
                "QBT_MOVIE_QUEUE_RADARR_ENABLED": "false",
                "QBT_LOG_FORMAT": "json",
                "QBT_DECISION_LOG_LEVEL": "info",
            }
            with mock.patch.dict("os.environ", env, clear=False):
                health_store = self.guard.TorrentHealthStore()
                now = datetime.now(timezone.utc)
                health_store.record_attempt(current, now)
                health_store.observe_torrents([current], now)
                client = FakeQbtClient([current, challenger])

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

        decision_events = [
            json.loads(line)
            for line in stdout.getvalue().splitlines()
            if line.startswith("{") and json.loads(line).get("event") == "qbt_guard_decision"
        ]
        actions = [item.get("action") for item in decision_events]
        keep_event = next(item for item in decision_events if item.get("action") == "keep_productive")

        self.assertNotIn("preempt_productive", actions)
        self.assertEqual("current", keep_event["selected_torrent"]["hash"])
        self.assertIn("selection lease active", keep_event["reason"])
        self.assertEqual(1, keep_event["candidate_counts"]["selection_lease_active"])
        self.assertEqual(1, keep_event["candidate_counts"]["selection_lease_preempt_blocked"])
        self.assertNotIn(["current"], client.stopped)
        self.assertNotIn(["challenger"], client.started)

    def test_selection_lease_keeps_recent_peer_worker_over_higher_score_candidate(self):
        current = {
            "hash": "current",
            "name": "Current.Movie.1080p",
            "category": "movies",
            "state": "downloading",
            "dlspeed": 0,
            "amount_left": 60 * 1024 * 1024 * 1024,
            "downloaded": 100,
            "progress": 0.25,
            "availability": 1.0,
            "num_seeds": 0,
            "num_leechs": 1,
            "num_complete": 1,
            "tags": "",
        }
        challenger = {
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
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            env = {
                "QBT_SINGLE_DOWNLOAD_MAX_ATTEMPTS_PER_RUN": "1",
                "QBT_SINGLE_DOWNLOAD_STALL_CHECK_SECONDS": "0",
                "QBT_SINGLE_DOWNLOAD_MAX_RUN_SECONDS": "3600",
                "QBT_SINGLE_DOWNLOAD_SELECTION_STRATEGY": "balanced",
                "QBT_SINGLE_DOWNLOAD_SELECTION_LEASE_SECONDS": "900",
                "QBT_SINGLE_DOWNLOAD_SELECTION_LEASE_PEER_GRACE_SECONDS": "900",
                "QBT_SINGLE_DOWNLOAD_TV_FILE_PRIORITY_ENABLED": "false",
                "QBT_TORRENT_HEALTH_SCORING_ENABLED": "true",
                "QBT_TORRENT_HEALTH_STATE_PATH": f"{tmpdir}/torrent-health.json",
                "QBT_TV_QUEUE_SONARR_ENABLED": "false",
                "QBT_TV_WATCH_JELLYFIN_ENABLED": "false",
                "QBT_MOVIE_QUEUE_RADARR_ENABLED": "false",
                "QBT_LOG_FORMAT": "json",
                "QBT_DECISION_LOG_LEVEL": "info",
            }
            with mock.patch.dict("os.environ", env, clear=False):
                health_store = self.guard.TorrentHealthStore()
                now = datetime.now(timezone.utc)
                health_store.record_attempt(current, now)
                health_store.observe_torrents([current], now)
                client = FakeQbtClient([current, challenger])

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

        decision_events = [
            json.loads(line)
            for line in stdout.getvalue().splitlines()
            if line.startswith("{") and json.loads(line).get("event") == "qbt_guard_decision"
        ]
        keep_event = next(item for item in decision_events if item.get("action") == "keep_selection_lease")

        self.assertEqual("current", keep_event["selected_torrent"]["hash"])
        self.assertIn("selection lease active", keep_event["reason"])
        self.assertEqual(1, keep_event["candidate_counts"]["selection_lease_active"])
        self.assertNotIn(["current"], client.stopped)
        self.assertIn(["challenger"], client.stopped)
        self.assertNotIn(["challenger"], client.started)

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
