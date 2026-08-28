import unittest
from unittest import mock


class ProbeClient:
    def __init__(self, samples):
        self.samples = list(samples)
        self.download_limits = []
        self.added_tags = []
        self.removed_tags = []
        self.reannounced = []
        self.started = []
        self.stopped = []
        self.deleted = []

    def set_torrent_download_limit(self, hashes, limit):
        self.download_limits.append((list(hashes), limit))

    def add_tags(self, hashes, tags):
        self.added_tags.append((list(hashes), list(tags)))

    def remove_tags(self, hashes, tags):
        self.removed_tags.append((list(hashes), list(tags)))

    def reannounce_hashes(self, hashes):
        self.reannounced.append(list(hashes))

    def start_hashes(self, hashes):
        self.started.append(list(hashes))

    def stop_hashes(self, hashes):
        self.stopped.append(list(hashes))

    def delete_hashes(self, hashes, delete_files):
        self.deleted.append((list(hashes), delete_files))

    def torrent_info(self, item_hash):
        if not self.samples:
            raise AssertionError("unexpected availability sample")
        sample = dict(self.samples.pop(0))
        sample.setdefault("hash", item_hash)
        sample.setdefault("name", "Probe Torrent")
        sample.setdefault("state", "downloading")
        sample.setdefault("progress", 0.5)
        sample.setdefault("amount_left", 1024)
        sample.setdefault("category", "tv")
        sample.setdefault("tags", "availability-probe")
        return sample


class StaticQueue:
    def __init__(self, metadata, configs=None):
        self.metadata = metadata
        self._configs = configs or []

    def torrent_metadata(self, torrent):
        return self.metadata

    def configs(self):
        return list(self._configs)


class CooldownStore:
    def __init__(self, states):
        self.states = dict(states)

    def cooldown_state(self, torrent, now=None, scope=None):
        return dict(self.states.get(torrent.get("hash"), {}))

    def active_cooldown_state(self, torrent, now, scope=None):
        state = self.cooldown_state(torrent, now, scope)
        if state.get("active"):
            return state
        return {}

    def storage_recovery_no_progress_samples(self, torrent):
        return 0


class AvailabilityAdmissionTests(unittest.TestCase):
    def setUp(self):
        from qbittorrent_smart_queues import guard

        self.guard = guard
        self.env = {
            "QBT_AVAILABILITY_ADMISSION_ENABLED": "true",
            "QBT_AVAILABILITY_PROBE_INTERVAL_SECONDS": "0",
            "QBT_AVAILABILITY_PROBE_SAMPLES": "6",
            "QBT_AVAILABILITY_REQUIRED_BELOW_MINIMUM_SAMPLES": "5",
            "QBT_AVAILABILITY_MIN_COMPLETE": "1",
            "QBT_AVAILABILITY_PROBE_DOWNLOAD_LIMIT_BYTES_PER_SEC": "1048576",
        }
        self.base_torrent = {
            "hash": "abc123",
            "name": "Probe Torrent",
            "state": "stalledDL",
            "progress": 0.999,
            "amount_left": 1024,
            "downloaded": 10_000,
            "availability": 0.999,
            "category": "tv",
            "tags": "availability-probe",
        }

    def test_transient_incomplete_availability_is_admitted_when_a_complete_sample_arrives(self):
        client = ProbeClient([
            {"availability": 0.851},
            {"availability": 1.851},
        ])

        with mock.patch.dict("os.environ", self.env, clear=True):
            result = self.guard.process_availability_admission_torrent(
                client,
                self.base_torrent,
                StaticQueue(None),
                StaticQueue(None),
            )

        self.assertEqual("admitted", result["status"])
        self.assertEqual(2, result["samples"])
        self.assertEqual(1, result["below_minimum_samples"])
        self.assertEqual([], client.deleted)
        self.assertEqual([(["abc123"], ["availability-probe"])], client.removed_tags)
        self.assertEqual(1, len(client.added_tags))
        self.assertEqual(["abc123"], client.added_tags[0][0])
        self.assertEqual(1, len(client.added_tags[0][1]))
        self.assertRegex(
            client.added_tags[0][1][0],
            r"^availability-verified-\d{8}T\d{6}Z$",
        )
        self.assertEqual([(["abc123"], 0)], client.download_limits)

    def test_stably_incomplete_availability_is_blocklisted_through_sonarr(self):
        client = ProbeClient([{"availability": 0.999}] * 6)
        sonarr = StaticQueue(
            {"queue_id": 42, "source": "sonarr"},
            [("sonarr", "http://sonarr.test", "api-key")],
        )

        with mock.patch.dict("os.environ", self.env, clear=True), \
                mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
            result = self.guard.process_availability_admission_torrent(
                client,
                self.base_torrent,
                sonarr,
                StaticQueue(None),
            )

        self.assertEqual("rejected", result["status"])
        self.assertEqual(6, result["samples"])
        self.assertEqual(6, result["below_minimum_samples"])
        request_json.assert_called_once()
        delete_url = request_json.call_args.args[2]
        self.assertIn("/api/v3/queue/42?", delete_url)
        self.assertIn("removeFromClient=true", delete_url)
        self.assertIn("blocklist=true", delete_url)
        self.assertIn("skipRedownload=false", delete_url)
        self.assertEqual([], client.deleted)

    def test_unknown_availability_is_stopped_and_deferred_without_deletion(self):
        client = ProbeClient([{"availability": 0}] * 6)

        with mock.patch.dict("os.environ", self.env, clear=True):
            result = self.guard.process_availability_admission_torrent(
                client,
                self.base_torrent,
                StaticQueue(None),
                StaticQueue(None),
            )

        self.assertEqual("deferred", result["status"])
        self.assertEqual(0, result["below_minimum_samples"])
        self.assertEqual([["abc123"]], client.stopped)
        self.assertEqual([], client.deleted)

    def test_failed_arr_rejection_is_stopped_and_excluded_from_selection(self):
        client = ProbeClient([{"availability": 0.999}] * 6)
        sonarr = StaticQueue(
            {"queue_id": 42, "source": "sonarr"},
            [("sonarr", "http://sonarr.test", "api-key")],
        )

        with mock.patch.dict("os.environ", self.env, clear=True), \
                mock.patch.object(
                    self.guard,
                    "request_json",
                    side_effect=self.guard.ApiError("Sonarr unavailable"),
                ):
            result = self.guard.process_availability_admission_torrent(
                client,
                self.base_torrent,
                sonarr,
                StaticQueue(None),
            )
            failed_torrent = dict(
                self.base_torrent,
                tags="availability-probe,availability-rejection-failed",
            )
            block_reason = self.guard.availability_admission_block_reason(failed_torrent)

        self.assertEqual("rejection-failed", result["status"])
        self.assertEqual("availability_rejection_failed", block_reason)
        self.assertEqual([["abc123"]], client.stopped)
        self.assertEqual(
            [(["abc123"], ["availability-rejection-failed"])],
            client.added_tags,
        )
        self.assertEqual([], client.removed_tags)

    def test_new_worker_is_capped_and_tagged_before_admission(self):
        torrent = dict(self.base_torrent, tags="", availability=0)
        client = ProbeClient([])

        with mock.patch.dict("os.environ", self.env, clear=True):
            prepared = self.guard.prepare_availability_probe_torrents(client, [torrent])

        self.assertEqual(["abc123"], prepared)
        self.assertEqual([(["abc123"], 1_048_576)], client.download_limits)
        self.assertEqual([(["abc123"], ["availability-probe"])], client.added_tags)

    def test_recent_verification_defers_reprobe_until_recheck_window_expires(self):
        torrent = dict(
            self.base_torrent,
            tags="availability-verified-20260827T100000Z",
        )

        with mock.patch.dict("os.environ", self.env, clear=True):
            fresh = self.guard.availability_probe_candidate(
                torrent,
                now=self.guard.datetime(2026, 8, 27, 12, 0, tzinfo=self.guard.timezone.utc),
            )
            expired = self.guard.availability_probe_candidate(
                torrent,
                now=self.guard.datetime(2026, 8, 27, 17, 0, tzinfo=self.guard.timezone.utc),
            )

        self.assertFalse(fresh)
        self.assertTrue(expired)

    def test_tracker_dead_probe_uses_shorter_retry_than_generic_cooldown(self):
        now = self.guard.datetime(2026, 8, 28, 12, 0, tzinfo=self.guard.timezone.utc)
        state = {
            "active": True,
            "reason": self.guard.STALL_COOLDOWN_REASON_TRACKER_DEAD,
            "last_tried_at": "2026-08-28T11:20:00Z",
            "next_retry_at": "2026-08-29T11:20:00Z",
        }

        with mock.patch.dict(
            "os.environ",
            {**self.env, "QBT_AVAILABILITY_PROBE_TRACKER_DEAD_RETRY_SECONDS": "1800"},
            clear=True,
        ):
            eligible = self.guard.availability_probe_candidate(
                self.base_torrent,
                CooldownStore({"abc123": state}),
                now,
            )

        self.assertTrue(eligible)

    def test_tracker_dead_probe_respects_dedicated_retry_window(self):
        now = self.guard.datetime(2026, 8, 28, 12, 0, tzinfo=self.guard.timezone.utc)
        state = {
            "active": True,
            "reason": self.guard.STALL_COOLDOWN_REASON_TRACKER_DEAD,
            "last_tried_at": "2026-08-28T11:50:00Z",
            "next_retry_at": "2026-08-29T11:50:00Z",
        }

        with mock.patch.dict(
            "os.environ",
            {**self.env, "QBT_AVAILABILITY_PROBE_TRACKER_DEAD_RETRY_SECONDS": "1800"},
            clear=True,
        ):
            eligible = self.guard.availability_probe_candidate(
                self.base_torrent,
                CooldownStore({"abc123": state}),
                now,
            )

        self.assertFalse(eligible)

    def test_tracker_dead_override_does_not_bypass_no_progress_cooldown(self):
        now = self.guard.datetime(2026, 8, 28, 12, 0, tzinfo=self.guard.timezone.utc)
        state = {
            "active": True,
            "reason": self.guard.STALL_COOLDOWN_REASON_NO_PROGRESS,
            "last_tried_at": "2026-08-28T10:00:00Z",
            "next_retry_at": "2026-08-29T10:00:00Z",
        }

        with mock.patch.dict(
            "os.environ",
            {**self.env, "QBT_AVAILABILITY_PROBE_TRACKER_DEAD_RETRY_SECONDS": "1800"},
            clear=True,
        ):
            eligible = self.guard.availability_probe_candidate(
                self.base_torrent,
                CooldownStore({"abc123": state}),
                now,
            )

        self.assertFalse(eligible)

    def test_probe_candidates_rotate_oldest_attempt_first(self):
        now = self.guard.datetime(2026, 8, 28, 12, 0, tzinfo=self.guard.timezone.utc)
        older = dict(self.base_torrent, hash="older", name="Older", progress=0.1)
        newer = dict(self.base_torrent, hash="newer", name="Newer", progress=0.9)
        store = CooldownStore({
            "older": {
                "active": True,
                "reason": self.guard.STALL_COOLDOWN_REASON_TRACKER_DEAD,
                "last_tried_at": "2026-08-28T09:00:00Z",
                "next_retry_at": "2026-08-29T09:00:00Z",
            },
            "newer": {
                "active": True,
                "reason": self.guard.STALL_COOLDOWN_REASON_TRACKER_DEAD,
                "last_tried_at": "2026-08-28T10:00:00Z",
                "next_retry_at": "2026-08-29T10:00:00Z",
            },
        })

        with mock.patch.dict(
            "os.environ",
            {**self.env, "QBT_AVAILABILITY_PROBE_TRACKER_DEAD_RETRY_SECONDS": "1800"},
            clear=True,
        ):
            candidates = self.guard.availability_probe_candidates(
                [newer, older],
                store,
                now,
            )

        self.assertEqual(["older", "newer"], [torrent["hash"] for torrent in candidates])

    def test_probe_attempt_limit_defaults_to_two(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(2, self.guard.availability_probe_max_attempts_per_run())

    def test_idle_probe_attempt_limit_can_exceed_normal_limit(self):
        env = {
            "QBT_AVAILABILITY_PROBE_MAX_ATTEMPTS_PER_RUN": "2",
            "QBT_AVAILABILITY_IDLE_MAX_ATTEMPTS_PER_RUN": "10",
        }

        with mock.patch.dict("os.environ", env, clear=True):
            self.assertEqual(
                2,
                self.guard.availability_probe_max_attempts_per_run(queue_idle=False),
            )
            self.assertEqual(
                10,
                self.guard.availability_probe_max_attempts_per_run(queue_idle=True),
            )

    def test_idle_probe_run_budget_never_shortens_normal_budget(self):
        with mock.patch.dict(
            "os.environ",
            {"QBT_AVAILABILITY_IDLE_MAX_RUN_SECONDS": "30"},
            clear=True,
        ):
            self.assertEqual(
                180,
                self.guard.availability_probe_max_run_seconds(180, queue_idle=True),
            )


if __name__ == "__main__":
    unittest.main()
