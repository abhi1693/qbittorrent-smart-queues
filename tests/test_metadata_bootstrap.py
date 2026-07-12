import importlib
import unittest


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += max(0.0, float(seconds))


class FakeMetadataClient:
    def __init__(
        self,
        guard,
        torrent_states,
        start_error=None,
        stop_error=None,
        stop_all_error=None,
        preallocate_all=False,
    ):
        self.guard = guard
        self.torrent_states = [dict(torrent) for torrent in torrent_states]
        self.start_error = start_error
        self.stop_error = stop_error
        self.stop_all_error = stop_all_error
        self.preallocate_all = preallocate_all
        self.calls = []

    def app_preferences(self):
        self.calls.append(("preferences",))
        return {"preallocate_all": self.preallocate_all}

    def top_priority(self, hashes):
        self.calls.append(("top_priority", list(hashes)))

    def set_torrent_download_limit(self, hashes, limit):
        self.calls.append(("download_limit", list(hashes), limit))

    def set_torrent_upload_limit(self, hashes, limit):
        self.calls.append(("upload_limit", list(hashes), limit))

    def start_hashes(self, hashes):
        self.calls.append(("start", list(hashes)))
        if self.start_error:
            raise self.guard.ApiError(self.start_error)

    def stop_hashes(self, hashes):
        self.calls.append(("stop", list(hashes)))
        if self.stop_error:
            raise self.guard.ApiError(self.stop_error)

    def stop_all(self):
        self.calls.append(("stop_all",))
        if self.stop_all_error:
            raise self.guard.ApiError(self.stop_all_error)

    def torrent_info(self, item_hash):
        self.calls.append(("info", item_hash))
        if len(self.torrent_states) > 1:
            return dict(self.torrent_states.pop(0))
        return dict(self.torrent_states[0]) if self.torrent_states else None


class MetadataBootstrapTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def torrent(self, has_metadata=False, **overrides):
        torrent = {
            "hash": "abc123",
            "name": "Example magnet",
            "state": "stoppedDL",
            "progress": 0.0,
            "amount_left": 0,
            "size": -1,
            "total_size": -1,
            "has_metadata": has_metadata,
            "dl_limit": -1,
            "up_limit": -1,
        }
        torrent.update(overrides)
        return torrent

    def bootstrap(self, client, torrent, timeout=2.0, poll=1.0):
        clock = FakeClock()
        result = self.guard.bootstrap_torrent_metadata(
            client,
            torrent,
            timeout,
            poll,
            download_limit_bytes_per_second=65_536,
            upload_limit_bytes_per_second=16_384,
            sleep_fn=clock.sleep,
            monotonic_fn=clock.monotonic,
        )
        return result, clock

    def test_fetches_metadata_then_stops_and_restores_limits(self):
        unknown = self.torrent()
        ready = self.torrent(has_metadata=True, amount_left=1_000, total_size=2_000)
        client = FakeMetadataClient(self.guard, [unknown, ready, ready])

        result, clock = self.bootstrap(client, unknown)

        self.assertEqual("ready", result.status)
        self.assertTrue(result.torrent["has_metadata"])
        self.assertEqual(1.0, clock.now)
        self.assertEqual(
            [
                ("top_priority", ["abc123"]),
                ("download_limit", ["abc123"], 65_536),
                ("upload_limit", ["abc123"], 16_384),
                ("start", ["abc123"]),
                ("info", "abc123"),
                ("info", "abc123"),
                ("stop", ["abc123"]),
                ("download_limit", ["abc123"], 0),
                ("upload_limit", ["abc123"], 0),
                ("info", "abc123"),
            ],
            client.calls,
        )

    def test_timeout_still_stops_and_final_refresh_closes_race(self):
        unknown = self.torrent()
        ready = self.torrent(has_metadata=True, amount_left=1_000, total_size=2_000)
        client = FakeMetadataClient(self.guard, [unknown, unknown, unknown, ready])

        result, clock = self.bootstrap(client, unknown)

        self.assertEqual("ready", result.status)
        self.assertEqual("torrent metadata received", result.reason)
        self.assertEqual(2.0, clock.now)
        self.assertIn(("stop", ["abc123"]), client.calls)
        self.assertLess(
            client.calls.index(("stop", ["abc123"])),
            len(client.calls) - 1,
        )

    def test_timeout_without_metadata_is_reported_and_cleaned_up(self):
        unknown = self.torrent()
        client = FakeMetadataClient(self.guard, [unknown])

        result, clock = self.bootstrap(client, unknown)

        self.assertEqual("timeout", result.status)
        self.assertIn("did not arrive", result.reason)
        self.assertEqual(2.0, clock.now)
        self.assertIn(("stop", ["abc123"]), client.calls)
        self.assertIn(("download_limit", ["abc123"], 0), client.calls)
        self.assertIn(("upload_limit", ["abc123"], 0), client.calls)

    def test_start_failure_is_error_but_cleanup_still_runs(self):
        unknown = self.torrent(dl_limit=32_768, up_limit=8_192)
        client = FakeMetadataClient(
            self.guard,
            [unknown],
            start_error="cannot start torrent",
        )

        result, _ = self.bootstrap(client, unknown)

        self.assertEqual("error", result.status)
        self.assertIn("cannot start torrent", result.reason)
        self.assertIn(("stop", ["abc123"]), client.calls)
        self.assertIn(("download_limit", ["abc123"], 32_768), client.calls)
        self.assertIn(("upload_limit", ["abc123"], 8_192), client.calls)

    def test_failed_stop_paths_retain_safety_limits(self):
        unknown = self.torrent()
        client = FakeMetadataClient(
            self.guard,
            [unknown],
            stop_error="targeted stop failed",
            stop_all_error="global stop failed",
        )

        result, _ = self.bootstrap(client, unknown, timeout=0)

        self.assertEqual("error", result.status)
        self.assertIn("retained metadata bootstrap traffic limits", result.reason)
        self.assertEqual(
            [("download_limit", ["abc123"], 65_536)],
            [call for call in client.calls if call[0] == "download_limit"],
        )
        self.assertEqual(
            [("upload_limit", ["abc123"], 16_384)],
            [call for call in client.calls if call[0] == "upload_limit"],
        )

    def test_preallocation_blocks_metadata_bootstrap(self):
        client = FakeMetadataClient(
            self.guard,
            [self.torrent()],
            preallocate_all=True,
        )

        reason = self.guard.metadata_bootstrap_safety_block_reason(client)

        self.assertIn("preallocation is enabled", reason)
        self.assertEqual([("preferences",)], client.calls)

    def test_already_available_metadata_does_not_touch_qbittorrent(self):
        ready = self.torrent(has_metadata=True, amount_left=1_000)
        client = FakeMetadataClient(self.guard, [ready])

        result, _ = self.bootstrap(client, ready)

        self.assertEqual("ready", result.status)
        self.assertEqual([], client.calls)

    def test_explicit_has_metadata_false_wins_over_stale_size_fields(self):
        torrent = self.torrent(has_metadata=False, amount_left=1_000, total_size=2_000)

        self.assertFalse(self.guard.torrent_has_metadata(torrent))
        self.assertTrue(self.guard.torrent_metadata_missing(torrent))


if __name__ == "__main__":
    unittest.main()
