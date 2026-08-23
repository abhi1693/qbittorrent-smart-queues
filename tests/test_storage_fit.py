import importlib
import math
import os
import tempfile
import unittest
from unittest.mock import patch


class StorageFitTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_selected_file_remaining_state_ignores_unselected_files(self):
        files = [
            {"name": "wanted.mkv", "size": 1000, "progress": 0.25, "priority": 1},
            {"name": "also-wanted.mkv", "size": 2000, "progress": 0.5, "priority": 6},
            {"name": "sample.mkv", "size": 5000, "progress": 0.0, "priority": 0},
        ]

        state = self.guard.selected_file_remaining_state(files)

        self.assertEqual(1750, state["remaining_bytes"])
        self.assertEqual(2, state["selected_count"])
        self.assertEqual(3000, state["selected_size"])
        self.assertEqual(1250, state["present_bytes"])

    def test_selected_file_remaining_state_marks_no_selected_files_unknown(self):
        files = [
            {"name": "ignored.mkv", "size": 5000, "progress": 0.0, "priority": 0},
        ]

        state = self.guard.selected_file_remaining_state(files)

        self.assertIsNone(state["remaining_bytes"])
        self.assertEqual(0, state["selected_count"])

    def test_directory_allocated_bytes_counts_hardlinks_once(self):
        with tempfile.TemporaryDirectory() as directory:
            source = os.path.join(directory, "source.bin")
            alias = os.path.join(directory, "alias.bin")
            with open(source, "wb") as file_handle:
                file_handle.write(b"x" * 8192)
            usage_before_link = self.guard.directory_allocated_bytes(directory)
            os.link(source, alias)

            usage_after_link = self.guard.directory_allocated_bytes(directory)

        self.assertEqual(usage_before_link, usage_after_link)

    def test_configured_capacity_uses_quota_root_usage_instead_of_filesystem_capacity(self):
        capacity_bytes = 100 * 1024 * 1024
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "payload.bin"), "wb") as file_handle:
                file_handle.write(b"x" * 8192)
            expected_usage = self.guard.directory_allocated_bytes(directory)
            with patch.dict(
                os.environ,
                {
                    "QBT_DOWNLOAD_STORAGE_PATH": directory,
                    "QBT_DOWNLOAD_STORAGE_CAPACITY_BYTES": str(capacity_bytes),
                    "QBT_DOWNLOAD_STORAGE_MIN_FREE_BYTES": "0",
                    "QBT_DOWNLOAD_STORAGE_MIN_FREE_FRACTION": "0",
                    "QBT_DOWNLOAD_STORAGE_USAGE_CACHE_SECONDS": "0",
                },
            ):
                state = self.guard.DownloadStorageGuard().snapshot()

        self.assertEqual("configured", state["capacity_source"])
        self.assertEqual(capacity_bytes, state["total_bytes"])
        self.assertEqual(expected_usage, state["used_bytes"])
        self.assertEqual(capacity_bytes - expected_usage, state["free_bytes"])

    def test_configured_capacity_sets_fractional_reserve_from_quota(self):
        capacity_bytes = 100 * 1024 * 1024
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {
                    "QBT_DOWNLOAD_STORAGE_PATH": directory,
                    "QBT_DOWNLOAD_STORAGE_CAPACITY_BYTES": str(capacity_bytes),
                    "QBT_DOWNLOAD_STORAGE_MIN_FREE_BYTES": "0",
                    "QBT_DOWNLOAD_STORAGE_MIN_FREE_FRACTION": "0.10",
                },
            ):
                state = self.guard.DownloadStorageGuard().snapshot()

        self.assertEqual(math.floor(capacity_bytes * 0.10), state["reserve_bytes"])

    def test_configured_capacity_fails_closed_when_usage_scan_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.dict(
                    os.environ,
                    {
                        "QBT_DOWNLOAD_STORAGE_PATH": directory,
                        "QBT_DOWNLOAD_STORAGE_CAPACITY_BYTES": "4000000000000",
                        "QBT_DOWNLOAD_STORAGE_FAIL_CLOSED": "true",
                    },
                ),
                patch.object(
                    self.guard,
                    "directory_allocated_bytes",
                    side_effect=OSError("permission denied"),
                ),
            ):
                state = self.guard.DownloadStorageGuard().snapshot()

        self.assertTrue(state["stop"])
        self.assertIn("usage scan failed", state["reason"])

    def test_storage_fit_blocks_incomplete_torrent_with_unknown_remaining_size(self):
        class Client:
            def torrent_files(self, item_hash):
                return []

        storage_state = {
            "enabled": True,
            "stop": True,
            "reason": "reserve reached",
            "free_bytes": 1000,
            "reserve_bytes": 2000,
            "headroom_bytes": 0,
        }

        class StorageGuard:
            require_torrent_fit = True

        reason = self.guard.storage_torrent_block_reason(
            Client(),
            {"hash": "abc", "name": "unknown", "amount_left": 0, "progress": 0.5},
            StorageGuard(),
            storage_state,
        )

        self.assertIn("unknown", reason)

    def test_storage_recovery_sort_places_unknown_remaining_size_last(self):
        class Client:
            def torrent_files(self, item_hash):
                if item_hash == "unknown":
                    return []
                return [
                    {"name": "known.mkv", "size": 1000, "progress": 0.5, "priority": 1},
                ]

        storage_state = {
            "enabled": True,
            "stop": True,
            "reason": "reserve reached",
            "free_bytes": 2000,
            "reserve_bytes": 3000,
            "headroom_bytes": 0,
        }

        class StorageGuard:
            require_torrent_fit = True

        ordered = sorted(
            [
                {"hash": "unknown", "name": "unknown", "amount_left": 0, "progress": 0.5},
                {"hash": "known", "name": "known", "amount_left": 500, "progress": 0.5},
            ],
            key=lambda torrent: self.guard.storage_recovery_sort_remaining_bytes(
                Client(),
                torrent,
                StorageGuard(),
                storage_state,
            ),
        )

        self.assertEqual(["known", "unknown"], [item["hash"] for item in ordered])


if __name__ == "__main__":
    unittest.main()
