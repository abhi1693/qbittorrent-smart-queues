import importlib
import unittest


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
