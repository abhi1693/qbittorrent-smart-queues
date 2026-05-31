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


if __name__ == "__main__":
    unittest.main()
