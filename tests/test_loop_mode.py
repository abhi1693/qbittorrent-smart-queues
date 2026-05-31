import importlib
import unittest
from unittest import mock


class LoopModeTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_continuous_guard_mode_dispatches_to_loop(self):
        with mock.patch.dict("os.environ", {"QBT_GUARD_MODE": "continuous"}, clear=True), \
                mock.patch.object(self.guard, "run_loop", return_value=0) as run_loop, \
                mock.patch.object(self.guard, "run_once", return_value=0) as run_once:
            result = self.guard.main()

        self.assertEqual(0, result)
        run_loop.assert_called_once()
        run_once.assert_not_called()

    def test_loop_enabled_env_dispatches_to_loop(self):
        with mock.patch.dict("os.environ", {"QBT_GUARD_LOOP_ENABLED": "true"}, clear=True), \
                mock.patch.object(self.guard, "run_loop", return_value=0) as run_loop, \
                mock.patch.object(self.guard, "run_once", return_value=0) as run_once:
            result = self.guard.main()

        self.assertEqual(0, result)
        run_loop.assert_called_once()
        run_once.assert_not_called()

    def test_full_guard_mode_keeps_one_shot_behavior(self):
        with mock.patch.dict("os.environ", {"QBT_GUARD_MODE": "full"}, clear=True), \
                mock.patch.object(self.guard, "run_loop", return_value=0) as run_loop, \
                mock.patch.object(self.guard, "run_once", return_value=0) as run_once:
            result = self.guard.main()

        self.assertEqual(0, result)
        run_once.assert_called_once()
        run_loop.assert_not_called()

    def test_fixed_rate_sleep_subtracts_elapsed_time(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(30.0, self.guard.loop_sleep_seconds(0, 30, 60, 120))
            self.assertEqual(0.0, self.guard.loop_sleep_seconds(0, 90, 60, 120))
            self.assertEqual(90.0, self.guard.loop_sleep_seconds(1, 30, 60, 120))


if __name__ == "__main__":
    unittest.main()
