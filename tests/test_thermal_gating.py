import contextlib
import importlib
import io
import unittest
from unittest import mock

from qbittorrent_smart_queues.providers import NetworkUsageProvider, UsageSnapshot


class FakeUsageProvider(NetworkUsageProvider):
    provider_name = "fake"
    latest_stats_at = None

    def usage_snapshot(self, now):
        return UsageSnapshot(0, 0)


class FakeQbtClient:
    base_url = "http://qbittorrent.test"

    def __init__(self):
        self.download_limits = []
        self.upload_limits = []
        self.stop_all_calls = 0

    def set_download_limit(self, limit):
        self.download_limits.append(limit)

    def set_upload_limit(self, limit):
        self.upload_limits.append(limit)

    def stop_all(self):
        self.stop_all_calls += 1


class FullGuardThermalGatingTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_full_guard_stops_and_does_not_start_torrents_when_thermal_stop_is_active(self):
        client = FakeQbtClient()
        env = {
            "QBT_STRUCTURED_DECISION_LOGS_ENABLED": "false",
        }

        with mock.patch.dict("os.environ", env, clear=False), \
                mock.patch.object(self.guard, "usage_provider_from_env", return_value=FakeUsageProvider()), \
                mock.patch.object(self.guard, "reachable_qbt_clients", return_value=[client]), \
                mock.patch.object(
                    self.guard,
                    "full_guard_thermal_state",
                    return_value={
                        "enabled": True,
                        "stop": True,
                        "reason": "NVMe thermal stop threshold 80.0C reached",
                        "readings": [{"node": "node-a", "temperature": 81.2}],
                    },
                ), \
                mock.patch.object(self.guard, "apply_single_download") as apply_single_download, \
                mock.patch.object(self.guard, "cleanup_qbt_clients"), \
                contextlib.redirect_stdout(io.StringIO()):
            result = self.guard.SmartQueueController().run_once()

        self.assertEqual(0, result)
        self.assertEqual([1], client.download_limits)
        self.assertEqual([1], client.upload_limits)
        self.assertEqual(1, client.stop_all_calls)
        apply_single_download.assert_not_called()

    def test_full_guard_starts_selection_when_thermal_state_is_clear(self):
        client = FakeQbtClient()
        env = {
            "QBT_STRUCTURED_DECISION_LOGS_ENABLED": "false",
        }

        with mock.patch.dict("os.environ", env, clear=False), \
                mock.patch.object(self.guard, "usage_provider_from_env", return_value=FakeUsageProvider()), \
                mock.patch.object(self.guard, "reachable_qbt_clients", return_value=[client]), \
                mock.patch.object(
                    self.guard,
                    "full_guard_thermal_state",
                    return_value={
                        "enabled": True,
                        "stop": False,
                        "reason": "all NVMe temperatures below 80.0C",
                        "readings": [{"node": "node-a", "temperature": 55.0}],
                    },
                ), \
                mock.patch.object(self.guard, "apply_single_download") as apply_single_download, \
                mock.patch.object(self.guard, "cleanup_qbt_clients"), \
                contextlib.redirect_stdout(io.StringIO()):
            result = self.guard.SmartQueueController().run_once()

        self.assertEqual(0, result)
        apply_single_download.assert_called_once()


if __name__ == "__main__":
    unittest.main()
