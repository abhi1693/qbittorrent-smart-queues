import contextlib
import importlib
import io
import unittest
from unittest import mock


class FakeUdmClient:
    latest_stats_at = None

    def __init__(self, backup_state=None, backup_error=None, usage_error=None):
        self.backup_state = backup_state
        self.backup_error = backup_error
        self.usage_error = usage_error
        self.usage_calls = 0

    def download_usage_snapshot(self, now):
        self.usage_calls += 1
        if self.usage_error:
            raise self.usage_error
        return 0, 0

    def backup_internet_state(self):
        if self.backup_error:
            raise self.backup_error
        return self.backup_state


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


class BackupInternetGuardTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")
        self.backup_state = {
            "enabled": True,
            "available": True,
            "backup_active": True,
            "active_role": "backup",
            "active_network": "Internet 2",
            "active_network_group": "WAN2",
            "active_interface": "wan2",
            "active_uplink": "eth7",
        }

    def run_guard(self, udm_client, client):
        env = {
            "QBT_DECISION_LOGS_ENABLED": "false",
            "UDM_BACKUP_INTERNET_STOP_ENABLED": "true",
        }
        with mock.patch.dict("os.environ", env, clear=True), \
                mock.patch.object(self.guard, "UdmClient", return_value=udm_client), \
                mock.patch.object(self.guard, "reachable_qbt_clients", return_value=[client]), \
                mock.patch.object(
                    self.guard,
                    "apply_rpi_thermal_cooling",
                    return_value={"enabled": False, "action": "clear"},
                ), \
                mock.patch.object(
                    self.guard,
                    "full_guard_thermal_state",
                    return_value={
                        "enabled": False,
                        "stop": False,
                        "reason": "disabled",
                        "readings": [],
                    },
                ) as thermal_state, \
                mock.patch.object(self.guard, "apply_single_download") as apply_single_download, \
                mock.patch.object(self.guard, "cleanup_qbt_clients"), \
                contextlib.redirect_stdout(io.StringIO()):
            result = self.guard.run_once()
        return result, thermal_state, apply_single_download

    def test_active_backup_internet_stops_all_torrents_before_other_guards(self):
        client = FakeQbtClient()
        udm_client = FakeUdmClient(backup_state=self.backup_state)

        result, thermal_state, apply_single_download = self.run_guard(
            udm_client,
            client,
        )

        self.assertEqual(0, result)
        self.assertEqual(0, udm_client.usage_calls)
        self.assertEqual([1], client.download_limits)
        self.assertEqual([1], client.upload_limits)
        self.assertEqual(1, client.stop_all_calls)
        thermal_state.assert_not_called()
        apply_single_download.assert_not_called()

    def test_primary_internet_allows_normal_selection(self):
        client = FakeQbtClient()
        primary_state = {
            **self.backup_state,
            "backup_active": False,
            "active_role": "primary",
            "active_network": "Internet 1",
            "active_network_group": "WAN",
            "active_interface": "wan1",
            "active_uplink": "ppp0",
        }

        result, thermal_state, apply_single_download = self.run_guard(
            FakeUdmClient(backup_state=primary_state),
            client,
        )

        self.assertEqual(0, result)
        self.assertEqual(0, client.stop_all_calls)
        thermal_state.assert_called_once()
        apply_single_download.assert_called_once()

    def test_quota_read_failure_is_not_a_backup_state_failure(self):
        client = FakeQbtClient()
        primary_state = {
            **self.backup_state,
            "backup_active": False,
            "active_role": "primary",
            "active_network": "Internet 1",
            "active_network_group": "WAN",
            "active_interface": "wan1",
            "active_uplink": "ppp0",
        }

        result, thermal_state, apply_single_download = self.run_guard(
            FakeUdmClient(
                backup_state=primary_state,
                usage_error=self.guard.ApiError("quota stats unavailable"),
            ),
            client,
        )

        self.assertEqual(0, result)
        self.assertEqual(0, client.stop_all_calls)
        thermal_state.assert_called_once()
        apply_single_download.assert_called_once()

    def test_state_read_failure_stops_all_torrents_by_default(self):
        client = FakeQbtClient()

        result, thermal_state, apply_single_download = self.run_guard(
            FakeUdmClient(backup_error=self.guard.ApiError("device status unavailable")),
            client,
        )

        self.assertEqual(1, result)
        self.assertEqual([1], client.download_limits)
        self.assertEqual([1], client.upload_limits)
        self.assertEqual(1, client.stop_all_calls)
        thermal_state.assert_not_called()
        apply_single_download.assert_not_called()


if __name__ == "__main__":
    unittest.main()
