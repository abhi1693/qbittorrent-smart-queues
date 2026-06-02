import importlib
import json
import os
import tempfile
import unittest
from unittest import mock


class RpiCoolingTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def manager(self, state_path):
        env = {
            "QBT_RPI_COOLING_ENABLED": "true",
            "PROMETHEUS_URL": "http://prometheus.test",
            "QBT_RPI_COOLING_STATE_PATH": state_path,
            "QBT_RPI_COOLING_NODES": "k8s-rpi1,k8s-rpi2,k8s-rpi3",
            "QBT_RPI_COOLING_SHUTDOWN_URL_TEMPLATE": "http://shutdown.test/{node}/shutdown",
            "QBT_RPI_COOLING_CPU_SHUTDOWN_CELSIUS": "75",
            "QBT_RPI_COOLING_NVME_SHUTDOWN_CELSIUS": "70",
        }
        with mock.patch.dict("os.environ", env, clear=True):
            return self.guard.RpiThermalCoolingManager()

    def manager_with_longhorn_check(self, state_path):
        env = {
            "QBT_RPI_COOLING_ENABLED": "true",
            "PROMETHEUS_URL": "http://prometheus.test",
            "QBT_RPI_COOLING_STATE_PATH": state_path,
            "QBT_RPI_COOLING_NODES": "k8s-rpi1,k8s-rpi2,k8s-rpi3",
            "QBT_RPI_COOLING_SHUTDOWN_URL_TEMPLATE": "http://shutdown.test/{node}/shutdown",
            "QBT_RPI_COOLING_CPU_SHUTDOWN_CELSIUS": "75",
            "QBT_RPI_COOLING_NVME_SHUTDOWN_CELSIUS": "70",
            "QBT_RPI_COOLING_LONGHORN_REPLICA_CHECK_ENABLED": "true",
        }
        with mock.patch.dict("os.environ", env, clear=True):
            return self.guard.RpiThermalCoolingManager()

    def test_hot_node_requests_one_shutdown_and_persists_lock(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            manager = self.manager(state_path)
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 60.0, "k8s-rpi2": 80.0, "k8s-rpi3": 59.0},
                    {"k8s-rpi1": 45.0, "k8s-rpi2": 55.0, "k8s-rpi3": 50.0},
                ]
            )

            with mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
                result = manager.reconcile()

            self.assertEqual("shutdown_requested", result["action"])
            self.assertEqual("k8s-rpi2", result["candidate"]["node"])
            request_json.assert_called_once()
            self.assertEqual(
                "http://shutdown.test/k8s-rpi2/shutdown",
                request_json.call_args.args[2],
            )
            with open(state_path, "r", encoding="utf-8") as state_file:
                state = json.load(state_file)
            self.assertEqual("k8s-rpi2", state["node"])
            self.assertEqual("shutdown_requested", state["phase"])

    def test_shutdown_is_skipped_when_any_peer_is_not_ready(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            manager = self.manager(state_path)
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": False, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock()

            with mock.patch.object(self.guard, "request_json") as request_json:
                result = manager.reconcile()

            self.assertEqual("skipped", result["action"])
            self.assertEqual("not all nodes are Ready", result["reason"])
            manager.prometheus_temperature_readings.assert_not_called()
            request_json.assert_not_called()
            self.assertFalse(os.path.exists(state_path))

    def test_existing_lock_moves_to_cooling_without_second_shutdown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            with open(state_path, "w", encoding="utf-8") as state_file:
                json.dump(
                    {
                        "node": "k8s-rpi1",
                        "phase": "shutdown_requested",
                        "started_at": "2026-06-01T00:00:00Z",
                    },
                    state_file,
                )
            manager = self.manager(state_path)
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": False, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock()

            with mock.patch.object(self.guard, "request_json") as request_json:
                result = manager.reconcile()

            self.assertEqual("active", result["action"])
            request_json.assert_not_called()
            manager.prometheus_temperature_readings.assert_not_called()
            with open(state_path, "r", encoding="utf-8") as state_file:
                state = json.load(state_file)
            self.assertEqual("cooling", state["phase"])
            self.assertEqual("k8s-rpi1", state["node"])

    def test_shutdown_failure_clears_prewritten_lock(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            manager = self.manager(state_path)
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 80.0, "k8s-rpi2": 60.0, "k8s-rpi3": 59.0},
                    {"k8s-rpi1": 45.0, "k8s-rpi2": 55.0, "k8s-rpi3": 50.0},
                ]
            )

            with mock.patch.object(
                self.guard,
                "request_json",
                side_effect=self.guard.ApiError("shutdown failed"),
            ):
                with self.assertRaises(self.guard.ApiError):
                    manager.reconcile()

            self.assertFalse(os.path.exists(state_path))

    def test_shutdown_is_skipped_when_candidate_hosts_sole_longhorn_replica(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            manager = self.manager_with_longhorn_check(state_path)
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.kubernetes.list_longhorn_replicas = mock.Mock(
                return_value=[
                    {
                        "metadata": {"name": "media-downloads-r1"},
                        "spec": {
                            "active": True,
                            "nodeID": "k8s-rpi2",
                            "volumeName": "media-downloads",
                            "desireState": "running",
                        },
                        "status": {"currentState": "running"},
                    }
                ]
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 60.0, "k8s-rpi2": 80.0, "k8s-rpi3": 59.0},
                    {"k8s-rpi1": 45.0, "k8s-rpi2": 55.0, "k8s-rpi3": 50.0},
                ]
            )

            with mock.patch.object(self.guard, "request_json") as request_json:
                result = manager.reconcile()

            self.assertEqual("skipped", result["action"])
            self.assertIn("sole active Longhorn replica", result["reason"])
            self.assertEqual("media-downloads", result["longhorn"]["blocked_replicas"][0]["volume"])
            request_json.assert_not_called()
            self.assertFalse(os.path.exists(state_path))

    def test_longhorn_blocked_cooling_requests_download_stop(self):
        reason = self.guard.rpi_cooling_stop_reason(
            {
                "enabled": True,
                "action": "skipped",
                "candidate": {"node": "k8s-rpi2"},
                "longhorn": {
                    "safe": False,
                    "reason": "node hosts sole active Longhorn replica(s): media-downloads",
                },
            }
        )

        self.assertEqual(
            "RPi thermal cooling blocked for k8s-rpi2: "
            "node hosts sole active Longhorn replica(s): media-downloads",
            reason,
        )

    def test_shutdown_requested_cooling_requests_download_stop(self):
        reason = self.guard.rpi_cooling_stop_reason(
            {
                "enabled": True,
                "action": "shutdown_requested",
                "candidate": {"node": "k8s-rpi2"},
            }
        )

        self.assertEqual("RPi thermal cooling shutdown requested for k8s-rpi2", reason)

    def test_cooling_check_error_requests_download_stop(self):
        reason = self.guard.rpi_cooling_stop_reason(
            {
                "enabled": True,
                "action": "error",
                "reason": "The read operation timed out",
            }
        )

        self.assertEqual("RPi thermal cooling check failed: The read operation timed out", reason)

    def test_shutdown_allows_candidate_when_longhorn_replica_has_peer(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            manager = self.manager_with_longhorn_check(state_path)
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.kubernetes.list_longhorn_replicas = mock.Mock(
                return_value=[
                    {
                        "metadata": {"name": "media-downloads-r1"},
                        "spec": {
                            "active": True,
                            "nodeID": "k8s-rpi2",
                            "volumeName": "media-downloads",
                        },
                    },
                    {
                        "metadata": {"name": "media-downloads-r2"},
                        "spec": {
                            "active": True,
                            "nodeID": "k8s-rpi3",
                            "volumeName": "media-downloads",
                        },
                    },
                ]
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 60.0, "k8s-rpi2": 80.0, "k8s-rpi3": 59.0},
                    {"k8s-rpi1": 45.0, "k8s-rpi2": 55.0, "k8s-rpi3": 50.0},
                ]
            )

            with mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
                result = manager.reconcile()

            self.assertEqual("shutdown_requested", result["action"])
            self.assertTrue(result["longhorn"]["safe"])
            request_json.assert_called_once()

    def test_cooling_lock_requests_power_on_after_cooldown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            with open(state_path, "w", encoding="utf-8") as state_file:
                json.dump(
                    {
                        "node": "k8s-rpi1",
                        "phase": "cooling",
                        "started_at": "2026-06-01T00:00:00Z",
                        "cooling_started_at": "2026-06-01T00:00:00Z",
                    },
                    state_file,
                )
            manager = self.manager(state_path)
            manager.power_on_urls = {"k8s-rpi1": "http://power.test/k8s-rpi1/on"}
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": False, "k8s-rpi2": True, "k8s-rpi3": True}
            )

            with mock.patch.object(manager, "request_plain_http", return_value=200) as request_plain_http:
                result = manager.reconcile()

            self.assertEqual("active", result["action"])
            request_plain_http.assert_called_once_with(
                "POST",
                "http://power.test/k8s-rpi1/on",
                manager.power_request_timeout,
            )
            with open(state_path, "r", encoding="utf-8") as state_file:
                state = json.load(state_file)
            self.assertEqual("booting", state["phase"])
            self.assertTrue(state["power_on_requested"])


if __name__ == "__main__":
    unittest.main()
