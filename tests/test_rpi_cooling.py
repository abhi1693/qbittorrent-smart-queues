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
            "QBT_RPI_COOLING_DRAIN_ENABLED": "false",
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
            "QBT_RPI_COOLING_DRAIN_ENABLED": "false",
        }
        with mock.patch.dict("os.environ", env, clear=True):
            return self.guard.RpiThermalCoolingManager()

    def manager_with_drain(self, state_path):
        env = {
            "QBT_RPI_COOLING_ENABLED": "true",
            "PROMETHEUS_URL": "http://prometheus.test",
            "QBT_RPI_COOLING_STATE_PATH": state_path,
            "QBT_RPI_COOLING_NODES": "k8s-rpi1,k8s-rpi2,k8s-rpi3",
            "QBT_RPI_COOLING_SHUTDOWN_URL_TEMPLATE": "http://shutdown.test/{node}/shutdown",
            "QBT_RPI_COOLING_CPU_SHUTDOWN_CELSIUS": "75",
            "QBT_RPI_COOLING_NVME_SHUTDOWN_CELSIUS": "70",
            "QBT_RPI_COOLING_DRAIN_ENABLED": "true",
            "QBT_RPI_COOLING_DRAIN_TIMEOUT_SECONDS": "300",
            "QBT_RPI_COOLING_DRAIN_POD_GRACE_PERIOD_SECONDS": "15",
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

    def test_hot_node_cordons_and_drains_before_shutdown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            manager = self.manager_with_drain(state_path)
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 60.0, "k8s-rpi2": 80.0, "k8s-rpi3": 59.0},
                    {"k8s-rpi1": 45.0, "k8s-rpi2": 55.0, "k8s-rpi3": 50.0},
                ]
            )
            manager.kubernetes.set_node_unschedulable = mock.Mock()
            manager.kubernetes.list_pods_on_node = mock.Mock(
                return_value=[
                    {
                        "metadata": {
                            "namespace": "media",
                            "name": "sonarr-123",
                            "labels": {"app.kubernetes.io/name": "sonarr"},
                        },
                        "status": {"phase": "Running"},
                    },
                    {
                        "metadata": {
                            "namespace": "media",
                            "name": "rpi-shutdown-k8s-rpi2-123",
                            "labels": {
                                "app.kubernetes.io/name": "rpi-shutdown-controller",
                                "app.kubernetes.io/instance": "k8s-rpi2",
                            },
                        },
                        "status": {"phase": "Running"},
                    },
                ]
            )
            manager.kubernetes.evict_pod = mock.Mock(return_value={})

            with mock.patch.object(self.guard, "request_json") as request_json:
                result = manager.reconcile()

            self.assertEqual("drain_requested", result["action"])
            manager.kubernetes.set_node_unschedulable.assert_called_once_with("k8s-rpi2", True)
            manager.kubernetes.evict_pod.assert_called_once_with("media", "sonarr-123", 15)
            request_json.assert_not_called()
            with open(state_path, "r", encoding="utf-8") as state_file:
                state = json.load(state_file)
            self.assertEqual("draining", state["phase"])
            self.assertEqual(["media/sonarr-123"], state["last_drain"]["pending_pods"])
            self.assertIn("media/rpi-shutdown-k8s-rpi2-123", state["last_drain"]["ignored_pods"])

    def test_existing_drain_requests_shutdown_once_pods_are_gone(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            with open(state_path, "w", encoding="utf-8") as state_file:
                json.dump(
                    {
                        "node": "k8s-rpi2",
                        "phase": "draining",
                        "started_at": "2026-06-01T00:00:00Z",
                        "drain_started_at": "2026-06-01T00:00:00Z",
                        "reason": "CPU temperature 80.0C reached threshold 75.0C",
                        "temperature_kind": "CPU",
                        "temperature_celsius": 80.0,
                        "threshold_celsius": 75.0,
                    },
                    state_file,
                )
            manager = self.manager_with_drain(state_path)
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.kubernetes.set_node_unschedulable = mock.Mock()
            manager.kubernetes.list_pods_on_node = mock.Mock(
                return_value=[
                    {
                        "metadata": {
                            "namespace": "media",
                            "name": "rpi-shutdown-k8s-rpi2-123",
                            "labels": {
                                "app.kubernetes.io/name": "rpi-shutdown-controller",
                                "app.kubernetes.io/instance": "k8s-rpi2",
                            },
                        },
                        "status": {"phase": "Running"},
                    },
                ]
            )
            manager.kubernetes.evict_pod = mock.Mock()

            with mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
                result = manager.reconcile()

            self.assertEqual("active", result["action"])
            manager.kubernetes.set_node_unschedulable.assert_called_once_with("k8s-rpi2", True)
            manager.kubernetes.evict_pod.assert_not_called()
            request_json.assert_called_once()
            with open(state_path, "r", encoding="utf-8") as state_file:
                state = json.load(state_file)
            self.assertEqual("shutdown_requested", state["phase"])

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

    def test_active_drain_requests_download_stop(self):
        reason = self.guard.rpi_cooling_stop_reason(
            {
                "enabled": True,
                "action": "active",
                "active": {"phase": "draining", "node": "k8s-rpi2"},
            }
        )

        self.assertEqual("RPi thermal cooling drain active for k8s-rpi2", reason)

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

    def test_ready_node_is_uncordoned_before_cooling_lock_clears(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            with open(state_path, "w", encoding="utf-8") as state_file:
                json.dump(
                    {
                        "node": "k8s-rpi1",
                        "phase": "booting",
                        "started_at": "2026-06-01T00:00:00Z",
                        "boot_started_at": "2026-06-01T00:00:00Z",
                    },
                    state_file,
                )
            manager = self.manager_with_drain(state_path)
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.kubernetes.set_node_unschedulable = mock.Mock()

            result = manager.reconcile()

            self.assertEqual("active", result["action"])
            manager.kubernetes.set_node_unschedulable.assert_called_once_with("k8s-rpi1", False)
            self.assertFalse(os.path.exists(state_path))


if __name__ == "__main__":
    unittest.main()
