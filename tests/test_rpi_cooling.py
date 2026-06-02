import importlib
import json
import os
import tempfile
import unittest
from unittest import mock


class FakeQbtClient:
    base_url = "http://qbittorrent.test"

    def __init__(self, torrents=None):
        self.download_limits = []
        self.upload_limits = []
        self.stop_all_calls = 0
        self.torrents = torrents if torrents is not None else [
            {"hash": "active", "name": "active", "state": "downloading", "progress": 0.5, "amount_left": 1000}
        ]

    def set_download_limit(self, limit):
        self.download_limits.append(limit)

    def set_upload_limit(self, limit):
        self.upload_limits.append(limit)

    def stop_all(self):
        self.stop_all_calls += 1

    def torrents_info(self, filter_name=None):
        return list(self.torrents)


class RpiCoolingTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def manager(self, state_path, extra_env=None):
        env = {
            "QBT_RPI_COOLING_ENABLED": "true",
            "PROMETHEUS_URL": "http://prometheus.test",
            "QBT_RPI_COOLING_STATE_PATH": state_path,
            "QBT_RPI_COOLING_NODES": "k8s-rpi1,k8s-rpi2,k8s-rpi3",
            "QBT_RPI_COOLING_SHUTDOWN_URL_TEMPLATE": "http://shutdown.test/{node}/shutdown",
            "QBT_RPI_COOLING_SHUTDOWN_ENABLED": "true",
            "QBT_RPI_COOLING_CPU_SHUTDOWN_CELSIUS": "75",
            "QBT_RPI_COOLING_NVME_SHUTDOWN_CELSIUS": "70",
        }
        if extra_env:
            env.update(extra_env)
        with mock.patch.dict("os.environ", env, clear=True):
            return self.guard.RpiThermalCoolingManager()

    def manager_with_longhorn_check(self, state_path, extra_env=None):
        env = {
            "QBT_RPI_COOLING_ENABLED": "true",
            "PROMETHEUS_URL": "http://prometheus.test",
            "QBT_RPI_COOLING_STATE_PATH": state_path,
            "QBT_RPI_COOLING_NODES": "k8s-rpi1,k8s-rpi2,k8s-rpi3",
            "QBT_RPI_COOLING_SHUTDOWN_URL_TEMPLATE": "http://shutdown.test/{node}/shutdown",
            "QBT_RPI_COOLING_SHUTDOWN_ENABLED": "true",
            "QBT_RPI_COOLING_CPU_SHUTDOWN_CELSIUS": "75",
            "QBT_RPI_COOLING_NVME_SHUTDOWN_CELSIUS": "70",
            "QBT_RPI_COOLING_LONGHORN_REPLICA_CHECK_ENABLED": "true",
        }
        if extra_env:
            env.update(extra_env)
        with mock.patch.dict("os.environ", env, clear=True):
            return self.guard.RpiThermalCoolingManager()

    def test_hot_node_throttles_qbittorrent_without_shutdown_by_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            manager = self.manager(
                state_path,
                {
                    "QBT_RPI_COOLING_SHUTDOWN_ENABLED": "false",
                    "QBT_RPI_COOLING_CPU_SHUTDOWN_CELSIUS": "85",
                    "QBT_RPI_COOLING_NVME_SHUTDOWN_CELSIUS": "80",
                },
            )
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 60.0, "k8s-rpi2": 72.0, "k8s-rpi3": 59.0},
                    {"k8s-rpi1": 45.0, "k8s-rpi2": 55.0, "k8s-rpi3": 50.0},
                ]
            )
            manager.batch_work.reconcile = mock.Mock(return_value={"enabled": True, "changed": [], "errors": []})

            with mock.patch.object(self.guard, "request_json") as request_json:
                result = manager.reconcile()

            self.assertEqual("throttle", result["action"])
            self.assertEqual("k8s-rpi2", result["candidate"]["node"])
            manager.batch_work.reconcile.assert_called_once_with(True)
            request_json.assert_not_called()
            with open(state_path, "r", encoding="utf-8") as state_file:
                state = json.load(state_file)
            self.assertEqual("throttle", state["phase"])

    def test_pause_mitigation_does_not_shutdown_until_last_resort_window_elapsed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            with open(state_path, "w", encoding="utf-8") as state_file:
                json.dump(
                    {
                        "node": "k8s-rpi2",
                        "phase": "pause",
                        "started_at": "2099-01-01T00:00:00Z",
                        "reason": "CPU temperature 86.0C reached threshold 74.0C",
                    },
                    state_file,
                )
            manager = self.manager(
                state_path,
                {
                    "QBT_RPI_COOLING_SHUTDOWN_ENABLED": "false",
                    "QBT_RPI_COOLING_LAST_RESORT_SHUTDOWN_ENABLED": "true",
                    "QBT_RPI_COOLING_CPU_SHUTDOWN_CELSIUS": "85",
                    "QBT_RPI_COOLING_NVME_SHUTDOWN_CELSIUS": "80",
                },
            )
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 60.0, "k8s-rpi2": 86.0, "k8s-rpi3": 59.0},
                    {"k8s-rpi1": 45.0, "k8s-rpi2": 55.0, "k8s-rpi3": 50.0},
                ]
            )
            manager.batch_work.reconcile = mock.Mock(return_value={"enabled": True, "changed": [], "errors": []})

            with mock.patch.object(self.guard, "request_json") as request_json:
                result = manager.reconcile()

            self.assertEqual("pause", result["action"])
            request_json.assert_not_called()

    def test_last_resort_shutdown_only_after_sustained_thermal_pressure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            with open(state_path, "w", encoding="utf-8") as state_file:
                json.dump(
                    {
                        "node": "k8s-rpi2",
                        "phase": "pause",
                        "started_at": "2026-01-01T00:00:00Z",
                        "reason": "CPU temperature 86.0C reached threshold 74.0C",
                    },
                    state_file,
                )
            manager = self.manager(
                state_path,
                {
                    "QBT_RPI_COOLING_SHUTDOWN_ENABLED": "false",
                    "QBT_RPI_COOLING_LAST_RESORT_SHUTDOWN_ENABLED": "true",
                    "QBT_RPI_COOLING_CPU_SHUTDOWN_CELSIUS": "85",
                    "QBT_RPI_COOLING_NVME_SHUTDOWN_CELSIUS": "80",
                },
            )
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 60.0, "k8s-rpi2": 86.0, "k8s-rpi3": 59.0},
                    {"k8s-rpi1": 45.0, "k8s-rpi2": 55.0, "k8s-rpi3": 50.0},
                ]
            )
            manager.batch_work.reconcile = mock.Mock(return_value={"enabled": True, "changed": [], "errors": []})

            with mock.patch.object(self.guard, "request_json", return_value=({}, object())) as request_json:
                result = manager.reconcile()

            self.assertEqual("shutdown_requested", result["action"])
            request_json.assert_called_once()

    def test_batch_work_stays_suspended_until_resume_hold_completes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            with open(state_path, "w", encoding="utf-8") as state_file:
                json.dump(
                    {
                        "node": "k8s-rpi2",
                        "phase": "throttle",
                        "started_at": "2026-01-01T00:00:00Z",
                        "reason": "CPU temperature 72.0C reached threshold 70.0C",
                    },
                    state_file,
                )
            manager = self.manager(
                state_path,
                {
                    "QBT_RPI_COOLING_SHUTDOWN_ENABLED": "false",
                    "QBT_RPI_COOLING_RESUME_HOLD_SECONDS": "900",
                },
            )
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 60.0, "k8s-rpi2": 60.0, "k8s-rpi3": 59.0},
                    {"k8s-rpi1": 45.0, "k8s-rpi2": 55.0, "k8s-rpi3": 50.0},
                ]
            )
            manager.batch_work.reconcile = mock.Mock(return_value={"enabled": True, "changed": [], "errors": []})

            result = manager.reconcile()

            self.assertEqual("throttle", result["action"])
            manager.batch_work.reconcile.assert_called_once_with(True)
            with open(state_path, "r", encoding="utf-8") as state_file:
                state = json.load(state_file)
            self.assertIn("clear_started_at", state)

    def test_qbittorrent_throttle_action_limits_without_pausing(self):
        client = FakeQbtClient()
        env = {
            "QBT_RPI_COOLING_THROTTLE_DOWNLOAD_LIMIT_BYTES_PER_SEC": "2097152",
            "QBT_RPI_COOLING_THROTTLE_UPLOAD_LIMIT_BYTES_PER_SEC": "131072",
        }

        with mock.patch.dict("os.environ", env, clear=True), \
                mock.patch.object(self.guard, "cleanup_qbt_clients"):
            result = self.guard.apply_rpi_cooling_stop(
                [client],
                {
                    "enabled": True,
                    "action": "throttle",
                    "candidate": {"node": "k8s-rpi2"},
                    "reason": "CPU temperature 72.0C reached threshold 70.0C",
                },
            )

        self.assertTrue(result)
        self.assertEqual([2097152], client.download_limits)
        self.assertEqual([131072], client.upload_limits)
        self.assertEqual(0, client.stop_all_calls)

    def test_qbittorrent_thermal_action_skips_limits_when_no_active_downloads(self):
        client = FakeQbtClient(
            torrents=[
                {"hash": "done", "name": "done", "state": "uploading", "progress": 1.0, "amount_left": 0},
                {"hash": "paused", "name": "paused", "state": "pausedDL", "progress": 0.5, "amount_left": 1000},
            ]
        )
        env = {
            "QBT_RPI_COOLING_THROTTLE_DOWNLOAD_LIMIT_BYTES_PER_SEC": "2097152",
            "QBT_RPI_COOLING_THROTTLE_UPLOAD_LIMIT_BYTES_PER_SEC": "131072",
        }

        with mock.patch.dict("os.environ", env, clear=True), \
                mock.patch.object(self.guard, "cleanup_qbt_clients"), \
                mock.patch.object(self.guard, "log_debug") as log_debug:
            result = self.guard.apply_rpi_cooling_stop(
                [client],
                {
                    "enabled": True,
                    "action": "throttle",
                    "candidate": {"node": "k8s-rpi2"},
                    "reason": "CPU temperature 72.0C reached threshold 70.0C",
                },
            )

        self.assertTrue(result)
        self.assertEqual([], client.download_limits)
        self.assertEqual([], client.upload_limits)
        self.assertEqual(0, client.stop_all_calls)
        log_debug.assert_called_once()

    def test_unrelated_hot_node_does_not_throttle_qbittorrent_when_topology_enabled(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            manager = self.manager(
                state_path,
                {
                    "QBT_RPI_COOLING_SHUTDOWN_ENABLED": "false",
                    "QBT_RPI_COOLING_CPU_PAUSE_CELSIUS": "80",
                    "QBT_RPI_COOLING_NVME_PAUSE_CELSIUS": "76",
                    "QBT_RPI_COOLING_QBT_TOPOLOGY_ENABLED": "true",
                    "QBT_RPI_COOLING_QBT_AFFECTED_NODES": "k8s-rpi1,k8s-rpi2",
                },
            )
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 60.0, "k8s-rpi2": 60.0, "k8s-rpi3": 78.0},
                    {"k8s-rpi1": 45.0, "k8s-rpi2": 55.0, "k8s-rpi3": 50.0},
                ]
            )
            manager.batch_work.reconcile = mock.Mock(return_value={"enabled": True, "changed": [], "errors": []})

            result = manager.reconcile()

            self.assertEqual("throttle", result["action"])
            self.assertEqual("k8s-rpi3", result["candidate"]["node"])
            self.assertEqual("", result["active"]["qbt_action"])
            self.assertEqual(["k8s-rpi1", "k8s-rpi2"], result["active"]["qbt_topology"]["nodes"])
            self.assertFalse(self.guard.apply_rpi_cooling_stop([FakeQbtClient()], result))

    def test_qbittorrent_topology_discovers_pod_frontend_and_replica_nodes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            manager = self.manager(
                state_path,
                {
                    "QBT_RPI_COOLING_SHUTDOWN_ENABLED": "false",
                    "QBT_RPI_COOLING_CPU_PAUSE_CELSIUS": "80",
                    "QBT_RPI_COOLING_NVME_PAUSE_CELSIUS": "76",
                    "QBT_RPI_COOLING_QBT_TOPOLOGY_ENABLED": "true",
                    "QBT_RPI_COOLING_QBT_VOLUME_CLAIMS": "media-downloads",
                },
            )
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 60.0, "k8s-rpi2": 60.0, "k8s-rpi3": 59.0},
                    {"k8s-rpi1": 74.5, "k8s-rpi2": 61.0, "k8s-rpi3": 55.0},
                ]
            )
            manager.kubernetes.list_pods = mock.Mock(
                return_value=[
                    {
                        "spec": {
                            "nodeName": "k8s-rpi3",
                            "volumes": [
                                {"name": "config", "persistentVolumeClaim": {"claimName": "qbittorrent-config"}},
                                {"name": "downloads", "persistentVolumeClaim": {"claimName": "media-downloads"}}
                            ],
                        }
                    }
                ]
            )
            manager.kubernetes.fetch_pvc = mock.Mock(
                return_value={"spec": {"volumeName": "pvc-media-downloads"}}
            )
            manager.kubernetes.fetch_pv = mock.Mock(
                return_value={
                    "metadata": {"name": "pvc-media-downloads"},
                    "spec": {"csi": {"volumeHandle": "pvc-media-downloads"}},
                }
            )
            manager.kubernetes.fetch_longhorn_volume = mock.Mock(
                return_value={
                    "spec": {"accessMode": "rwx", "nodeID": "k8s-rpi1"},
                    "status": {"currentNodeID": "k8s-rpi1", "ownerID": "k8s-rpi1", "shareState": "running"},
                }
            )
            manager.kubernetes.fetch_longhorn_share_manager = mock.Mock(
                return_value={"status": {"ownerID": "k8s-rpi1"}}
            )
            manager.kubernetes.list_longhorn_replicas = mock.Mock(
                return_value=[
                    {
                        "spec": {
                            "active": True,
                            "nodeID": "k8s-rpi2",
                            "volumeName": "pvc-media-downloads",
                        },
                        "status": {"currentState": "running"},
                    }
                ]
            )
            manager.batch_work.reconcile = mock.Mock(return_value={"enabled": True, "changed": [], "errors": []})

            result = manager.reconcile()

            self.assertEqual("throttle", result["action"])
            self.assertEqual("k8s-rpi1", result["candidate"]["node"])
            self.assertEqual("throttle", result["active"]["qbt_action"])
            self.assertEqual(
                ["k8s-rpi1", "k8s-rpi2", "k8s-rpi3"],
                result["active"]["qbt_topology"]["nodes"],
            )
            manager.kubernetes.fetch_pvc.assert_called_once_with("media", "media-downloads")

    def test_qbittorrent_topology_failure_fails_open_by_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "rpi-cooling.json")
            manager = self.manager(
                state_path,
                {
                    "QBT_RPI_COOLING_SHUTDOWN_ENABLED": "false",
                    "QBT_RPI_COOLING_CPU_PAUSE_CELSIUS": "80",
                    "QBT_RPI_COOLING_NVME_PAUSE_CELSIUS": "76",
                    "QBT_RPI_COOLING_QBT_TOPOLOGY_ENABLED": "true",
                },
            )
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )
            manager.prometheus_temperature_readings = mock.Mock(
                side_effect=[
                    {"k8s-rpi1": 60.0, "k8s-rpi2": 60.0, "k8s-rpi3": 59.0},
                    {"k8s-rpi1": 74.5, "k8s-rpi2": 61.0, "k8s-rpi3": 55.0},
                ]
            )
            manager.kubernetes.list_pods = mock.Mock(side_effect=self.guard.ApiError("forbidden"))
            manager.batch_work.reconcile = mock.Mock(return_value={"enabled": True, "changed": [], "errors": []})

            result = manager.reconcile()

            self.assertEqual("throttle", result["action"])
            self.assertEqual("", result["active"]["qbt_action"])
            self.assertEqual("error-fail-open", result["active"]["qbt_topology"]["source"])

    def test_qbittorrent_pause_action_pauses_torrents(self):
        client = FakeQbtClient()

        with mock.patch.object(self.guard, "cleanup_qbt_clients"):
            result = self.guard.apply_rpi_cooling_stop(
                [client],
                {
                    "enabled": True,
                    "action": "pause",
                    "candidate": {"node": "k8s-rpi2"},
                    "reason": "CPU temperature 75.0C reached threshold 74.0C",
                },
            )

        self.assertTrue(result)
        self.assertEqual([1], client.download_limits)
        self.assertEqual([1], client.upload_limits)
        self.assertEqual(1, client.stop_all_calls)

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

    def test_hot_node_requests_shutdown_without_cordon_or_drain(self):
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
            self.assertFalse(hasattr(manager.kubernetes, "set_node_unschedulable"))
            self.assertFalse(hasattr(manager.kubernetes, "evict_pod"))
            request_json.assert_called_once()
            with open(state_path, "r", encoding="utf-8") as state_file:
                state = json.load(state_file)
            self.assertEqual("shutdown_requested", state["phase"])

    def test_legacy_drain_state_is_cleared_without_shutdown(self):
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
            manager = self.manager(state_path)
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )

            with mock.patch.object(self.guard, "request_json") as request_json:
                result = manager.reconcile()

            self.assertEqual("active", result["action"])
            request_json.assert_not_called()
            self.assertFalse(os.path.exists(state_path))

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

            self.assertEqual("pause", result["action"])
            self.assertIn("sole active Longhorn replica", result["reason"])
            self.assertEqual("media-downloads", result["longhorn"]["blocked_replicas"][0]["volume"])
            request_json.assert_not_called()
            with open(state_path, "r", encoding="utf-8") as state_file:
                state = json.load(state_file)
            self.assertEqual("pause", state["phase"])

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

    def test_ready_node_clears_cooling_lock_without_uncordon(self):
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
            manager = self.manager(state_path)
            manager.kubernetes.ready_map = mock.Mock(
                return_value={"k8s-rpi1": True, "k8s-rpi2": True, "k8s-rpi3": True}
            )

            result = manager.reconcile()

            self.assertEqual("active", result["action"])
            self.assertFalse(hasattr(manager.kubernetes, "set_node_unschedulable"))
            self.assertFalse(os.path.exists(state_path))


if __name__ == "__main__":
    unittest.main()
