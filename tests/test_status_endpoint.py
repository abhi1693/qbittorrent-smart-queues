import importlib
import json
import unittest
import urllib.request
from unittest import mock


class StatusEndpointTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")
        self.original_status = self.guard.QUEUE_STATUS
        self.guard.QUEUE_STATUS = self.guard.QueueStatusStore()

    def tearDown(self):
        self.guard.stop_status_http_server()
        self.guard.QUEUE_STATUS = self.original_status

    def test_decision_status_updates_even_when_decision_logs_are_disabled(self):
        env = {"QBT_DECISION_LOGS_ENABLED": "false"}
        with mock.patch.dict("os.environ", env, clear=True):
            self.guard.emit_decision_log(
                "qbt_guard_decision",
                action="try_candidate",
                reason="unit test",
                selected_torrent={
                    "hash": "abc123",
                    "name": "Example.S01E01",
                    "category": "tv",
                    "state": "stalledDL",
                    "progress": 0.5,
                    "amount_left_bytes": 1048576,
                    "download_speed_bytes_per_sec": 0,
                    "eta_seconds": 3600,
                    "availability": 0.5,
                    "connected_seeds": 0,
                    "reported_seeds": 1,
                },
                selected_torrents=[
                    {
                        "hash": "abc123",
                        "name": "Example.S01E01",
                        "category": "tv",
                        "state": "downloading",
                        "progress": 0.5,
                        "amount_left_bytes": 1048576,
                        "download_speed_bytes_per_sec": 1024,
                        "eta_seconds": 3600,
                        "availability": 0.5,
                        "connected_seeds": 0,
                        "reported_seeds": 1,
                    },
                    {
                        "hash": "def456",
                        "name": "Example.S01E02",
                        "category": "tv",
                        "state": "downloading",
                        "progress": 0.25,
                        "amount_left_bytes": 2097152,
                        "download_speed_bytes_per_sec": 2048,
                        "eta_seconds": 7200,
                        "availability": 1.2,
                        "connected_seeds": 2,
                        "reported_seeds": 5,
                    },
                ],
                winner_torrent={
                    "hash": "abc123",
                    "name": "Example.S01E01",
                    "category": "tv",
                    "state": "stalledDL",
                    "progress": 0.5,
                    "amount_left_bytes": 1048576,
                    "download_speed_bytes_per_sec": 0,
                    "availability": 0.5,
                },
                runner_up_torrent={
                    "hash": "runner123",
                    "name": "Runner.S01E02",
                    "category": "tv",
                    "state": "stoppedDL",
                    "progress": 0.4,
                    "amount_left_bytes": 4096,
                    "download_speed_bytes_per_sec": 0,
                    "availability": 1.0,
                },
                current_active_torrent={
                    "hash": "active123",
                    "name": "Active.S01E03",
                    "category": "tv",
                    "state": "downloading",
                    "progress": 0.2,
                    "amount_left_bytes": 8192,
                    "download_speed_bytes_per_sec": 100,
                    "availability": 1.0,
                },
                parked_torrents=[
                    {
                        "hash": "parked123",
                        "name": "Parked.S01E02",
                        "category": "tv",
                        "state": "stalledDL",
                        "progress": 0.75,
                        "amount_left_bytes": 2048,
                        "download_speed_bytes_per_sec": 0,
                        "availability": 0,
                    }
                ],
                candidate_counts={"available": 3, "tracker_health_observed": 2},
                rejected_counts={"cooldown": 1},
                effective_cap={
                    "requested_download_limit_bytes_per_sec": 1024,
                    "download_limit_bytes_per_sec": 0,
                    "upload_limit_bytes_per_sec": 512,
                    "configured_download_ceiling_bytes_per_sec": 2048,
                    "isp_usable_download_limit_bytes_per_sec": 2048,
                    "slow_reference_limit_bytes_per_sec": 2048,
                },
                storage={
                    "path": "/downloads",
                    "reason": "enough space",
                    "stop": False,
                    "total_bytes": 100000,
                    "free_bytes": 50000,
                    "reserve_bytes": 10000,
                    "headroom_bytes": 40000,
                },
            )

        snapshot = self.guard.QUEUE_STATUS.snapshot()
        self.assertEqual("try_candidate", snapshot["last_event"]["action"])
        self.assertEqual("Example.S01E01", snapshot["last_event"]["selected_torrent"]["name"])
        self.assertEqual("Runner.S01E02", snapshot["last_event"]["runner_up_torrent"]["name"])
        self.assertEqual("Active.S01E03", snapshot["last_event"]["current_active_torrent"]["name"])

        metrics = self.guard.QUEUE_STATUS.prometheus_metrics()
        self.assertIn('qbt_guard_last_decision_info{action="try_candidate"', metrics)
        self.assertIn('selected_name="Example.S01E01"', metrics)
        self.assertIn('qbt_guard_selected_torrent_progress_ratio 0.5', metrics)
        self.assertIn('qbt_guard_selected_torrent_amount_left_bytes 1048576.0', metrics)
        self.assertIn('qbt_guard_selected_torrent_eta_seconds 3600.0', metrics)
        self.assertIn('qbt_guard_selected_torrent_seeds{type="connected"} 0.0', metrics)
        self.assertIn('qbt_guard_decision_torrent_count{role="selected"} 2.0', metrics)
        self.assertIn(
            'qbt_guard_torrent_download_speed_bytes_per_sec{category="tv",hash="def456",index="2",name="Example.S01E02",role="selected",state="downloading"} 2048.0',
            metrics,
        )
        self.assertIn(
            'qbt_guard_torrent_eta_seconds{category="tv",hash="def456",index="2",name="Example.S01E02",role="selected",state="downloading"} 7200.0',
            metrics,
        )
        self.assertIn(
            'qbt_guard_torrent_seeds{category="tv",hash="def456",index="2",name="Example.S01E02",role="selected",state="downloading",type="reported"} 5.0',
            metrics,
        )
        self.assertIn('qbt_guard_effective_cap_bytes_per_sec{type="download"} 0.0', metrics)
        self.assertIn('qbt_guard_storage_bytes{type="headroom"} 40000.0', metrics)
        self.assertIn('qbt_guard_decision_torrent_count{role="parked"} 1.0', metrics)
        self.assertIn('qbt_guard_decision_torrent_count{role="winner"} 1.0', metrics)
        self.assertIn('qbt_guard_decision_torrent_count{role="runner_up"} 1.0', metrics)
        self.assertIn('qbt_guard_decision_torrent_count{role="current_active"} 1.0', metrics)
        self.assertIn('qbt_guard_torrent_info{category="tv",hash="parked123"', metrics)
        self.assertIn('qbt_guard_candidate_count{type="available"} 3.0', metrics)
        self.assertIn('qbt_guard_rejected_count{reason="cooldown"} 1.0', metrics)

    def test_status_http_endpoints(self):
        self.guard.QUEUE_STATUS.record(
            "qbt_guard_decision",
            source="test",
            action="keep_productive",
            selected_torrent={"hash": "def456", "name": "Movie.2026"},
            reason="amount left decreased by 10 MB",
        )
        env = {
            "QBT_STATUS_HTTP_ENABLED": "true",
            "QBT_STATUS_HTTP_HOST": "127.0.0.1",
            "QBT_STATUS_HTTP_PORT": "0",
        }

        with mock.patch.dict("os.environ", env, clear=True):
            server = self.guard.start_status_http_server()
            port = server.server_address[1]
            status = urllib.request.urlopen(f"http://127.0.0.1:{port}/status", timeout=5)
            metrics = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5)
            healthz = urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=5)

        status_payload = json.loads(status.read().decode("utf-8"))
        metrics_text = metrics.read().decode("utf-8")
        health_text = healthz.read().decode("utf-8")

        self.assertEqual("keep_productive", status_payload["last_event"]["action"])
        self.assertIn("qbt_guard_status_up 1", metrics_text)
        self.assertIn('selected_name="Movie.2026"', metrics_text)
        self.assertEqual("ok\n", health_text)

    def test_summary_update_preserves_structured_count_context(self):
        self.guard.QUEUE_STATUS.record(
            "qbt_guard_decision",
            source="structured",
            action="keep_productive",
            selected_torrent={"hash": "abc123", "name": "Example.S01E01"},
            candidate_counts={"available": 4, "productive": 1},
            rejected_counts={"cooldown": 2},
        )
        self.guard.QUEUE_STATUS.record(
            "qbt_guard_decision",
            source="summary",
            action="keep_productive",
            message="Keeping active: Example.S01E01",
            selected="Example.S01E01",
        )

        snapshot = self.guard.QUEUE_STATUS.snapshot()
        self.assertEqual({"available": 4, "productive": 1}, snapshot["last_event"]["candidate_counts"])
        self.assertEqual({"cooldown": 2}, snapshot["last_event"]["rejected_counts"])
        metrics = self.guard.QUEUE_STATUS.prometheus_metrics()
        self.assertIn('qbt_guard_candidate_count{type="available"} 4.0', metrics)
        self.assertIn('qbt_guard_rejected_count{reason="cooldown"} 2.0', metrics)


if __name__ == "__main__":
    unittest.main()
