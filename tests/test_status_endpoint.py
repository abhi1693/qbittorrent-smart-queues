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
                },
                candidate_counts={"available": 3, "tracker_health_observed": 2},
                rejected_counts={"cooldown": 1},
            )

        snapshot = self.guard.QUEUE_STATUS.snapshot()
        self.assertEqual("try_candidate", snapshot["last_event"]["action"])
        self.assertEqual("Example.S01E01", snapshot["last_event"]["selected_torrent"]["name"])

        metrics = self.guard.QUEUE_STATUS.prometheus_metrics()
        self.assertIn('qbt_guard_last_decision_info{action="try_candidate"', metrics)
        self.assertIn('selected_name="Example.S01E01"', metrics)
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


if __name__ == "__main__":
    unittest.main()
