import contextlib
import importlib
import io
import json
import unittest
from datetime import datetime, timezone
from unittest import mock


class FakeResponse:
    headers = {}


class UdmParsingTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_sum_download_bytes_ignores_time_non_rows_and_nonpositive_values(self):
        client = self.guard.UdmClient()
        rows = [
            {"time": 1234567890, "wan-rx_bytes": 100, "wan2-rx_bytes": 20},
            {"wan-rx_bytes": -100, "wan2-rx_bytes": 4.9},
            {"wan-rx_bytes": "not-a-number", "wan2-rx_bytes": 8},
            ["not", "a", "row"],
        ]

        total = client.sum_download_bytes(rows, ["wan-rx_bytes", "wan2-rx_bytes", "time"])

        self.assertEqual(132, total)

    def test_stats_rows_accepts_wrapped_data_response(self):
        client = self.guard.UdmClient()
        start = datetime(2026, 6, 1, tzinfo=timezone.utc)
        end = datetime(2026, 6, 2, tzinfo=timezone.utc)
        calls = []

        def fake_request_json(opener, method, url, headers=None, body=None, timeout=30):
            calls.append({
                "method": method,
                "url": url,
                "headers": headers,
                "body": body,
                "timeout": timeout,
            })
            return {"data": [{"wan-rx_bytes": 10}]}, FakeResponse()

        with mock.patch.object(self.guard, "request_json", side_effect=fake_request_json):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rows = client.stats_rows("hourly", start, end, ["wan-rx_bytes", "time"])

        self.assertEqual([{"wan-rx_bytes": 10}], rows)
        self.assertEqual("POST", calls[0]["method"])
        self.assertIn("/proxy/network/api/s/default/stat/report/hourly.site", calls[0]["url"])
        payload = json.loads(calls[0]["body"].decode("utf-8"))
        self.assertEqual(int(start.timestamp() * 1000), payload["start"])
        self.assertEqual(int(end.timestamp() * 1000), payload["end"])
        self.assertEqual(["wan-rx_bytes", "time"], payload["attrs"])

    def test_stats_rows_accepts_raw_list_response(self):
        client = self.guard.UdmClient()
        row_time = datetime(2026, 6, 1, tzinfo=timezone.utc)
        row_time_ms = int(row_time.timestamp() * 1000)

        with mock.patch.object(
            self.guard,
            "request_json",
            return_value=([{"time": row_time_ms, "wan-rx_bytes": 10}], FakeResponse()),
        ):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rows = client.stats_rows(
                    "daily",
                    datetime(2026, 6, 1, tzinfo=timezone.utc),
                    datetime(2026, 6, 2, tzinfo=timezone.utc),
                    ["wan-rx_bytes", "time"],
                )

        self.assertEqual([{"time": row_time_ms, "wan-rx_bytes": 10}], rows)
        self.assertEqual(row_time, client.latest_stats_at)

    def test_stats_rows_rejects_unexpected_shape(self):
        client = self.guard.UdmClient()

        with mock.patch.object(
            self.guard,
            "request_json",
            return_value=({"data": {"wan-rx_bytes": 10}}, FakeResponse()),
        ):
            with self.assertRaisesRegex(self.guard.ApiError, "unexpected shape"):
                client.stats_rows(
                    "daily",
                    datetime(2026, 6, 1, tzinfo=timezone.utc),
                    datetime(2026, 6, 2, tzinfo=timezone.utc),
                    ["wan-rx_bytes", "time"],
                )

    def test_active_clients_accepts_wrapped_data_response(self):
        client = self.guard.UdmClient()

        with mock.patch.object(client, "login"), mock.patch.object(
            self.guard,
            "request_json",
            return_value=({"data": [{"name": "ABHI-PC"}]}, FakeResponse()),
        ):
            rows = client.active_clients()

        self.assertEqual([{"name": "ABHI-PC"}], rows)


if __name__ == "__main__":
    unittest.main()
