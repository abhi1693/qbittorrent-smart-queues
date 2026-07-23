import importlib
import json
import unittest
from datetime import datetime, timezone
from unittest import mock


class FakeResponse:
    headers = {}


class UdmParsingTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def wan_network_rows(self):
        return [
            {
                "name": "Internet 1",
                "purpose": "wan",
                "wan_networkgroup": "WAN",
                "wan_type": "pppoe",
                "wan_load_balance_type": "weighted",
            },
            {
                "name": "Internet 2",
                "purpose": "wan",
                "wan_networkgroup": "WAN2",
                "wan_type": "dhcp",
                "wan_load_balance_type": "failover-only",
            },
        ]

    def gateway_row(self, active_uplink):
        return {
            "type": "udm",
            "last_wan_status": {
                "WAN": "offline",
                "WAN2": "online",
            },
            "uplink": {
                "name": active_uplink,
                "type": "wire",
                "up": True,
            },
            "wan1": {
                "name": "eth8",
                "ifname": "eth8",
                "uplink_ifname": "ppp0",
                "up": True,
            },
            "wan2": {
                "name": "eth7",
                "ifname": "eth7",
                "uplink_ifname": "eth7",
                "up": active_uplink == "eth7",
            },
        }

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

    def test_backup_state_uses_active_uplink_instead_of_last_wan_status(self):
        state = self.guard.classify_udm_backup_internet_state(
            self.wan_network_rows(),
            [self.gateway_row("ppp0")],
        )

        self.assertFalse(state["backup_active"])
        self.assertEqual("primary", state["active_role"])
        self.assertEqual("Internet 1", state["active_network"])
        self.assertEqual("WAN", state["active_network_group"])
        self.assertEqual("wan1", state["active_interface"])
        self.assertEqual("ppp0", state["active_uplink"])

    def test_backup_state_detects_failover_only_network_as_active(self):
        network_rows = self.wan_network_rows()
        network_rows[1]["name"] = "Editable backup label"
        state = self.guard.classify_udm_backup_internet_state(
            network_rows,
            [self.gateway_row("eth7")],
        )

        self.assertTrue(state["backup_active"])
        self.assertEqual("backup", state["active_role"])
        self.assertEqual("Editable backup label", state["active_network"])
        self.assertEqual("WAN2", state["active_network_group"])
        self.assertEqual("wan2", state["active_interface"])
        self.assertEqual("eth7", state["active_uplink"])

    def test_backup_state_requires_dynamic_failover_role(self):
        with self.assertRaisesRegex(
            self.guard.ApiError,
            "no WAN configured with failover-only role",
        ):
            self.guard.classify_udm_backup_internet_state(
                [
                    {
                        "name": "Internet 1",
                        "purpose": "wan",
                        "wan_networkgroup": "WAN",
                        "wan_load_balance_type": "weighted",
                    },
                    {
                        "name": "Cellular",
                        "purpose": "wan",
                        "wan_networkgroup": "WAN2",
                        "wan_load_balance_type": "weighted",
                    },
                ],
                [self.gateway_row("eth7")],
            )

    def test_backup_state_rejects_unmapped_active_uplink(self):
        with self.assertRaisesRegex(
            self.guard.ApiError,
            "Could not uniquely map active UniFi uplink",
        ):
            self.guard.classify_udm_backup_internet_state(
                self.wan_network_rows(),
                [self.gateway_row("wwan0")],
            )

    def test_backup_internet_state_reads_network_configuration_and_device_status(self):
        calls = []

        def fake_request_json(opener, method, url, headers=None, body=None, timeout=30):
            calls.append({"method": method, "url": url, "headers": headers})
            if url.endswith("/rest/networkconf"):
                return {"data": self.wan_network_rows()}, FakeResponse()
            if url.endswith("/stat/device"):
                return {"data": [self.gateway_row("eth7")]}, FakeResponse()
            self.fail(f"Unexpected URL {url}")

        env = {
            "UDM_URL": "https://unifi.test",
            "UDM_API_KEY": "test-api-key",
        }
        with mock.patch.dict("os.environ", env, clear=True), \
                mock.patch.object(self.guard, "request_json", side_effect=fake_request_json):
            state = self.guard.UdmClient().backup_internet_state()

        self.assertTrue(state["backup_active"])
        self.assertEqual(
            [
                "https://unifi.test/proxy/network/api/s/default/rest/networkconf",
                "https://unifi.test/proxy/network/api/s/default/stat/device",
            ],
            [call["url"] for call in calls],
        )
        self.assertEqual(["GET", "GET"], [call["method"] for call in calls])
        self.assertEqual("test-api-key", calls[0]["headers"]["X-API-KEY"])


if __name__ == "__main__":
    unittest.main()
