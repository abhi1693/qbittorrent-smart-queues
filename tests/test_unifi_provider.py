import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock
from zoneinfo import ZoneInfo

from qbittorrent_smart_queues.providers import unifi
from qbittorrent_smart_queues.quota import local_billing_cycle_window


class FakeResponse:
    headers = {}


class UnifiProviderTests(unittest.TestCase):
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
            "type": "gateway",
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
        client = unifi.UnifiProvider()
        rows = [
            {"time": 1234567890, "wan-rx_bytes": 100, "wan2-rx_bytes": 20},
            {"wan-rx_bytes": -100, "wan2-rx_bytes": 4.9},
            {"wan-rx_bytes": "not-a-number", "wan2-rx_bytes": 8},
            ["not", "a", "row"],
        ]

        total = client.sum_download_bytes(rows, ["wan-rx_bytes", "wan2-rx_bytes", "time"])

        self.assertEqual(132, total)

    def test_sum_download_bytes_applies_saved_day_correction_only_once(self):
        local_day_start = datetime(2026, 7, 22, 18, 30, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as state_dir:
            state_path = os.path.join(state_dir, "usage-corrections.json")
            with mock.patch.dict(
                os.environ,
                {"UNIFI_USAGE_CORRECTION_STATE_PATH": state_path},
            ):
                client = unifi.UnifiProvider()
                client.usage_correction_store.update(
                    "2026-07-23",
                    {"wan-rx_bytes": 150},
                    "Asia/Kolkata",
                )
                rows = [
                    {
                        "time": int(
                            (
                                local_day_start
                                + timedelta(hours=index)
                            ).timestamp()
                            * 1000
                        ),
                        "wan-rx_bytes": 100,
                    }
                    for index in range(3)
                ]

                total = client.sum_download_bytes(
                    rows,
                    ["wan-rx_bytes", "time"],
                    local_timezone=ZoneInfo("Asia/Kolkata"),
                    apply_saved_corrections=True,
                )

        self.assertEqual(150, total)
        self.assertEqual(150, client.usage_corrected_bytes)

    def test_billing_cycle_window_uses_configured_local_day(self):
        local_timezone = ZoneInfo("Asia/Kolkata")

        before_start, before_end, before_days = (
            local_billing_cycle_window(
                datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc),
                local_timezone,
                17,
            )
        )
        after_start, after_end, after_days = (
            local_billing_cycle_window(
                datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
                local_timezone,
                17,
            )
        )

        self.assertEqual(
            datetime(2026, 6, 16, 18, 30, tzinfo=timezone.utc),
            before_start,
        )
        self.assertEqual(
            datetime(2026, 7, 16, 18, 30, tzinfo=timezone.utc),
            before_end,
        )
        self.assertEqual(30, before_days)
        self.assertEqual(before_end, after_start)
        self.assertEqual(
            datetime(2026, 8, 16, 18, 30, tzinfo=timezone.utc),
            after_end,
        )
        self.assertEqual(31, after_days)

    def test_billing_cycle_day_must_be_in_calendar_range(self):
        for invalid_day in (0, 32):
            with self.subTest(invalid_day=invalid_day):
                with self.assertRaises(ValueError):
                    local_billing_cycle_window(
                        datetime(2026, 7, 20, tzinfo=timezone.utc),
                        timezone.utc,
                        invalid_day,
                    )

    def test_usage_snapshot_starts_at_configured_billing_cycle_day(self):
        now = datetime(2026, 7, 23, 16, 0, tzinfo=timezone.utc)
        cycle_start = datetime(2026, 7, 16, 18, 30, tzinfo=timezone.utc)
        local_day_start = datetime(2026, 7, 22, 18, 30, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as state_dir:
            with mock.patch.dict(
                os.environ,
                {
                    "QBT_BILLING_CYCLE_DAY": "17",
                    "UNIFI_STATS_TIMEZONE": "Asia/Kolkata",
                    "UNIFI_USAGE_CORRECTION_STATE_PATH": os.path.join(
                        state_dir,
                        "usage-corrections.json",
                    ),
                },
            ):
                client = unifi.UnifiProvider()
                client.authenticated = True
                client.network_rows = [
                    {
                        "purpose": "wan",
                        "wan_networkgroup": "WAN",
                        "wan_provider_capabilities": {
                            "download_kilobits_per_second": 300000,
                        },
                    }
                ]
                calls = []

                def fake_stats_rows(interval, start, end, attrs):
                    calls.append((interval, start, end, attrs))
                    if interval == "daily":
                        return [
                            {
                                "time": int(cycle_start.timestamp() * 1000),
                                "wan-rx_bytes": 100,
                            }
                        ]
                    return [
                        {
                            "time": int(local_day_start.timestamp() * 1000),
                            "wan-rx_bytes": 50,
                        }
                    ]

                with mock.patch.object(
                    client,
                    "stats_rows",
                    side_effect=fake_stats_rows,
                ):
                    snapshot = client.usage_snapshot(now)

        self.assertEqual(150, snapshot.cycle_usage_bytes)
        self.assertEqual(50, snapshot.day_usage_bytes)
        self.assertEqual(17, client.billing_cycle_day)
        self.assertEqual(cycle_start, client.billing_cycle_start)
        self.assertEqual(
            datetime(2026, 8, 16, 18, 30, tzinfo=timezone.utc),
            client.billing_cycle_end,
        )
        self.assertEqual(31, client.billing_cycle_days)
        self.assertEqual(cycle_start, calls[0][1])
        self.assertEqual(local_day_start, calls[0][2])

    def test_stats_attrs_follow_dynamic_primary_role_not_wan_name(self):
        network_rows = self.wan_network_rows()
        network_rows[0]["name"] = "Editable backup label"
        network_rows[0]["wan_load_balance_type"] = "failover-only"
        network_rows[1]["name"] = "Editable primary label"
        network_rows[1]["wan_load_balance_type"] = "weighted"

        with mock.patch.dict(
            os.environ,
            {
                "QBT_BACKUP_INTERNET_STOP_ENABLED": "true",
                "QBT_USAGE_INCLUDE_UPLOAD": "true",
                "UNIFI_USAGE_ATTRS": "wan-rx_bytes,wan2-rx_bytes",
            },
        ):
            client = unifi.UnifiProvider()
            client.network_rows = network_rows

            attrs = client.stats_attrs()

        self.assertEqual(
            ["wan2-rx_bytes", "wan2-tx_bytes", "time"],
            attrs,
        )
        self.assertEqual("primary", client.usage_scope)
        self.assertEqual(["WAN2"], client.usage_network_groups)

    def test_usage_snapshot_uses_unifi_timezone_and_corrects_counter_spike(self):
        now = datetime(2026, 7, 23, 16, 0, tzinfo=timezone.utc)
        history_total = 1_746_254_978_409
        local_month_start = datetime(2026, 6, 30, 18, 30, tzinfo=timezone.utc)
        local_day_start = datetime(2026, 7, 22, 18, 30, tzinfo=timezone.utc)
        hourly_rows = [
            {
                "time": int(local_day_start.timestamp() * 1000),
                "wan-rx_bytes": 1_170_896_864,
                "wan2-rx_bytes": 0,
            },
            {
                "time": int(datetime(2026, 7, 22, 19, 30, tzinfo=timezone.utc).timestamp() * 1000),
                "wan-rx_bytes": 1_908_088_794_584,
                "wan2-rx_bytes": 1_132_826_094,
            },
            {
                "time": int(datetime(2026, 7, 22, 20, 30, tzinfo=timezone.utc).timestamp() * 1000),
                "wan-rx_bytes": 45_563_013,
                "wan2-rx_bytes": 329_044_157,
            },
        ]
        expected_day_total = 3_849_226_992
        expected_correction = 1_906_917_897_720

        with tempfile.TemporaryDirectory() as state_dir:
            state_path = os.path.join(state_dir, "usage-corrections.json")
            with mock.patch.dict(
                os.environ,
                {
                    "UNIFI_STATS_TIMEZONE": "Asia/Kolkata",
                    "UNIFI_STATS_MAX_DOWNLOAD_RATE_BYTES_PER_SEC": "37500000",
                    "UNIFI_USAGE_CORRECTION_STATE_PATH": state_path,
                },
            ):
                client = unifi.UnifiProvider()
                client.authenticated = True
                calls = []

                def fake_stats_rows(interval, start, end, attrs):
                    calls.append((interval, start, end, attrs))
                    if interval == "daily":
                        return [
                            {
                                "time": int(local_month_start.timestamp() * 1000),
                                "wan-rx_bytes": history_total,
                            }
                        ]
                    return hourly_rows

                with mock.patch.object(client, "stats_rows", side_effect=fake_stats_rows):
                    snapshot = client.usage_snapshot(now)

                self.assertEqual(expected_day_total, snapshot.day_usage_bytes)
                self.assertEqual(
                    history_total + expected_day_total,
                    snapshot.cycle_usage_bytes,
                )
                self.assertEqual(local_month_start, calls[0][1])
                self.assertEqual(local_day_start, calls[0][2])
                self.assertEqual(local_day_start, calls[1][1])
                self.assertEqual(1, len(client.usage_anomalies))
                self.assertEqual(expected_correction, client.usage_corrected_bytes)
                self.assertEqual("Asia/Kolkata", client.stats_timezone_name)
                with open(state_path, "r", encoding="utf-8") as state_file:
                    state = json.load(state_file)
                self.assertEqual(
                    expected_correction,
                    state["days"]["2026-07-23"]["corrections"]["wan-rx_bytes"],
                )

    def test_usage_snapshot_applies_persisted_correction_to_daily_history(self):
        local_month_start = datetime(2026, 6, 30, 18, 30, tzinfo=timezone.utc)
        affected_day_start = datetime(2026, 7, 22, 18, 30, tzinfo=timezone.utc)
        next_day_start = datetime(2026, 7, 23, 18, 30, tzinfo=timezone.utc)
        correction = 1_906_917_897_720
        corrected_affected_day = 3_849_226_992
        earlier_history = 1_746_254_978_409
        affected_raw = corrected_affected_day + correction

        with tempfile.TemporaryDirectory() as state_dir:
            state_path = os.path.join(state_dir, "usage-corrections.json")
            with open(state_path, "w", encoding="utf-8") as state_file:
                json.dump(
                    {
                        "version": 1,
                        "days": {
                            "2026-07-23": {
                                "corrections": {"wan-rx_bytes": correction},
                                "timezone": "Asia/Kolkata",
                            }
                        },
                    },
                    state_file,
                )
            with mock.patch.dict(
                os.environ,
                {
                    "UNIFI_STATS_TIMEZONE": "Asia/Kolkata",
                    "UNIFI_STATS_MAX_DOWNLOAD_RATE_BYTES_PER_SEC": "37500000",
                    "UNIFI_USAGE_CORRECTION_STATE_PATH": state_path,
                },
            ):
                client = unifi.UnifiProvider()
                client.authenticated = True
                daily_rows = [
                    {
                        "time": int(local_month_start.timestamp() * 1000),
                        "wan-rx_bytes": earlier_history,
                    },
                    {
                        "time": int(affected_day_start.timestamp() * 1000),
                        "wan-rx_bytes": affected_raw,
                    },
                ]
                current_rows = [
                    {
                        "time": int(next_day_start.timestamp() * 1000),
                        "wan-rx_bytes": 100,
                    }
                ]
                with mock.patch.object(
                    client,
                    "stats_rows",
                    side_effect=[daily_rows, current_rows],
                ):
                    snapshot = client.usage_snapshot(
                        datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
                    )

                self.assertEqual(100, snapshot.day_usage_bytes)
                self.assertEqual(
                    earlier_history + corrected_affected_day + 100,
                    snapshot.cycle_usage_bytes,
                )
                self.assertEqual(correction, client.usage_corrected_bytes)

    def test_usage_correction_store_merges_new_counter_corrections(self):
        with tempfile.TemporaryDirectory() as state_dir:
            state_path = os.path.join(state_dir, "usage-corrections.json")
            store = unifi.UnifiUsageCorrectionStore(state_path)

            self.assertTrue(
                store.update(
                    "2026-07-23",
                    {"wan-rx_bytes": 1_906_917_897_720},
                    "Asia/Kolkata",
                )
            )
            self.assertTrue(
                store.update(
                    "2026-07-23",
                    {"wan-tx_bytes": 4_565_054_403_750},
                    "Asia/Kolkata",
                )
            )
            self.assertFalse(
                store.update(
                    "2026-07-23",
                    {"wan-tx_bytes": 4_565_054_403_750},
                    "Asia/Kolkata",
                )
            )

            with open(state_path, "r", encoding="utf-8") as state_file:
                state = json.load(state_file)
            self.assertEqual(
                {
                    "wan-rx_bytes": 1_906_917_897_720,
                    "wan-tx_bytes": 4_565_054_403_750,
                },
                state["days"]["2026-07-23"]["corrections"],
            )

    def test_usage_snapshot_backfills_new_upload_counter_correction(self):
        local_month_start = datetime(2026, 6, 30, 18, 30, tzinfo=timezone.utc)
        affected_day_start = datetime(2026, 7, 22, 18, 30, tzinfo=timezone.utc)
        current_day_start = datetime(2026, 7, 23, 18, 30, tzinfo=timezone.utc)
        rx_correction = 1_906_917_897_720
        tx_correction = 4_565_054_403_750
        affected_rx = 3_849_226_992
        affected_tx = 566_331_938
        earlier_usage = 3_000
        current_usage = 150
        attrs = ["wan-rx_bytes", "wan-tx_bytes", "time"]

        daily_rows = [
            {
                "time": int(local_month_start.timestamp() * 1000),
                "wan-rx_bytes": 1_000,
                "wan-tx_bytes": 2_000,
            },
            {
                "time": int(affected_day_start.timestamp() * 1000),
                "wan-rx_bytes": affected_rx + rx_correction,
                "wan-tx_bytes": affected_tx + tx_correction,
            },
        ]
        replay_rows = [
            {
                "time": int(affected_day_start.timestamp() * 1000),
                "wan-tx_bytes": 233_165_969,
            },
            {
                "time": int(
                    datetime(2026, 7, 22, 19, 30, tzinfo=timezone.utc).timestamp()
                    * 1000
                ),
                "wan-tx_bytes": 4_565_287_569_719,
            },
            {
                "time": int(
                    datetime(2026, 7, 22, 20, 30, tzinfo=timezone.utc).timestamp()
                    * 1000
                ),
                "wan-tx_bytes": 100_000_000,
            },
        ]
        current_rows = [
            {
                "time": int(current_day_start.timestamp() * 1000),
                "wan-rx_bytes": 100,
                "wan-tx_bytes": 50,
            }
        ]

        with tempfile.TemporaryDirectory() as state_dir:
            state_path = os.path.join(state_dir, "usage-corrections.json")
            with open(state_path, "w", encoding="utf-8") as state_file:
                json.dump(
                    {
                        "version": 1,
                        "days": {
                            "2026-07-23": {
                                "corrections": {
                                    "wan-rx_bytes": rx_correction,
                                },
                                "timezone": "Asia/Kolkata",
                            }
                        },
                    },
                    state_file,
                )

            with mock.patch.dict(
                os.environ,
                {
                    "UNIFI_STATS_TIMEZONE": "Asia/Kolkata",
                    "UNIFI_USAGE_CORRECTION_STATE_PATH": state_path,
                },
            ):
                client = unifi.UnifiProvider()
                client.authenticated = True
                client.network_rows = [
                    {
                        "purpose": "wan",
                        "wan_networkgroup": "WAN",
                        "wan_provider_capabilities": {
                            "download_kilobits_per_second": 300000,
                            "upload_kilobits_per_second": 100000,
                        },
                    }
                ]
                calls = []

                def fake_stats_rows(interval, start, end, requested_attrs):
                    calls.append((interval, start, end, requested_attrs))
                    if interval == "daily":
                        return daily_rows
                    if start == affected_day_start:
                        return replay_rows
                    return current_rows

                with (
                    mock.patch.object(client, "stats_attrs", return_value=attrs),
                    mock.patch.object(
                        client,
                        "stats_rows",
                        side_effect=fake_stats_rows,
                    ),
                ):
                    snapshot = client.usage_snapshot(
                        datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
                    )

                    self.assertEqual(current_usage, snapshot.day_usage_bytes)
                    self.assertEqual(
                        earlier_usage
                        + affected_rx
                        + affected_tx
                        + current_usage,
                        snapshot.cycle_usage_bytes,
                    )
                    self.assertEqual(
                        rx_correction + tx_correction,
                        client.usage_corrected_bytes,
                    )
                    self.assertEqual(1, len(client.usage_anomalies))
                    self.assertEqual(
                        ["daily", "hourly", "hourly"],
                        [call[0] for call in calls],
                    )

                    calls.clear()
                    repeated_snapshot = client.usage_snapshot(
                        datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
                    )

                self.assertEqual(
                    snapshot.cycle_usage_bytes,
                    repeated_snapshot.cycle_usage_bytes,
                )
                self.assertEqual(
                    snapshot.day_usage_bytes,
                    repeated_snapshot.day_usage_bytes,
                )
                self.assertEqual(
                    ["daily", "hourly"],
                    [call[0] for call in calls],
                )
                with open(state_path, "r", encoding="utf-8") as state_file:
                    state = json.load(state_file)
                self.assertEqual(
                    {
                        "wan-rx_bytes": rx_correction,
                        "wan-tx_bytes": tx_correction,
                    },
                    state["days"]["2026-07-23"]["corrections"],
                )

    def test_stats_rate_limits_use_unifi_provider_capabilities(self):
        client = unifi.UnifiProvider()
        client.network_rows = [
            {
                "purpose": "wan",
                "wan_networkgroup": "WAN",
                "wan_provider_capabilities": {
                    "download_kilobits_per_second": 300000,
                    "upload_kilobits_per_second": 100000,
                },
            },
            {
                "purpose": "wan",
                "wan_networkgroup": "WAN2",
                "wan_provider_capabilities": None,
            },
        ]

        limits = client.stats_rate_limits(
            ["wan-rx_bytes", "wan-tx_bytes", "wan2-rx_bytes", "time"]
        )

        self.assertEqual(37_500_000, limits["wan-rx_bytes"])
        self.assertEqual(12_500_000, limits["wan-tx_bytes"])
        self.assertNotIn("wan2-rx_bytes", limits)

    def test_stats_timezone_is_discovered_from_unifi_sysinfo(self):
        client = unifi.UnifiProvider()
        with mock.patch.object(
            client,
            "api_rows",
            return_value=[{"timezone": "Asia/Kolkata"}],
        ):
            local_timezone = client.resolve_stats_timezone()

        self.assertEqual("Asia/Kolkata", str(local_timezone))
        self.assertEqual("stat/sysinfo", client.stats_timezone_source)

    def test_stats_rows_accepts_wrapped_data_response(self):
        client = unifi.UnifiProvider()
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

        with mock.patch.object(unifi, "request_json", side_effect=fake_request_json):
            rows = client.stats_rows("hourly", start, end, ["wan-rx_bytes", "time"])

        self.assertEqual([{"wan-rx_bytes": 10}], rows)
        self.assertEqual("POST", calls[0]["method"])
        self.assertIn("/proxy/network/api/s/default/stat/report/hourly.site", calls[0]["url"])
        payload = json.loads(calls[0]["body"].decode("utf-8"))
        self.assertEqual(int(start.timestamp() * 1000), payload["start"])
        self.assertEqual(int(end.timestamp() * 1000), payload["end"])
        self.assertEqual(["wan-rx_bytes", "time"], payload["attrs"])

    def test_stats_rows_accepts_raw_list_response(self):
        client = unifi.UnifiProvider()
        row_time = datetime(2026, 6, 1, tzinfo=timezone.utc)
        row_time_ms = int(row_time.timestamp() * 1000)

        with mock.patch.object(
            unifi,
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
        client = unifi.UnifiProvider()

        with mock.patch.object(
            unifi,
            "request_json",
            return_value=({"data": {"wan-rx_bytes": 10}}, FakeResponse()),
        ):
            with self.assertRaisesRegex(unifi.ApiError, "unexpected shape"):
                client.stats_rows(
                    "daily",
                    datetime(2026, 6, 1, tzinfo=timezone.utc),
                    datetime(2026, 6, 2, tzinfo=timezone.utc),
                    ["wan-rx_bytes", "time"],
                )

    def test_backup_state_uses_active_uplink_instead_of_last_wan_status(self):
        state = unifi.classify_unifi_backup_internet_state(
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
        state = unifi.classify_unifi_backup_internet_state(
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
            unifi.ApiError,
            "no WAN configured with failover-only role",
        ):
            unifi.classify_unifi_backup_internet_state(
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
            unifi.ApiError,
            "Could not uniquely map active UniFi uplink",
        ):
            unifi.classify_unifi_backup_internet_state(
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
            "UNIFI_URL": "https://unifi.test",
            "UNIFI_API_KEY": "test-api-key",
        }
        with mock.patch.dict("os.environ", env, clear=True), \
                mock.patch.object(unifi, "request_json", side_effect=fake_request_json):
            state = unifi.UnifiProvider().backup_internet_state()

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
