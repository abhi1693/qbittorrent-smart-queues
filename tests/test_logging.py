import contextlib
import importlib
import io
import json
import unittest
from unittest import mock


class LoggingTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")
        self.guard._DECISION_SUMMARY_REPEAT_STATE.clear()

    def test_default_log_format_is_plain_text_at_info_level(self):
        stdout = io.StringIO()
        with mock.patch.dict("os.environ", {}, clear=True), contextlib.redirect_stdout(stdout):
            self.guard.log_debug("hidden detail")
            self.guard.log_info("visible message")

        lines = stdout.getvalue().splitlines()
        self.assertEqual(1, len(lines))
        self.assertFalse(lines[0].startswith("{"))
        self.assertIn("INFO", lines[0])
        self.assertIn("visible message", lines[0])
        self.assertNotIn("hidden detail", lines[0])

    def test_warning_level_suppresses_info_and_emits_warning_to_stderr(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        env = {"QBT_LOG_LEVEL": "warning"}

        with mock.patch.dict("os.environ", env, clear=True), \
                contextlib.redirect_stdout(stdout), \
                contextlib.redirect_stderr(stderr):
            self.guard.log_info("hidden info")
            self.guard.log_warning("visible warning")

        self.assertEqual("", stdout.getvalue())
        self.assertIn("WARNING", stderr.getvalue())
        self.assertIn("visible warning", stderr.getvalue())
        self.assertNotIn("hidden info", stderr.getvalue())

    def test_default_decision_logs_are_debug(self):
        stdout = io.StringIO()
        env = {"QBT_LOG_FORMAT": "json"}

        with mock.patch.dict("os.environ", env, clear=True), contextlib.redirect_stdout(stdout):
            self.guard.emit_decision_log("qbt_guard_decision", action="hidden_at_info")

        self.assertEqual("", stdout.getvalue())

    def test_critical_decision_summary_is_info_by_default(self):
        stdout = io.StringIO()
        env = {"QBT_LOG_FORMAT": "json"}

        with mock.patch.dict("os.environ", env, clear=True), contextlib.redirect_stdout(stdout):
            self.guard.log_decision_info(
                "try_candidate",
                "Trying torrent 1/3: Example.S01E04",
                selected="Example.S01E04",
            )

        record = json.loads(stdout.getvalue())
        self.assertEqual("INFO", record["level"])
        self.assertEqual("qbt_guard_decision", record["event"])
        self.assertEqual("try_candidate", record["action"])
        self.assertEqual("Example.S01E04", record["selected"])

    def test_json_log_format_preserves_decision_fields_when_enabled_at_info(self):
        stdout = io.StringIO()
        env = {"QBT_LOG_FORMAT": "json", "QBT_DECISION_LOG_LEVEL": "info"}

        with mock.patch.dict("os.environ", env, clear=True), contextlib.redirect_stdout(stdout):
            self.guard.emit_decision_log(
                "qbt_guard_decision",
                action="keep_productive",
                winner_torrent={
                    "name": "Winner",
                    "score_breakdown": {
                        "total": 93.0,
                        "queue": 20.0,
                        "health": 40.0,
                        "progress": 18.0,
                        "observed_download_speed_bytes_per_sec": 15,
                    },
                },
                runner_up_torrent={"name": "Runner", "score_breakdown": {"total": 81.5}},
                current_active_torrent={"name": "Active", "score_breakdown": {"total": 70.0}},
                rejected_counts={"complete": 2},
            )

        record = json.loads(stdout.getvalue())
        self.assertEqual("INFO", record["level"])
        self.assertEqual("qbt_guard_decision", record["event"])
        self.assertEqual("keep_productive", record["action"])
        self.assertIn("winner=Winner", record["message"])
        self.assertIn("winner_score=93.0", record["message"])
        self.assertIn("queue=20.0", record["message"])
        self.assertIn("health=40.0", record["message"])
        self.assertIn("progress=18.0", record["message"])
        self.assertIn("speed=15", record["message"])
        self.assertIn("runner_up=Runner", record["message"])
        self.assertIn("current_active=Active", record["message"])
        self.assertEqual({"complete": 2}, record["rejected_counts"])

    def test_decision_logs_can_be_disabled_with_legacy_env(self):
        stdout = io.StringIO()
        env = {"QBT_STRUCTURED_DECISION_LOGS_ENABLED": "false"}

        with mock.patch.dict("os.environ", env, clear=True), contextlib.redirect_stdout(stdout):
            self.guard.emit_decision_log("qbt_guard_decision", action="hidden")

        self.assertEqual("", stdout.getvalue())

    def test_repeated_decision_summary_is_suppressed_until_repeat_window(self):
        stdout = io.StringIO()
        env = {"QBT_LOG_FORMAT": "json", "QBT_DECISION_SUMMARY_REPEAT_SECONDS": "60"}

        with mock.patch.dict("os.environ", env, clear=True), \
                mock.patch.object(self.guard.time, "monotonic", side_effect=[100.0, 130.0, 161.0]), \
                contextlib.redirect_stdout(stdout):
            self.guard.log_decision_info(
                "throttle",
                "Throttled qBittorrent; first reason",
                summary_key=("rpi", "throttle", "k8s-rpi2"),
                reason="first reason",
            )
            self.guard.log_decision_info(
                "throttle",
                "Throttled qBittorrent; second reason",
                summary_key=("rpi", "throttle", "k8s-rpi2"),
                reason="second reason",
            )
            self.guard.log_decision_info(
                "throttle",
                "Throttled qBittorrent; latest reason",
                summary_key=("rpi", "throttle", "k8s-rpi2"),
                reason="latest reason",
            )

        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(2, len(records))
        self.assertEqual("first reason", records[0]["reason"])
        self.assertEqual("latest reason", records[1]["reason"])
        self.assertEqual(1, records[1]["suppressed_decision_log_count"])

    def test_decision_summary_repeat_window_can_be_disabled(self):
        stdout = io.StringIO()
        env = {"QBT_LOG_FORMAT": "json", "QBT_DECISION_SUMMARY_REPEAT_SECONDS": "0"}

        with mock.patch.dict("os.environ", env, clear=True), \
                mock.patch.object(self.guard.time, "monotonic", side_effect=[100.0, 101.0]), \
                contextlib.redirect_stdout(stdout):
            self.guard.log_decision_info(
                "throttle",
                "first",
                summary_key=("same",),
            )
            self.guard.log_decision_info(
                "throttle",
                "second",
                summary_key=("same",),
            )

        self.assertEqual(2, len(stdout.getvalue().splitlines()))

    def test_text_log_can_hide_fields_kept_in_json(self):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.guard.log_decision_info(
                "throttle",
                "Thermal throttle: limited qBittorrent",
                text_omit_fields={"action", "reason"},
                reason="same reason repeated elsewhere",
            )

        line = stdout.getvalue()
        self.assertIn("Thermal throttle: limited qBittorrent", line)
        self.assertNotIn("action=throttle", line)
        self.assertNotIn("same reason repeated elsewhere", line)

        stdout = io.StringIO()
        env = {"QBT_LOG_FORMAT": "json"}
        with mock.patch.dict("os.environ", env, clear=True), contextlib.redirect_stdout(stdout):
            self.guard.log_decision_info(
                "throttle",
                "Thermal throttle: limited qBittorrent",
                text_omit_fields={"action", "reason"},
                reason="kept in json",
            )

        record = json.loads(stdout.getvalue())
        self.assertEqual("throttle", record["action"])
        self.assertEqual("kept in json", record["reason"])

    def test_thermal_qbt_limit_text_summary_is_compact(self):
        stdout = io.StringIO()
        context = {
            "rpi_cooling": {
                "action": "throttle",
                "candidate": {
                    "node": "k8s-rpi2",
                    "kind": "NVMe",
                    "temperature": 73.85,
                    "threshold": 70.0,
                },
            },
        }

        with contextlib.redirect_stdout(stdout):
            self.guard.apply_qbt_limits(
                [mock.Mock()],
                "RPi thermal mitigation throttle active for k8s-rpi2: NVMe temperature 73.9C reached threshold 70.0C",
                False,
                2 * 1024 * 1024,
                128 * 1024,
                context,
            )

        line = stdout.getvalue()
        self.assertIn(
            "Thermal throttle: limited qBittorrent to 2.10 MB/s down / 131 KB/s up for k8s-rpi2; NVMe 73.8C >= 70.0C",
            line,
        )
        self.assertNotIn("reason=", line)
        self.assertNotIn("action=throttle", line)

    def test_guard_limit_decision_is_logged_when_qbittorrent_is_idle(self):
        stdout = io.StringIO()
        client = mock.Mock()
        client.base_url = "http://qbittorrent.example"
        client.torrents_info.return_value = [
            {
                "hash": "abc",
                "name": "stopped torrent",
                "state": "stoppedDL",
                "progress": 0.0,
                "amount_left": 100,
            }
        ]
        env = {"QBT_LOG_FORMAT": "json"}

        with mock.patch.dict("os.environ", env, clear=True), contextlib.redirect_stdout(stdout):
            self.guard.apply_qbt_limits(
                [client],
                "daily UDM quota guardrail reached",
                True,
                1,
                1,
                {"budget": {"daily_remaining_bytes": 0}},
            )

        client.set_download_limit.assert_not_called()
        client.set_upload_limit.assert_not_called()
        client.stop_all.assert_not_called()

        record = json.loads(stdout.getvalue())
        self.assertEqual("INFO", record["level"])
        self.assertEqual("qbt_guard_decision", record["event"])
        self.assertEqual("pause_all", record["action"])
        self.assertEqual("daily UDM quota guardrail reached", record["reason"])
        self.assertTrue(record["skipped_no_active_downloads"])
        self.assertEqual(0, record["active_download_count"])
        self.assertEqual(1, record["torrent_count"])
        self.assertIn("No active qBittorrent downloads to pause", record["message"])


if __name__ == "__main__":
    unittest.main()
