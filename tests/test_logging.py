import contextlib
import importlib
import io
import json
import unittest
from unittest import mock


class LoggingTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

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

    def test_json_log_format_preserves_decision_fields(self):
        stdout = io.StringIO()
        env = {"QBT_LOG_FORMAT": "json"}

        with mock.patch.dict("os.environ", env, clear=True), contextlib.redirect_stdout(stdout):
            self.guard.emit_decision_log(
                "qbt_guard_decision",
                action="keep_productive",
                rejected_counts={"complete": 2},
            )

        record = json.loads(stdout.getvalue())
        self.assertEqual("INFO", record["level"])
        self.assertEqual("qbt_guard_decision", record["event"])
        self.assertEqual("keep_productive", record["action"])
        self.assertEqual({"complete": 2}, record["rejected_counts"])

    def test_decision_logs_can_be_disabled_with_legacy_env(self):
        stdout = io.StringIO()
        env = {"QBT_STRUCTURED_DECISION_LOGS_ENABLED": "false"}

        with mock.patch.dict("os.environ", env, clear=True), contextlib.redirect_stdout(stdout):
            self.guard.emit_decision_log("qbt_guard_decision", action="hidden")

        self.assertEqual("", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
