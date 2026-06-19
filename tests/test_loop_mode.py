import contextlib
import importlib
import io
import json
import unittest
from unittest import mock


class StopAfterFirstWait:
    def __init__(self):
        self.stopped = False

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def wait(self, seconds):
        self.stopped = True
        return True


class LoopModeTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_main_always_dispatches_to_loop(self):
        with mock.patch.dict("os.environ", {}, clear=True), \
                mock.patch.object(self.guard, "run_loop", return_value=0) as run_loop:
            result = self.guard.main()

        self.assertEqual(0, result)
        run_loop.assert_called_once()

    def test_main_never_dispatches_directly_to_single_pass(self):
        with mock.patch.dict("os.environ", {}, clear=True), \
                mock.patch.object(self.guard, "run_loop", return_value=0) as run_loop, \
                mock.patch.object(self.guard, "run_once", return_value=1) as run_once:
            result = self.guard.main()

        self.assertEqual(0, result)
        run_loop.assert_called_once()
        run_once.assert_not_called()

    def test_fixed_rate_sleep_subtracts_elapsed_time(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(30.0, self.guard.loop_sleep_seconds(0, 30, 60, 120))
            self.assertEqual(0.0, self.guard.loop_sleep_seconds(0, 90, 60, 120))
            self.assertEqual(90.0, self.guard.loop_sleep_seconds(1, 30, 60, 120))

    def test_run_loop_logs_qbt_services_once_at_startup(self):
        stdout = io.StringIO()
        env = {
            "QBT_GUARD_POLL_SECONDS": "1",
            "QBT_GUARD_ERROR_POLL_SECONDS": "1",
            "QBT_LOG_FORMAT": "json",
            "QBT_URLS": "http://qbittorrent.one:8080,http://qbittorrent.two:8080",
        }

        with mock.patch.dict("os.environ", env, clear=True), \
                mock.patch.object(self.guard.threading, "Event", StopAfterFirstWait), \
                mock.patch.object(self.guard, "install_loop_signal_handlers"), \
                mock.patch.object(self.guard, "run_once", return_value=0), \
                contextlib.redirect_stdout(stdout):
            result = self.guard.run_loop()

        self.assertEqual(0, result)
        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        service_logs = [
            record for record in records
            if record.get("message") == "Configured qBittorrent service endpoint(s)"
        ]
        self.assertEqual(1, len(service_logs))
        self.assertEqual(
            ["http://qbittorrent.one:8080", "http://qbittorrent.two:8080"],
            service_logs[0]["qbt_urls"],
        )

    def test_reachable_qbt_clients_ensures_blacklist_tag(self):
        instances = []

        class FakeQbtClient:
            def __init__(self, url):
                self.url = url
                self.created_tags = []
                instances.append(self)

            def login(self):
                pass

            def request(self, method, path, form=None):
                self.requested_version = (method, path, form)
                return b"v5.2.1"

            def create_tags(self, tags):
                self.created_tags.append(list(tags))

        with mock.patch.dict("os.environ", {"QBT_URLS": "http://qbittorrent.test:8080"}, clear=True), \
                mock.patch.object(self.guard, "QbtClient", FakeQbtClient):
            clients = self.guard.reachable_qbt_clients()

        self.assertEqual(instances, clients)
        self.assertEqual(("GET", "/api/v2/app/version", None), instances[0].requested_version)
        self.assertEqual([["blacklist"]], instances[0].created_tags)

    def test_qbt_client_create_tags_uses_global_tags_endpoint(self):
        client = self.guard.QbtClient("http://qbittorrent.test:8080")
        with mock.patch.object(client, "request", return_value=b"") as request:
            client.create_tags(["blacklist"])

        request.assert_called_once_with(
            "POST",
            "/api/v2/torrents/createTags",
            {"tags": "blacklist"},
        )


if __name__ == "__main__":
    unittest.main()
