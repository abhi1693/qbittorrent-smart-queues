import json
import unittest
from unittest import mock

from qbittorrent_smart_queues.clients import QbtClient
from qbittorrent_smart_queues.errors import ApiError


class QbtClientTests(unittest.TestCase):
    def client(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            return QbtClient("http://qbittorrent.test/")

    def test_hash_actions_share_the_same_encoded_form(self):
        client = self.client()
        with mock.patch.object(client, "request") as request:
            client.reannounce_hashes(["one", "two"])

        request.assert_called_once_with(
            "POST",
            "/api/v2/torrents/reannounce",
            {"hashes": "one|two"},
        )

    def test_start_falls_back_to_resume_for_older_qbittorrent(self):
        client = self.client()
        with mock.patch.object(
            client,
            "request",
            side_effect=[ApiError("start unsupported"), b""],
        ) as request:
            client.start_hashes(["abc"])

        self.assertEqual(
            [
                mock.call("POST", "/api/v2/torrents/start", {"hashes": "abc"}),
                mock.call("POST", "/api/v2/torrents/resume", {"hashes": "abc"}),
            ],
            request.call_args_list,
        )

    def test_torrent_lookup_normalizes_hash_case(self):
        client = self.client()
        payload = json.dumps([{"hash": "ABC123", "name": "Example"}]).encode()
        with mock.patch.object(client, "request", return_value=payload):
            torrent = client.torrent_info("abc123")

        self.assertEqual("Example", torrent["name"])

    def test_empty_hash_action_is_a_noop(self):
        client = self.client()
        with mock.patch.object(client, "request") as request:
            client.delete_hashes([], delete_files=True)

        request.assert_not_called()
