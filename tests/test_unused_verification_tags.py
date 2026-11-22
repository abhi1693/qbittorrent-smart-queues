import unittest
from unittest.mock import Mock, patch

from qbittorrent_smart_queues import guard
from qbittorrent_smart_queues.errors import ApiError


class VerificationTagCleanupTests(unittest.TestCase):
    def test_only_unassigned_controller_timestamps_are_deleted(self):
        client = Mock()
        used = "availability-verified-20261101T000000Z"
        unused = "availability-verified-20261102T000000Z"
        client.all_tags.return_value = [
            used, unused, "availability-verified", "availability-verified-custom",
            "availability-verified-20261399T000000Z", "manual-review",
        ]
        with patch.object(guard, "single_download_torrents",
                          return_value=[{"hash": "abc", "tags": used}]):
            guard.cleanup_stall_tags(client)
        client.delete_tags.assert_called_once_with([unused])

    def test_failed_inventory_preserves_tags(self):
        client = Mock()
        with patch.object(guard, "single_download_torrents", side_effect=ApiError("offline")):
            guard.cleanup_stall_tags(client)
        client.delete_tags.assert_not_called()
