import unittest
from unittest import mock


class FakeQbtClient:
    def __init__(self, errored):
        self.errored = list(errored)
        self.deleted = []

    def torrents_info(self, filter_name=None):
        if filter_name in {"errored", "error"}:
            return list(self.errored)
        return []

    def delete_hashes(self, hashes, delete_files):
        self.deleted.append((list(hashes), delete_files))


class MissingFilesCleanupTests(unittest.TestCase):
    def setUp(self):
        from qbittorrent_smart_queues import guard

        self.guard = guard

    def test_missing_files_entries_never_delete_filesystem_data(self):
        client = FakeQbtClient([
            {
                "hash": "generic-missing",
                "name": "Generic missing payload",
                "state": "missingFiles",
                "category": "movies",
            },
        ])

        self.guard.cleanup_qbt_client(client)

        self.assertEqual([(["generic-missing"], False)], client.deleted)

    def test_ryokan_missing_files_entry_is_retained_for_receipt_reconciliation(self):
        client = FakeQbtClient([
            {
                "hash": "dragon-ball",
                "name": "Dragon Ball complete",
                "state": "missingFiles",
                "category": "anime",
            },
        ])

        with mock.patch.dict(
            "os.environ",
            {"QBT_RYOKAN_IMPORTED_ANIME_CLEANUP_ENABLED": "true"},
        ):
            self.guard.cleanup_qbt_client(client)

        self.assertEqual([], client.deleted)

    def test_ryokan_category_uses_generic_metadata_cleanup_when_reconciler_is_disabled(self):
        client = FakeQbtClient([
            {
                "hash": "dragon-ball",
                "name": "Dragon Ball complete",
                "state": "missingFiles",
                "category": "anime",
            },
        ])

        with mock.patch.dict(
            "os.environ",
            {"QBT_RYOKAN_IMPORTED_ANIME_CLEANUP_ENABLED": "false"},
        ):
            self.guard.cleanup_qbt_client(client)

        self.assertEqual([(["dragon-ball"], False)], client.deleted)

    def test_full_cleanup_pass_forwards_retained_ryokan_entry_to_reconciler(self):
        torrent = {
            "hash": "dragon-ball",
            "name": "Dragon Ball complete",
            "state": "missingFiles",
            "category": "anime",
            "progress": 1,
            "amount_left": 0,
        }
        client = FakeQbtClient([torrent])

        with (
            mock.patch.object(
                self.guard,
                "process_manual_blacklist_torrents",
                return_value={"attempted": 0},
            ),
            mock.patch.object(self.guard, "cleanup_arr_managed_completed_torrents"),
            mock.patch.object(self.guard, "process_ryokan_imported_anime_torrents") as reconcile,
            mock.patch.object(self.guard, "maintain_stale_stalled_torrents"),
            mock.patch.object(self.guard, "cleanup_stall_tags"),
        ):
            with mock.patch.dict(
                "os.environ",
                {"QBT_RYOKAN_IMPORTED_ANIME_CLEANUP_ENABLED": "true"},
            ):
                self.guard.cleanup_qbt_clients([client])

        self.assertEqual([], client.deleted)
        reconcile.assert_called_once()
        self.assertIn(torrent, reconcile.call_args.args[1])

    def test_other_error_states_are_retained_for_recovery(self):
        client = FakeQbtClient([
            {"hash": "recoverable", "name": "Recoverable", "state": "error"},
        ])

        self.guard.cleanup_qbt_client(client)

        self.assertEqual([], client.deleted)


if __name__ == "__main__":
    unittest.main()
