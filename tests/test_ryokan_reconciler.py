import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from qbittorrent_smart_queues import ryokan_reconciler


class RyokanReconcilerTests(unittest.TestCase):
    def create_fixture(self, root, *, target_count=2, source_paths=None):
        root = Path(root)
        db_path = root / "ryokan.db"
        media_root = root / "anime"
        season_dir = media_root / "Example Show" / "Season 01"
        season_dir.mkdir(parents=True)
        source_paths = source_paths or [
            "/downloads/Batch/Example - 01.mkv",
            "/downloads/Batch/Example - 02.mkv",
        ]

        database = sqlite3.connect(db_path)
        database.executescript(
            """
            CREATE TABLE grabbed_torrents (
                id INTEGER PRIMARY KEY,
                hash TEXT NOT NULL,
                torrent_name TEXT NOT NULL,
                series_id INTEGER NOT NULL,
                episode_numbers TEXT NOT NULL,
                state TEXT NOT NULL,
                grabbed_at TEXT NOT NULL,
                imported_at TEXT,
                imported_source_paths TEXT
            );
            CREATE TABLE series (
                id INTEGER PRIMARY KEY,
                folder_name TEXT NOT NULL
            );
            CREATE TABLE episode_grab_history (
                id INTEGER PRIMARY KEY,
                series_id INTEGER NOT NULL,
                episode_number INTEGER NOT NULL,
                release_title TEXT NOT NULL,
                file_name TEXT NOT NULL,
                state TEXT NOT NULL,
                grabbed_at TEXT NOT NULL
            );
            CREATE TABLE episode_quality_tags (
                series_id INTEGER NOT NULL,
                episode_number INTEGER NOT NULL,
                state TEXT NOT NULL,
                PRIMARY KEY (series_id, episode_number)
            );
            """
        )
        database.execute("INSERT INTO series VALUES (7, 'Example Show')")
        database.execute(
            """
            INSERT INTO grabbed_torrents
                (id, hash, torrent_name, series_id, episode_numbers, state,
                 grabbed_at, imported_at, imported_source_paths)
            VALUES (11, 'ABC123', 'Example Batch', 7, '[1,2]', 'imported',
                    '2026-08-13 00:00:00', '2026-08-13 00:05:00', ?)
            """,
            (json.dumps(source_paths),),
        )
        for episode_number in range(1, target_count + 1):
            file_name = f"Example Show - S01E{episode_number:02}.mkv"
            (season_dir / file_name).write_bytes(
                b"x" * (101 if episode_number == 1 else 202)
            )
            database.execute(
                """
                INSERT INTO episode_grab_history
                    (id, series_id, episode_number, release_title, file_name,
                     state, grabbed_at)
                VALUES (?, 7, ?, 'Example Batch', ?, 'completed',
                        '2026-08-13 00:00:00')
                """,
                (episode_number, episode_number, file_name),
            )
            database.execute(
                "INSERT INTO episode_quality_tags VALUES (7, ?, 'completed')",
                (episode_number,),
            )
        database.commit()
        database.close()
        return db_path, media_root

    @staticmethod
    def expected_files():
        return [
            {
                "source_path_candidates": [
                    "/downloads/Batch/Example - 01.mkv",
                    "/downloads/Example - 01.mkv",
                ],
                "size_bytes": 101,
            },
            {
                "source_path_candidates": [
                    "/downloads/Batch/Example - 02.mkv",
                    "/downloads/Example - 02.mkv",
                ],
                "size_bytes": 202,
            },
        ]

    def test_complete_receipts_allow_deletion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path, media_root = self.create_fixture(tmpdir)
            result = ryokan_reconciler.reconcile_import(
                db_path,
                media_root,
                "abc123",
                self.expected_files(),
            )

        self.assertEqual("complete", result["status"])
        self.assertTrue(result["delete_allowed"])
        self.assertEqual(2, result["receipt_source_count"])
        self.assertEqual(2, result["distinct_episode_count"])
        self.assertEqual(2, result["distinct_target_count"])
        self.assertEqual(2, result["existing_target_count"])

    def test_collapsed_batch_target_is_requeued_for_ryokan(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path, media_root = self.create_fixture(tmpdir, target_count=1)
            result = ryokan_reconciler.reconcile_import(
                db_path,
                media_root,
                "abc123",
                self.expected_files(),
            )

            database = sqlite3.connect(db_path)
            grab = database.execute(
                "SELECT state, imported_at, imported_source_paths FROM grabbed_torrents WHERE id = 11"
            ).fetchone()
            history_state = database.execute(
                "SELECT state FROM episode_grab_history WHERE id = 1"
            ).fetchone()[0]
            tag_state = database.execute(
                "SELECT state FROM episode_quality_tags WHERE series_id = 7 AND episode_number = 1"
            ).fetchone()[0]
            database.close()

            second = ryokan_reconciler.reconcile_import(
                db_path,
                media_root,
                "abc123",
                self.expected_files(),
            )

        self.assertEqual("requeued", result["status"])
        self.assertFalse(result["delete_allowed"])
        self.assertEqual(1, result["distinct_target_count"])
        self.assertEqual(("pending", None, None), grab)
        self.assertEqual("grabbed", history_state)
        self.assertEqual("grabbed", tag_state)
        self.assertEqual("pending", second["status"])
        self.assertFalse(second["delete_allowed"])

    def test_source_receipt_mismatch_is_requeued(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path, media_root = self.create_fixture(
                tmpdir,
                source_paths=[
                    "/downloads/Batch/Example - 01.mkv",
                    "/downloads/Batch/Unexpected - 99.mkv",
                ],
            )
            result = ryokan_reconciler.reconcile_import(
                db_path,
                media_root,
                "abc123",
                self.expected_files(),
            )

        self.assertEqual("requeued", result["status"])
        self.assertFalse(result["sources_match"])
        self.assertTrue(result["targets_match"])

    def test_library_size_mismatch_is_requeued(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path, media_root = self.create_fixture(tmpdir)
            target = Path(media_root) / "Example Show" / "Season 01" / "Example Show - S01E02.mkv"
            target.write_bytes(b"truncated")

            result = ryokan_reconciler.reconcile_import(
                db_path,
                media_root,
                "abc123",
                self.expected_files(),
            )

        self.assertEqual("requeued", result["status"])
        self.assertFalse(result["sizes_match"])
        self.assertFalse(result["targets_match"])

    def test_selected_file_count_mismatch_fails_closed_without_requeue(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path, media_root = self.create_fixture(tmpdir)
            database = sqlite3.connect(db_path)
            database.execute(
                "UPDATE grabbed_torrents SET episode_numbers = '[1]' WHERE id = 11"
            )
            database.commit()
            database.close()

            result = ryokan_reconciler.reconcile_import(
                db_path,
                media_root,
                "abc123",
                self.expected_files(),
            )

            database = sqlite3.connect(db_path)
            grab = database.execute(
                "SELECT state, imported_at, imported_source_paths FROM grabbed_torrents WHERE id = 11"
            ).fetchone()
            history_states = database.execute(
                "SELECT state FROM episode_grab_history ORDER BY id"
            ).fetchall()
            database.close()

        self.assertEqual("batch_shape_mismatch", result["status"])
        self.assertFalse(result["delete_allowed"])
        self.assertEqual(1, result["grabbed_episode_count"])
        self.assertEqual(2, result["expected_count"])
        self.assertEqual("imported", grab[0])
        self.assertIsNotNone(grab[1])
        self.assertIsNotNone(grab[2])
        self.assertEqual([("completed",), ("completed",)], history_states)

    def test_unknown_hash_fails_closed_without_database_mutation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path, media_root = self.create_fixture(tmpdir)
            result = ryokan_reconciler.reconcile_import(
                db_path,
                media_root,
                "missing",
                self.expected_files(),
            )

        self.assertEqual("not_found", result["status"])
        self.assertFalse(result["delete_allowed"])

    def test_extra_stored_batch_episodes_are_allowed_when_receipts_are_exact(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path, media_root = self.create_fixture(tmpdir)
            database = sqlite3.connect(db_path)
            database.execute(
                "UPDATE grabbed_torrents SET episode_numbers = '[1,2,3]' WHERE id = 11"
            )
            database.commit()
            database.close()

            result = ryokan_reconciler.reconcile_import(
                db_path,
                media_root,
                "abc123",
                self.expected_files(),
            )

        self.assertEqual("complete", result["status"])
        self.assertTrue(result["delete_allowed"])
        self.assertEqual(3, result["grabbed_episode_count"])
        self.assertEqual(2, result["expected_count"])

    def test_post_processing_disabled_completion_is_not_requeued(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path, media_root = self.create_fixture(tmpdir, target_count=0)
            database = sqlite3.connect(db_path)
            database.execute(
                "UPDATE grabbed_torrents SET imported_at = NULL WHERE id = 11"
            )
            database.commit()
            database.close()

            result = ryokan_reconciler.reconcile_import(
                db_path,
                media_root,
                "abc123",
                self.expected_files(),
            )

        self.assertEqual("completed_without_import", result["status"])
        self.assertFalse(result["delete_allowed"])


if __name__ == "__main__":
    unittest.main()
