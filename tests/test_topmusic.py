import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# main.py reads BOT_TOKEN at import time; no real Telegram token is needed for Cache tests.
os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN")

from main import Cache, _claim_processing  # noqa: E402


class TopMusicRankingTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_musicbot.db"
        self.cache = Cache(self.db_path)
        self.now = 2_000_000_000

    def tearDown(self):
        self.cache.db.close()
        self.temp_dir.cleanup()

    def test_one_user_can_support_only_the_first_song_in_seven_days(self):
        with patch("main.time.time", return_value=self.now):
            self.cache.record_download("video_A123", "Song A", "Artist A", user_id=101)
            self.cache.record_download("video_B123", "Song B", "Artist B", user_id=101)
            self.cache.record_download("video_C123", "Song C", "Artist C", user_id=101)
            rows = self.cache.top_downloads(days=7, limit=10)

        self.assertEqual(rows, [("video_A123", "Song A", "Artist A", 1)])

    def test_first_song_remains_selected_after_repeated_downloads(self):
        with patch("main.time.time", return_value=self.now):
            self.cache.record_download("video_A123", "Song A", "Artist A", user_id=101)
            self.cache.record_download("video_A123", "Song A", "Artist A", user_id=101)
            self.cache.record_download("video_B123", "Song B", "Artist B", user_id=101)
            rows = self.cache.top_downloads(days=7, limit=10)

        self.assertEqual(rows, [("video_A123", "Song A", "Artist A", 1)])

    def test_different_users_can_support_the_same_song(self):
        with patch("main.time.time", return_value=self.now):
            self.cache.record_download("video_A123", "Song A", "Artist A", user_id=101)
            self.cache.record_download("video_B123", "Song B", "Artist B", user_id=202)
            self.cache.record_download("video_A123", "Song A", "Artist A", user_id=303)
            rows = self.cache.top_downloads(days=7, limit=10)

        self.assertEqual(rows[0], ("video_A123", "Song A", "Artist A", 2))
        self.assertEqual(rows[1], ("video_B123", "Song B", "Artist B", 1))

    def test_first_download_wins_when_timestamps_are_equal(self):
        with patch("main.time.time", return_value=self.now):
            self.cache.record_download("video_A123", "Song A", "Artist A", user_id=101)
            self.cache.record_download("video_B123", "Song B", "Artist B", user_id=101)
            rows = self.cache.top_downloads(days=7, limit=10)

        self.assertEqual(rows, [("video_A123", "Song A", "Artist A", 1)])

    def test_rolling_seven_day_window_excludes_older_events(self):
        cutoff = self.now - 7 * 24 * 60 * 60
        with self.cache.lock:
            self.cache.db.executemany(
                "INSERT INTO downloads(video_id, title, performer, downloaded_at, user_id) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    ("old_video1", "Old song", "Artist", cutoff - 1, 101),
                    ("new_video1", "Recent song", "Artist", cutoff + 1, 202),
                ],
            )
            self.cache.db.commit()

        with patch("main.time.time", return_value=self.now):
            rows = self.cache.top_downloads(days=7, limit=10)

        self.assertEqual([row[0] for row in rows], ["new_video1"])

    def test_prunes_events_older_than_seven_days(self):
        old_timestamp = self.now - 7 * 24 * 60 * 60 - 1
        with self.cache.lock:
            self.cache.db.execute(
                "INSERT INTO downloads(video_id, title, performer, downloaded_at, user_id) "
                "VALUES (?, ?, ?, ?, ?)",
                ("old_video1", "Old song", "Artist", old_timestamp, 101),
            )
            self.cache.db.commit()

        with patch("main.time.time", return_value=self.now):
            self.cache.record_download("fresh_video1", "Fresh song", "Artist", user_id=202)
            remaining = self.cache.db.execute(
                "SELECT video_id FROM downloads ORDER BY downloaded_at"
            ).fetchall()

        self.assertEqual(remaining, [("fresh_video1",)])

    def test_missing_user_id_is_ignored(self):
        with patch("main.time.time", return_value=self.now):
            self.cache.record_download("video_A123", "Song", "Artist")
            self.cache.record_download("", "Song", "Artist", user_id=101)
            self.cache.record_download(None, "Song", "Artist", user_id=101)
            count = self.cache.db.execute("SELECT COUNT(*) FROM downloads").fetchone()[0]

        self.assertEqual(count, 0)

    def test_limit_is_respected_and_unique_users_determine_order(self):
        with patch("main.time.time", return_value=self.now):
            for index in range(12):
                for user_id in range(index + 1):
                    self.cache.record_download(
                        f"video_{index:02d}", f"Song {index}", "Artist", user_id=1000 + index * 20 + user_id
                    )
            rows = self.cache.top_downloads(days=7, limit=10)

        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0][0], "video_11")
        self.assertEqual(rows[0][3], 12)
        self.assertEqual(rows[-1][0], "video_02")
        self.assertEqual(rows[-1][3], 3)

    def test_existing_database_is_migrated_without_losing_track_cache(self):
        legacy_path = Path(self.temp_dir.name) / "legacy.db"
        legacy = sqlite3.connect(str(legacy_path))
        legacy.execute(
            "CREATE TABLE downloads ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, video_id TEXT NOT NULL, "
            "title TEXT, performer TEXT, downloaded_at REAL NOT NULL)"
        )
        legacy.execute(
            "INSERT INTO downloads(video_id, title, performer, downloaded_at) "
            "VALUES ('legacy_video', 'Legacy song', 'Artist', ?)",
            (self.now,),
        )
        legacy.commit()
        legacy.close()

        migrated = Cache(legacy_path)
        try:
            columns = {row[1] for row in migrated.db.execute("PRAGMA table_info(downloads)")}
            self.assertIn("user_id", columns)
            self.assertEqual(
                migrated.db.execute("SELECT COUNT(*) FROM downloads").fetchone()[0], 1
            )
            with patch("main.time.time", return_value=self.now):
                self.assertEqual(migrated.top_downloads(days=7, limit=10), [])
            migrated.put_track("video_A123", "telegram-file-id", "Song A", "Artist A", 180)
            self.assertEqual(
                migrated.get_track("video_A123"),
                ("telegram-file-id", "Song A", "Artist A", 180),
            )
        finally:
            migrated.db.close()

    def test_track_file_id_cache_still_works(self):
        self.cache.put_track("video_A123", "telegram-file-id", "Song A", "Artist A", 180)
        self.assertEqual(
            self.cache.get_track("video_A123"),
            ("telegram-file-id", "Song A", "Artist A", 180),
        )
        self.cache.drop_track("video_A123")
        self.assertIsNone(self.cache.get_track("video_A123"))


class PickerProcessingLockTests(unittest.TestCase):
    def test_same_picker_token_can_only_be_claimed_once(self):
        user_data = {}

        self.assertTrue(_claim_processing(user_data, "token-a"))
        self.assertFalse(_claim_processing(user_data, "token-a"))
        self.assertEqual(user_data["music_processing_token"], "token-a")

    def test_different_picker_token_can_be_claimed(self):
        user_data = {"music_processing_token": "token-a"}

        self.assertTrue(_claim_processing(user_data, "token-b"))
        self.assertEqual(user_data["music_processing_token"], "token-b")


if __name__ == "__main__":
    unittest.main()
