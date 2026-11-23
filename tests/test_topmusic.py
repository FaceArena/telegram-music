import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# main.py reads BOT_TOKEN at import time; no real Telegram token is needed for Cache tests.
os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN")

from main import Cache  # noqa: E402


class TopMusicRankingTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.cache = Cache(Path(self.temp_dir.name) / "test_musicbot.db")
        self.now = 2_000_000_000

    def tearDown(self):
        self.cache.db.close()
        self.temp_dir.cleanup()

    def test_counts_successful_download_events_per_video(self):
        with patch("main.time.time", return_value=self.now):
            self.cache.record_download("video_A123", "Song A", "Artist A")
            self.cache.record_download("video_A123", "Song A", "Artist A")
            self.cache.record_download("video_B123", "Song B", "Artist B")

            rows = self.cache.top_downloads(days=7, limit=10)

        self.assertEqual(
            rows,
            [
                ("video_A123", "Song A", "Artist A", 2),
                ("video_B123", "Song B", "Artist B", 1),
            ],
        )

    def test_rolling_seven_day_window_excludes_older_events(self):
        cutoff = self.now - 7 * 24 * 60 * 60
        with self.cache.lock:
            self.cache.db.executemany(
                "INSERT INTO downloads(video_id, title, performer, downloaded_at) "
                "VALUES (?, ?, ?, ?)",
                [
                    ("old_video1", "Old song", "Artist", cutoff - 1),
                    ("new_video1", "Recent song", "Artist", cutoff + 1),
                ],
            )
            self.cache.db.commit()

        with patch("main.time.time", return_value=self.now):
            rows = self.cache.top_downloads(days=7, limit=10)

        self.assertEqual([row[0] for row in rows], ["new_video1"])

    def test_record_download_prunes_events_older_than_seven_days(self):
        old_timestamp = self.now - 7 * 24 * 60 * 60 - 1
        with self.cache.lock:
            self.cache.db.execute(
                "INSERT INTO downloads(video_id, title, performer, downloaded_at) "
                "VALUES (?, ?, ?, ?)",
                ("old_video1", "Old song", "Artist", old_timestamp),
            )
            self.cache.db.commit()

        with patch("main.time.time", return_value=self.now):
            self.cache.record_download("fresh_video1", "Fresh song", "Artist")
            remaining = self.cache.db.execute(
                "SELECT video_id FROM downloads ORDER BY downloaded_at"
            ).fetchall()

        self.assertEqual(remaining, [("fresh_video1",)])

    def test_empty_video_id_is_ignored(self):
        with patch("main.time.time", return_value=self.now):
            self.cache.record_download("", "Song", "Artist")
            self.cache.record_download(None, "Song", "Artist")
            count = self.cache.db.execute(
                "SELECT COUNT(*) FROM downloads"
            ).fetchone()[0]

        self.assertEqual(count, 0)

    def test_limit_is_respected_and_highest_count_ranks_first(self):
        with patch("main.time.time", return_value=self.now):
            for index in range(12):
                for _ in range(index + 1):
                    self.cache.record_download(
                        f"video_{index:02d}", f"Song {index}", "Artist"
                    )
            rows = self.cache.top_downloads(days=7, limit=10)

        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0][0], "video_11")
        self.assertEqual(rows[0][3], 12)
        self.assertEqual(rows[-1][0], "video_02")
        self.assertEqual(rows[-1][3], 3)

    def test_track_file_id_cache_still_works(self):
        self.cache.put_track("video_A123", "telegram-file-id", "Song A", "Artist A", 180)
        self.assertEqual(
            self.cache.get_track("video_A123"),
            ("telegram-file-id", "Song A", "Artist A", 180),
        )
        self.cache.drop_track("video_A123")
        self.assertIsNone(self.cache.get_track("video_A123"))


if __name__ == "__main__":
    unittest.main()
