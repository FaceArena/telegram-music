import asyncio
from datetime import timedelta
import json
import base64
import os
import stat
import sqlite3
import time
import tempfile
import unittest

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from telegram.error import BadRequest, RetryAfter
from telegram.ext import Application

# main.py reads BOT_TOKEN at import time; no real Telegram token is needed for Cache tests.
os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN")

from main import AUDIO_FORMAT, _metric_increment, _metric_observe, _percentile, _track_runtime_metric, log_runtime_metrics, MAX_CONCURRENT_UPDATES, MAX_PENDING_UPDATES, ERROR_NOTIFICATION_TTL, Cache, _claim_processing, _get_search_state, _is_invalid_file_id_error, _record_successful_delivery, _release_processing, _safe_cache_call, _send_audio_limited, _env_float, _env_int, bass_filter_for_preset, build_bass_keyboard, _apply_bass_boost, _encode_audio, _ydl_options, _download, clean_metadata, is_duration_over_limit, is_youtube_url, load_cookies, schedule_prefetch, strip_bot_mention, send_generic_error, update_ytdlp, on_pick, on_bass_menu, on_bass_pick, on_picker_back, build_download_keyboard, rank_search_results  # noqa: E402


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

    def test_old_audio_cache_entries_are_invalidated(self):
        legacy_keys = [
            f"video_A123:{AUDIO_FORMAT}",
            f"video_A123:{AUDIO_FORMAT}:q320-v2",
        ]
        with self.cache.lock:
            self.cache.db.executemany(
                "INSERT INTO tracks VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (key, f"old-telegram-file-id-{index}", "Song A", "Artist A", 180, time.time())
                    for index, key in enumerate(legacy_keys)
                ],
            )
            self.cache.db.commit()

        self.assertIsNone(self.cache.get_track("video_A123"))
        self.cache.put_track("video_A123", "new-telegram-file-id", "Song A", "Artist A", 180)
        self.assertEqual(
            self.cache.get_track("video_A123"),
            ("new-telegram-file-id", "Song A", "Artist A", 180),
        )


class PickerProcessingLockTests(unittest.TestCase):
    def test_shared_picker_token_can_only_be_claimed_once(self):
        # All callback handlers in a group receive the same application-wide set.
        shared_processing_tokens = set()

        self.assertTrue(_claim_processing(shared_processing_tokens, "token-a"))
        self.assertFalse(_claim_processing(shared_processing_tokens, "token-a"))
        self.assertIn("token-a", shared_processing_tokens)

    def test_processing_claim_is_released_after_completion(self):
        shared_processing_tokens = set()
        self.assertTrue(_claim_processing(shared_processing_tokens, "token-a"))

        _release_processing(shared_processing_tokens, "token-a")

        self.assertTrue(_claim_processing(shared_processing_tokens, "token-a"))

    def test_picker_state_is_available_to_other_members_of_same_chat(self):
        state = {"chat_id": -100123, "created": 2_000_000_000, "results": ["song"]}
        searches = {"token-a": state}

        self.assertIs(_get_search_state(searches, "token-a", -100123), state)
        self.assertIsNone(_get_search_state(searches, "token-a", 12345))

    def test_expired_picker_state_is_removed(self):
        searches = {"token-a": {"chat_id": -100123, "created": 1, "results": []}}

        self.assertIsNone(_get_search_state(searches, "token-a", -100123))
        self.assertNotIn("token-a", searches)


class YouTubeUrlValidationTests(unittest.TestCase):
    def test_accepts_real_youtube_hosts(self):
        valid_urls = [
            "https://www.youtube.com/watch?v=abcdefghijk",
            "https://youtube.com/watch?v=abcdefghijk",
            "https://music.youtube.com/watch?v=abcdefghijk",
            "https://youtu.be/abcdefghijk",
            "http://m.youtube.com/watch?v=abcdefghijk",
        ]
        for url in valid_urls:
            with self.subTest(url=url):
                self.assertTrue(is_youtube_url(url))

    def test_rejects_hosts_that_only_contain_youtube_text(self):
        invalid_urls = [
            "https://youtube.com.attacker.test/watch?v=abcdefghijk",
            "https://attacker.test/?next=https://youtube.com/watch?v=abcdefghijk",
            "https://youtu.be.attacker.test/abcdefghijk",
            "http://127.0.0.1:8080/?next=youtube.com",
            "http://localhost/?url=youtube.com",
            "https://youtube.com@attacker.test/watch?v=abcdefghijk",
            "ftp://youtube.com/watch?v=abcdefghijk",
            "https://youtube.com:444/watch?v=abcdefghijk",
            "https://youtube.com:invalid/watch?v=abcdefghijk",
        ]
        for url in invalid_urls:
            with self.subTest(url=url):
                self.assertFalse(is_youtube_url(url))


class YtDlpUpdateAsyncTests(unittest.TestCase):
    def test_package_update_does_not_block_event_loop(self):
        async def scenario():
            finished = asyncio.Event()
            current_version = "test-version"

            def slow_update():
                time.sleep(0.08)
                return current_version

            async def run_update():
                with patch("main.yt_dlp.version.__version__", current_version), patch(
                    "main._update_ytdlp_sync", side_effect=slow_update
                ):
                    await update_ytdlp(None)
                finished.set()

            async def check_loop_responsive():
                await asyncio.sleep(0.01)
                return not finished.is_set()

            _, remained_responsive = await asyncio.gather(
                run_update(), check_loop_responsive()
            )
            return remained_responsive

        self.assertTrue(asyncio.run(scenario()))


class PrefetchSchedulingTests(unittest.TestCase):
    def test_only_one_prefetch_task_runs_at_a_time(self):
        async def scenario():
            gate = asyncio.Event()
            bot_data = {"_cleanup_tasks": set()}
            ctx = type("Context", (), {
                "application": type("Application", (), {"bot_data": bot_data})()
            })()

            async def wait_for_release(urls):
                await gate.wait()

            with patch("main._prefetch", side_effect=wait_for_release):
                schedule_prefetch(ctx, ["https://www.youtube.com/watch?v=first"])
                first_task = bot_data["_prefetch_task"]
                schedule_prefetch(ctx, ["https://www.youtube.com/watch?v=second"])
                second_task = bot_data["_prefetch_task"]
                self.assertIs(first_task, second_task)
                self.assertFalse(first_task.done())
                gate.set()
                await first_task

        asyncio.run(scenario())

    def test_empty_prefetch_does_not_create_a_task(self):
        bot_data = {"_cleanup_tasks": set()}
        ctx = type("Context", (), {
            "application": type("Application", (), {"bot_data": bot_data})()
        })()

        schedule_prefetch(ctx, [])

        self.assertNotIn("_prefetch_task", bot_data)


class SearchCacheRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.cache = Cache(Path(self.temp_dir.name) / "search_cache.db")

    def tearDown(self):
        self.cache.db.close()
        self.temp_dir.cleanup()

    def test_corrupted_json_is_removed_and_treated_as_cache_miss(self):
        with self.cache.lock:
            self.cache.db.execute(
                "INSERT INTO searches(q, results, created) VALUES (?, ?, ?)",
                ("broken query", "{not-json", time.time()),
            )
            self.cache.db.commit()

        self.assertIsNone(self.cache.get_search("broken query"))
        with self.cache.lock:
            count = self.cache.db.execute(
                "SELECT COUNT(*) FROM searches WHERE q=?", ("broken query",)
            ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_invalid_search_cache_shape_is_removed(self):
        with self.cache.lock:
            self.cache.db.execute(
                "INSERT INTO searches(q, results, created) VALUES (?, ?, ?)",
                ("wrong shape", '{"not": "a list"}', time.time()),
            )
            self.cache.db.commit()

        self.assertIsNone(self.cache.get_search("wrong shape"))

    def test_valid_search_cache_still_loads(self):
        expected = [{"title": "Song", "url": "https://www.youtube.com/watch?v=abcdefghijk"}]
        self.cache.put_search("valid query", expected)

        self.assertEqual(self.cache.get_search("valid query"), expected)

    def test_invalid_duration_in_cached_result_is_removed(self):
        for query, duration in (
            ("text duration", "180"),
            ("negative duration", -1),
            ("nan duration", float("nan")),
            ("boolean duration", True),
            ("duration at configured limit", 1200),
            ("unreasonably large integer duration", 10 ** 1000),
        ):
            with self.subTest(query=query):
                with self.cache.lock:
                    self.cache.db.execute(
                        "INSERT INTO searches(q, results, created) VALUES (?, ?, ?)",
                        (query, json.dumps([{
                            "title": "Song",
                            "url": "https://www.youtube.com/watch?v=abcdefghijk",
                            "duration": duration,
                        }]), time.time()),
                    )
                    self.cache.db.commit()

                self.assertIsNone(self.cache.get_search(query))
                with self.cache.lock:
                    count = self.cache.db.execute(
                        "SELECT COUNT(*) FROM searches WHERE q=?", (query,)
                    ).fetchone()[0]
                self.assertEqual(count, 0)

    def test_search_cache_rejects_more_than_ten_results(self):
        results = [
            {"title": f"Song {i}", "url": f"https://youtu.be/{i:011d}"}
            for i in range(11)
        ]
        with self.cache.lock:
            self.cache.db.execute(
                "INSERT INTO searches(q, results, created) VALUES (?, ?, ?)",
                ("too many", json.dumps(results), time.time()),
            )
            self.cache.db.commit()

        self.assertIsNone(self.cache.get_search("too many"))


class DeliveryPersistenceTests(unittest.TestCase):
    def test_track_cache_failure_does_not_skip_ranking_record(self):
        class CacheWithTrackWriteFailure:
            def __init__(self):
                self.recorded = []

            def put_track(self, *args):
                raise OSError("disk full")

            def record_download(self, *args, **kwargs):
                self.recorded.append((args, kwargs))

        cache = CacheWithTrackWriteFailure()

        _record_successful_delivery(
            cache, "video_A123", "telegram-file-id", "Song", "Artist", 180, 101
        )

        self.assertEqual(len(cache.recorded), 1)
        self.assertEqual(cache.recorded[0][0][0], "video_A123")
        self.assertEqual(cache.recorded[0][1]["user_id"], 101)

    def test_ranking_write_failure_does_not_raise_after_delivery(self):
        class CacheWithRankingFailure:
            def __init__(self):
                self.track_saved = False

            def put_track(self, *args):
                self.track_saved = True

            def record_download(self, *args, **kwargs):
                raise OSError("database unavailable")

        cache = CacheWithRankingFailure()

        _record_successful_delivery(
            cache, "video_A123", "telegram-file-id", "Song", "Artist", 180, 101
        )

        self.assertTrue(cache.track_saved)

    def test_cached_delivery_records_vote_without_rewriting_file_id(self):
        class CacheForCachedDelivery:
            def __init__(self):
                self.track_writes = 0
                self.downloads = 0

            def put_track(self, *args):
                self.track_writes += 1

            def record_download(self, *args, **kwargs):
                self.downloads += 1

        cache = CacheForCachedDelivery()

        _record_successful_delivery(
            cache, "video_A123", None, "Song", "Artist", 180, 101
        )

        self.assertEqual(cache.track_writes, 0)
        self.assertEqual(cache.downloads, 1)


class RuntimeCacheFallbackTests(unittest.TestCase):
    def test_cache_read_failure_returns_default_instead_of_raising(self):
        class BrokenCache:
            def get_track(self, video_id):
                raise sqlite3.OperationalError("database is locked")

        self.assertIsNone(_safe_cache_call(BrokenCache(), "get_track", "video_A123"))

    def test_cache_write_failure_does_not_escape(self):
        class BrokenCache:
            def put_search(self, query, results):
                raise sqlite3.OperationalError("disk I/O error")

        result = _safe_cache_call(
            BrokenCache(), "put_search", "song", [{"title": "Song", "url": "https://youtu.be/abcdefghijk"}]
        )
        self.assertIsNone(result)

    def test_missing_cache_uses_default(self):
        self.assertEqual(_safe_cache_call(None, "get_track", "video_A123", default="miss"), "miss")

    def test_successful_cache_operation_returns_its_value(self):
        class WorkingCache:
            def get_track(self, video_id):
                return ("file-id", "Song", "Artist", 120)

        self.assertEqual(
            _safe_cache_call(WorkingCache(), "get_track", "video_A123"),
            ("file-id", "Song", "Artist", 120),
        )


class DurationLimitTests(unittest.TestCase):
    def test_duration_below_limit_is_allowed(self):
        self.assertFalse(is_duration_over_limit(1199, 1200))

    def test_duration_equal_to_strict_limit_is_rejected(self):
        self.assertTrue(is_duration_over_limit(1200, 1200))

    def test_duration_above_limit_is_rejected(self):
        self.assertTrue(is_duration_over_limit(1201, 1200))

    def test_unknown_duration_preserves_existing_search_behavior(self):
        self.assertFalse(is_duration_over_limit(None, 1200))


class CachedFileIdErrorTests(unittest.TestCase):
    def test_explicit_invalid_file_identifier_error_allows_cache_invalidation(self):
        self.assertTrue(
            _is_invalid_file_id_error(BadRequest("Wrong file identifier/http URL specified"))
        )

    def test_chat_error_does_not_invalidate_cached_file_id(self):
        self.assertFalse(_is_invalid_file_id_error(BadRequest("Chat not found")))

    def test_transient_network_error_does_not_invalidate_cached_file_id(self):
        self.assertFalse(_is_invalid_file_id_error(TimeoutError("request timed out")))


class YtDlpUpdateCoordinationTests(unittest.TestCase):
    def test_update_waits_for_active_download_and_search_work(self):
        async def scenario():
            download_gate = asyncio.Semaphore(1)
            search_gate = asyncio.Semaphore(1)
            with patch("main.DL_LOCK", download_gate), patch(
                "main.MAX_PARALLEL_DL", 1
            ), patch("main.SEARCH_SEMAPHORE", search_gate), patch(
                "main.MAX_PARALLEL_SEARCH", 1
            ), patch("main.yt_dlp.version.__version__", "same-version"), patch(
                "main._update_ytdlp_sync", return_value="same-version"
            ):
                await download_gate.acquire()
                download_update = asyncio.create_task(update_ytdlp(None))
                await asyncio.sleep(0.01)
                self.assertFalse(download_update.done())
                download_gate.release()
                await asyncio.wait_for(download_update, timeout=2)

                await search_gate.acquire()
                search_update = asyncio.create_task(update_ytdlp(None))
                await asyncio.sleep(0.01)
                self.assertFalse(search_update.done())
                search_gate.release()
                await asyncio.wait_for(search_update, timeout=2)

                self.assertTrue(await asyncio.wait_for(download_gate.acquire(), timeout=1))
                download_gate.release()
                self.assertTrue(await asyncio.wait_for(search_gate.acquire(), timeout=1))
                search_gate.release()

        asyncio.run(scenario())


class TrackMetadataSanitizationTests(unittest.TestCase):
    def test_newlines_are_flattened_to_prevent_fake_picker_rows(self):
        self.assertEqual(
            clean_metadata("Song title\n🥇 1. Fake row\r\nArtist"),
            "Song title 🥇 1. Fake row Artist",
        )

    def test_control_characters_are_removed(self):
        self.assertEqual(clean_metadata("Song\x00\x01 title"), "Song title")

    def test_metadata_is_truncated_to_limit(self):
        self.assertEqual(clean_metadata("x" * 20, 8), "x" * 8)

    def test_missing_metadata_returns_none(self):
        self.assertIsNone(clean_metadata(None))
        self.assertIsNone(clean_metadata("   "))


class YouTubeCookiePermissionTests(unittest.TestCase):
    def test_base64_cookie_file_is_created_with_private_permissions(self):
        with tempfile.TemporaryDirectory() as tmp:
            encoded = base64.b64encode(b"# Netscape HTTP Cookie File\\nsecret").decode()
            with patch("main.tempfile.gettempdir", return_value=tmp), patch.dict(
                os.environ,
                {"YOUTUBE_COOKIES_B64": encoded, "YOUTUBE_COOKIES_FILE": ""},
            ):
                path = load_cookies()

            self.assertIsNotNone(path)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(path.read_bytes(), b"# Netscape HTTP Cookie File\\nsecret")


class EnvironmentConfigurationTests(unittest.TestCase):
    def test_invalid_integer_uses_default(self):
        with patch.dict(os.environ, {"MAX_DURATION": "not-a-number"}):
            self.assertEqual(_env_int("MAX_DURATION", 1200, minimum=1), 1200)

    def test_integer_below_minimum_uses_default(self):
        with patch.dict(os.environ, {"UPDATE_EVERY_HOURS": "0"}):
            self.assertEqual(_env_int("UPDATE_EVERY_HOURS", 12, minimum=1), 12)

    def test_valid_integer_is_preserved(self):
        with patch.dict(os.environ, {"MAX_PARALLEL_DL": "5"}):
            self.assertEqual(_env_int("MAX_PARALLEL_DL", 3, minimum=1), 5)

    def test_invalid_float_uses_default(self):
        with patch.dict(os.environ, {"MIN_DELAY": "fast"}):
            self.assertEqual(_env_float("MIN_DELAY", 0.0, minimum=0.0), 0.0)

    def test_negative_and_non_finite_floats_use_default(self):
        for raw in ("-1.5", "nan", "inf", "-inf"):
            with self.subTest(raw=raw), patch.dict(os.environ, {"MIN_DELAY": raw}):
                self.assertEqual(_env_float("MIN_DELAY", 0.0, minimum=0.0), 0.0)

    def test_zero_is_allowed_when_minimum_is_zero(self):
        with patch.dict(os.environ, {"SEARCH_CACHE_TTL": "0"}):
            self.assertEqual(_env_int("SEARCH_CACHE_TTL", 21600, minimum=0), 0)


class GroupMentionParsingTests(unittest.TestCase):
    def test_exact_mention_is_removed(self):
        mentioned, query = strip_bot_mention("@MusicBot Daft Punk - One More Time", "musicbot")
        self.assertTrue(mentioned)
        self.assertEqual(query, "Daft Punk - One More Time")

    def test_username_with_bot_name_as_prefix_does_not_trigger(self):
        mentioned, query = strip_bot_mention("@musicbot_fake song", "musicbot")
        self.assertFalse(mentioned)
        self.assertEqual(query, "@musicbot_fake song")

    def test_mention_inside_another_username_does_not_trigger(self):
        mentioned, query = strip_bot_mention("user@musicbot song", "musicbot")
        self.assertFalse(mentioned)
        self.assertEqual(query, "user@musicbot song")

    def test_missing_bot_username_does_not_trigger(self):
        self.assertEqual(strip_bot_mention("@musicbot song", None), (False, "@musicbot song"))


class RankingTieBreakTests(unittest.TestCase):
    def test_equal_scores_and_timestamps_have_stable_video_id_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = Cache(Path(tmp) / "ranking.db")
            try:
                with patch("main.time.time", return_value=2_000_000_000):
                    cache.record_download("video_B123", "Song B", "Artist", user_id=101)
                    cache.record_download("video_A123", "Song A", "Artist", user_id=202)
                rows = cache.top_downloads(days=7, limit=10)
                self.assertEqual(
                    [row[0] for row in rows],
                    ["video_A123", "video_B123"],
                )
            finally:
                cache.db.close()


if __name__ == "__main__":
    unittest.main()


class ErrorNotificationTtlTests(unittest.IsolatedAsyncioTestCase):
    def make_context(self):
        class FakeBot:
            def __init__(self):
                self.sent = []
                self.deleted = []

            async def send_message(self, chat_id, text):
                message = type("Message", (), {
                    "chat_id": chat_id,
                    "message_id": len(self.sent) + 1,
                })()
                self.sent.append((chat_id, text, message))
                return message

            async def delete_message(self, chat_id, message_id):
                self.deleted.append((chat_id, message_id))

        bot = FakeBot()
        bot_data = {}
        ctx = type("Context", (), {
            "bot": bot,
            "application": type("Application", (), {"bot_data": bot_data})(),
        })()
        return ctx, bot, bot_data

    async def test_every_simultaneous_error_gets_its_own_ten_second_cleanup(self):
        ctx, bot, bot_data = self.make_context()
        self.assertEqual(ERROR_NOTIFICATION_TTL, 10)

        with patch("main.ERROR_NOTIFICATION_TTL", 0.01):
            await asyncio.gather(*[
                send_generic_error(ctx, -100123, f"⚠️ Ошибка {index}")
                for index in range(5)
            ])
            self.assertEqual(len(bot.sent), 5)
            await asyncio.sleep(0.04)

        self.assertEqual(
            sorted(bot.deleted),
            [(-100123, 1), (-100123, 2), (-100123, 3), (-100123, 4), (-100123, 5)],
        )
        self.assertEqual(bot_data.get("_cleanup_tasks", set()), set())

    async def test_a_new_error_has_an_independent_ten_second_lifetime(self):
        ctx, bot, bot_data = self.make_context()

        with patch("main.ERROR_NOTIFICATION_TTL", 0.01):
            await send_generic_error(ctx, -100123, "❌ Не удалось загрузить.")
            await asyncio.sleep(0.02)
            await send_generic_error(ctx, -100123, "❌ Не удалось загрузить.")
            await asyncio.sleep(0.02)

        self.assertEqual(len(bot.sent), 2)
        self.assertEqual(bot.deleted, [(-100123, 1), (-100123, 2)])
        self.assertEqual(bot_data.get("_cleanup_tasks", set()), set())


class PickerAlertTests(unittest.IsolatedAsyncioTestCase):
    class FakeCallback:
        def __init__(self, data, message):
            self.data = data
            self.message = message
            self.answers = []

        async def answer(self, text=None, show_alert=False):
            self.answers.append((text, show_alert))

    class FakePicker:
        next_id = 700

        def __init__(self, message_id=456, text="🎵 Выбери трек:", reply_markup=None):
            self.chat_id = 123
            self.message_id = message_id
            self.text = text
            self.reply_markup = reply_markup
            self.deleted = False
            self.replies = []

        async def edit_text(self, text, reply_markup=None):
            self.text = text
            self.reply_markup = reply_markup
            return self

        async def edit_reply_markup(self, reply_markup=None):
            self.reply_markup = reply_markup
            return self

        async def reply_text(self, text, reply_markup=None):
            type(self).next_id += 1
            message = type(self)(type(self).next_id, text, reply_markup)
            self.replies.append(message)
            return message

        async def delete(self):
            self.deleted = True

    def make_context(self, state):
        bot_data = {
            "music_searches": {"live-token": state} if state else {},
            "music_processing_tokens": set(),
        }
        ctx = type("Context", (), {
            "application": type("Application", (), {"bot_data": bot_data})(),
        })()
        return ctx, bot_data

    async def test_expired_picker_shows_telegram_alert(self):
        picker = self.FakePicker()
        callback = self.FakeCallback("pick:expired-token:0", picker)
        ctx, _ = self.make_context(None)
        update = type("Update", (), {"callback_query": callback})()

        await on_pick(update, ctx)

        self.assertEqual(callback.answers, [("❗ Время истекло. Выполните новый поиск.", True)])

    async def test_track_pick_sends_original_immediately_with_inline_bass_button(self):
        picker = self.FakePicker()
        callback = self.FakeCallback("pick:live-token:0", picker)
        state = {
            "chat_id": 123,
            "message_id": 456,
            "created": time.time(),
            "results": [
                {"url": "https://www.youtube.com/watch?v=abcdefghijk", "title": "Test song"}
            ],
        }
        ctx, bot_data = self.make_context(state)
        update = type("Update", (), {"callback_query": callback})()

        with patch("main.process", new_callable=unittest.mock.AsyncMock, return_value=True) as process_mock:
            await on_pick(update, ctx)

        self.assertEqual(state["stage"], "sent")
        self.assertEqual(state["selected_index"], 0)
        self.assertEqual(state["selected_preset"], "original")
        process_mock.assert_awaited_once()
        self.assertEqual(process_mock.await_args.kwargs["bass_preset"], "original")
        markup = process_mock.await_args.kwargs["reply_markup"]
        button = markup.inline_keyboard[0][0]
        self.assertEqual(button.text, "🔊 Добавить басс")
        self.assertEqual(button.callback_data, "bassmenu:live-token:0")
        self.assertNotIn("live-token", bot_data["music_processing_tokens"])

    async def test_old_audio_message_keeps_its_own_bass_menu_after_another_version_is_picked(self):
        audio = self.FakePicker(message_id=900)
        audio.audio = object()
        callback = self.FakeCallback("bassmenu:live-token:0", audio)
        state = {
            "chat_id": 123,
            "created": time.time(),
            "stage": "sent",
            "selected_index": 1,
            "results": [
                {"url": "https://www.youtube.com/watch?v=abcdefghijk", "title": "First"},
                {"url": "https://www.youtube.com/watch?v=abcdefghijl", "title": "Second"},
            ],
        }
        ctx, _ = self.make_context(state)
        update = type("Update", (), {"callback_query": callback})()

        await on_bass_menu(update, ctx)

        self.assertEqual(callback.answers, [(None, False)])
        buttons = [button for row in audio.reply_markup.inline_keyboard for button in row]
        self.assertEqual([button.callback_data for button in buttons], [
            "bassapply:live-token:0:low_freq",
            "bassapply:live-token:0:bass",
        ])

    async def test_old_audio_message_can_still_apply_bass_after_another_version_is_picked(self):
        audio = self.FakePicker(message_id=901)
        audio.audio = object()
        callback = self.FakeCallback("bassapply:live-token:0:bass", audio)
        state = {
            "chat_id": 123,
            "created": time.time(),
            "stage": "sent",
            "selected_index": 1,
            "results": [
                {"url": "https://www.youtube.com/watch?v=abcdefghijk", "title": "First"},
                {"url": "https://www.youtube.com/watch?v=abcdefghijl", "title": "Second"},
            ],
        }
        ctx, _ = self.make_context(state)
        update = type("Update", (), {"callback_query": callback})()

        with patch("main.process", new_callable=unittest.mock.AsyncMock, return_value=True) as process_mock:
            await on_bass_pick(update, ctx)

        process_mock.assert_awaited_once()
        self.assertEqual(
            process_mock.await_args.args[2],
            "https://www.youtube.com/watch?v=abcdefghijk",
        )
        self.assertEqual(process_mock.await_args.kwargs["bass_preset"], "bass")
        self.assertEqual(callback.answers, [("Применяю обработку…", False)])

    async def test_out_of_range_audio_button_is_rejected(self):
        audio = self.FakePicker(message_id=902)
        audio.audio = object()
        callback = self.FakeCallback("bassmenu:live-token:9", audio)
        state = {
            "chat_id": 123,
            "created": time.time(),
            "stage": "sent",
            "selected_index": 1,
            "results": [
                {"url": "https://www.youtube.com/watch?v=abcdefghijk", "title": "First"},
                {"url": "https://www.youtube.com/watch?v=abcdefghijl", "title": "Second"},
            ],
        }
        ctx, _ = self.make_context(state)
        update = type("Update", (), {"callback_query": callback})()

        await on_bass_menu(update, ctx)

        self.assertEqual(callback.answers, [
            ("❗ Эта кнопка уже устарела. Выбери песню заново.", True)
        ])

    async def test_duplicate_picker_click_does_not_release_existing_claim(self):
        picker = self.FakePicker()
        callback = self.FakeCallback("pick:live-token:0", picker)
        state = {
            "chat_id": 123,
            "message_id": 456,
            "created": time.time(),
            "results": [{"url": "https://www.youtube.com/watch?v=abcdefghijk", "title": "Test song"}],
        }
        ctx, bot_data = self.make_context(state)
        bot_data["music_processing_tokens"].add("live-token")
        update = type("Update", (), {"callback_query": callback})()

        await on_pick(update, ctx)

        self.assertIn("live-token", bot_data["music_processing_tokens"])
        self.assertEqual(picker.replies, [])


class SearchRelevanceRankingTests(unittest.TestCase):
    def make_result(self, title, video_id):
        return {
            "title": title,
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "duration": 180,
        }

    def test_all_query_words_beat_unrelated_tiktok_only_results(self):
        results = [
            self.make_result("Ke$ha - TiK ToK (Official Video)", "abcdefgh001"),
            self.make_result("TikTok Shop tutorial", "abcdefgh002"),
            self.make_result("All I Need - Clams Casino (TikTok Version)", "abcdefgh003"),
            self.make_result("Clams Casino - All I Need", "abcdefgh004"),
        ]
        ranked = rank_search_results("All I Need TikTok", results)
        self.assertEqual(ranked[0]["url"], results[2]["url"])
        self.assertEqual(ranked[1]["url"], results[3]["url"])
        self.assertNotIn(results[0], ranked[:2])
        self.assertNotIn(results[1], ranked[:2])

    def test_split_tik_tok_query_matches_tiktok_title_without_false_positive(self):
        results = [
            self.make_result("Ke$ha - TiK ToK (Official Video)", "abcdefgh018"),
            self.make_result("All I Need - Clams Casino (TikTok Version)", "abcdefgh019"),
            self.make_result("Clams Casino - All I Need", "abcdefgh020"),
        ]
        ranked = rank_search_results("All I Need tik tok", results)
        self.assertEqual(ranked[0]["url"], results[1]["url"])
        self.assertNotEqual(ranked[0]["url"], results[0]["url"])

    def test_ke_sha_tik_tok_query_keeps_its_own_title_relevant(self):
        results = [
            self.make_result("All I Need - Clams Casino", "abcdefgh005"),
            self.make_result("Ke$ha - TiK ToK", "abcdefgh006"),
        ]
        ranked = rank_search_results("Ke$ha TiK ToK", results)
        self.assertEqual(ranked[0]["url"], results[1]["url"])

    def test_case_punctuation_and_accents_are_normalized(self):
        results = [
            self.make_result("Beyoncé — Halo (Live)", "abcdefgh007"),
            self.make_result("Halo - Beyoncé", "abcdefgh008"),
        ]
        ranked = rank_search_results("BEYONCE halo!", results)
        self.assertEqual(ranked[0]["url"], results[0]["url"])

    def test_exact_duplicate_video_ids_are_removed(self):
        results = [
            self.make_result("All I Need", "abcdefgh009"),
            self.make_result("All I Need (duplicate metadata)", "abcdefgh009"),
            self.make_result("All I Need (Slowed)", "abcdefgh010"),
        ]
        ranked = rank_search_results("All I Need", results)
        self.assertEqual(len(ranked), 2)

    def test_near_duplicate_versions_do_not_fill_first_positions(self):
        results = [
            self.make_result("All I Need Slowed", "abcdefgh011"),
            self.make_result("All I Need Slowed + Reverb", "abcdefgh012"),
            self.make_result("All I Need Live Acoustic", "abcdefgh013"),
        ]
        ranked = rank_search_results("All I Need", results)
        self.assertEqual(ranked[0]["url"], results[0]["url"])
        self.assertIn(results[2], ranked[:2])

    def test_short_query_does_not_aggressively_filter(self):
        results = [
            self.make_result("Stay", "abcdefgh014"),
            self.make_result("Stay With Me", "abcdefgh015"),
        ]
        self.assertEqual(len(rank_search_results("stay", results)), 2)

    def test_empty_query_and_url_bypass_ranking(self):
        results = [self.make_result("Other", "abcdefgh016")]
        self.assertEqual(rank_search_results("", results), results)
        url = "https://www.youtube.com/watch?v=abcdefgh016"
        self.assertEqual(rank_search_results(url, results), results)

    def test_empty_title_does_not_crash(self):
        results = [{"title": "", "url": "https://youtu.be/abcdefgh017"}]
        self.assertEqual(rank_search_results("some song", results), [])


class StartChannelInviteTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_sends_greeting_then_one_channel_invite(self):
        from main import cmd_start

        message = type("Message", (), {
            "reply_text": unittest.mock.AsyncMock(
                side_effect=[
                    type("SentMessage", (), {"message_id": 11})(),
                    type("SentMessage", (), {"message_id": 12})(),
                ]
            )
        })()
        update = type("Update", (), {
            "effective_message": message,
            "effective_chat": type("Chat", (), {"id": 123})(),
        })()
        job_queue = type("JobQueue", (), {
            "run_once": unittest.mock.Mock()
        })()
        ctx = type("Context", (), {
            "bot": type("Bot", (), {"username": "music_test_bot"})(),
            "job_queue": job_queue,
        })()

        await cmd_start(update, ctx)
        job_queue.run_once.assert_called_once()
        self.assertEqual(job_queue.run_once.call_args.kwargs["when"], 30)
        self.assertEqual(
            job_queue.run_once.call_args.kwargs["data"],
            {"chat_id": 123, "message_ids": [11, 12]},
        )

        self.assertEqual(message.reply_text.await_count, 2)
        greeting_call, invite_call = message.reply_text.await_args_list

        greeting_args, greeting_kwargs = greeting_call
        self.assertIn("Привет!", greeting_args[0])
        self.assertIn("название песни", greeting_args[0])
        self.assertNotIn("reply_markup", greeting_kwargs)

        invite_args, invite_kwargs = invite_call
        self.assertIn("Помоги нашему проекту расти", invite_args[0])
        markup = invite_kwargs["reply_markup"]
        buttons = [button for row in markup.inline_keyboard for button in row]
        self.assertEqual(len(buttons), 1)
        self.assertEqual(buttons[0].text, "📢 Подписаться на канал")
        self.assertEqual(buttons[0].url, "https://t.me/LunaAiroll")
        self.assertIsNone(buttons[0].callback_data)


class AudioSendLimitsTests(unittest.IsolatedAsyncioTestCase):
    async def test_cached_file_id_send_records_api_and_total_duration(self):
        class FakeBot:
            async def send_audio(self, **kwargs):
                await asyncio.sleep(0.001)
                return "sent"

        metrics = {}
        with patch("main.log.info") as log_info:
            result = await _send_audio_limited(
                FakeBot(), metrics=metrics, chat_id=123, audio="cached-file-id"
            )

        self.assertEqual(result, "sent")
        samples = metrics["_runtime_metrics"]
        self.assertEqual(len(samples["telegram_send_api_seconds"]), 1)
        self.assertEqual(len(samples["telegram_send_total_seconds"]), 1)
        self.assertGreaterEqual(
            samples["telegram_send_total_seconds"][0],
            samples["telegram_send_api_seconds"][0],
        )
        self.assertGreaterEqual(samples["send_wait_seconds"][0], 0.0)
        self.assertEqual(log_info.call_args.args[1], "file_id")

    async def test_fresh_upload_send_records_upload_size_and_duration(self):
        class FakeAudio:
            def __init__(self, name):
                self.name = name

        class FakeBot:
            async def send_audio(self, **kwargs):
                await asyncio.sleep(0.001)
                return "sent"

        with tempfile.TemporaryDirectory() as tmp:
            file_path = Path(tmp) / "song.mp3"
            file_path.write_bytes(b"x" * 2048)
            metrics = {}
            with patch("main.log.info") as log_info:
                result = await _send_audio_limited(
                    FakeBot(), metrics=metrics, chat_id=123,
                    audio=FakeAudio(str(file_path)),
                )

        self.assertEqual(result, "sent")
        self.assertEqual(log_info.call_args.args[1], "upload")
        self.assertAlmostEqual(log_info.call_args.args[4], 2048 / (1024 * 1024), places=5)
        self.assertEqual(len(metrics["_runtime_metrics"]["telegram_send_total_seconds"]), 1)

    async def test_audio_sends_share_a_concurrency_limit(self):
        active = 0
        peak = 0

        class FakeBot:
            async def send_audio(self, **kwargs):
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0.005)
                active -= 1
                return "sent"

        with patch("main.SEND_LOCK", asyncio.Semaphore(2)):
            results = await asyncio.gather(*[
                _send_audio_limited(FakeBot(), chat_id=index, audio=f"file-{index}")
                for index in range(10)
            ])

        self.assertEqual(results, ["sent"] * 10)
        self.assertLessEqual(peak, 2)

    async def test_retry_after_rewinds_upload_file_and_retries_once(self):
        class FakeAudio:
            def __init__(self):
                self.positions = []

            def seek(self, position):
                self.positions.append(position)

        class FakeBot:
            def __init__(self):
                self.calls = 0

            async def send_audio(self, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise RetryAfter(0.01)
                return "sent"

        audio = FakeAudio()
        bot = FakeBot()
        metrics = {}
        with patch("main.asyncio.sleep", new_callable=unittest.mock.AsyncMock) as sleep:
            result = await _send_audio_limited(
                bot, metrics=metrics, chat_id=123, audio=audio
            )

        self.assertEqual(result, "sent")
        self.assertEqual(bot.calls, 2)
        self.assertEqual(audio.positions, [0])
        self.assertEqual(metrics["_runtime_metrics"]["telegram_retry_after"], 1)
        sleep.assert_awaited_once_with(0.51)


class BoundedUpdateQueueTests(unittest.TestCase):
    def test_application_uses_bounded_update_queue_and_explicit_concurrency(self):
        app = (
            Application.builder()
            .token("123456:TEST_TOKEN")
            .update_queue(asyncio.Queue(maxsize=MAX_PENDING_UPDATES))
            .concurrent_updates(MAX_CONCURRENT_UPDATES)
            .build()
        )

        self.assertEqual(app.update_queue.maxsize, 128)
        self.assertEqual(app.update_processor.max_concurrent_updates, 32)


    async def test_retry_after_accepts_timedelta_values(self):
        class FakeBot:
            def __init__(self):
                self.calls = 0

            async def send_audio(self, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise RetryAfter(timedelta(seconds=0.01))
                return "sent"

        bot = FakeBot()
        metrics = {}
        with patch("main.asyncio.sleep", new_callable=unittest.mock.AsyncMock) as sleep:
            result = await _send_audio_limited(
                bot, metrics=metrics, chat_id=123, audio="cached-file-id"
            )

        self.assertEqual(result, "sent")
        self.assertEqual(bot.calls, 2)
        self.assertEqual(metrics["_runtime_metrics"]["telegram_retry_after"], 1)
        sleep.assert_awaited_once_with(0.51)


class RuntimeMetricsTests(unittest.IsolatedAsyncioTestCase):
    def test_metric_samples_are_bounded_and_percentiles_are_stable(self):
        bot_data = {}
        _metric_increment(bot_data, "requests")
        _metric_increment(bot_data, "requests", 2)
        for value in range(250):
            _metric_observe(bot_data, "latency", value)

        metrics = bot_data["_runtime_metrics"]
        self.assertEqual(metrics["requests"], 3)
        self.assertEqual(len(metrics["latency"]), 200)
        self.assertEqual(metrics["latency"][0], 50.0)
        self.assertEqual(_percentile([0.1, 0.2, 0.3, 0.4], 0.95), 0.4)
        self.assertEqual(_percentile([], 0.95), 0.0)

    async def test_runtime_decorator_tracks_active_count_and_latency(self):
        @_track_runtime_metric("probe")
        async def operation(update, ctx):
            await asyncio.sleep(0)
            return "ok"

        ctx = SimpleNamespace(application=SimpleNamespace(bot_data={}))
        result = await operation(None, ctx)

        metrics = ctx.application.bot_data["_runtime_metrics"]
        self.assertEqual(result, "ok")
        self.assertEqual(metrics["probe_active"], 0)
        self.assertEqual(len(metrics["probe_latency_seconds"]), 1)
        self.assertGreaterEqual(metrics["probe_latency_seconds"][0], 0.0)

    async def test_runtime_metrics_log_queue_cache_latency_and_resources(self):
        metrics = {
            "search_active": 2,
            "process_active": 1,
            "video_active": 0,
            "picker_active": 1,
            "search_cache_hits": 3,
            "search_cache_misses": 1,
            "search_wait_seconds": [0.01, 0.02, 0.03],
            "send_wait_seconds": [0.01, 0.02, 0.03],
            "telegram_send_total_seconds": [0.2, 0.3, 0.4],
            "telegram_send_api_seconds": [0.1, 0.2, 0.3],
            "search_latency_seconds": [0.1, 0.2, 0.3],
            "process_latency_seconds": [0.4, 0.5],
            "video_latency_seconds": [],
        }
        queue = SimpleNamespace(qsize=lambda: 7, maxsize=128)
        app = SimpleNamespace(
            bot_data={"_runtime_metrics": metrics},
            update_queue=queue,
        )
        ctx = SimpleNamespace(application=app)
        usage = SimpleNamespace(ru_maxrss=1024, ru_utime=1.5, ru_stime=0.5)

        with patch("main.CACHE", None), patch(
            "main.resource.getrusage", return_value=usage
        ), patch(
            "main.shutil.disk_usage",
            return_value=SimpleNamespace(free=2 * 1024 ** 3),
        ), patch("main.log.info") as log_info:
            await log_runtime_metrics(ctx)

        log_info.assert_called_once()
        args = log_info.call_args.args
        message = args[0]
        self.assertIn("RUNTIME_METRICS queue=%d/%d", message)
        self.assertEqual(args[1:3], (7, 128))
        self.assertIn("cache_hit(search=%.1f%%", message)
        self.assertIn("send_p95_p99_ms(total=%.1f/%.1f api=%.1f/%.1f)", message)
        self.assertEqual(args[7], 75.0)
        self.assertIn("rss_peak_mb=%.1f", message)


class DownloadWithoutIntermediateTranscodeTests(unittest.TestCase):
    def test_ytdlp_does_not_configure_audio_extraction_postprocessor(self):
        with tempfile.TemporaryDirectory() as directory:
            options = _ydl_options(Path(directory), False, ["web"])

        self.assertNotIn("postprocessors", options)
        self.assertNotIn("postprocessor_args", options)
        self.assertEqual(options["format"], "bestaudio/best")

    def test_download_returns_original_audio_extension_for_final_encoder(self):
        with tempfile.TemporaryDirectory() as directory:
            workdir = Path(directory)

            class FakeYoutubeDL:
                def __init__(self, options):
                    self.options = options

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    return False

                def extract_info(self, target, download=True):
                    output = self.options["outtmpl"].replace(
                        "%(id)s.%(ext)s", "abcdefghijk.webm"
                    )
                    Path(output).write_bytes(b"source-webm-audio")
                    return {
                        "id": "abcdefghijk",
                        "title": "Test track",
                        "ext": "webm",
                    }

            with patch("main.yt_dlp.YoutubeDL", FakeYoutubeDL):
                info, source = _download(
                    "https://www.youtube.com/watch?v=abcdefghijk",
                    True, workdir, False, ["web"],
                )

            self.assertEqual(info["id"], "abcdefghijk")
            self.assertIsNotNone(source)
            self.assertEqual(source.suffix, ".webm")
            self.assertEqual(source.read_bytes(), b"source-webm-audio")


class BassProcessingTests(unittest.TestCase):
    def test_two_visible_presets_keep_their_previous_audio_gains(self):
        self.assertIsNone(bass_filter_for_preset("original"))
        self.assertIn("bass=g=12.0:f=90", bass_filter_for_preset("bass"))
        self.assertIn("bass=g=5.0:f=90", bass_filter_for_preset("low_freq"))
        self.assertIn("alimiter=limit=0.95", bass_filter_for_preset("bass"))

    def test_removed_presets_are_rejected(self):
        for preset in ("boost", "extreme", "maximum++"):
            with self.subTest(preset=preset), self.assertRaises(ValueError):
                bass_filter_for_preset(preset)

    def test_bass_keyboard_contains_only_the_two_requested_presets(self):
        markup = build_bass_keyboard("0123456789abcdef", 9)
        buttons = [button for row in markup.inline_keyboard for button in row]
        self.assertEqual(len(buttons), 2)
        self.assertEqual([button.text for button in buttons], [
            "🔊 Усилить низкие частоты", "🔥 Bass"
        ])
        self.assertEqual(
            [button.callback_data.rsplit(":", 1)[-1] for button in buttons],
            ["low_freq", "bass"],
        )
        self.assertTrue(all(len(button.callback_data) <= 64 for button in buttons))

    def test_audio_keyboard_has_inline_add_bass_button(self):
        from main import build_audio_keyboard
        buttons = [button for row in build_audio_keyboard("0123456789abcdef", 9).inline_keyboard for button in row]
        self.assertEqual(len(buttons), 1)
        self.assertEqual(buttons[0].text, "🔊 Добавить басс")
        self.assertEqual(buttons[0].callback_data, "bassmenu:0123456789abcdef:9")

    def test_standard_audio_uses_same_320k_encoder_without_bass_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.mp3"
            target = Path(directory) / "processed.mp3"
            source.write_bytes(b"source-audio")

            def fake_ffmpeg(command, **kwargs):
                Path(command[-1]).write_bytes(b"encoded-audio")
                return SimpleNamespace(returncode=0, stderr="")

            with patch("main.subprocess.run", side_effect=fake_ffmpeg) as run:
                _encode_audio(source, target, "original")

            command = run.call_args.args[0]
            self.assertEqual(command[0], "ffmpeg")
            self.assertNotIn("-af", command)
            self.assertEqual(command[command.index("-b:a") + 1], "320k")
            self.assertTrue(target.is_file())
            self.assertGreater(target.stat().st_size, 0)
            self.assertEqual(run.call_args.kwargs["timeout"], 180)

    def test_apply_bass_boost_invokes_ffmpeg_and_validates_output(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.mp3"
            target = Path(directory) / "processed.mp3"
            source.write_bytes(b"source-audio")

            def fake_ffmpeg(command, **kwargs):
                Path(command[-1]).write_bytes(b"processed-audio")
                return SimpleNamespace(returncode=0, stderr="")

            with patch("main.subprocess.run", side_effect=fake_ffmpeg) as run:
                _apply_bass_boost(source, target, "low_freq")

            command = run.call_args.args[0]
            self.assertEqual(command[0], "ffmpeg")
            self.assertIn("-af", command)
            self.assertIn("bass=g=5.0:f=90", command[command.index("-af") + 1])
            self.assertEqual(command[command.index("-b:a") + 1], "320k")
            self.assertTrue(target.is_file())
            self.assertGreater(target.stat().st_size, 0)
            self.assertEqual(run.call_args.kwargs["timeout"], 180)

    def test_apply_bass_boost_rejects_missing_ffmpeg_output(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.mp3"
            target = Path(directory) / "processed.mp3"
            source.write_bytes(b"source-audio")
            with patch(
                "main.subprocess.run",
                return_value=SimpleNamespace(returncode=0, stderr=""),
            ):
                with self.assertRaises(RuntimeError):
                    _apply_bass_boost(source, target, "bass")