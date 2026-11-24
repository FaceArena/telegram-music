import asyncio
import base64
import copy
import logging
import math
import os
import random
import resource
import re
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
import secrets
from pathlib import Path
from functools import wraps
from urllib.parse import urlsplit


from dotenv import load_dotenv

load_dotenv()

import yt_dlp
import httpx
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.constants import ChatAction
from telegram.error import BadRequest, RetryAfter
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from yt_dlp.utils import match_filter_func

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO
)
log = logging.getLogger("musicbot")
logging.getLogger("httpx").setLevel(logging.WARNING)

def _env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else default
    except (TypeError, ValueError):
        log.warning("Некорректное значение %s=%r; использую %s", name, raw, default)
        return default
    if minimum is not None and value < minimum:
        log.warning("%s=%s меньше допустимого минимума %s; использую %s", name, value, minimum, default)
        return default
    return value


def _env_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    try:
        value = float(raw) if raw is not None else default
    except (TypeError, ValueError):
        log.warning("Некорректное значение %s=%r; использую %s", name, raw, default)
        return default
    if not math.isfinite(value) or (minimum is not None and value < minimum):
        log.warning("Недопустимое значение %s=%r; использую %s", name, raw, default)
        return default
    return value


def _metric_increment(bot_data: dict | None, key: str, amount: int = 1) -> None:
    if not isinstance(bot_data, dict):
        return
    metrics = bot_data.setdefault("_runtime_metrics", {})
    metrics[key] = int(metrics.get(key, 0)) + amount


def _metric_observe(bot_data: dict | None, key: str, value: float, limit: int = 200) -> None:
    if not isinstance(bot_data, dict):
        return
    metrics = bot_data.setdefault("_runtime_metrics", {})
    samples = metrics.setdefault(key, [])
    samples.append(float(value))
    if len(samples) > limit:
        del samples[:-limit]


def _percentile(samples: list[float], percentile: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = min(len(ordered) - 1, max(0, math.ceil((len(ordered) - 1) * percentile)))
    return ordered[index]


def _track_runtime_metric(name: str):
    def decorator(func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            ctx = args[1] if len(args) > 1 else kwargs.get("ctx")
            application = getattr(ctx, "application", None)
            bot_data = getattr(application, "bot_data", None)
            if not isinstance(bot_data, dict):
                return await func(*args, **kwargs)

            metrics = bot_data.setdefault("_runtime_metrics", {})
            active_key = name + "_active"
            metrics[active_key] = metrics.get(active_key, 0) + 1
            started = time.monotonic()
            try:
                return await func(*args, **kwargs)
            finally:
                metrics[active_key] = max(0, metrics.get(active_key, 1) - 1)
                _metric_observe(
                    bot_data, name + "_latency_seconds", time.monotonic() - started
                )

        return wrapped
    return decorator


async def log_runtime_metrics(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Emit a bounded rolling snapshot; no external metrics service is required."""
    app = ctx.application
    bot_data = app.bot_data
    metrics = bot_data.get("_runtime_metrics", {})
    update_queue = app.update_queue
    search_hits = metrics.get("search_cache_hits", 0)
    search_misses = metrics.get("search_cache_misses", 0)
    track_hits = metrics.get("track_cache_hits", 0)
    track_misses = metrics.get("track_cache_misses", 0)
    search_total = search_hits + search_misses
    track_total = track_hits + track_misses
    search_hit_rate = 100.0 * search_hits / search_total if search_total else 0.0
    track_hit_rate = 100.0 * track_hits / track_total if track_total else 0.0

    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        rss_peak_mb = usage.ru_maxrss / 1024
        cpu_seconds = usage.ru_utime + usage.ru_stime
    except Exception:
        rss_peak_mb = 0.0
        cpu_seconds = 0.0
    try:
        disk_path = CACHE.path.parent if CACHE else Path(tempfile.gettempdir())
        disk_free_gb = shutil.disk_usage(disk_path).free / (1024 ** 3)
    except Exception:
        disk_free_gb = 0.0

    def percentile_ms(key, percentile):
        return _percentile(metrics.get(key, []), percentile) * 1000

    log.info(
        "RUNTIME_METRICS queue=%d/%d active(search=%d process=%d video=%d picker=%d) "
        "cache_hit(search=%.1f%% track=%.1f%%) "
        "wait_p95_ms(search=%.1f download=%.1f send=%.1f) "
        "latency_p95_p99_ms(search=%.1f/%.1f process=%.1f/%.1f video=%.1f/%.1f) "
        "errors(youtube_search=%d youtube_download=%d audd=%d ffmpeg=%d telegram_send=%d telegram_api=%d retry_after=%d) "
        "cpu_s=%.1f rss_peak_mb=%.1f disk_free_gb=%.2f",
        update_queue.qsize(), update_queue.maxsize,
        metrics.get("search_active", 0), metrics.get("process_active", 0),
        metrics.get("video_active", 0), metrics.get("picker_active", 0),
        search_hit_rate, track_hit_rate,
        percentile_ms("search_wait_seconds", 0.95),
        percentile_ms("download_wait_seconds", 0.95),
        percentile_ms("send_wait_seconds", 0.95),
        percentile_ms("search_latency_seconds", 0.95),
        percentile_ms("search_latency_seconds", 0.99),
        percentile_ms("process_latency_seconds", 0.95),
        percentile_ms("process_latency_seconds", 0.99),
        percentile_ms("video_latency_seconds", 0.95),
        percentile_ms("video_latency_seconds", 0.99),
        metrics.get("youtube_search_errors", 0),
        metrics.get("youtube_download_errors", 0),
        metrics.get("audd_errors", 0), metrics.get("ffmpeg_errors", 0),
        metrics.get("telegram_send_errors", 0), metrics.get("telegram_api_errors", 0),
        metrics.get("telegram_retry_after", 0),
        cpu_seconds, rss_peak_mb, disk_free_gb,
    )


BOT_TOKEN = os.environ["BOT_TOKEN"]
MAX_DURATION = _env_int("MAX_DURATION", 1200, minimum=1)
# Small configurable cooldown; no multi-second artificial wait by default.
MIN_DELAY = _env_float("MIN_DELAY", 0.0, minimum=0.0)
MAX_DELAY = max(MIN_DELAY, _env_float("MAX_DELAY", 0.0, minimum=0.0))
AUDIO_QUALITY = os.getenv("AUDIO_QUALITY", "320")
GROUP_REACT_ALL = os.getenv("GROUP_REACT_ALL", "0") == "1"
POT_PROVIDER_URL = os.getenv("POT_PROVIDER_URL", "").strip()
UPDATE_EVERY_HOURS = _env_int("UPDATE_EVERY_HOURS", 12, minimum=1)
# Предзагрузка данных о первых N результатах поиска: пока пользователь выбирает трек,
# проходит обязательное ожидание YouTube (available_at), и при скачивании yt-dlp не спит.
PREFETCH_TOP = max(0, _env_int("PREFETCH_TOP", 1))
PREFETCH_TTL = 20 * 60
PREFETCH_CACHE: dict = {}
MWEB_FIRST = os.getenv("MWEB_FIRST", "1") == "1"
# mp3 (по умолчанию) требует перекодирования в ffmpeg — это самый долгий этап на длинных треках.
# AUDIO_FORMAT=m4a отдаёт AAC без перекодирования (почти мгновенно); Telegram его тоже проигрывает.
AUDIO_FORMAT = os.getenv("AUDIO_FORMAT", "mp3").strip().lower()
if AUDIO_FORMAT not in ("mp3", "m4a"):
    AUDIO_FORMAT = "mp3"
TG_LIMIT = 49 * 1024 * 1024
ERROR_NOTIFICATION_TTL = 10
START_MESSAGE_TTL = 30
AUDD_API_TOKEN = os.getenv("AUDD_API_TOKEN", "").strip()
AUDD_API_URL = "https://api.audd.io/"
MAX_VIDEO_RECOGNITION_SIZE = 20 * 1024 * 1024

# web требует решения JS-challenge (медленно) — оставлен только как запасной вариант
CLIENTS = [c.strip() for c in os.getenv("YT_CLIENTS", "tv,web_embedded").split(",") if c.strip()]
URL_RE = re.compile(r"https?://\S+")

MAX_PARALLEL_DL = max(1, _env_int("MAX_PARALLEL_DL", 3, minimum=1))
DL_LOCK = asyncio.Semaphore(MAX_PARALLEL_DL)
MAX_PARALLEL_SEARCH = 3
SEARCH_SEMAPHORE = asyncio.Semaphore(MAX_PARALLEL_SEARCH)
MAX_PARALLEL_SEND = _env_int("MAX_PARALLEL_SEND", 5, minimum=1)
SEND_LOCK = asyncio.Semaphore(MAX_PARALLEL_SEND)
MAX_PARALLEL_VIDEO = _env_int("MAX_PARALLEL_VIDEO", 2, minimum=1)
VIDEO_LOCK = asyncio.Semaphore(MAX_PARALLEL_VIDEO)
MAX_PARALLEL_BASS = _env_int("MAX_PARALLEL_BASS", 2, minimum=1)
BASS_LOCK = asyncio.Semaphore(MAX_PARALLEL_BASS)
BASS_PRESETS = {"original": None, "bass": 12.0, "low_freq": 5.0}
MAX_CONCURRENT_UPDATES = _env_int("MAX_CONCURRENT_UPDATES", 32, minimum=1)
MAX_PENDING_UPDATES = _env_int("MAX_PENDING_UPDATES", 128, minimum=1)
_last_finish = 0.0


def load_cookies() -> Path | None:
    dest = Path(tempfile.gettempdir()) / "yt_cookies_src.txt"
    b64 = os.getenv("YOUTUBE_COOKIES_B64", "").strip()
    path = os.getenv("YOUTUBE_COOKIES_FILE", "").strip()
    try:
        if b64:
            dest.write_bytes(base64.b64decode(b64))
            dest.chmod(0o600)
            log.info("Cookies загружены из YOUTUBE_COOKIES_B64")
            return dest
        if path and Path(path).is_file():
            shutil.copy(path, dest)
            dest.chmod(0o600)
            log.info("Cookies загружены из файла %s", path)
            return dest
    except Exception as e:
        log.warning("Не удалось прочитать cookies: %s", e)
    log.info("Cookies не заданы — работаю без них")
    return None


COOKIES_SRC = load_cookies()


# ───────────────────────── Кэш ─────────────────────────
# 1) file_id Telegram по id видео: повторный запрос отправляется мгновенно, без YouTube и ffmpeg.
# 2) результаты поиска по тексту запроса на SEARCH_CACHE_TTL секунд.
# БД лежит в CACHE_DIR (по умолчанию /data, если есть том; иначе временная папка — тогда
# кэш сбрасывается при деплое).
SEARCH_CACHE_TTL = _env_int("SEARCH_CACHE_TTL", 6 * 3600, minimum=0)
VIDEO_ID_RE = re.compile(r"(?:v=|youtu\.be/|/shorts/|/embed/|/live/)([\w-]{11})")


def _cache_path() -> Path:
    for cand in (os.getenv("CACHE_DIR", "").strip(), "/data", tempfile.gettempdir()):
        if cand and Path(cand).is_dir() and os.access(cand, os.W_OK):
            return Path(cand) / "musicbot_cache.db"
    return Path(tempfile.gettempdir()) / "musicbot_cache.db"


class Cache:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS tracks (key TEXT PRIMARY KEY, file_id TEXT NOT NULL, "
            "title TEXT, performer TEXT, duration INTEGER, created REAL)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS searches (q TEXT PRIMARY KEY, results TEXT NOT NULL, created REAL)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS downloads ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, video_id TEXT NOT NULL, "
            "title TEXT, performer TEXT, downloaded_at REAL NOT NULL, user_id INTEGER)"
        )
        # Безопасная миграция существующей БД: старые события останутся в таблице,
        # но не попадут в новый рейтинг, поскольку Telegram user_id для них неизвестен.
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(downloads)")}
        if "user_id" not in columns:
            self.db.execute("ALTER TABLE downloads ADD COLUMN user_id INTEGER")
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_downloads_time_video "
            "ON downloads(downloaded_at, video_id)"
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_downloads_user_time "
            "ON downloads(user_id, downloaded_at, id)"
        )
        self.db.commit()

    @staticmethod
    def track_key(video_id: str) -> str:
        # Versioned key invalidates Telegram file_ids created before the unified
        # final audio encoding pipeline. Old rows remain unused; new requests re-download.
        return f"{video_id}:{AUDIO_FORMAT}:q320-v3"

    def get_track(self, video_id: str):
        with self.lock:
            return self.db.execute(
                "SELECT file_id, title, performer, duration FROM tracks WHERE key=?",
                (self.track_key(video_id),),
            ).fetchone()

    def put_track(self, video_id: str, file_id: str, title, performer, duration):
        with self.lock:
            self.db.execute(
                "INSERT OR REPLACE INTO tracks VALUES (?,?,?,?,?,?)",
                (self.track_key(video_id), file_id, title, performer, duration, time.time()),
            )
            self.db.commit()

    def drop_track(self, video_id: str):
        with self.lock:
            self.db.execute("DELETE FROM tracks WHERE key=?", (self.track_key(video_id),))
            self.db.commit()

    def record_download(self, video_id: str, title, performer, duration=None, user_id=None):
        # Без Telegram user_id нельзя применить правило «один пользователь — одна песня».
        if not video_id or user_id is None:
            return
        with self.lock:
            now = time.time()
            self.db.execute(
                "INSERT INTO downloads(video_id, title, performer, downloaded_at, user_id) "
                "VALUES (?,?,?,?,?)",
                (video_id, title, performer, now, int(user_id)),
            )
            # Рейтинг использует скользящее окно последних 7 дней.
            self.db.execute("DELETE FROM downloads WHERE downloaded_at < ?", (now - 7 * 86400,))
            self.db.commit()

    def top_downloads(self, days: int = 7, limit: int = 10):
        since = time.time() - days * 24 * 60 * 60
        with self.lock:
            return self.db.execute(
                "WITH ranked_events AS ("
                " SELECT video_id, title, performer, user_id, downloaded_at, id,"
                " ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY downloaded_at ASC, id ASC) AS rn"
                " FROM downloads WHERE downloaded_at >= ? AND user_id IS NOT NULL"
                "), first_votes AS ("
                " SELECT video_id, title, performer, downloaded_at FROM ranked_events WHERE rn = 1"
                ") "
                "SELECT video_id, MAX(title), MAX(performer), COUNT(*) AS total "
                "FROM first_votes GROUP BY video_id "
                "ORDER BY total DESC, MAX(downloaded_at) DESC, video_id ASC LIMIT ?",
                (since, limit),
            ).fetchall()

    def get_search(self, q: str):
        with self.lock:
            row = self.db.execute("SELECT results, created FROM searches WHERE q=?", (q,)).fetchone()
        if not row or time.time() - row[1] >= SEARCH_CACHE_TTL:
            return None
        try:
            results = json.loads(row[0])
            if not isinstance(results, list) or len(results) > 10 or any(
                not isinstance(item, dict)
                or not isinstance(item.get("url"), str)
                or not item["url"].startswith(("https://", "http://"))
                or not isinstance(item.get("title"), str)
                or (
                    item.get("duration") is not None
                    and (
                        isinstance(item.get("duration"), bool)
                        or not isinstance(item.get("duration"), (int, float))
                        or (
                            isinstance(item["duration"], float)
                            and not math.isfinite(item["duration"])
                        )
                        or item["duration"] < 0
                        or item["duration"] >= MAX_DURATION
                    )
                )
                for item in results
            ):
                raise ValueError("invalid search cache structure")
            return results
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            log.warning("Повреждён кэш поиска для «%s», выполняю новый поиск: %s", q, exc)
            # Delete only the exact stale/corrupt version; do not erase a newer write.
            with self.lock:
                self.db.execute(
                    "DELETE FROM searches WHERE q=? AND created=?", (q, row[1])
                )
                self.db.commit()
            return None

    def put_search(self, q: str, results: list):
        with self.lock:
            self.db.execute(
                "INSERT OR REPLACE INTO searches VALUES (?,?,?)", (q, json.dumps(results), time.time())
            )
            self.db.execute("DELETE FROM searches WHERE created < ?", (time.time() - SEARCH_CACHE_TTL,))
            self.db.commit()


try:
    CACHE = Cache(_cache_path())
    log.info("Кэш: %s", CACHE.path)
except Exception as _e:  # кэш — оптимизация, бот должен работать и без него
    CACHE = None
    log.warning("Кэш недоступен: %s", _e)


def is_youtube_url(url: str) -> bool:
    """Accept only HTTP(S) URLs whose actual hostname belongs to YouTube."""
    try:
        parsed = urlsplit(url)
        if parsed.scheme.lower() not in ("http", "https"):
            return False
        if parsed.username is not None or parsed.password is not None:
            return False
        hostname = (parsed.hostname or "").rstrip(".").lower()
        if not (hostname == "youtu.be" or hostname == "youtube.com" or hostname.endswith(".youtube.com")):
            return False
        if parsed.port not in (None, 80, 443):
            return False
        return True
    except ValueError:
        return False


def _is_invalid_file_id_error(exc: Exception) -> bool:
    """Only discard a cached Telegram file_id for an explicit identifier error."""
    if not isinstance(exc, BadRequest):
        return False
    message = str(exc).lower()
    return "file identifier" in message or "wrong file id" in message


def _safe_cache_call(cache, method: str, *args, default=None, **kwargs):
    """A runtime cache failure must not take down search, delivery, or ranking UX."""
    if cache is None:
        return default
    try:
        return getattr(cache, method)(*args, **kwargs)
    except Exception as exc:
        log.warning("Операция кэша %s не удалась: %s", method, exc)
        return default


def video_id_of(url: str):
    m = VIDEO_ID_RE.search(url or "")
    return m.group(1) if m else None


def is_duration_over_limit(duration, max_duration: int) -> bool:
    """The yt-dlp filter is strict (< max_duration), so equality is rejected too."""
    return duration is not None and duration >= max_duration


def clean_metadata(value, max_length: int = 160):
    """Flatten untrusted external metadata before displaying or sending it."""
    if value is None:
        return None
    text = str(value).replace("\r", " ").replace("\n", " ").replace("\t", " ")
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Cc")
    text = " ".join(text.split())
    return text[:max_length] or None


def norm_query(q: str) -> str:
    return " ".join(q.lower().split())[:200]


_SEARCH_TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)


def _search_tokens(value: str) -> list[str]:
    """Normalize title/query words consistently, including accents and punctuation."""
    normalized = unicodedata.normalize("NFKD", str(value or "").casefold())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    raw_tokens = _SEARCH_TOKEN_RE.findall(normalized)
    # Treat the common split spelling "tik tok" like the single word "tiktok".
    tokens = []
    index = 0
    while index < len(raw_tokens):
        if raw_tokens[index:index + 2] == ["tik", "tok"]:
            tokens.append("tiktok")
            index += 2
        else:
            tokens.append(raw_tokens[index])
            index += 1
    return tokens


def _query_match_score(query: str, title: str) -> float:
    """Score how much of a query is represented in a candidate title (0..1)."""
    query_tokens = _search_tokens(query)
    title_tokens = _search_tokens(title)
    if not query_tokens or not title_tokens:
        return 0.0

    title_set = set(title_tokens)
    coverage = sum(token in title_set for token in query_tokens) / len(query_tokens)

    # Longest common subsequence rewards candidates that preserve query word order.
    previous = [0] * (len(title_tokens) + 1)
    for query_token in query_tokens:
        current = [0]
        for index, title_token in enumerate(title_tokens, 1):
            current.append(
                previous[index - 1] + 1
                if query_token == title_token
                else max(previous[index], current[-1])
            )
        previous = current
    ordered_coverage = previous[-1] / len(query_tokens)

    # A contiguous phrase match is useful, but should not outweigh full token coverage.
    phrase = " ".join(query_tokens)
    normalized_title = " ".join(title_tokens)
    phrase_bonus = 0.12 if phrase and phrase in normalized_title else 0.0
    return min(1.0, coverage * 0.72 + ordered_coverage * 0.28 + phrase_bonus)


def rank_search_results(query: str, results: list[dict]) -> list[dict]:
    """Reorder yt-dlp candidates by query relevance, then diversify close variants."""
    if not isinstance(query, str) or not query.strip() or URL_RE.fullmatch(query.strip()):
        return list(results or [])

    query_tokens = _search_tokens(query)
    if not query_tokens:
        return list(results or [])

    candidates = []
    seen_video_ids = set()
    for original_index, item in enumerate(results or []):
        if not isinstance(item, dict):
            continue
        title = clean_metadata(item.get("title")) or ""
        url = item.get("url") or ""
        video_id = video_id_of(url)
        if video_id:
            if video_id in seen_video_ids:
                continue
            seen_video_ids.add(video_id)

        tokens = set(_search_tokens(title))
        if not tokens:
            continue
        score = _query_match_score(query, title)
        # For multi-word queries, a result matching only a small fragment is noise.
        if len(query_tokens) >= 3 and score < 0.34:
            continue
        candidates.append({
            "item": item,
            "index": original_index,
            "score": score,
            "tokens": tokens,
        })

    # Greedy maximal-marginal-relevance ordering: relevance remains dominant,
    # while near-identical edits are spaced apart instead of filling the whole list.
    ranked = []
    remaining = candidates[:]
    while remaining and len(ranked) < 10:
        best = None
        best_adjusted = float("-inf")
        for candidate in remaining:
            similarity = 0.0
            if ranked and candidate["tokens"]:
                similarity = max(
                    len(candidate["tokens"] & chosen["tokens"])
                    / max(1, len(candidate["tokens"] | chosen["tokens"]))
                    for chosen in ranked
                )
            adjusted = candidate["score"] - similarity * 0.16
            # Stable original-order tie-break keeps results predictable.
            adjusted -= candidate["index"] * 1e-9
            if adjusted > best_adjusted:
                best_adjusted = adjusted
                best = candidate
        ranked.append(best)
        remaining.remove(best)

    return [candidate["item"] for candidate in ranked]


def build_strategies():
    # Keep a small, purposeful fallback chain. Trying every client separately
    # can multiply one failed download into several long network timeouts.
    # mweb + use_ad_playback_context пропускает рекламу и обязательное ожидание перед загрузкой.
    # Не использовать с cookies Premium-аккаунта (теряются premium-форматы): MWEB_FIRST=0.
    fast = [("mweb без ожидания рекламы", bool(COOKIES_SRC), ["mweb"])] if MWEB_FIRST else []
    if COOKIES_SRC:
        return fast + [
            ("cookies + tv/web_embedded", True, CLIENTS),
            ("cookies + web", True, ["web"]),
            ("без cookies + web", False, ["web"]),
        ]
    return fast + [
        ("tv/web_embedded", False, CLIENTS),
        ("web", False, ["web"]),
    ]


STRATEGIES = build_strategies()


def _ydl_options(workdir: Path, use_cookies: bool, clients: list):
    opts = {
        "format": "bestaudio/best",
        "outtmpl": str(workdir / "%(id)s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "retries": 1,
        "extractor_retries": 1,
        "fragment_retries": 1,
        "file_access_retries": 1,
        "socket_timeout": 15,
        "match_filter": match_filter_func(f"duration < {MAX_DURATION}"),
        # Не перекодируем здесь: единый финальный FFmpeg-кодировщик запускается
        # один раз после загрузки для standard и Bass.
        "extractor_args": {"youtube": {"player_client": clients}},
        "js_runtimes": {"deno": {}},
    }
    if clients == ["mweb"]:
        opts["extractor_args"]["youtube"]["use_ad_playback_context"] = ["true"]
    if POT_PROVIDER_URL:
        opts["extractor_args"]["youtubepot-bgutilhttp"] = {"base_url": [POT_PROVIDER_URL]}
    if use_cookies and COOKIES_SRC:
        cj = workdir / "cookies.txt"
        shutil.copy(COOKIES_SRC, cj)
        opts["cookiefile"] = str(cj)
    return opts


class _StageLogger:
    """Пишет этапы yt-dlp с временем от начала — видно, где именно тратятся секунды."""

    def __init__(self):
        self.t0 = time.monotonic()

    def debug(self, msg):
        if msg.startswith("[debug]"):
            return
        log.info("  yt-dlp +%.1f с: %s", time.monotonic() - self.t0, str(msg)[:140])

    def info(self, msg):
        self.debug(msg)

    def warning(self, msg):
        log.warning("  yt-dlp: %s", str(msg)[:200])

    def error(self, msg):
        log.warning("  yt-dlp: %s", str(msg)[:200])


def _download(query: str, is_url: bool, workdir: Path, use_cookies: bool, clients: list, prefetched=None):
    opts = _ydl_options(workdir, use_cookies, clients)
    t0 = time.monotonic()
    marks = {}

    def _on_progress(d):
        st = d.get("status")
        if st == "downloading" and "first" not in marks:
            marks["first"] = time.monotonic()
        elif st == "finished":
            marks["dl_done"] = time.monotonic()

    opts["logger"] = _StageLogger()
    opts["progress_hooks"] = [_on_progress]
    target = query if is_url else f"ytsearch1:{query}"
    with yt_dlp.YoutubeDL(opts) as ydl:
        if prefetched is not None:
            info = ydl.process_ie_result(copy.deepcopy(prefetched), download=True)
        else:
            info = ydl.extract_info(target, download=True)
    end = time.monotonic()
    first = marks.get("first", t0)
    dl_done = marks.get("dl_done", first)
    log.info(
        "ТАЙМИНГ: извлечение данных %.1f с | загрузка %.1f с | yt-dlp всего %.1f с",
        first - t0, dl_done - first, end - t0,
    )
    if info and "entries" in info:
        entries = [e for e in info["entries"] if e]
        info = entries[0] if entries else None

    # Без FFmpegExtractAudio исходник сохраняет исходное расширение (webm/m4a/opus).
    # Сначала используем точный filepath от yt-dlp, затем безопасный fallback в папке.
    candidates = []
    if isinstance(info, dict):
        requested = info.get("requested_downloads") or []
        for item in requested:
            filepath = item.get("filepath") if isinstance(item, dict) else None
            if filepath:
                candidates.append(Path(filepath))
        filepath = info.get("filepath")
        if filepath:
            candidates.append(Path(filepath))
    source = next((p for p in candidates if p.is_file() and p.stat().st_size > 0), None)
    if source is None:
        files = [
            p for p in workdir.iterdir()
            if p.is_file() and p.name != "cookies.txt"
            and not p.name.endswith((".part", ".ytdl"))
            and p.stat().st_size > 0
        ]
        source = max(files, key=lambda p: p.stat().st_size) if files else None
    return info, source


def bass_filter_for_preset(preset: str) -> str | None:
    """Return a conservative low-shelf boost plus a peak limiter."""
    if preset not in BASS_PRESETS:
        raise ValueError("unknown bass preset")
    gain = BASS_PRESETS[preset]
    if gain is None:
        return None
    return (
        f"bass=g={gain:.1f}:f=90:width_type=q:width=0.7,"
        "alimiter=limit=0.95:attack=5:release=50:level=disabled"
    )


def _encode_audio(input_path: Path, output_path: Path, preset: str = "original") -> None:
    """Encode every delivered track with the same codec/bitrate; bass is optional."""
    audio_filter = bass_filter_for_preset(preset)
    if AUDIO_FORMAT == "m4a":
        codec_args = ["-c:a", "aac", "-b:a", "320k", "-movflags", "+faststart"]
    else:
        codec_args = ["-c:a", "libmp3lame", "-b:a", "320k"]
    command = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
        "-i", str(input_path), "-map", "0:a:0", "-vn",
    ]
    # The standard mode deliberately has no audio filter: it gets the same final
    # codec/bitrate as Bass, without changing its frequency balance.
    if audio_filter is not None:
        command.extend(["-af", audio_filter])
    command.extend(["-map_metadata", "0", *codec_args, str(output_path)])
    completed = subprocess.run(
        command, capture_output=True, text=True, timeout=180, check=False
    )
    if (
        completed.returncode != 0
        or not output_path.is_file()
        or output_path.stat().st_size <= 0
    ):
        details = (completed.stderr or "FFmpeg audio encoding failed").strip()
        raise RuntimeError(details[-500:])


def _apply_bass_boost(input_path: Path, output_path: Path, preset: str) -> None:
    """Backward-compatible wrapper for callers that explicitly apply bass."""
    if preset == "original":
        raise ValueError("original preset does not apply bass")
    _encode_audio(input_path, output_path, preset)


def _extract_only(url: str):
    """Получить данные о видео без скачивания (ссылки на форматы остаются в info)."""
    workdir = Path(tempfile.mkdtemp(prefix="pre_"))
    try:
        opts = _ydl_options(workdir, bool(COOKIES_SRC), CLIENTS)
        opts.pop("postprocessors", None)
        opts["skip_download"] = True
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


async def _prefetch(urls: list):
    loop = asyncio.get_running_loop()
    for url in urls:
        cached = PREFETCH_CACHE.get(url)
        if cached and time.monotonic() - cached[0] < PREFETCH_TTL:
            continue
        started = time.monotonic()
        try:
            async with SEARCH_SEMAPHORE:
                info = await loop.run_in_executor(None, _extract_only, url)
        except Exception as exc:
            log.info("Предзагрузка не удалась: %s", str(exc)[:120])
            return  # при ошибке (бот-проверка, 429) не продолжаем
        if info and info.get("formats"):
            PREFETCH_CACHE[url] = (time.monotonic(), info)
            log.info("ПРЕДЗАГРУЗКА: %s готова за %.1f с", url, time.monotonic() - started)
    now = time.monotonic()
    for k in [k for k, v in PREFETCH_CACHE.items() if now - v[0] > PREFETCH_TTL]:
        PREFETCH_CACHE.pop(k, None)


def _search_tracks(query: str, limit: int = 10):
    """Получить ограниченное число результатов поиска без скачивания аудио."""
    limit = max(1, min(int(limit), 30))
    workdir = Path(tempfile.mkdtemp(prefix="search_"))
    try:
        last_error = None
        for use_cookies, clients in (
            (True, CLIENTS) if COOKIES_SRC else (False, CLIENTS),
            (False, ["web"]),
        ):
            try:
                opts = _ydl_options(workdir, use_cookies, clients)
                opts.pop("format", None)
                opts.pop("postprocessors", None)
                opts.pop("match_filter", None)
                opts["extract_flat"] = "in_playlist"
                opts["skip_download"] = True
                with yt_dlp.YoutubeDL(opts) as ydl:
                    data = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
                entries = (data or {}).get("entries") or []
                results = []
                for entry in entries:
                    if not entry:
                        continue
                    duration = entry.get("duration")
                    if is_duration_over_limit(duration, MAX_DURATION):
                        continue
                    video_id = entry.get("id")
                    url = entry.get("url") or entry.get("webpage_url")
                    if not url or not str(url).startswith("http"):
                        if not video_id:
                            continue
                        url = f"https://www.youtube.com/watch?v={video_id}"
                    results.append({
                        "title": clean_metadata(entry.get("title")) or "Без названия",
                        "url": url,
                        "duration": duration,
                    })
                return results[:limit]
            except Exception as exc:
                last_error = exc
                log.warning("Поиск не сработал для клиента %s: %s", clients, str(exc)[:250])
        if last_error:
            raise last_error
        return []
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


async def _delete_message_later(bot, chat_id: int, message_id: int, delay: int):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as exc:
        log.debug("Не удалось удалить временное сообщение %s: %s", message_id, exc)


def schedule_delete(ctx: ContextTypes.DEFAULT_TYPE, message, delay: int):
    tasks = ctx.application.bot_data.setdefault("_cleanup_tasks", set())
    task = asyncio.create_task(
        _delete_message_later(ctx.bot, message.chat_id, message.message_id, delay)
    )
    tasks.add(task)
    task.add_done_callback(tasks.discard)

async def _expire_search_later(bot, searches: dict, token: str, chat_id: int, message_id: int, delay: int):
    await asyncio.sleep(delay)
    state = searches.get(token)
    bass_message_id = None
    if state and state.get("message_id") == message_id:
        bass_message_id = state.get("bass_message_id")
        searches.pop(token, None)
    for target_id in (message_id, bass_message_id):
        if not target_id:
            continue
        try:
            await bot.delete_message(chat_id=chat_id, message_id=target_id)
        except Exception as exc:
            log.debug("Не удалось удалить сообщение выбора %s: %s", target_id, exc)


def schedule_search_expiry(ctx: ContextTypes.DEFAULT_TYPE, message, token: str, delay: int = 600):
    tasks = ctx.application.bot_data.setdefault("_cleanup_tasks", set())
    searches = ctx.application.bot_data.setdefault("music_searches", {})
    task = asyncio.create_task(
        _expire_search_later(ctx.bot, searches, token, message.chat_id, message.message_id, delay)
    )
    tasks.add(task)
    task.add_done_callback(tasks.discard)

def schedule_prefetch(ctx: ContextTypes.DEFAULT_TYPE, urls: list) -> None:
    """Keep at most one prefetch job active; prefetch is optional background work."""
    if not urls:
        return
    bot_data = ctx.application.bot_data
    current = bot_data.get("_prefetch_task")
    if current is not None and not current.done():
        return
    task = asyncio.create_task(_prefetch(urls))
    bot_data["_prefetch_task"] = task
    tasks = bot_data.setdefault("_cleanup_tasks", set())
    tasks.add(task)
    task.add_done_callback(tasks.discard)


async def _delete_error_later(bot, bot_data: dict, chat_id: int, message_id: int, delay: int):
    """Delete each error independently; simultaneous errors keep independent TTLs."""
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as exc:
        log.debug("Не удалось удалить уведомление об ошибке %s: %s", message_id, exc)


async def send_generic_error(
    ctx: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str = "⚠️ Произошла ошибка. Попробуй ещё раз.",
):
    bot_data = ctx.application.bot_data
    try:
        message = await ctx.bot.send_message(chat_id=chat_id, text=text)
    except Exception:
        _metric_increment(bot_data, "telegram_send_errors")
        log.exception("Не удалось отправить сообщение об ошибке")
        return

    # Every notification gets its own 10-second timer, including concurrent errors.
    tasks = bot_data.setdefault("_cleanup_tasks", set())
    task = asyncio.create_task(
        _delete_error_later(
            ctx.bot, bot_data, chat_id, message.message_id, ERROR_NOTIFICATION_TTL
        )
    )
    tasks.add(task)
    task.add_done_callback(tasks.discard)


@_track_runtime_metric("search")
async def show_search(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    query: str,
    *,
    video_recognition: bool = False,
):
    chat_id = update.effective_chat.id
    try:
        nq = norm_query(query)
        # Recognition needs a wider candidate pool because short previews and
        # unrelated results are filtered before presenting the same 10-item picker.
        results = None if video_recognition else _safe_cache_call(CACHE, "get_search", nq)
        cache_hit = bool(results)
        if not video_recognition:
            _metric_increment(
                ctx.application.bot_data,
                "search_cache_hits" if cache_hit else "search_cache_misses",
            )
        if cache_hit:
            log.info("КЭШ: поиск «%s» из кэша (%d результатов)", nq, len(results))
        else:
            search_wait_started = time.monotonic()
            try:
                async with SEARCH_SEMAPHORE:
                    _metric_observe(
                        ctx.application.bot_data,
                        "search_wait_seconds",
                        time.monotonic() - search_wait_started,
                    )
                    loop = asyncio.get_running_loop()
                    search_started = time.monotonic()
                    results = await loop.run_in_executor(
                        None, _search_tracks, query, 30 if video_recognition else 10
                    )
                    log.info("ТАЙМИНГ: поиск %.1f с, результатов: %d", time.monotonic() - search_started, len(results))
            except Exception:
                _metric_increment(ctx.application.bot_data, "youtube_search_errors")
                raise
        # Rank on both cache hits and fresh searches so existing 6-hour cache entries
        # immediately benefit from ranking changes without a schema migration.
        results = rank_search_results(query, results)
        if video_recognition:
            results = [
                item for item in results
                if (item.get("duration") is None or item.get("duration") >= 60)
                and _query_match_score(query, clean_metadata(item.get("title")) or "") >= 0.34
            ]
            if not results:
                await send_generic_error(
                    ctx, chat_id,
                    "🎵 Песня распознана, но подходящих полных версий пока не найдено. Попробуй другое видео."
                )
                return
        if results and not cache_hit and not video_recognition:
            _safe_cache_call(CACHE, "put_search", nq, results)
        if not results:
            await send_generic_error(
                ctx, chat_id,
                "🎵 Не удалось найти эту песню. Проверь название или попробуй отправить другое видео."
            )
            return

        token = secrets.token_hex(8)
        searches = ctx.application.bot_data.setdefault("music_searches", {})
        # Register before sending: a very fast callback may arrive before reply_text returns.
        searches[token] = {
            "results": results,
            "created": time.time(),
            "chat_id": chat_id,
        }
        lines = ["🎵 Выбери трек:"]
        buttons = []
        for i, item in enumerate(results, 1):
            duration = item.get("duration")
            time_label = f" · {int(duration // 60)}:{int(duration % 60):02d}" if duration else ""
            title = clean_metadata(item.get("title")) or "Без названия"
            lines.append(f"{i}. {title}{time_label}")
            buttons.append(InlineKeyboardButton(str(i), callback_data=f"pick:{token}:{i-1}"))
        keyboard = [buttons[i:i + 5] for i in range(0, len(buttons), 5)]
        try:
            result_message = await update.effective_message.reply_text(
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        except Exception:
            searches.pop(token, None)
            raise
        searches[token]["message_id"] = result_message.message_id
        schedule_search_expiry(ctx, result_message, token, 600)
        if PREFETCH_TOP:
            to_prefetch = [
                x["url"] for x in results[:PREFETCH_TOP]
                if not (CACHE and video_id_of(x["url"]) and _safe_cache_call(CACHE, "get_track", video_id_of(x["url"])))
            ]
            schedule_prefetch(ctx, to_prefetch)
    except Exception:
        log.exception("Ошибка поиска")
        await send_generic_error(ctx, chat_id)


def _record_successful_delivery(cache, video_id, file_id, title, performer, duration, user_id):
    """Persist cache and ranking independently after Telegram confirms delivery."""
    if not cache or not video_id:
        return
    if file_id:
        try:
            cache.put_track(video_id, file_id, title, performer, duration)
        except Exception as exc:
            log.warning("Не удалось сохранить file_id в кэш: %s", exc)
    try:
        cache.record_download(
            video_id, title, performer, duration, user_id=user_id
        )
    except Exception as exc:
        log.warning("Не удалось записать скачивание в рейтинг: %s", exc)


async def _send_audio_limited(bot, *, on_started=None, metrics=None, **kwargs):
    """Apply one process-wide limit and bounded retries to Telegram audio sends."""
    wait_started = time.monotonic()
    async with SEND_LOCK:
        _metric_observe(metrics, "send_wait_seconds", time.monotonic() - wait_started)
        if on_started is not None:
            await on_started()
        for attempt in range(3):
            try:
                return await bot.send_audio(**kwargs)
            except RetryAfter as exc:
                _metric_increment(metrics, "telegram_retry_after")
                if attempt == 2:
                    _metric_increment(metrics, "telegram_send_errors")
                    raise
                audio = kwargs.get("audio")
                if hasattr(audio, "seek"):
                    audio.seek(0)
                retry_after = exc.retry_after
                delay = max(
                    0.0,
                    retry_after.total_seconds()
                    if hasattr(retry_after, "total_seconds")
                    else float(retry_after),
                )
                log.warning(
                    "Telegram ограничил отправку аудио; повтор %d/2 через %.1f с",
                    attempt + 1, delay,
                )
                await asyncio.sleep(delay + 0.5)
            except Exception:
                _metric_increment(metrics, "telegram_send_errors")
                raise


@_track_runtime_metric("process")
async def process(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    query: str,
    error_text: str = "⚠️ Произошла ошибка.",
    notify_errors: bool = True,
    on_started=None,
    bass_preset: str = "original",
    reply_markup=None,
):
    global _last_finish
    if bass_preset not in BASS_PRESETS:
        raise ValueError("unknown bass preset")
    chat_id = update.effective_chat.id
    is_url = bool(URL_RE.fullmatch(query))
    if is_url and not is_youtube_url(query):
        if notify_errors:
            await send_generic_error(ctx, chat_id, error_text)
        return False

    vid = video_id_of(query) if is_url else None
    if vid and CACHE and bass_preset == "original":
        row = _safe_cache_call(CACHE, "get_track", vid)
        _metric_increment(
            ctx.application.bot_data,
            "track_cache_hits" if row else "track_cache_misses",
        )
        if row:
            try:
                await _send_audio_limited(
                    ctx.bot,
                    on_started=on_started,
                    metrics=ctx.application.bot_data,
                    chat_id=chat_id, audio=row[0],
                    title=clean_metadata(row[1], 128),
                    performer=clean_metadata(row[2], 128),
                    duration=row[3], read_timeout=60, write_timeout=60,
                    reply_markup=reply_markup,
                )
            except Exception as exc:
                if _is_invalid_file_id_error(exc):
                    log.warning("КЭШ: Telegram отклонил file_id для %s, удаляю запись и качаю заново", vid)
                    _safe_cache_call(CACHE, "drop_track", vid)
                else:
                    # Rate limits, network errors, and chat errors do not invalidate a file_id.
                    log.warning("КЭШ: отправка %s временно не удалась; сохраняю file_id: %s", vid, str(exc)[:120])
                    if notify_errors:
                        await send_generic_error(ctx, chat_id, error_text)
                    return False
            else:
                _record_successful_delivery(
                    CACHE, vid, None, row[1], row[2], row[3],
                    update.effective_user.id if update.effective_user else None,
                )
                log.info("КЭШ: трек %s отправлен по file_id", vid)
                return True

    download_wait_started = time.monotonic()
    async with DL_LOCK:
        _metric_observe(
            ctx.application.bot_data,
            "download_wait_seconds",
            time.monotonic() - download_wait_started,
        )
        if _last_finish and MAX_DELAY > 0:
            wait = _last_finish + random.uniform(MIN_DELAY, MAX_DELAY) - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)

        if on_started is not None:
            await on_started()

        workdir = Path(tempfile.mkdtemp(prefix="dl_"))
        info = path = None
        last_err = ""
        delivered = False
        try:
            loop = asyncio.get_running_loop()
            for i, (name, use_cookies, clients) in enumerate(STRATEGIES, 1):
                attempt_dir = workdir / f"attempt_{i}"
                attempt_dir.mkdir(parents=True, exist_ok=True)
                attempt_started = time.monotonic()
                try:
                    log.info("Попытка %d/%d: %s | %s", i, len(STRATEGIES), name, query)
                    pre = None
                    if i == 1 and is_url:
                        cached = PREFETCH_CACHE.pop(query, None)
                        if cached and time.monotonic() - cached[0] < PREFETCH_TTL:
                            pre = cached[1]
                            log.info("Использую предзагруженные данные (возраст %.0f с)", time.monotonic() - cached[0])
                    info, path = await loop.run_in_executor(
                        None, _download, query, is_url, attempt_dir, use_cookies, clients, pre
                    )
                    if path:
                        log.info("Скачивание и конвертация завершены за %.1f с", time.monotonic() - attempt_started)
                        break
                    if info is None:
                        last_err = "no result"
                        break
                    last_err = "track exceeds configured duration"
                    break
                except Exception as exc:
                    _metric_increment(ctx.application.bot_data, "youtube_download_errors")
                    last_err = str(exc)
                    log.warning(
                        "Стратегия «%s» не сработала за %.1f с: %s",
                        name, time.monotonic() - attempt_started, last_err[:300]
                    )

            if not path:
                raise RuntimeError(last_err or "download failed")

            # Finalize standard and bass variants through exactly the same encoder.
            # "original" skips the EQ filter but still gets the same 320 kbps output.
            processed_path = workdir / f"processed.{AUDIO_FORMAT}"
            encode_started = time.monotonic()
            try:
                if bass_preset == "original":
                    await loop.run_in_executor(
                        None, _encode_audio, path, processed_path, bass_preset
                    )
                else:
                    async with BASS_LOCK:
                        await loop.run_in_executor(
                            None, _encode_audio, path, processed_path, bass_preset
                        )
            except Exception:
                _metric_increment(ctx.application.bot_data, "ffmpeg_errors")
                raise
            log.info(
                "ТАЙМИНГ: финальное FFmpeg-кодирование (%s) %.1f с",
                bass_preset, time.monotonic() - encode_started,
            )
            path = processed_path

            if path.stat().st_size > TG_LIMIT:
                raise RuntimeError("file exceeds Telegram upload limit")

            info = info or {}
            upload_started = time.monotonic()
            with open(path, "rb") as audio_file:
                sent = await _send_audio_limited(
                    ctx.bot,
                    metrics=ctx.application.bot_data,
                    chat_id=chat_id,
                    audio=audio_file,
                    title=clean_metadata(info.get("track") or info.get("title"), 128),
                    performer=clean_metadata(info.get("artist") or info.get("uploader"), 128),
                    duration=int(info.get("duration") or 0) or None,
                    filename=f"{clean_metadata(info.get('title'), 60) or 'track'}.{AUDIO_FORMAT}",
                    reply_markup=reply_markup,
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=30,
                    pool_timeout=30,
                )
            successful_video_id = vid or info.get("id")
            title = info.get("track") or info.get("title")
            performer = info.get("artist") or info.get("uploader")
            duration = int(info.get("duration") or 0) or None
            if CACHE and sent and sent.audio and successful_video_id:
                user_id = update.effective_user.id if update.effective_user else None
                if bass_preset == "original":
                    _record_successful_delivery(
                        CACHE, successful_video_id, sent.audio.file_id, title, performer,
                        duration, user_id,
                    )
                else:
                    # Never cache a processed Telegram file_id as the original track.
                    _safe_cache_call(
                        CACHE, "record_download", successful_video_id, title, performer,
                        duration, user_id=user_id,
                    )
            log.info("Отправка аудио в Telegram завершена за %.1f с (%.1f МБ)",
                     time.monotonic() - upload_started, path.stat().st_size / (1024 * 1024))
            delivered = True
        except Exception:
            log.exception("Не удалось обработать или отправить трек")
            if notify_errors:
                await send_generic_error(ctx, chat_id, error_text)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
            _last_finish = time.monotonic()
    return delivered


def _extract_video_audio_sample(video_path: Path, audio_path: Path) -> None:
    """Extract a short, compact audio sample; standard AudD accepts audio, not video."""
    command = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-i", str(video_path),
        "-t", "20",
        "-vn", "-ac", "1", "-ar", "22050", "-b:a", "96k",
        "-f", "mp3", str(audio_path),
    ]
    completed = subprocess.run(
        command, capture_output=True, text=True, timeout=45, check=False
    )
    if completed.returncode != 0 or not audio_path.is_file() or audio_path.stat().st_size == 0:
        details = (completed.stderr or "FFmpeg could not extract an audio stream").strip()
        raise RuntimeError(details[-500:])


async def recognize_video_with_audd(audio_path: Path):
    """Recognize an extracted audio sample using AudD's standard endpoint."""
    if not AUDD_API_TOKEN:
        raise RuntimeError("AUDD_API_TOKEN is not configured")
    timeout = httpx.Timeout(35.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        with audio_path.open("rb") as audio_file:
            response = await client.post(
                AUDD_API_URL,
                data={"api_token": AUDD_API_TOKEN},
                files={"file": ("video-sample.mp3", audio_file, "audio/mpeg")},
            )
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, dict) or payload.get("status") != "success":
        error = payload.get("error_message") or payload.get("error") or "AudD returned an unsuccessful response"
        raise RuntimeError(str(error)[:240])
    result = payload.get("result")
    return result if isinstance(result, dict) else None


async def _process_video_pipeline(update: Update, ctx: ContextTypes.DEFAULT_TYPE, message, chat, media, filename: str):
    workdir = Path(tempfile.mkdtemp(prefix="video_recognition_"))
    progress_message = None
    try:
        progress_message = await message.reply_text("🎧 Распознаю музыку в видео…")
        video_path = workdir / filename
        audio_path = workdir / "recognition-sample.mp3"
        try:
            telegram_file = await ctx.bot.get_file(media.file_id)
            await telegram_file.download_to_drive(custom_path=str(video_path))
        except Exception:
            _metric_increment(ctx.application.bot_data, "telegram_api_errors")
            raise
        actual_size = video_path.stat().st_size
        if actual_size <= 0:
            raise RuntimeError("Downloaded video is empty")
        if actual_size > MAX_VIDEO_RECOGNITION_SIZE:
            await send_generic_error(ctx, chat.id, "⚠️ Видео слишком большое. Максимальный размер — 20 МБ.")
            return

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, _extract_video_audio_sample, video_path, audio_path)
        except Exception:
            _metric_increment(ctx.application.bot_data, "ffmpeg_errors")
            raise
        try:
            recognition = await recognize_video_with_audd(audio_path)
        except Exception:
            _metric_increment(ctx.application.bot_data, "audd_errors")
            raise
        if not recognition:
            await send_generic_error(ctx, chat.id, "🎵 Не удалось распознать музыку. Попробуй видео, где музыка слышна с самого начала.")
            return

        title = clean_metadata(recognition.get("title"), 120)
        artist = clean_metadata(recognition.get("artist"), 100)
        if not title or not artist:
            await send_generic_error(ctx, chat.id, "🎵 AudD не вернул название и исполнителя.")
            return

        # Reuse the normal picker: show up to 10 relevant, diversified versions
        # instead of silently downloading the first match returned by YouTube.
        query = f"{artist} {title}"
        await show_search(update, ctx, query, video_recognition=True)
    except httpx.TimeoutException:
        log.warning("AudD timeout while recognizing Telegram video")
        await send_generic_error(ctx, chat.id, "⚠️ AudD не ответил вовремя. Попробуй ещё раз.")
    except subprocess.TimeoutExpired:
        log.warning("FFmpeg timed out while extracting video audio")
        await send_generic_error(ctx, chat.id, "⚠️ Не удалось обработать видео вовремя.")
    except Exception:
        log.exception("Не удалось распознать музыку в видео")
        await send_generic_error(ctx, chat.id, "⚠️ Не удалось обработать видео. Проверь, что в нём есть звук.")
    finally:
        if progress_message:
            try:
                await progress_message.delete()
            except Exception:
                pass
        shutil.rmtree(workdir, ignore_errors=True)

    

@_track_runtime_metric("video")
async def on_video(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Recognize a track in a Telegram video and send its full YouTube audio."""
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or (update.effective_user and update.effective_user.is_bot):
        return

    media = message.video or message.video_note
    filename = "telegram-video.mp4"
    if media is None and message.document:
        document = message.document
        document_name = Path(document.file_name or "telegram-video.mp4").name
        is_video = (document.mime_type or "").lower().startswith("video/") or Path(document_name).suffix.lower() in {
            ".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v"
        }
        if not is_video:
            return
        media = document
        filename = document_name
    elif media is not None:
        filename = Path(getattr(media, "file_name", None) or "telegram-video.mp4").name

    if media is None:
        return

    if chat.type != "private" and not GROUP_REACT_ALL:
        mentioned, _ = strip_bot_mention(message.caption or "", ctx.bot.username)
        if not mentioned:
            return

    if not AUDD_API_TOKEN:
        await send_generic_error(ctx, chat.id, "⚠️ Распознавание видео не настроено: добавь AUDD_API_TOKEN в Railway Variables.")
        return

    reported_size = getattr(media, "file_size", None)
    if reported_size and reported_size > MAX_VIDEO_RECOGNITION_SIZE:
        await send_generic_error(ctx, chat.id, "⚠️ Видео слишком большое. Максимальный размер — 20 МБ.")
        return

    if VIDEO_LOCK.locked():
        await send_generic_error(
            ctx, chat.id,
            "⚠️ Сейчас уже обрабатываются два видео. Попробуй ещё раз чуть позже."
        )
        return

    async with VIDEO_LOCK:
        await _process_video_pipeline(update, ctx, message, chat, media, filename)


SPINNER = ["🕐", "🕑", "🕒", "🕓", "🕔", "🕕", "🕖", "🕗", "🕘", "🕙", "🕚", "🕛"]
ANIM_INTERVAL = 1.2


async def _animate_picker(message, index: int):
    """Крутит часы на нажатой кнопке и «Загружаю…» в заголовке, пока идёт загрузка/отправка."""
    text = message.text or ""
    markup = message.reply_markup
    if not text or not markup:
        return
    lines = text.split("\n")
    step = 0
    while True:
        step += 1
        frame = SPINNER[step % len(SPINNER)]
        new_lines = lines[:]
        new_lines[0] = f"{frame} Загружаю{'.' * (step % 4)}"
        keyboard = [
            [
                InlineKeyboardButton(
                    frame if sum(len(previous) for previous in markup.inline_keyboard[:r]) + c == index else b.text,
                    callback_data=b.callback_data,
                )
                for c, b in enumerate(row)
            ]
            for r, row in enumerate(markup.inline_keyboard)
        ]
        try:
            await message.edit_text("\n".join(new_lines), reply_markup=InlineKeyboardMarkup(keyboard))
        except RetryAfter as exc:
            await asyncio.sleep(float(exc.retry_after) + 0.5)
            continue
        except Exception as exc:
            log.debug("Анимация: %s", exc)
        await asyncio.sleep(ANIM_INTERVAL)


def build_audio_keyboard(token: str, index: int, label: str = "🔊 Добавить басс") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(label, callback_data=f"bassmenu:{token}:{index}")
    ]])


def build_bass_keyboard(token: str, index: int) -> InlineKeyboardMarkup:
    options = [
        ("🔊 Усилить низкие частоты", "low_freq"),
        ("🔥 Bass", "bass"),
    ]
    buttons = [
        InlineKeyboardButton(label, callback_data=f"bassapply:{token}:{index}:{preset}")
        for label, preset in options
    ]
    return InlineKeyboardMarkup([buttons[:2], buttons[2:]])


def build_download_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬇️ Скачать выбранный трек", callback_data=f"download:{token}")],
        [
            InlineKeyboardButton("🎚 Изменить бас", callback_data=f"basschange:{token}"),
            InlineKeyboardButton("↩️ Другая версия", callback_data=f"back:{token}"),
        ],
    ])


def _claim_processing(processing_tokens: set, token: str) -> bool:
    """Atomically claim a picker token shared by every user in the chat."""
    if token in processing_tokens:
        return False
    processing_tokens.add(token)
    return True


def _release_processing(processing_tokens: set, token: str) -> None:
    processing_tokens.discard(token)


def _get_search_state(searches: dict, token: str, chat_id: int):
    """Return a live picker state only for the chat where it was created."""
    state = searches.get(token)
    if not state or state.get("chat_id") != chat_id:
        return None
    if time.time() - state.get("created", 0) > 600:
        searches.pop(token, None)
        return None
    return state


@_track_runtime_metric("picker")
async def on_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Send the selected track immediately with an animated clock while it loads."""
    callback = update.callback_query
    token = None
    picker = callback.message if callback else None
    anim = None
    claimed = False
    processing_tokens = None
    original_text = picker.text if picker else None
    original_markup = picker.reply_markup if picker else None
    try:
        _, token, raw_index = callback.data.split(":")
        index = int(raw_index)
        if not picker:
            await callback.answer()
            return

        searches = ctx.application.bot_data.setdefault("music_searches", {})
        data = _get_search_state(searches, token, picker.chat_id)
        if not data or data.get("message_id") != picker.message_id:
            await callback.answer("❗ Время истекло. Выполните новый поиск.", show_alert=True)
            return
        results = data.get("results") or []
        if index < 0 or index >= len(results):
            await callback.answer("⚠️ Не удалось выбрать трек. Выполните поиск ещё раз.", show_alert=True)
            return

        processing_tokens = ctx.application.bot_data.setdefault("music_processing_tokens", set())
        if not _claim_processing(processing_tokens, token):
            await callback.answer("⏳ Подожди, текущий трек ещё отправляется.")
            return
        claimed = True
        await callback.answer("Загружаю…")

        # Start immediately on button press so the user sees the clock even
        # while this request is waiting for the download semaphore.
        anim = asyncio.create_task(_animate_picker(picker, index))

        selected = results[index]
        data["stage"] = "sent"
        data["selected_index"] = index
        data["selected_preset"] = "original"
        delivered = await process(
            update,
            ctx,
            selected["url"],
            notify_errors=False,
            bass_preset="original",
            reply_markup=build_audio_keyboard(token, index),
        )
        if not delivered:
            data["stage"] = "select"
            data["selected_index"] = None
            data["selected_preset"] = None
            await send_generic_error(
                ctx, picker.chat_id,
                "🎵 Не удалось найти или отправить эту версию. Попробуй выбрать другой вариант.",
            )
    except (ValueError, AttributeError, TypeError):
        if callback:
            await callback.answer("⚠️ Не удалось отправить выбранную версию.", show_alert=True)
    except Exception:
        log.exception("Ошибка отправки выбранной версии")
        if picker:
            await send_generic_error(ctx, picker.chat_id, "⚠️ Не удалось отправить музыку.")
    finally:
        if anim:
            anim.cancel()
            try:
                await anim
            except (asyncio.CancelledError, Exception):
                pass
            # Restore the original 10-choice picker after the clock animation.
            if picker and original_text is not None:
                try:
                    await picker.edit_text(original_text, reply_markup=original_markup)
                except Exception as exc:
                    log.debug("Не удалось восстановить список версий после загрузки: %s", exc)
        if claimed and processing_tokens is not None and token is not None:
            _release_processing(processing_tokens, token)

async def on_bass_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Open the bass presets directly under the delivered audio message."""
    callback = update.callback_query
    audio_message = callback.message if callback else None
    try:
        _, token, raw_index = callback.data.split(":")
        index = int(raw_index)
        if not audio_message:
            await callback.answer()
            return
        data = _get_search_state(
            ctx.application.bot_data.setdefault("music_searches", {}),
            token,
            audio_message.chat_id,
        )
        results = data.get("results") or [] if data else []
        if (
            not data
            or data.get("stage") != "sent"
            or not audio_message.audio
            or index < 0
            or index >= len(results)
        ):
            await callback.answer("❗ Эта кнопка уже устарела. Выбери песню заново.", show_alert=True)
            return
        await callback.answer()
        await audio_message.edit_reply_markup(
            reply_markup=build_bass_keyboard(token, index)
        )
    except Exception:
        log.exception("Ошибка открытия меню баса")
        if callback:
            await callback.answer("⚠️ Не удалось открыть настройки баса.", show_alert=True)


@_track_runtime_metric("picker")
async def on_bass_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Apply a chosen bass preset and send the processed copy immediately."""
    callback = update.callback_query
    token = None
    audio_message = callback.message if callback else None
    claimed = False
    processing_tokens = None
    try:
        _, token, raw_index, preset = callback.data.split(":")
        index = int(raw_index)
        if not audio_message:
            await callback.answer()
            return
        data = _get_search_state(
            ctx.application.bot_data.setdefault("music_searches", {}),
            token,
            audio_message.chat_id,
        )
        results = data.get("results") or [] if data else []
        if (
            not data
            or data.get("stage") != "sent"
            or not audio_message.audio
            or index < 0
            or index >= len(results)
        ):
            await callback.answer("❗ Эта настройка устарела. Выбери песню заново.", show_alert=True)
            return
        if preset not in BASS_PRESETS:
            await callback.answer("⚠️ Неизвестный режим обработки.", show_alert=True)
            return

        processing_tokens = ctx.application.bot_data.setdefault("music_processing_tokens", set())
        if not _claim_processing(processing_tokens, token):
            await callback.answer("⏳ Трек уже обрабатывается.")
            return
        claimed = True
        await callback.answer("Применяю обработку…")
        selected = results[index]
        await audio_message.edit_reply_markup(reply_markup=None)
        data["selected_preset"] = preset
        delivered = await process(
            update,
            ctx,
            selected["url"],
            notify_errors=False,
            bass_preset=preset,
            reply_markup=build_audio_keyboard(token, index, "🎚 Изменить басс"),
        )
        if not delivered:
            await audio_message.edit_reply_markup(
                reply_markup=build_bass_keyboard(token, index)
            )
            await send_generic_error(
                ctx, audio_message.chat_id,
                "⚠️ Не удалось обработать музыку. Попробуй другой режим.",
            )
    except (ValueError, AttributeError, TypeError, IndexError):
        if callback:
            await callback.answer("⚠️ Не удалось применить обработку. Попробуй ещё раз.", show_alert=True)
    except Exception:
        log.exception("Ошибка обработки баса")
        if audio_message:
            await send_generic_error(ctx, audio_message.chat_id, "⚠️ Не удалось обработать музыку.")
    finally:
        if claimed and processing_tokens is not None and token is not None:
            _release_processing(processing_tokens, token)


async def on_bass_change(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    callback = update.callback_query
    prompt = callback.message if callback else None
    try:
        _, token = callback.data.split(":")
        if not prompt:
            await callback.answer()
            return
        data = _get_search_state(
            ctx.application.bot_data.setdefault("music_searches", {}),
            token,
            prompt.chat_id,
        )
        if not data or data.get("bass_message_id") != prompt.message_id or data.get("stage") != "bass":
            await callback.answer("❗ Время выбора истекло.", show_alert=True)
            return
        index = data.get("selected_index")
        results = data.get("results") or []
        if index is None or index < 0 or index >= len(results):
            await callback.answer("Сначала выбери версию из списка.", show_alert=True)
            return
        data["selected_preset"] = None
        selected = results[index]
        title = clean_metadata(selected.get("title"), 120) or "Без названия"
        await callback.answer()
        await prompt.edit_text(
            f"🎵 {title}\n\nВыбери обработку баса:",
            reply_markup=build_bass_keyboard(token, index),
        )
    except Exception:
        log.exception("Ошибка возврата к выбору баса")
        if callback:
            await callback.answer("⚠️ Не удалось открыть настройки.", show_alert=True)


async def on_picker_back(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Close the settings prompt and return to the untouched 10-result list."""
    callback = update.callback_query
    prompt = callback.message if callback else None
    try:
        _, token = callback.data.split(":")
        if not prompt:
            await callback.answer()
            return
        searches = ctx.application.bot_data.setdefault("music_searches", {})
        data = _get_search_state(searches, token, prompt.chat_id)
        if not data or data.get("bass_message_id") != prompt.message_id:
            await callback.answer("❗ Время выбора истекло.", show_alert=True)
            return
        data["stage"] = "select"
        data["selected_index"] = None
        data["selected_preset"] = None
        data["bass_message_id"] = None
        await callback.answer("Возвращаю к 10 версиям")
        await prompt.delete()
    except Exception:
        log.exception("Ошибка возврата к списку версий")
        if callback:
            await callback.answer("⚠️ Не удалось вернуться к списку.", show_alert=True)


@_track_runtime_metric("picker")
async def on_download_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Download only after the user confirms the exact version and bass preset."""
    callback = update.callback_query
    token = None
    prompt = callback.message if callback else None
    anim = None
    claimed = False
    processing_tokens = None
    try:
        _, token = callback.data.split(":")
        if not prompt:
            await callback.answer()
            return
        searches = ctx.application.bot_data.setdefault("music_searches", {})
        data = _get_search_state(searches, token, prompt.chat_id)
        if (
            not data
            or data.get("stage") != "bass"
            or data.get("bass_message_id") != prompt.message_id
            or data.get("selected_index") is None
            or data.get("selected_preset") not in BASS_PRESETS
        ):
            await callback.answer("Сначала выбери версию и обработку баса.", show_alert=True)
            return
        processing_tokens = ctx.application.bot_data.setdefault("music_processing_tokens", set())
        if not _claim_processing(processing_tokens, token):
            await callback.answer("⏳ Трек уже обрабатывается.")
            return
        claimed = True
        await callback.answer()

        index = data["selected_index"]
        preset = data["selected_preset"]
        results = data.get("results") or []
        if index < 0 or index >= len(results):
            await callback.answer("Выбранная версия устарела. Запусти поиск заново.", show_alert=True)
            return
        selected = results[index]
        confirmation_text = prompt.text
        confirmation_markup = prompt.reply_markup

        async def start_animation():
            nonlocal anim
            if anim is None:
                anim = asyncio.create_task(_animate_picker(prompt, 0))

        delivered = await process(
            update,
            ctx,
            selected["url"],
            notify_errors=False,
            on_started=start_animation,
            bass_preset=preset,
        )
        if anim:
            anim.cancel()
            try:
                await anim
            except (asyncio.CancelledError, Exception):
                pass
            anim = None
        if delivered:
            await prompt.edit_text(
                "✅ Трек отправлен!\n\nМожешь выбрать другую версию в списке из 10 вариантов выше.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("↩️ Выбрать другую версию", callback_data=f"back:{token}")
                ]]),
            )
        else:
            await prompt.edit_text(confirmation_text, reply_markup=confirmation_markup)
            await send_generic_error(
                ctx, prompt.chat_id,
                "⚠️ Не удалось обработать или отправить музыку. Попробуй скачать ещё раз.",
            )
    except (ValueError, AttributeError, TypeError):
        if callback:
            await callback.answer("⚠️ Не удалось начать скачивание. Попробуй ещё раз.", show_alert=True)
    except Exception:
        log.exception("Ошибка подтверждения скачивания")
        if prompt:
            await send_generic_error(ctx, prompt.chat_id, "⚠️ Не удалось обработать или отправить музыку.")
    finally:
        if anim:
            anim.cancel()
            try:
                await anim
            except (asyncio.CancelledError, Exception):
                pass
        if claimed and processing_tokens is not None and token is not None:
            _release_processing(processing_tokens, token)


async def _delete_start_messages(bot, chat_id: int, message_ids: list[int]) -> None:
    """Удаляет только приветствие и приглашение из конкретного вызова /start."""
    for message_id in message_ids:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        except BadRequest as exc:
            # Пользователь мог удалить сообщение сам или оно уже могло исчезнуть.
            log.debug("Не удалось удалить стартовое сообщение %s: %s", message_id, exc)
        except Exception:
            log.exception("Ошибка автоудаления стартового сообщения %s", message_id)


async def delete_start_messages_job(ctx: ContextTypes.DEFAULT_TYPE) -> None:
    data = ctx.job.data or {}
    await _delete_start_messages(
        ctx.bot, data["chat_id"], data.get("message_ids", [])
    )


async def _delete_start_messages_after_delay(bot, chat_id: int, message_ids: list[int]) -> None:
    await asyncio.sleep(START_MESSAGE_TTL)
    await _delete_start_messages(bot, chat_id, message_ids)


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    sent_messages = []

    # Сначала отдельное приветствие и краткая инструкция по использованию бота.
    welcome = await message.reply_text(
        "Привет! 👋\n\n"
        "Отправь мне название песни или ссылку на YouTube — "
        "я найду музыку и пришлю её тебе. 🎵"
    )
    sent_messages.append(welcome.message_id)

    # Единственное приглашение подписаться показывается только при /start.
    subscription = await message.reply_text(
        "💙 Помоги нашему проекту расти!\n\n"
        "Подпишись на Telegram-канал и поддержи нашу работу.\n\n"
        "✨ Подписка необязательна — ботом можно пользоваться и без неё.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📢 Подписаться на канал", url="https://t.me/LunaAiroll")
        ]]),
    )
    sent_messages.append(subscription.message_id)

    # Удаляем оба сообщения через 30 секунд, не затрагивая команды и музыку.
    payload = {
        "chat_id": update.effective_chat.id,
        "message_ids": sent_messages,
    }
    if ctx.job_queue:
        ctx.job_queue.run_once(
            delete_start_messages_job,
            when=START_MESSAGE_TTL,
            data=payload,
        )
    else:
        # JobQueue может отсутствовать в минимальной установке PTB.
        ctx.application.create_task(
            _delete_start_messages_after_delay(
                ctx.bot, update.effective_chat.id, sent_messages
            )
        )


async def cmd_topmusic(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not CACHE:
        await update.effective_message.reply_text("Рейтинг временно недоступен.")
        return
    rows = _safe_cache_call(CACHE, "top_downloads", days=7, limit=10)
    if rows is None:
        await update.effective_message.reply_text("Рейтинг временно недоступен.")
        return
    if not rows:
        await update.effective_message.reply_text(
            "🎧 Топ музыки за 7 дней\n\nПока скачиваний недостаточно. Будь первым!"
        )
        return
    lines = ["🎧 Топ музыки | Top music", "За последние 7 дней:", ""]
    medals = ["🥇", "🥈", "🥉"]
    for position, (video_id, title, performer, total) in enumerate(rows, 1):
        marker = medals[position - 1] if position <= 3 else f"{position}."
        name = clean_metadata(title, 120) or "Без названия"
        safe_artist = clean_metadata(performer, 80)
        artist = f" — {safe_artist}" if safe_artist else ""
        lines.append(f"{marker} {name}{artist} · {total} скач.")
    await update.effective_message.reply_text("\n".join(lines))


def strip_bot_mention(text: str, username: str | None):
    """Remove only an exact @bot_username mention, not usernames with the same prefix."""
    username = (username or "").lstrip("@")
    if not username:
        return False, text
    pattern = re.compile(
        rf"(?<![A-Za-z0-9_])@{re.escape(username)}(?![A-Za-z0-9_])",
        flags=re.IGNORECASE,
    )
    mentioned = pattern.search(text) is not None
    return mentioned, pattern.sub("", text).strip() if mentioned else text


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.text or (update.effective_user and update.effective_user.is_bot):
        return
    text = msg.text.strip()
    chat = update.effective_chat
    if chat.type != "private":
        mentioned, text = strip_bot_mention(text, ctx.bot.username)
        if not GROUP_REACT_ALL and not mentioned:
            return
        if not text or len(text) > 120 or text.count("\n") > 1:
            return
    if URL_RE.fullmatch(text):
        await process(update, ctx, text[:500])
    else:
        await show_search(update, ctx, text[:200])


def _update_ytdlp_sync() -> str:
    """Run package installation outside the asyncio event-loop thread."""
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-U", "--no-cache-dir", "yt-dlp[default]"],
        check=True, capture_output=True, timeout=300,
    )
    result = subprocess.run(
        [sys.executable, "-c", "import yt_dlp.version as v;print(v.__version__)"],
        check=True, capture_output=True, text=True, timeout=15,
    )
    return result.stdout.strip()


async def update_ytdlp(ctx: ContextTypes.DEFAULT_TYPE):
    before = yt_dlp.version.__version__
    acquired = []
    try:
        # Pause all yt-dlp users before pip mutates the installed package.
        # Search, prefetch, and download paths must acquire these same semaphores.
        for _ in range(MAX_PARALLEL_DL):
            await DL_LOCK.acquire()
            acquired.append(DL_LOCK)
        for _ in range(MAX_PARALLEL_SEARCH):
            await SEARCH_SEMAPHORE.acquire()
            acquired.append(SEARCH_SEMAPHORE)

        loop = asyncio.get_running_loop()
        out = await loop.run_in_executor(None, _update_ytdlp_sync)
        if out and out != before:
            log.info("yt-dlp обновлён %s → %s, перезапуск", before, out)
            os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        log.warning("Не удалось обновить yt-dlp: %s", e)
    finally:
        for semaphore in reversed(acquired):
            semaphore.release()


async def post_init(app: Application):
    await app.bot.set_my_commands([
        BotCommand("start", "Запустить бота"),
        BotCommand("topmusic", "🎧 Топ музыки | Top music"),
    ])


def main():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .update_queue(asyncio.Queue(maxsize=MAX_PENDING_UPDATES))
        .concurrent_updates(MAX_CONCURRENT_UPDATES)
        .post_init(post_init)
        .build()
    )
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("topmusic", cmd_topmusic))
    app.add_handler(CallbackQueryHandler(on_pick, pattern=r"^pick:"))
    app.add_handler(CallbackQueryHandler(on_bass_menu, pattern=r"^bassmenu:"))
    app.add_handler(CallbackQueryHandler(on_bass_pick, pattern=r"^bassapply:"))
    app.add_handler(MessageHandler(filters.VIDEO | filters.VIDEO_NOTE | filters.Document.ALL, on_video))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    if app.job_queue:
        app.job_queue.run_repeating(
            update_ytdlp, interval=UPDATE_EVERY_HOURS * 3600, first=UPDATE_EVERY_HOURS * 3600
        )
        app.job_queue.run_repeating(
            log_runtime_metrics, interval=60, first=60, name="runtime-metrics"
        )
    log.info("yt-dlp %s, стратегий: %d, PO-provider: %s",
             yt_dlp.version.__version__, len(STRATEGIES), POT_PROVIDER_URL or "нет")
    log.info(
        "Лимиты: updates=%d pending_updates=%d search=%d downloads=%d sends=%d video=%d",
        MAX_CONCURRENT_UPDATES, MAX_PENDING_UPDATES, MAX_PARALLEL_SEARCH,
        MAX_PARALLEL_DL, MAX_PARALLEL_SEND, MAX_PARALLEL_VIDEO,
    )
    log.info("Бот запущен, polling начат")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()