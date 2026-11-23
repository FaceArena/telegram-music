import asyncio
import base64
import copy
import logging
import os
import random
import re
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import secrets
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

import yt_dlp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.constants import ChatAction
from telegram.error import RetryAfter
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

BOT_TOKEN = os.environ["BOT_TOKEN"]
MAX_DURATION = int(os.getenv("MAX_DURATION", "1200"))
# Small configurable cooldown; no multi-second artificial wait by default.
MIN_DELAY = max(0.0, float(os.getenv("MIN_DELAY", "0")))
MAX_DELAY = max(MIN_DELAY, float(os.getenv("MAX_DELAY", "0")))
AUDIO_QUALITY = os.getenv("AUDIO_QUALITY", "128")
GROUP_REACT_ALL = os.getenv("GROUP_REACT_ALL", "0") == "1"
POT_PROVIDER_URL = os.getenv("POT_PROVIDER_URL", "").strip()
UPDATE_EVERY_HOURS = int(os.getenv("UPDATE_EVERY_HOURS", "12"))
# Предзагрузка данных о первых N результатах поиска: пока пользователь выбирает трек,
# проходит обязательное ожидание YouTube (available_at), и при скачивании yt-dlp не спит.
PREFETCH_TOP = max(0, int(os.getenv("PREFETCH_TOP", "1")))
PREFETCH_TTL = 20 * 60
PREFETCH_CACHE: dict = {}
MWEB_FIRST = os.getenv("MWEB_FIRST", "1") == "1"
# mp3 (по умолчанию) требует перекодирования в ffmpeg — это самый долгий этап на длинных треках.
# AUDIO_FORMAT=m4a отдаёт AAC без перекодирования (почти мгновенно); Telegram его тоже проигрывает.
AUDIO_FORMAT = os.getenv("AUDIO_FORMAT", "mp3").strip().lower()
if AUDIO_FORMAT not in ("mp3", "m4a"):
    AUDIO_FORMAT = "mp3"
TG_LIMIT = 49 * 1024 * 1024

# web требует решения JS-challenge (медленно) — оставлен только как запасной вариант
CLIENTS = [c.strip() for c in os.getenv("YT_CLIENTS", "tv,web_embedded").split(",") if c.strip()]
URL_RE = re.compile(r"https?://\S+")
YT_RE = re.compile(r"(youtube\.com|youtu\.be|music\.youtube\.com)", re.I)

MAX_PARALLEL_DL = max(1, int(os.getenv("MAX_PARALLEL_DL", "3")))
DL_LOCK = asyncio.Semaphore(MAX_PARALLEL_DL)
SEARCH_SEMAPHORE = asyncio.Semaphore(3)
_last_finish = 0.0


def load_cookies() -> Path | None:
    dest = Path(tempfile.gettempdir()) / "yt_cookies_src.txt"
    b64 = os.getenv("YOUTUBE_COOKIES_B64", "").strip()
    path = os.getenv("YOUTUBE_COOKIES_FILE", "").strip()
    try:
        if b64:
            dest.write_bytes(base64.b64decode(b64))
            log.info("Cookies загружены из YOUTUBE_COOKIES_B64")
            return dest
        if path and Path(path).is_file():
            shutil.copy(path, dest)
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
SEARCH_CACHE_TTL = int(os.getenv("SEARCH_CACHE_TTL", str(6 * 3600)))
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
            "title TEXT, performer TEXT, downloaded_at REAL NOT NULL)"
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_downloads_time_video "
            "ON downloads(downloaded_at, video_id)"
        )
        self.db.commit()

    @staticmethod
    def track_key(video_id: str) -> str:
        return f"{video_id}:{AUDIO_FORMAT}"

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

    def record_download(self, video_id: str, title, performer, duration=None):
        if not video_id:
            return
        with self.lock:
            now = time.time()
            self.db.execute(
                "INSERT INTO downloads(video_id, title, performer, downloaded_at) VALUES (?,?,?,?)",
                (video_id, title, performer, now),
            )
            # Рейтинг использует только последние 7 дней — старые события не храним.
            self.db.execute("DELETE FROM downloads WHERE downloaded_at < ?", (now - 7 * 86400,))
            self.db.commit()

    def top_downloads(self, days: int = 7, limit: int = 10):
        since = time.time() - days * 24 * 60 * 60
        with self.lock:
            return self.db.execute(
                "SELECT video_id, MAX(title), MAX(performer), COUNT(*) AS total "
                "FROM downloads WHERE downloaded_at >= ? "
                "GROUP BY video_id ORDER BY total DESC, MAX(downloaded_at) DESC LIMIT ?",
                (since, limit),
            ).fetchall()

    def get_search(self, q: str):
        with self.lock:
            row = self.db.execute("SELECT results, created FROM searches WHERE q=?", (q,)).fetchone()
        if row and time.time() - row[1] < SEARCH_CACHE_TTL:
            return json.loads(row[0])
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


def video_id_of(url: str):
    m = VIDEO_ID_RE.search(url or "")
    return m.group(1) if m else None


def norm_query(q: str) -> str:
    return " ".join(q.lower().split())[:200]


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
        "format": "bestaudio[ext=m4a]/bestaudio/best",
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
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": AUDIO_FORMAT,
            "preferredquality": AUDIO_QUALITY,
        }],
        # compression_level 9 — быстрее кодирует mp3 (≈на 40%) при том же качестве
        "postprocessor_args": {"extractaudio": ["-compression_level", "9"]} if AUDIO_FORMAT == "mp3" else {},
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

    def _on_pp(d):
        if d.get("status") == "started":
            marks["pp_start"] = time.monotonic()
        elif d.get("status") == "finished":
            marks["pp_done"] = time.monotonic()

    opts["logger"] = _StageLogger()
    opts["progress_hooks"] = [_on_progress]
    opts["postprocessor_hooks"] = [_on_pp]
    target = query if is_url else f"ytsearch1:{query}"
    with yt_dlp.YoutubeDL(opts) as ydl:
        if prefetched is not None:
            info = ydl.process_ie_result(copy.deepcopy(prefetched), download=True)
        else:
            info = ydl.extract_info(target, download=True)
    end = time.monotonic()
    first = marks.get("first", t0)
    dl_done = marks.get("dl_done", first)
    pp_start = marks.get("pp_start", dl_done)
    pp_done = marks.get("pp_done", end)
    log.info(
        "ТАЙМИНГ: извлечение данных %.1f с | загрузка %.1f с | ffmpeg %.1f с | всего %.1f с",
        first - t0, dl_done - first, pp_done - pp_start, end - t0,
    )
    if info and "entries" in info:
        entries = [e for e in info["entries"] if e]
        info = entries[0] if entries else None
    files = list(workdir.glob(f"*.{AUDIO_FORMAT}"))
    return info, (files[0] if files else None)


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


def _search_tracks(query: str):
    """Получить до 10 результатов поиска без скачивания аудио."""
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
                    data = ydl.extract_info(f"ytsearch10:{query}", download=False)
                entries = (data or {}).get("entries") or []
                results = []
                for entry in entries:
                    if not entry:
                        continue
                    duration = entry.get("duration")
                    if duration and duration > MAX_DURATION:
                        continue
                    video_id = entry.get("id")
                    url = entry.get("url") or entry.get("webpage_url")
                    if not url or not str(url).startswith("http"):
                        if not video_id:
                            continue
                        url = f"https://www.youtube.com/watch?v={video_id}"
                    results.append({
                        "title": entry.get("title") or "Без названия",
                        "url": url,
                        "duration": duration,
                    })
                return results[:10]
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


async def send_generic_error(
    ctx: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str = "Произошла ошибка.",
):
    try:
        message = await ctx.bot.send_message(chat_id=chat_id, text=text)
        schedule_delete(ctx, message, 15)
    except Exception:
        log.exception("Не удалось отправить сообщение об ошибке")


async def show_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE, query: str):
    chat_id = update.effective_chat.id
    try:
        nq = norm_query(query)
        results = CACHE.get_search(nq) if CACHE else None
        if results:
            log.info("КЭШ: поиск «%s» из кэша (%d результатов)", nq, len(results))
        else:
            async with SEARCH_SEMAPHORE:
                loop = asyncio.get_running_loop()
                search_started = time.monotonic()
                results = await loop.run_in_executor(None, _search_tracks, query)
                log.info("ТАЙМИНГ: поиск %.1f с, результатов: %d", time.monotonic() - search_started, len(results))
            if results and CACHE:
                CACHE.put_search(nq, results)
        if not results:
            await send_generic_error(ctx, chat_id)
            return

        token = secrets.token_hex(4)
        ctx.user_data["music_search"] = {
            "token": token,
            "results": results,
            "created": time.time(),
        }
        lines = ["🎵 Выбери трек:"]
        buttons = []
        for i, item in enumerate(results, 1):
            duration = item.get("duration")
            time_label = f" · {int(duration // 60)}:{int(duration % 60):02d}" if duration else ""
            lines.append(f"{i}. {item['title']}{time_label}")
            buttons.append(InlineKeyboardButton(str(i), callback_data=f"pick:{token}:{i-1}"))
        keyboard = [buttons[i:i + 5] for i in range(0, len(buttons), 5)]
        result_message = await update.effective_message.reply_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        schedule_delete(ctx, result_message, 600)
        if PREFETCH_TOP:
            tasks = ctx.application.bot_data.setdefault("_cleanup_tasks", set())
            to_prefetch = [
                x["url"] for x in results[:PREFETCH_TOP]
                if not (CACHE and video_id_of(x["url"]) and CACHE.get_track(video_id_of(x["url"])))
            ]
            t = asyncio.create_task(_prefetch(to_prefetch))
            tasks.add(t)
            t.add_done_callback(tasks.discard)
    except Exception:
        log.exception("Ошибка поиска")
        await send_generic_error(ctx, chat_id)


async def process(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    query: str,
    error_text: str = "Произошла ошибка.",
):
    global _last_finish
    chat_id = update.effective_chat.id
    is_url = bool(URL_RE.fullmatch(query))
    if is_url and not YT_RE.search(query):
        await send_generic_error(ctx, chat_id, error_text)
        return

    vid = video_id_of(query) if is_url else None
    if vid and CACHE:
        row = CACHE.get_track(vid)
        if row:
            try:
                await ctx.bot.send_audio(
                    chat_id=chat_id, audio=row[0], title=row[1], performer=row[2],
                    duration=row[3], read_timeout=60, write_timeout=60,
                )
                if CACHE:
                    CACHE.record_download(vid, row[1], row[2], row[3])
                log.info("КЭШ: трек %s отправлен по file_id", vid)
                return
            except Exception as exc:
                log.warning("КЭШ: file_id для %s не сработал (%s), качаю заново", vid, str(exc)[:100])
                CACHE.drop_track(vid)

    async with DL_LOCK:
        if _last_finish and MAX_DELAY > 0:
            wait = _last_finish + random.uniform(MIN_DELAY, MAX_DELAY) - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)

        workdir = Path(tempfile.mkdtemp(prefix="dl_"))
        info = path = None
        last_err = ""
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
                    last_err = str(exc)
                    log.warning(
                        "Стратегия «%s» не сработала за %.1f с: %s",
                        name, time.monotonic() - attempt_started, last_err[:300]
                    )

            if not path:
                raise RuntimeError(last_err or "download failed")

            if path.stat().st_size > TG_LIMIT:
                raise RuntimeError("file exceeds Telegram upload limit")

            info = info or {}
            upload_started = time.monotonic()
            with open(path, "rb") as audio_file:
                sent = await ctx.bot.send_audio(
                    chat_id=chat_id,
                    audio=audio_file,
                    title=info.get("track") or info.get("title"),
                    performer=info.get("artist") or info.get("uploader"),
                    duration=int(info.get("duration") or 0) or None,
                    filename=f"{(info.get('title') or 'track')[:60]}.{AUDIO_FORMAT}",
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=30,
                    pool_timeout=30,
                )
            try:
                successful_video_id = vid or info.get("id")
                title = info.get("track") or info.get("title")
                performer = info.get("artist") or info.get("uploader")
                duration = int(info.get("duration") or 0) or None
                if CACHE and sent and sent.audio and successful_video_id:
                    CACHE.put_track(
                        successful_video_id, sent.audio.file_id, title, performer, duration,
                    )
                    CACHE.record_download(successful_video_id, title, performer, duration)
            except Exception as exc:
                log.warning("Не удалось сохранить в кэш: %s", exc)
            log.info("Отправка аудио в Telegram завершена за %.1f с (%.1f МБ)",
                     time.monotonic() - upload_started, path.stat().st_size / (1024 * 1024))
        except Exception:
            log.exception("Не удалось обработать или отправить трек")
            await send_generic_error(ctx, chat_id, error_text)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
            _last_finish = time.monotonic()


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
                    frame if r * 5 + c == index else b.text,
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


async def on_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    callback = update.callback_query
    data = ctx.user_data.get("music_search") or {}
    try:
        _, token, raw_index = callback.data.split(":")
        index = int(raw_index)
        if token != data.get("token") or time.time() - data.get("created", 0) > 600:
            await callback.answer()
            await send_generic_error(ctx, update.effective_chat.id)
            return
        if ctx.user_data.get("music_processing_token") == token:
            await callback.answer()
            return

        results = data.get("results") or []
        if index < 0 or index >= len(results):
            await callback.answer()
            await send_generic_error(ctx, update.effective_chat.id)
            return

        selected = results[index]
        await callback.answer("Загружаю…")
        ctx.user_data["music_processing_token"] = token
        picker = callback.message
        anim = asyncio.create_task(_animate_picker(picker, index)) if picker else None
        try:
            await process(
                update,
                ctx,
                selected["url"],
                error_text="Не удалось загрузить.",
            )
        finally:
            if anim:
                anim.cancel()
                try:
                    await anim
                except (asyncio.CancelledError, Exception):
                    pass
                try:  # вернуть окно выбора в исходный вид
                    await picker.edit_text(picker.text, reply_markup=picker.reply_markup)
                except Exception as exc:
                    log.debug("Не удалось восстановить окно выбора: %s", exc)
            if ctx.user_data.get("music_processing_token") == token:
                ctx.user_data.pop("music_processing_token", None)
    except (ValueError, AttributeError):
        try:
            await callback.answer()
        except Exception:
            pass
        await send_generic_error(ctx, update.effective_chat.id)


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "🎧 Привет! Я найду и пришлю любую песню.\n\n"
        "Просто напиши название или имя исполнителя — я покажу список, "
        "а ты выберешь нужный трек цифрой.\n"
        "Можно сразу прислать ссылку на YouTube.\n\n"
        f"В группе упомяни меня: @{ctx.bot.username} название песни"
    )


async def cmd_topmusic(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not CACHE:
        await update.effective_message.reply_text("Рейтинг временно недоступен.")
        return
    rows = CACHE.top_downloads(days=7, limit=10)
    if not rows:
        await update.effective_message.reply_text(
            "🎧 Топ музыки за 7 дней\n\nПока скачиваний недостаточно. Будь первым!"
        )
        return
    lines = ["🎧 Топ музыки | Top music", "За последние 7 дней:", ""]
    medals = ["🥇", "🥈", "🥉"]
    for position, (video_id, title, performer, total) in enumerate(rows, 1):
        marker = medals[position - 1] if position <= 3 else f"{position}."
        name = title or "Без названия"
        artist = f" — {performer}" if performer else ""
        lines.append(f"{marker} {name}{artist} · {total} скач.")
    await update.effective_message.reply_text("\n".join(lines))


async def cmd_music(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = " ".join(ctx.args).strip()
    if not query:
        await update.effective_message.reply_text("Напиши так: /music Кино — Группа крови")
        return
    if URL_RE.fullmatch(query):
        await process(update, ctx, query[:500])
    else:
        await show_search(update, ctx, query[:200])


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.text or (update.effective_user and update.effective_user.is_bot):
        return
    text = msg.text.strip()
    chat = update.effective_chat
    if chat.type != "private":
        uname = f"@{ctx.bot.username}"
        mentioned = uname.lower() in text.lower()
        if not GROUP_REACT_ALL and not mentioned:
            return
        text = re.sub(re.escape(uname), "", text, flags=re.I).strip()
        if not text or len(text) > 120 or text.count("\n") > 1:
            return
    if URL_RE.fullmatch(text):
        await process(update, ctx, text[:500])
    else:
        await show_search(update, ctx, text[:200])


async def update_ytdlp(ctx: ContextTypes.DEFAULT_TYPE):
    before = yt_dlp.version.__version__
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-U", "--no-cache-dir", "yt-dlp[default]"],
            check=True, capture_output=True, timeout=300,
        )
        out = subprocess.run(
            [sys.executable, "-c", "import yt_dlp.version as v;print(v.__version__)"],
            capture_output=True, text=True,
        ).stdout.strip()
        if out and out != before:
            log.info("yt-dlp обновлён %s → %s, перезапуск", before, out)
            os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        log.warning("Не удалось обновить yt-dlp: %s", e)


async def post_init(app: Application):
    await app.bot.set_my_commands([
        BotCommand("start", "Запустить бота"),
        BotCommand("music", "Найти музыку"),
        BotCommand("topmusic", "🎧 Топ музыки | Top music"),
    ])


def main():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .post_init(post_init)
        .build()
    )
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("music", cmd_music))
    app.add_handler(CommandHandler("topmusic", cmd_topmusic))
    app.add_handler(CallbackQueryHandler(on_pick, pattern=r"^pick:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    if app.job_queue:
        app.job_queue.run_repeating(
            update_ytdlp, interval=UPDATE_EVERY_HOURS * 3600, first=UPDATE_EVERY_HOURS * 3600
        )
    log.info("yt-dlp %s, стратегий: %d, PO-provider: %s",
             yt_dlp.version.__version__, len(STRATEGIES), POT_PROVIDER_URL or "нет")
    log.info("Бот запущен, polling начат")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
