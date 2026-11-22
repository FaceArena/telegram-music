import asyncio
import base64
import logging
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
import secrets
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

import yt_dlp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction
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
TG_LIMIT = 49 * 1024 * 1024

# web требует решения JS-challenge (медленно) — оставлен только как запасной вариант
CLIENTS = ["tv", "web_embedded"]
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


def build_strategies():
    # Keep a small, purposeful fallback chain. Trying every client separately
    # can multiply one failed download into several long network timeouts.
    if COOKIES_SRC:
        return [
            ("cookies + tv/web_embedded", True, CLIENTS),
            ("cookies + web", True, ["web"]),
            ("без cookies + web", False, ["web"]),
        ]
    return [
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
            "preferredcodec": "mp3",
            "preferredquality": AUDIO_QUALITY,
        }],
        "extractor_args": {"youtube": {"player_client": clients}},
        "js_runtimes": {"deno": {}},
    }
    if POT_PROVIDER_URL:
        opts["extractor_args"]["youtubepot-bgutilhttp"] = {"base_url": [POT_PROVIDER_URL]}
    if use_cookies and COOKIES_SRC:
        cj = workdir / "cookies.txt"
        shutil.copy(COOKIES_SRC, cj)
        opts["cookiefile"] = str(cj)
    return opts


def _download(query: str, is_url: bool, workdir: Path, use_cookies: bool, clients: list):
    opts = _ydl_options(workdir, use_cookies, clients)
    target = query if is_url else f"ytsearch1:{query}"
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(target, download=True)
    if info and "entries" in info:
        entries = [e for e in info["entries"] if e]
        info = entries[0] if entries else None
    files = list(workdir.glob("*.mp3"))
    return info, (files[0] if files else None)


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
        async with SEARCH_SEMAPHORE:
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(None, _search_tracks, query)
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
                    info, path = await loop.run_in_executor(
                        None, _download, query, is_url, attempt_dir, use_cookies, clients
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
                await ctx.bot.send_audio(
                    chat_id=chat_id,
                    audio=audio_file,
                    title=info.get("track") or info.get("title"),
                    performer=info.get("artist") or info.get("uploader"),
                    duration=int(info.get("duration") or 0) or None,
                    filename=f"{(info.get('title') or 'track')[:60]}.mp3",
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=30,
                    pool_timeout=30,
                )
            log.info("Отправка MP3 в Telegram завершена за %.1f с (%.1f МБ)",
                     time.monotonic() - upload_started, path.stat().st_size / (1024 * 1024))
        except Exception:
            log.exception("Не удалось обработать или отправить трек")
            await send_generic_error(ctx, chat_id, error_text)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
            _last_finish = time.monotonic()


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
        try:
            await process(
                update,
                ctx,
                selected["url"],
                error_text="Не удалось загрузить.",
            )
        finally:
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
        "Привет! Напиши название трека — покажу до 10 вариантов. "
        "Выбери номер, и я отправлю mp3.\n"
        "Также можно прислать ссылку на YouTube. В группах: /music название"
    )


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


def main():
    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("music", cmd_music))
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
