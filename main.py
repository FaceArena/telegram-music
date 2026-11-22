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
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

import yt_dlp
from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
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
MAX_DURATION = int(os.getenv("MAX_DURATION", "1200"))  # секунд
MIN_DELAY = float(os.getenv("MIN_DELAY", "5"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "10"))
AUDIO_QUALITY = os.getenv("AUDIO_QUALITY", "128")  # kbps
GROUP_REACT_ALL = os.getenv("GROUP_REACT_ALL", "0") == "1"
POT_PROVIDER_URL = os.getenv("POT_PROVIDER_URL", "").strip()
UPDATE_EVERY_HOURS = int(os.getenv("UPDATE_EVERY_HOURS", "12"))
TG_LIMIT = 49 * 1024 * 1024

CLIENTS = ["tv", "web_embedded", "web"]  # android намеренно не используется
URL_RE = re.compile(r"https?://\S+")
YT_RE = re.compile(r"(youtube\.com|youtu\.be|music\.youtube\.com)", re.I)

DL_LOCK = asyncio.Lock()
_last_finish = 0.0


def load_cookies() -> Path | None:
    """Стратегия A: cookies из YOUTUBE_COOKIES_B64 или файла YOUTUBE_COOKIES_FILE."""
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
    """Каскад: A (cookies) + B (player_client) ; C (PO token) подключается автоматически,
    если задан POT_PROVIDER_URL."""
    s = []
    if COOKIES_SRC:
        s.append(("cookies + tv/web_embedded/web", True, CLIENTS))
        for c in CLIENTS:
            s.append((f"cookies + {c}", True, [c]))
    s.append(("без cookies: tv/web_embedded/web", False, CLIENTS))
    for c in CLIENTS:
        s.append((f"без cookies: {c}", False, [c]))
    return s


STRATEGIES = build_strategies()


def _download(query: str, is_url: bool, workdir: Path, use_cookies: bool, clients: list):
    opts = {
        "format": "bestaudio/best",
        "outtmpl": str(workdir / "%(id)s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "retries": 2,
        "socket_timeout": 30,
        "match_filter": match_filter_func(f"duration < {MAX_DURATION}"),
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": AUDIO_QUALITY,
            }
        ],
        "extractor_args": {"youtube": {"player_client": clients}},
        # JS-рантайм нужен новым версиям yt-dlp для расшифровки ссылок
        "js_runtimes": {"deno": {}},
        "remote_components": ["ejs:github"],
    }
    if POT_PROVIDER_URL:
        opts["extractor_args"]["youtubepot-bgutilhttp"] = {"base_url": [POT_PROVIDER_URL]}
    if use_cookies and COOKIES_SRC:
        cj = workdir / "cookies.txt"
        shutil.copy(COOKIES_SRC, cj)
        opts["cookiefile"] = str(cj)

    target = query if is_url else f"ytsearch1:{query}"
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(target, download=True)
    if info and "entries" in info:
        entries = [e for e in info["entries"] if e]
        info = entries[0] if entries else None
    files = list(workdir.glob("*.mp3"))
    return info, (files[0] if files else None)


def friendly_error(err: str) -> str:
    e = err.lower()
    if "sign in to confirm" in e or "not a bot" in e:
        return "YouTube заблокировал запрос с IP сервера. Нужны свежие cookies (см. README)."
    if "403" in e:
        return "YouTube вернул 403 (доступ запрещён)."
    if "private" in e or "unavailable" in e:
        return "Видео недоступно (приватное или удалено)."
    return "Не удалось скачать трек."


async def process(update: Update, ctx: ContextTypes.DEFAULT_TYPE, query: str):
    global _last_finish
    msg = update.effective_message
    is_url = bool(URL_RE.fullmatch(query))
    if is_url and not YT_RE.search(query):
        await msg.reply_text("Поддерживаются только ссылки YouTube. Напиши название трека.")
        return

    status = await msg.reply_text("🔎 Ищу…")
    async with DL_LOCK:
        # пауза между скачиваниями — чтобы не провоцировать блокировку
        if _last_finish:
            wait = _last_finish + random.uniform(MIN_DELAY, MAX_DELAY) - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)

        workdir = Path(tempfile.mkdtemp(prefix="dl_"))
        info = path = None
        last_err = ""
        try:
            loop = asyncio.get_running_loop()
            for i, (name, use_cookies, clients) in enumerate(STRATEGIES, 1):
                try:
                    await ctx.bot.send_chat_action(update.effective_chat.id, ChatAction.UPLOAD_VOICE)
                    log.info("Попытка %d/%d: %s | %s", i, len(STRATEGIES), name, query)
                    info, path = await loop.run_in_executor(
                        None, _download, query, is_url, workdir, use_cookies, clients
                    )
                    if path:
                        break
                    if info is None:
                        last_err = "ничего не найдено"
                        break
                    # info есть, файла нет — отфильтровано по длительности
                    await status.edit_text(
                        f"⏱ Трек длиннее {MAX_DURATION // 60} мин — не скачиваю."
                    )
                    return
                except Exception as e:  # noqa: BLE001
                    last_err = str(e)
                    log.warning("Стратегия «%s» не сработала: %s", name, last_err[:300])
                    if i < len(STRATEGIES):
                        try:
                            await status.edit_text(f"🔄 Пробую другой способ ({i}/{len(STRATEGIES) - 1})…")
                        except Exception:
                            pass

            if not path:
                text = "😕 Ничего не найдено." if last_err == "ничего не найдено" else (
                    "❌ " + friendly_error(last_err)
                )
                await status.edit_text(text)
                return

            if path.stat().st_size > TG_LIMIT:
                await status.edit_text("📦 Файл больше 50 МБ — Telegram не даёт отправить.")
                return

            if not info:
                info = {}
            await status.edit_text("📤 Отправляю…")
            with open(path, "rb") as f:
                await msg.reply_audio(
                    audio=f,
                    title=info.get("track") or info.get("title"),
                    performer=info.get("artist") or info.get("uploader"),
                    duration=int(info.get("duration") or 0) or None,
                    filename=f"{(info.get('title') or 'track')[:60]}.mp3",
                    read_timeout=180,
                    write_timeout=180,
                )
            await status.delete()
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
            _last_finish = time.monotonic()


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "Привет! Напиши название трека (например, «Кино — Группа крови») "
        "или пришли ссылку на YouTube — я отправлю mp3.\n"
        "В группах можно также: /music название"
    )


async def cmd_music(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = " ".join(ctx.args).strip()
    if not query:
        await update.effective_message.reply_text("Напиши так: /music Кино — Группа крови")
        return
    await process(update, ctx, query[:200])


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
        # в группах не реагируем на длинные «разговорные» сообщения
        if not text or len(text) > 120 or text.count("\n") > 1:
            return
    await process(update, ctx, text[:200])


async def update_ytdlp(ctx: ContextTypes.DEFAULT_TYPE):
    """Регулярное обновление yt-dlp; при новой версии — перезапуск процесса."""
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
    except Exception as e:  # noqa: BLE001
        log.warning("Не удалось обновить yt-dlp: %s", e)


def main():
    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("music", cmd_music))
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
