#!/bin/sh
# Обновляем yt-dlp при каждом запуске (YouTube часто меняет защиту)
pip install -U --no-cache-dir "yt-dlp[default]" bgutil-ytdlp-pot-provider || echo "update skipped"
exec python main.py
