FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg curl ca-certificates unzip \
 && rm -rf /var/lib/apt/lists/*

# Deno — JS-рантайм для новых версий yt-dlp
RUN curl -fsSL -o /tmp/deno.zip \
      https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip \
 && unzip -o /tmp/deno.zip -d /usr/local/bin/ \
 && chmod +x /usr/local/bin/deno \
 && rm /tmp/deno.zip \
 && deno --version

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py start.sh ./
RUN chmod +x start.sh

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["./start.sh"]
