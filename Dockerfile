# syntax=docker/dockerfile:1

######## waweb ########
FROM node:20-alpine AS waweb
WORKDIR /app

# системный Chromium и шрифты
RUN apk add --no-cache chromium nss freetype harfbuzz ttf-freefont

# не скачивать chromium при установке puppeteer
ENV PUPPETEER_SKIP_DOWNLOAD=1 \
    PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=1

# deps JS
COPY apps/waweb/package.json apps/waweb/package-lock.json* apps/waweb/.npmrc ./
RUN npm ci --omit=dev || npm i --omit=dev

# код
COPY apps/waweb/ .

# puppeteer будет использовать системный chromium
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium-browser
RUN [ -e /usr/bin/chromium-browser ] || ln -s /usr/lib/chromium/chrome /usr/bin/chromium-browser

EXPOSE 8088
CMD ["node","index.js"]

######## shared python base ########
FROM python:3.11-slim AS python-base
WORKDIR /app
ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        libglib2.0-0 \
        libsm6 \
        libxrender1 \
        libxext6 \
        libgl1 \
        libglu1-mesa \
        libjpeg62-turbo \
        libpng16-16 \
        libopenblas-dev \
        ocrmypdf \
        tesseract-ocr \
        tesseract-ocr-rus \
        ghostscript \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /data && chown -R $(id -u):$(id -g) /data
COPY requirements.txt ./
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt
COPY . .

######## app ########
FROM python-base AS app
WORKDIR /app
ENV PYTHONPATH=/app
EXPOSE 8000
HEALTHCHECK CMD curl -fsS http://localhost:8000/health || exit 1
CMD ["gunicorn","-k","uvicorn.workers.UvicornWorker","-w","4","-b","0.0.0.0:8000","apps.api.main:app","--timeout","60","--keep-alive","20"]

######## worker ########
FROM python-base AS worker
WORKDIR /app
ENV PYTHONPATH=/app
CMD ["python","-m","apps.worker.main"]
