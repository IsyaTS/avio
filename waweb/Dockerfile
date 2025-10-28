FROM node:20-bullseye

ENV PUPPETEER_SKIP_DOWNLOAD=1 \
    PUPPETEER_CACHE_DIR=/data/.cache/puppeteer \
    CHROME_PATH=/usr/bin/google-chrome

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y curl gnupg ca-certificates \
    && install -d /usr/share/keyrings \
    && curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux.gpg] https://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y \
        google-chrome-stable \
        fonts-liberation fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /data/.cache/puppeteer

WORKDIR /app
COPY package.json package-lock.json* .npmrc ./
RUN npm ci --omit=dev

COPY . .
USER root
EXPOSE 8088
CMD ["node","index.js"]
