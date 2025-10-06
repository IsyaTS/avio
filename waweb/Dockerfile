FROM node:20-bullseye

ENV PUPPETEER_SKIP_DOWNLOAD=1 \
    CHROME_PATH=/usr/bin/chromium

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    chromium \
    fonts-liberation fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm i --omit=dev

COPY . .
EXPOSE 8088
CMD ["node","index.js"]
