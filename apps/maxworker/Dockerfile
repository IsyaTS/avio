FROM node:20-slim

WORKDIR /app/apps/maxworker

RUN apt-get update \
  && apt-get install -y --no-install-recommends xauth \
  && rm -rf /var/lib/apt/lists/*

COPY apps/maxworker/package.json ./
COPY apps/maxworker/package-lock.json ./
RUN npm ci --omit=optional
RUN npx playwright install --with-deps chromium

COPY apps/maxworker/ ./

EXPOSE 9010

CMD ["npm", "run", "start"]
