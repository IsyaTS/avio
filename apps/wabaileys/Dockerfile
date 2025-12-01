FROM node:20-alpine AS deps
WORKDIR /app
COPY apps/wabaileys/package.json apps/wabaileys/package-lock.json ./
RUN npm ci --omit=dev

FROM node:20-alpine AS runner
WORKDIR /app
RUN apk add --no-cache curl
ENV NODE_ENV=production
COPY apps/wabaileys/package.json apps/wabaileys/package-lock.json ./
COPY --from=deps /app/node_modules ./node_modules
COPY apps/wabaileys/src ./src
EXPOSE 9002
CMD ["node","src/server.js"]
