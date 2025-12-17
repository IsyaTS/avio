# Client SPA Runbook

## Build (production)
```bash
cd apps/frontend/client-portal
npm ci
npm run build
```

Build output goes to `apps/api/static/spa/client/` and is served by FastAPI at `/static/spa/client/`.

## Dev (optional)
```bash
cd apps/frontend/client-portal
npm install
npm run dev -- --host 0.0.0.0 --port 5173
```
Then set:
```
VITE_DEV_SERVER_URL=http://localhost:5173
```
FastAPI will inject dev assets from the Vite server instead of the built manifest.

## URLs
- Client SPA: `/client/{tenant}/settings?k=...` (use `?legacy=1` to load old UI)
- Connect redirects:
  - `/connect/wa` → `#/channels/whatsapp`
  - `/connect/tg` → `#/channels/telegram`
  - `/connect/avito` → `#/channels/avito`

## Notes
- The SPA reads `client-settings-state` JSON embedded in the page.
- No secrets are stored in frontend code; API calls use `tenant` + `k` query params.
