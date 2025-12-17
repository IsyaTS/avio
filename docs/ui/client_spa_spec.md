# Client SPA Redesign Spec (PASS1)

## 1) Current Frontend Stack & Pages

### Templates (Jinja)
- Client settings: `apps/api/templates/client/settings.html`
- Connect pages:
  - Telegram: `apps/api/templates/connect/tg.html`
  - WhatsApp: `apps/api/templates/connect/wa.html`
  - Avito: `apps/api/templates/connect/avito.html`
- Layout: `apps/api/templates/layouts/base.html`

### JS/CSS
- Core client settings logic: `apps/api/static/js/client-settings.js`
- Bootstrapping state: `apps/api/static/js/boot.js`
- Catalog upload helper: `apps/api/static/js/catalog-upload.js`
- Follow-ups fallback: `apps/api/static/js/followups-inline.js`
- Connect JS:
  - Telegram: `apps/api/static/js/connect-tg.js`
- Styles: `apps/api/static/css/portal.css`

### Current client pages
- Settings: `GET /client/{tenant}/settings`
- Connect pages:
  - `GET /connect/wa?tenant={id}&k=...`
  - `GET /connect/tg?tenant={id}&k=...`
  - `GET /connect/avito?tenant={id}&k=...`

## 2) Endpoint Map (used by settings/connect pages)

### Settings & Persona
- `GET /pub/settings/get?tenant={id}&k={key}`
  - Response: `{ ok: true, cfg: {..}, persona: "..." }`
- `POST /pub/settings/save?tenant={id}&k={key}`
  - Body: `{ cfg?: object, persona?: string, passport?: object, behavior?: object, ... }`
  - Response: `{ ok: true }`
- `POST /client/{tenant}/settings/save?k={key}`
  - Body: `{ brand, agent, city, currency?, tone? }`
  - Response: `{ ok: true }`
- `POST /client/{tenant}/persona?k={key}`
  - Body: `{ text: string }`
  - Response: `{ ok: true }`

### Behavior (Avito auto-reply, smart-reply, silence triggers)
- `POST /client/{tenant}/behavior/save?k={key}`
  - Body:
    - `auto_reply: bool`
    - `auto_reply_text: string`
    - `avito_phone_tg_template: string`
    - `avito_smart_reply_enabled: bool`
    - `send_catalog_on_first_message: bool`
    - `triggers: [{ phrases: string[], channels: string[], silence: bool, notify: bool }]`
    - `photo_expected_markers: string[]`
    - `photo_expected_reply: string`
    - `photo_expected_ttl: number`
  - Response: `{ ok: true }`

### Follow-ups
- `GET /client/{tenant}/follow-ups?k={key}`
  - Response: `{ ok: true, rules: [...] }`
- `POST /client/{tenant}/follow-ups?k={key}`
  - Body: `{ rules: [{ channel, delay_minutes, text, max_attempts, active }] }`
  - Response: `{ ok: true, rules_saved: number }`

### Catalog (upload + CSV editor)
- `POST /pub/catalog/upload?tenant={id}&k={key}` (multipart file)
  - Response: `{ ok: true, job_id?, csv_path?, filename?, ... }`
- `GET /pub/catalog/status?tenant={id}&k={key}&job={job_id}`
  - Response: `{ state: 'queued'|'processing'|'done'|'failed', ... }`
- `GET /pub/catalog/csv?tenant={id}&k={key}`
  - Response: `{ ok: true, columns: string[], rows: string[][], csv_text, delimiter, path }`
- `POST /pub/catalog/csv?tenant={id}&k={key}`
  - Body: `{ columns: string[], rows: string[][] }`
  - Response: `{ ok: true, rows: number }`

### Training / Learning
- `POST /client/{tenant}/training/upload?k={key}` (multipart file)
  - Response: `{ ok: true, pairs: number, ... }`
- `GET /client/{tenant}/training/status?k={key}`
  - Response: `{ info, manifest, export_stats }`
- `GET /client/{tenant}/training/export?k={key}&days=...&limit=...&per=...`
  - Response: ZIP file

### Dialogs & Feedback
- `GET /api/dialogs?tenant={id}&k={key}`
  - Response: `[ { id, channel, title, contact, last_message, last_ts, unread } ]`
- `GET /api/dialogs/{lead_id}?tenant={id}&k={key}`
  - Response: `{ dialog_id, messages: [{ id, direction, text, ts, status, from_bot, feedbacked }] }`
- `POST /api/dialogs/{lead_id}/send?tenant={id}&k={key}`
  - Body: `{ text }`
  - Response: `{ ok: true, queued?: true, message?: {..} }`
- `POST /api/feedback?tenant={id}&k={key}`
  - Body:
    - like: `{ message_id, rating:'like' }`
    - dislike: `{ message_id, rating:'dislike', expected_answer }`
  - Response: `{ ok: true, feedback_id, already_exists? }`
- `GET /api/feedback/stats?tenant={id}&k={key}`
  - Response: `{ ok: true, counts: { like, dislike } }`

### Channels (connect/status)
- WhatsApp:
  - `GET /pub/wa/status?tenant={id}&k={key}`
  - `GET /pub/wa/start?tenant={id}&k={key}`
  - `GET /pub/wa/qr.svg?tenant={id}&k={key}`
- Telegram:
  - `GET /pub/tg/status?tenant={id}&k={key}`
  - `GET /pub/tg/start?tenant={id}&k={key}`
  - `GET /pub/tg/qr.png?tenant={id}&k={key}`
  - `POST /pub/tg/2fa?tenant={id}&k={key}`
  - `POST /pub/tg/logout?tenant={id}&k={key}` (if used)
- Avito:
  - `GET /v1/oauth/avito/status?tenant={id}&k={key}`
  - `GET /v1/oauth/avito/authorize?tenant={id}&k={key}` → `{ authorize_url }`
  - `POST /v1/oauth/avito/webhook?tenant={id}&k={key}`
  - `POST /v1/oauth/avito/disconnect?tenant={id}&k={key}`

## 3) Minimal Function Set for SPA

**Настройки**
- All existing fields from settings.html:
  - Паспорт бренда: brand, agent, city, currency, tone
  - Персона (textarea + save + download config)
  - Поведение: автоответ Avito, смарт-реплай, PDF каталог в TG, автоответ текст, TG шаблон, фото-маркеры, фото-ответ, TTL, silence triggers
  - Фоллоу-апы: list + add + save

**Каналы**
- WhatsApp: QR, refresh QR, status polling, copy link
- Telegram: QR, refresh, status polling, 2FA password
- Avito: status, connect (OAuth), refresh, disconnect

**Каталог**
- Upload (CSV/XLSX/PDF), status, CSV editor (view/edit rows, add row, save)

**Обучение**
- Training upload + status
- Dialogs (messenger UI) + feedback
- Export dialogs (WhatsApp archive)

**Статистика**
- Placeholder cards (future charts), feedback counts (like/dislike)

## 4) SPA Architecture

**Frontend**
- Vite + React + TypeScript + TailwindCSS
- Routing: react-router-dom (hash routes for tabs)
- Notifications: react-hot-toast
- Build output: `apps/api/static/spa/client/`

**Backend**
- Add Vite manifest helper to resolve CSS/JS (reads `static/spa/client/manifest.json`)
- New template: `apps/api/templates/client/spa.html`
  - `div#root`
  - `script#client-settings-state` with JSON
  - load SPA assets via helper
- Routes:
  - `GET /client/{tenant}/settings` → SPA by default, legacy with `?legacy=1`
  - `GET /connect/wa|tg|avito` → 302 redirect to `#/channels/{channel}` (legacy with `?legacy=1`)

**URL & State**
- tenant_id from path
- key from `k` query or cookie
- use `client-settings-state` JSON for URLs and config defaults

## 5) Migration Plan (PASS2)

1) Scaffold SPA in `apps/frontend/client-portal/`.
2) Add Tailwind + base theme tokens (white background, blue accents, black text).
3) Implement layout: sidebar + topbar + content cards + mobile drawer.
4) Implement tabs:
   - Настройки: passport/persona/behavior/followups
   - Каналы: Avito/TG/WA connection cards
   - Каталог: upload + CSV editor
   - Обучение: training upload + dialogs + feedback + export
   - Статистика: placeholder + feedback counts
5) Wire API calls to existing endpoints (no API changes).
6) Backend: serve SPA + manifest + redirects.
7) Add runbook: `docs/ui/client_spa_runbook.md`.

## 6) Acceptance Criteria
- Desktop: sidebar + topbar + card layout, blue accents, readable typography
- Mobile: burger menu, cards in single column, inputs readable
- SPA tabs switch without full reload; URL hash persists
- All existing features still work (settings save, persona, behavior, followups, catalog, training, dialogs, feedback, connect flows)
- Old URLs still reachable via `?legacy=1`
- No secrets committed

