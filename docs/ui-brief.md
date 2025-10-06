# UI/UX Brief · Admin & Client Surfaces

## 1. Current Surfaces
- **Admin dashboard (`/admin`)** – single-page HTML assembled inline in `app/web/admin.py`. Handles token login, tenant switching, key management, WA status shortcuts.
- **Client settings portal (`/client/{tenant}/settings`)** – inline template in `app/web/client.py` for brand/passport fields, persona editing, and raw JSON overrides.
- **WhatsApp connect screen (`/connect/wa`)** – inline template in `app/web/public.py` showing QR/status poller, tenant settings editor, catalog preview.
- **Ops console (`ops/app/templates/*.html`)** – Jinja templates served by `ops/app`, showing KPIs, lead tables, and conversation log. Styling via a minimal CSS file.
- **Public marketing site** – currently absent; tenants receive the functional `/connect/wa` screen instead of a polished marketing-facing experience.

## 2. Observed Pain Points
- **Inline templates** block reuse and make theming hard; CSS/JS duplicated across admin & client flows.
- **Visual debt**: inconsistent palettes, default system fonts, little hierarchy, no empty/loading states, poor spacing on wide screens.
- **Non-responsive layouts**: fixed widths (e.g., 860 px card) and grid assumptions break on mobile/tablet; no mobile nav or collapsible tables.
- **Accessibility gaps**: insufficient contrast messaging, no focus styles, text inside buttons/links lacking descriptive labels, heavy reliance on color.
- **Feedback & UX**: destructive actions (key delete, configuration save) lack confirmations or inline toasts. WA status polling offers little troubleshooting guidance.
- **Code duplication & brittleness**: repeated logic and copy-pasted scripts with bugs (e.g., duplicated `const cl` assignments in admin JS), no asset pipeline.

## 3. Personas & Core Journeys
- **Operations admin (internal)**: monitors WA queues, manages tenant keys, triages delivery issues. Needs quick status cues, filters, export tools.
- **Customer success / onboarding**: onboards new clients, tunes persona, shares WA connect portal. Needs guided forms, validation, walk-through.
- **Tenant representative (external client)**: connects WhatsApp device, adjusts prompts, reviews catalog. Needs trust-building UI, clear progress, responsive design.

## 4. UI Goals
- **Admin experience**: dashboard-level clarity (health cards, queue charts, recent errors), modular panels for keys, tenants, logs; dark theme OK but polish typography, spacing, iconography.
- **Client portal**: light, trustworthy look with onboarding checklist, contextual help, autosave, diff viewer for persona. Handle responsive gracefully.
- **Connect flow**: wizard-like guidance (scan QR → confirm → test message), include WA troubleshooting tips, CTA to download marketing assets.
- **Ops console**: richer analytics (filters, sparkline charts, lead search), conversation inspector with transcript grouping and tagging.
- **Design system**: shared tokens (colors, spacing, elevation), reusable components (cards, tabs, tables, alerts, buttons) across FastAPI & ops apps.

## 5. Deliverables & Scope Proposal
1. **Design audit & IA**: site map, component inventory, content hierarchy sketches (Figma). Output: annotated wireframes for admin, client, connect, ops.
2. **Design system foundation**: typography scale, color tokens for light/dark, spacing scale, states (hover/focus/disabled), icon set decision.
3. **Hi-fi mockups**: key screens for desktop + responsive variants, incl. admin overview, tenant detail, WA connect, client settings, marketing landing.
4. **Implementation plan**: choose stack (Tailwind build inside FastAPI, or React SPA), asset pipeline, migration path (template extraction, component partials).
5. **Rollout checklist**: accessibility QA, i18n handling (RU/EN), analytics hooks, telemetry additions, documentation updates.

## 6. Technical Considerations
- Extract HTML into Jinja templates (`app/templates/...`) and serve static assets; add build step (e.g., Vite or Tailwind CLI) in repo tooling.
- Consolidate JS into modules, avoid inline scripts; adopt Stimulus/Alpine or lightweight React depending on complexity.
- Introduce `ui/design-tokens.json` and SCSS/Tailwind config referencing shared tokens. Mirror tokens in ops panel.
- Plan for authentication refactor: session cookie instead of query tokens for client portal; guard routes consistently.
- Prepare API endpoints for richer data (metrics, logs) to support dashboards.

## 7. Open Questions
- Do we keep a dark theme for internal tools and light theme for client-facing, or offer theme toggle?
- Should the client-facing marketing site live inside this repo or in a separate marketing project?
- Do we invest in a component library (e.g., Chakra/Mantine) or stay with utility CSS for speed?
- Any regulatory or brand constraints (logos, fonts) from parent company that must be reflected?

## 8. Implementation Snapshot (Q4)
- **Shared tokens & assets**: добавлены `app/ui/design_tokens.json`, базовый лэйаут и глобальная тема в `app/static/css/main.css`, подключён StaticFiles в FastAPI.
- **Админка**: вынесена в шаблон `admin/dashboard.html`, добавлены метрики, управление ключами и живой мониторинг WA со скриптом `admin-dashboard.js`.
- **Клиентский портал**: создано оформление на основе вкладок (`client/settings.html`) + JS для сохранения `tenant.json`, persona и быстрых настроек.
- **Страница подключения WA**: редизайн (`public/connect_wa.html`, `public-connect.js`) с чек-листом и статусом устройства.
- **Ops-консоль**: обновлены шаблоны и стили для метрик и отображения диалогов (`ops/app/templates/*.html`, `static/style.css`).
- **Каталоги**: после загрузки файла фронт отображает путь к нормализованному CSV, количество позиций и превью первых колонок; при `items = 0` выводится предупреждение про OCR/стоп-слова.

Approval of this brief will unlock detailed design work (wireframes + token definition) and inform the tech stack decision for implementation.
