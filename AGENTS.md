# Repository Guidelines

## Project Structure & Module Organization
- `apps/api/` — FastAPI приложение: точка входа `main.py`, роуты в `web/`, шаблоны и статика.
- `apps/worker/` — фоновые обработчики и очереди (`main.py`, `http.py`, `outbox.py`).
- `apps/tgworker/` — отдельный Telegram transport (FastAPI + Telethon).
- `apps/waweb/` — WhatsApp bridge (Node.js + puppeteer).
- `libs/core/` — общая доменная логика, интеграции, репозитории, сервисы.
- `db/init/` — SQL-инициализация PostgreSQL.
- `tests/` — `pytest`-тесты.
- `data/tenants/<id>/` — конфиги тенантов, персоны, каталоги.

## Build, Test, and Development Commands
- `docker-compose up app worker waweb redis postgres` — поднять основной стек локально.
- `uvicorn apps.api.main:app --reload --port 8000` — быстрый запуск только API.
- `python -m apps.worker.main` — запуск воркера при API-only разработке.
- `cd apps/waweb && npm install && node index.js` — запуск WhatsApp bridge.
- `pytest tests -q` — запуск Python-тестов.
- `.venv/bin/pytest -q` — предпочтительный полный локальный pytest в подготовленном окружении.
- `.venv/bin/ruff check --select E,F --ignore E402,E501 apps/tgworker tests apps/api/web/public.py tests/conftest.py tests/test_main_webhook.py tests/test_public_tg.py scripts/inbox_worker_smoke.py scripts/restart_persistence_smoke.py scripts/critical_smoke.py` — lint как в CI.
- `.venv/bin/flake8 --select=E,F --extend-ignore=E402,E501 apps/tgworker tests apps/api/web/public.py tests/conftest.py tests/test_main_webhook.py tests/test_public_tg.py scripts/inbox_worker_smoke.py scripts/restart_persistence_smoke.py scripts/critical_smoke.py` — flake8 как в CI.
- `python scripts/monolith_guard.py` — обязательный gate против возврата крупных функций в guarded runtime surfaces; также фиксирует file-line budgets для бывших крупных entrypoint-файлов, чтобы они больше не росли.
- `python scripts/test_truth_audit.py tests` — аудит тестов на mock-heavy критичные проверки; для CI/release предпочтительно использовать fail-gate режим, если он включён в скрипте.
- `python scripts/release_scope_guard.py` — read-only отчёт по release slice manifests; перед prod используйте `--strict`.
- `python scripts/runtime_log_guard.py --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml --service app --service worker --tail 1200 --outbox-disabled` — read-only gate логов после runtime smoke.
- `python scripts/ui_http_smoke.py --base-url http://127.0.0.1:8000 --tenant 999999 --public-key "$PUBLIC_KEY"` — HTTP smoke ключевых UI-страниц без браузера.

## Coding Style & Naming Conventions
- Python: 3.11, 4 пробела, `snake_case` для функций, `PascalCase` для классов, явные type hints.
- Импорты: stdlib → third-party → local.
- Redis-ключи: lowercase + `:` (пример: `handoff:silence:101:<lead_id>`).
- JavaScript (`apps/waweb/`): CommonJS, camelCase, 2 пробела.

## Anti-Monolith Policy (Mandatory)
- Не добавляйте новую бизнес-логику в крупные агрегаторы (`apps/worker/main.py`, `apps/api/web/public.py`, `apps/api/web/client.py`, `apps/api/web/webhooks.py`), если это не точечный hotfix. Новую логику выносите в отдельные модули (`libs/core/services/*`, `libs/core/repo/*`, `apps/*/services/*`).
- Роуты и loop-обработчики должны быть «тонкими»: только валидация/авторизация/маршрутизация/вызов сервиса/маппинг ответа.
- Общая логика для нескольких приложений (`api`, `worker`, `tgworker`) должна жить в `libs/core/*`, без дублирования по `apps/*`.
- Если функция становится длиннее ~80 строк или тянет несколько внешних зависимостей, выносите её в отдельный модуль с понятным API.
- Для каждой нетривиальной новой функции или сценария обязателен тест (unit/smoke), чтобы рефакторинг не скатывался обратно в монолит.
- После изменений в guarded surfaces запускайте `python scripts/monolith_guard.py`; CI должен падать, если функция в этих зонах стала длиннее 80 строк или если бывший монолитный entrypoint превысил свой file-line budget.

## Architecture Boundary Rules (Mandatory)
- Любая новая бизнес-логика должна иметь понятную границу: service, repo, policy, normalizer, adapter или transport. Если границу нельзя назвать одним словом, сначала разрежьте задачу.
- `apps/*` отвечают за transport/runtime: HTTP, loop, dependency wiring, serialization. `libs/core/*` отвечает за доменную логику, решения, merge, persistence contracts и policy.
- Не дублируйте tenant resolution, config merge, queue payload shape, learning retrieval или response invocation в разных приложениях. Если логика нужна двум runtime, она должна жить в `libs/core/*`.
- Broad `except Exception` в критичных runtime-путях допустим только с явным structured log, tenant/channel/context полями и безопасным fallback result. Нельзя молча проглатывать события Avito, AmoCRM, queue или outbox.
- Новые “rules” в `AGENTS.md` по возможности должны иметь исполняемый guard: test, smoke, lint script или CI step. Если guard пока не добавлен, явно укажите это в финальном отчёте как остаточный риск.

## Testing Guidelines
- Фреймворк: `pytest` (+ `pytest-asyncio` для async-кейсов).
- Имена файлов: `test_*.py`.
- Новые изменения в интеграциях покрывайте unit/smoke-тестами с моками внешних API.
- Для queue/db сценариев используйте поднятые `redis` и `postgres` из `docker-compose`.
- Тесты размечаются маркерами `unit`, `integration`, `e2e`, `prod_readonly` из `pytest.ini`.
- Не считать `pytest` достаточной проверкой для критичных сценариев, если не запускались соответствующие truth/smoke проверки ниже.
- Мокать можно внешние границы (Avito API, OpenAI, Telegram/WA transport), но для интеграционных проверок не мокайте собственную маршрутизацию, merge настроек, Redis queue и response pipeline без необходимости.
- Критичные тесты должны проверять наблюдаемый продуктовый результат: persisted config, queue item, DB row, outbox payload, prompt content, API response или UI-visible state. Проверка только “функция была вызвана” не считается достаточной.
- Если тест использует много mocks вокруг критичного сценария, рядом должен быть truth/integration/smoke тест, который проходит через реальную внутреннюю маршрутизацию.
- Новые тестовые helpers не должны становиться второй реализацией бизнес-логики. Они могут собирать fixtures, но не должны повторять production decision tree.
- Warnings не скрывайте ради “green”. Если warnings не относятся к изменённой поверхности, зафиксируйте их в финальном отчёте; если появились из-за текущего изменения, исправьте или объясните blocker.

## Evidence & Reporting Rules
- Не пишите “работает”, “green”, “стабильно” или “проверено”, если нет точной команды и результата. Минимальный формат evidence: команда, окружение, результат pass/fail, что именно она доказывает.
- Финальный ответ после инженерной задачи должен содержать:
  - что изменено и какие файлы/модули затронуты;
  - какие проверки прошли;
  - что не проверялось и почему;
  - dev evidence и prod evidence отдельно;
  - что ещё остаётся.
- Если prod не проверялся на `195.133.15.7:/opt/avio`, прямо пишите “prod не проверялся”. Нельзя переносить выводы dev на prod.
- Если задача только документационная, не запускайте тяжёлые runtime smoke без необходимости; достаточно проверить, что изменённый документ читается и не противоречит существующим rules.
- Если пользователь просит аудит/review, выводите сначала findings по severity с файлами/строками, затем assumptions, затем краткое резюме. Не смешивайте audit findings с планом реализации.

## Critical Verification Playbook

### Environment Identity / Prod vs Dev (Mandatory)
- Настоящий prod: сервер `195.133.15.7`, пользователь `deploy`, рабочая директория `/opt/avio`, compose project `avio`.
- Dev/staging: сервер `72.56.87.229` и/или директория `/opt/avio-dev`. Нельзя считать его prod даже если контейнеры называются `avio-app-1`, `avio-worker-1`, `avio-postgres-1`.
- Каждую задачу сначала классифицируйте как `dev-only`, `prod-readonly` или `prod-deploy`. Если пользователь не просил prod, работа по умолчанию `dev-only`.
- Перед любым действием, которое пользователь называет “prod”, обязательно сначала подтвердить окружение:
  - `hostname -I` или внешний IP должен включать `195.133.15.7`;
  - `pwd` должен быть `/opt/avio`;
  - `docker inspect avio-app-1 --format '{{json .Config.Labels}}'` должен показывать `com.docker.compose.project=avio` и `com.docker.compose.project.working_dir=/opt/avio`;
  - `docker ps` должен показывать prod-контейнеры на этом сервере.
- Если любой из этих пунктов не совпал, остановиться и явно сказать пользователю, что это не prod. Не делать выводы о prod по данным из `/opt/avio-dev`.
- Для prod-проверок и prod-фиксов подключаться к `deploy@195.133.15.7` и работать только в `/opt/avio`, если пользователь не дал другое явное указание.
- Не переносить выводы dev-smoke на prod. Формулировка “green” для prod допустима только после prod-readonly проверок на `195.133.15.7:/opt/avio`.
- Если на dev-сервере есть директория `/opt/avio`, не считать её prod без доказательства IP `195.133.15.7` и compose project `avio`.

### Tenant settings / Avito auth persistence
- Риск: слетают настройки tenant, `behavior.avito_smart_reply_enabled`, persona, follow-up, Avito tokens.
- Обязательные тесты после изменений в settings/client/public/tenant runtime:
  - `.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_tenant_runtime_atomic.py tests/test_tenant_config_merge.py tests/test_client_settings.py tests/test_public_settings.py -q`
  - `python scripts/critical_smoke.py --base-url http://127.0.0.1:8000 --tenants 1,3`
  - `python scripts/critical_smoke.py --base-url http://127.0.0.1:8000 --tenants 1,3 --mode test-tenant-write --write-tenant 999999 --public-key "$PUBLIC_KEY"`
  - `python scripts/restart_persistence_smoke.py --base-url http://127.0.0.1:8000 --tenant 999999 --public-key "$PUBLIC_KEY" --services app --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml`
- После `restart_persistence_smoke.py` дождитесь `docker compose ps app worker redis postgres`, чтобы `app` и `worker` были healthy.

### Avito OAuth
- Риск: `invalid_state`, `missing_state`, неправильный host/proto/cookie domain, потеря Redis state.
- Обязательные тесты после изменений в OAuth:
  - `.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_public_settings.py tests/test_avito_oauth.py -q`
- Ключевой тест: `test_avito_oauth_callback_persists_tokens_with_signed_state_after_redis_loss`.
- Не удаляйте Redis state вручную как “fix”, пока не проверили signed state path.

### Avito incoming / worker / outbox
- Риск: webhook попал в Redis, но worker не создал ответ; или тесты проверяют только мок-функцию.
- Обязательные тесты после изменений в `apps/worker/main.py`, Avito integration, queues:
  - `.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_worker_avito_send.py tests/test_worker_incoming.py -q`
  - live stack smoke:
    ```bash
    docker compose -f docker-compose.yml -f compose/ci/docker-compose.yml up -d app worker redis postgres
    python scripts/inbox_worker_smoke.py --base-url http://127.0.0.1:8000 --tenant 999999 --public-key "$PUBLIC_KEY" --compose-file docker-compose.yml --compose-file compose/ci/docker-compose.yml
    docker compose up -d app worker
    ```
- `compose/ci/docker-compose.yml` ставит `OUTBOX_ENABLED=0`; это нужно, чтобы worker создал payload в `outbox:send`, но не отправлял его во внешние API.
- При `OUTBOX_ENABLED=0` outbox loop не должен потреблять очередь. Если smoke не видит payload, проверьте `docker compose logs worker`.

### Learning / self-training
- Риск: обучение включено, но примеры не доходят до ответа или остаются только в shadow.
- Обязательные тесты после изменений в `libs/core/learning/*`, `libs/core/training/*`, `libs/core/response_pipeline.py`:
  - `.venv/bin/pytest tests/test_truth_critical_flows.py tests/test_learning_policy_v2.py tests/test_learning_feedback.py tests/test_learning_manager_capture_hooks.py -q`
  - `python scripts/dialog_quality_runner.py --cases scripts/dialog_quality_ci_cases.json --iterations 1`
- Ключевой тест: `test_learning_examples_from_db_reach_response_pipeline_prompt`.
- Проверяйте не только capture/finalize, но и то, что examples block попал в system prompt.

### Message pipeline / answer quality
- После изменений в sales core, response pipeline, prompt/guard логике:
  - `.venv/bin/pytest tests/test_sales_policy_guard.py tests/test_sales_engine.py tests/test_brain_quality.py tests/test_truth_critical_flows.py -q`
  - `python scripts/message_pipeline_smoke.py`
  - `python scripts/dialog_quality_runner.py --cases scripts/dialog_quality_ci_cases.json --iterations 1`

### Prod checks
- На prod не запускать write-smoke на tenant `1`/`3`.
- Безопасный минимум после деплоя:
  - `python scripts/critical_smoke.py --base-url https://avio.website --tenants 1,3 --mode readonly`
  - проверить `/internal/health/deep?tenants=1,3` с `X-Admin-Token`;
  - вручную сверить в UI Avito connected/configured и learning enabled/apply mode для tenant `1` и `3`.
- Логи перед публикацией санитайзить: не показывать access/refresh tokens, телефоны, raw payloads.

## Commit & Pull Request Guidelines
- Коммиты: императивный стиль, коротко и предметно (например: `Fix amoCRM chat link reconciliation`).
- PR должен содержать:
  - цель изменений и затронутые модули;
  - шаги деплоя/миграции (если есть);
  - результаты проверок (`pytest`, smoke, логи);
  - скриншоты/логи для UI и интеграционных фиксов.

## Security & Configuration Tips
- Секреты храните только в `.env`; не коммитьте реальные ключи/токены.
- Перед публикацией логов удаляйте телефоны, user id, access tokens, payloads.
- Сбросы интеграций выполняйте через API/сервисные процедуры, а не удалением файлов состояния вручную.
- Не делайте `source .env` вслепую: в этом проекте могут быть readonly shell vars вроде `UID`. Для smoke-параметров извлекайте конкретные значения (`PUBLIC_KEY`, `ADMIN_TOKEN`) через безопасный parser/`awk`/`grep`, не экспортируя весь `.env`.
- Не печатайте `.env`, OAuth callback query с code/state, Avito tokens, refresh tokens, amoCRM tokens, телефоны или raw customer messages в ответ пользователю.

## Git, Dirty Worktree & Release Hygiene
- Перед изменениями фиксируйте `git status --short`. Если worktree уже dirty, не считайте эти изменения своими и не откатывайте их.
- Не смешивайте несвязанные изменения в одном логическом блоке. Документация, tests, runtime refactor, UI и deploy scripts должны быть разделены хотя бы на уровне отчёта, а при commit/PR — на уровне отдельных commits/PRs, если это возможно.
- Не деплойте из неизвестного dirty state. Перед deploy должен быть понятен diff: что новое, что было до вас, какие файлы generated, какие ручные.
- Generated artifacts, backups, screenshots, dumps и временные smoke файлы не должны попадать в git без явной причины. Если они нужны для диагностики, храните их вне tracked tree или удаляйте после проверки.
- Перед любым deploy/release отчёт должен отдельно перечислять migrations, env changes, data backfill/cleanup, restart requirements и rollback plan, если они есть.

### Worktree Cleanliness Rules
- Перед любым изменением классифицируйте ожидаемые файлы как `source`, `test`, `docs`, `migration`, `generated build`, `runtime data` или `diagnostic artifact`. В финальном отчёте группируйте изменения по этим категориям.
- Не запускайте генераторы/сборки вслепую. Если команда создаёт assets, snapshots, dumps, logs, exports или cache, заранее проверьте `.gitignore` и ожидаемый output path.
- Runtime/raw данные не должны появляться как untracked release files. Для Avito/dialog exports используйте только `/data/tenants/{tenant_id}/uploads/dialogs/` или `dialogs/`, причём `dialogs/` должен оставаться ignored и не должен входить в release scope.
- SPA build artifacts в `apps/api/static/spa/client/` допустимы только если UI реально менялся и build входит в релиз. Старые hashed assets, удалённые новой сборкой, должны быть либо частью осознанного UI diff, либо добавлены в accepted deletions для release guard.
- После любой команды, которая могла создать файлы (`npm run build`, export/download scripts, smoke с артефактами, snapshots), сразу запускайте `git status --short --untracked-files=all` и отделяйте generated/runtime artifacts от source changes.
- Не добавляйте новые untracked директории в release случайно. Если директория является runtime/diagnostic output, добавьте точечный `.gitignore` pattern. Если это новый source module, включите его в релевантный release slice/pathspec.
- Перед `python scripts/release_scope_guard.py --strict` убедитесь, что:
  - raw exports и локальные диагностики ignored или удалены;
  - все modified/untracked source paths покрыты release slice manifests;
  - tracked deletions либо восстановлены, либо перечислены в `docs/release/<date>/accepted-tracked-deletions.txt`;
  - `uncovered_dirty_paths=0`, `local_generated_candidates=0`, `unaccepted=0`.
- Нельзя чистить worktree разрушительными командами (`git reset --hard`, `git checkout --`, массовый `rm`) ради красивого статуса. Если файл не ваш или его назначение неясно, зафиксируйте это как риск/вопрос, а не удаляйте.

## Codex Stabilization Rules

These rules define future Codex behavior when improving stability, refactoring AI-code, auditing tests, or bringing the project to a stable green state.

### Mission
You are working on Avio, a production product with real tenants and real integrations. Your job is not to make broad cosmetic refactors. Your job is to make the system measurably more stable without breaking existing behavior.

Primary goals:
- tenant settings and Avito authorization must not disappear after restarts, cleanup, deploys, or partial failures;
- Avito OAuth must not fail with `invalid_state` / `missing_state` when the signed state fallback can recover the flow;
- incoming messages must travel through the real route: webhook/API -> Redis/DB -> worker -> response pipeline -> messages/outbox/UI;
- self-learning must affect responses only when relevant examples reach the prompt and must not silently degrade answer quality;
- tests must prove real product behavior, not only mocked helper calls;
- prod and dev must never be confused.

### Non-Negotiable Safety Rules
- Never treat `/opt/avio-dev` as prod. Prod is only `195.133.15.7:/opt/avio` with compose project `avio`.
- Do not edit prod unless the user explicitly asks to deploy or fix prod. Prod diagnosis must be read-only by default.
- Do not run write-smoke against prod tenant `1` or `3`.
- Do not delete Redis keys, tenant files, Avito tokens, DB rows, Docker volumes, or upload data as a “fix” unless the user explicitly approves the exact destructive action.
- Do not use `git reset --hard`, `git checkout --`, or destructive cleanup commands to hide dirty state.
- Do not add new business logic to `apps/worker/main.py`, `apps/api/web/public.py`, `apps/api/web/client.py`, or `apps/api/web/webhooks.py` except for a minimal hotfix. Move new behavior into focused modules under `libs/core/services/*`, `libs/core/repo/*`, or smaller `apps/*/services/*`.
- Do not declare “green” after only unit tests if the changed behavior is a runtime integration, queue, persistence, OAuth, or worker path.
- Do not claim prod works based on dev tests. Prod needs prod-readonly checks.
- Sanitize logs before reporting: no access/refresh tokens, no phone numbers, no raw customer payloads.

### Evidence Discipline
- Treat “green” as an evidence claim, not a feeling. Every green claim must map to commands, environment identity, and observable results.
- If a command was skipped, say why. Acceptable reasons: unrelated surface, docs-only change, missing secret, external service unavailable, user explicitly limited scope.
- If a smoke/test passes only because external calls are mocked or disabled, state that boundary. Example: `OUTBOX_ENABLED=0` proves payload creation, not real Avito delivery.
- For runtime incidents, collect evidence from both state and logs: DB/Redis/API result plus app/worker logs. One without the other is incomplete.
- Never hide failing checks behind partial success. Report the failing command, the key failure line, and the smallest next action.

### Required Working Style
Work without intermediate summaries when the user asks to “do it end to end”, but still give short progress updates during long work. Continue until one of these is true:
- implementation is complete and all relevant checks are green;
- a real blocker exists and is documented with exact failing command/output summary;
- the user redirects the task.

Always start by establishing a baseline:
1. `pwd`, `hostname -I`, `git status --short`.
2. Identify whether the task is dev-only, prod-readonly, or prod-deploy.
3. Map the changed behavior to the required verification playbook above.
4. Inspect existing modules/tests before adding anything new.

For large changes:
- Prefer vertical behavior-preserving slices, but do not stop after every small extraction if the user asked to continue. Batch compatible slices only when their write scopes are clear and tests can isolate regressions.
- Never rewrite a whole monolith in one pass. Extract stable boundaries first, keep old entry points as thin adapters, then remove dead code after tests pass.
- If a refactor changes public behavior, call it a feature/fix, not a pure refactor, and run the corresponding runtime smoke.

### Senior-Grade Standard
The target is not “more files” or “less AI-looking code”. The target is a codebase a senior engineer can trust because:
- critical product paths have explicit contracts and tests;
- runtime behavior is proven by smoke/truth checks, not only mocks;
- domain logic has clear ownership and is not hidden in route handlers or loops;
- deploy and rollback steps are known;
- logs are useful for diagnosis without leaking secrets;
- prod evidence is separated from dev evidence;
- remaining risks are documented instead of being implied away.

Do not claim the project is “senior-grade”, “stable”, or “fully fixed” unless the relevant Definition of Green below is satisfied and any remaining risks are explicitly listed.

### Stabilization Roadmap

#### Phase 1: Source-of-truth and environment hygiene
- Make the active workspace explicit in docs and final reports.
- Keep dev, prod, baseline, and backup directories clearly separated:
  - dev/staging: `72.56.87.229:/opt/avio-dev`;
  - prod: `195.133.15.7:/opt/avio`;
  - old/baseline copies are not active deploy targets unless the user says so.
- Do not make conclusions from `/opt/avio` on the dev server unless you first prove it is the active compose working directory.
- Before large changes, record dirty files and distinguish user changes from your own changes.

#### Phase 2: Test truthfulness audit
- For each critical test, classify it:
  - unit: pure logic only;
  - integration: real in-process path with DB/Redis or app route;
  - smoke: running stack path;
  - prod_readonly: safe production observation.
- Flag tests that mock the system under test instead of external boundaries.
- For Avito incoming, ensure at least one test/smoke proves webhook -> queue -> worker -> message/outbox.
- For tenant settings, ensure restart persistence is verified against the real config merge path and DB/file fallback.
- For learning, ensure examples from DB enter the actual response pipeline prompt.
- For OAuth, ensure callback persists tokens even after Redis state loss through signed state recovery.

#### Phase 3: Critical persistence hardening
- Tenant config writes must be atomic.
- Tenant config reads must merge defaults, DB-backed config, and file-backed config deterministically.
- Avito tokens/settings must not be overwritten by partial UI saves.
- Follow-up settings, persona, smart reply toggles, and learning settings must survive app/worker restarts.
- Add focused regression tests before or with the fix.

#### Phase 4: Runtime pipeline hardening
- Split worker/webhook logic out of monoliths only along real behavioral boundaries:
  - Avito incoming normalization and tenant resolution;
  - queue enqueue/dequeue contracts;
  - response pipeline invocation;
  - outbox send and status update;
  - AmoCRM chat sync;
  - learning capture/finalize/retrieve.
- Replace silent broad `except Exception` in critical paths with structured logging and explicit fallback results.
- Add tenant/channel/lead/stage fields to logs where useful, but do not log sensitive payloads.

#### Phase 5: Answer quality and learning
- Keep learning retrieval separate from policy application.
- Do not mark all examples as good by default if there is negative feedback for the source message.
- Add or preserve tests that prove:
  - bad examples are excluded;
  - low-similarity examples do not drive answers;
  - selected examples are visible in the prompt;
  - answer quality runner catches irrelevant CTA, forbidden unsupported claims, and repeated useless questions.

#### Phase 6: CI and deployment gates
- Keep CI fast but meaningful:
  - lint for touched Python surfaces;
  - full pytest when feasible;
  - dialog quality CI cases;
  - compose smoke for app/worker/redis/postgres.
- Keep architecture gates executable:
  - `python scripts/monolith_guard.py` for guarded surfaces;
  - `python scripts/test_truth_audit.py tests` for critical test honesty;
  - `python scripts/release_scope_guard.py --strict` before prod release candidate approval;
  - `python scripts/runtime_log_guard.py` after runtime smoke for critical app/worker logs;
  - `python scripts/ui_http_smoke.py` for login/register/settings/connect page availability;
  - add/extend scripts when a repeated manual rule can be checked reliably.
- Deploy only after local/dev green.
- After deploy, run prod-readonly checks and report exact commands/results.
- If prod and dev differ, say so directly and do not infer.

### Definition of Green
For a narrow pure-code change:
- relevant unit tests pass;
- lint target from this AGENTS file passes for touched surfaces.
- `python scripts/monolith_guard.py` passes if guarded surfaces were touched.

For tenant settings / auth / OAuth changes:
- required tests from “Tenant settings / Avito auth persistence” and “Avito OAuth” pass;
- restart persistence smoke passes on a test tenant;
- no write-smoke is run on prod tenant `1` or `3`.

For incoming/worker/outbox changes:
- required worker tests pass;
- live stack `inbox_worker_smoke.py` passes with `OUTBOX_ENABLED=0`;
- worker logs show no `unknown_tenant`, unhandled exception loop, or consumed outbox when it should be disabled.

For learning/response changes:
- required learning tests pass;
- `dialog_quality_runner.py` passes;
- at least one test proves DB examples reach the system prompt;
- bad/irrelevant examples are not applied as strong guidance.

For test-suite or CI changes:
- the changed tests fail for the old broken behavior or are justified as coverage/gate improvements;
- `python scripts/test_truth_audit.py tests` reports no unreviewed critical mock-heavy tests, or each finding is documented with a follow-up;
- no critical runtime scenario is downgraded from smoke/integration to unit-only coverage.

For production:
- app/worker are healthy;
- `critical_smoke.py --mode readonly` passes for tenants `1,3`;
- `/internal/health/deep?tenants=1,3` is checked with `X-Admin-Token`;
- UI-visible Avito configured/connected and learning apply mode are manually or programmatically verified where possible;
- final answer clearly separates dev checks from prod checks.

For documentation-only changes:
- the edited instructions are consistent with the active repo layout and commands;
- no runtime “green” claim is made unless runtime checks were actually run;
- final answer says that runtime was not changed.
