## Learning v1 — notes and boot commands

- Repo: `IsyaTS/avio`, branch `feature/learning-v1`.
- Dev stack already running; to start locally: `docker compose up -d app worker tgworker redis postgres` (optionally add `waweb`/`wabaileys` as needed). Check status: `docker compose ps`.

### Quick findings (step 1)
- Prompt/LLM build: `libs/core/sales_core/__init__.py` `build_llm_messages` assembles system blocks (persona, branding, catalog, training block) and `ask_llm` calls OpenAI via planner + direct fallback. Training examples are inserted via `training_retriever.build_examples_block`.
- Existing training retriever: `libs/core/training/retriever.py` uses TF-IDF indexes stored under `data/tenants/<id>/indexes/training_*.pkl` built from uploads via `libs/core/training/indexer.py`. Config per tenant via `tenant.json["learning"]`.
- Tenant/messages storage: PostgreSQL tables defined in `db/init/002_schema.sql` (`leads`, `messages`, `message_feedback`, `outbox`, etc.). Access layer in `libs/core/db.py` (`insert_message_*`, `fetch_dialogs_for_tenant`, `list_messages_for_lead`, `create_message_feedback`, etc.). Tenant is resolved per request with `_resolve_tenant_and_key` in `apps/api/web/client.py` (query/header/cookie key + tenant param).
- Dialogs UI: `apps/api/templates/client/settings.html` (tab “Диалоги”) with JS inline; endpoints configured in `apps/api/web/client.py` (`/api/dialogs`, `/api/dialogs/{lead_id}`, `/api/dialogs/{lead_id}/send`, `/api/feedback`). Static helpers also in `apps/api/static/js/client-settings.js`.
- Migrations style: plain SQL files under `db/migrations/*.sql`; base schema in `db/init/*.sql` (extensions/persona). Latest migration present: `20241210_add_feedback_and_bot_flag.sql`.
- Learning schema added: `db/migrations/20241211_learning_v1.sql` introduces `training_examples`, `bad_bot_messages`, `tenant_models`, expands `message_feedback` (expected_answer, lead_id) and attempts `pgvector` (extension absent in current image, so vector is skipped gracefully).
- Runtime commands (current dev stack): `docker compose up -d app worker tgworker redis postgres`; verify via `docker compose ps`. Migration applied via `docker compose exec postgres bash -lc "psql -U postgres -d postgres <<'SQL' ..."` (see migration file).
- Retrieval now prefers Postgres `training_examples` (per-tenant) via `build_examples_block_async`; falls back to legacy TF-IDF indexes if none. Embeddings worker (worker service) computes vectors when `OPENAI_API_KEY` present; otherwise TF-IDF path stays active.
- Fine-tune path: table `tenant_models` stores `finetune_model` and `use_finetune` (default false). `ask_llm` picks the tenant model only if the flag is enabled.

### Next steps (planned)
- Add Postgres tables for training examples/feedback corrections (per-tenant).
- Wire feedback endpoint to create training examples with required `expected_answer` for dislikes and mark bad bot replies.
- Add retrieval from Postgres (pgvector or TF-IDF fallback) into prompt; keep per-tenant isolation.
- Add background embedding worker + dataset export stubs; keep fine-tune flag disabled by default.
