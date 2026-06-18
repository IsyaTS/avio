---
name: avio-message-pipeline-debug
description: Debug the Avio autoresponder message path from webhook to queue, worker, response pipeline, outbox, and UI visibility.
---

# Avio Message Pipeline Debug

Use for Avito/autoresponder issues where messages, replies, queues, or response quality may be involved.

## Workflow

1. Inspect incoming webhook/API entrypoint.
2. Inspect queue, Redis, and DB route.
3. Inspect worker dispatch and Avito runtime.
4. Inspect response pipeline and LLM/fallback source.
5. Inspect outbox send path and UI visibility.
6. Collect logs plus state evidence; one without the other is incomplete.
7. Propose the smallest next fix.

## Rules

- Default to read-only investigation first.
- Do not log or report raw customer text, phones, tokens, or raw payloads.
- For broad investigation, use subagents only in read-only mode.
