---
name: avio-release-checklist
description: Prepare an Avio release checklist with scope, checks, rollback, and safety gates.
policy:
  allow_implicit_invocation: false
---

# Avio Release Checklist

Use only when the user explicitly asks for release preparation or checklist work.

## Workflow

1. Classify release scope.
2. Check dirty worktree and separate pre-existing changes.
3. List migrations, env changes, generated assets, restarts, and data backfills.
4. Run `python scripts/release_scope_guard.py` or `--strict` when applicable.
5. List required smoke checks from `docs/codex/critical-verification-playbook.md`.
6. Produce rollback plan.
7. Do not deploy unless explicitly instructed.

## Rules

- Do not commit, push, merge, tag, or deploy without explicit user approval.
- Do not include secrets, raw payloads, phone numbers, or runtime dumps in release artifacts.
- Separate dev evidence from prod evidence.
