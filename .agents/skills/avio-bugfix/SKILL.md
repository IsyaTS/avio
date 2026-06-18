---
name: avio-bugfix
description: Fix a focused Avio bug with reproduction, root-cause analysis, a small patch, and relevant checks.
---

# Avio Bugfix

Use for focused Avio bugs in `/opt/avio-dev`.

## Workflow

1. Reproduce the bug, or explain why reproduction is impossible.
2. Inspect related files and existing tests before editing.
3. Identify the likely root cause.
4. Write or update a focused test when possible.
5. Make the smallest fix that addresses the root cause.
6. Run relevant checks from `AGENTS.md` and `docs/codex/critical-verification-playbook.md`.
7. Explain the diff, remaining risks, and rollback path in simple language.

## Rules

- Default scope is dev-only unless the user explicitly asks for prod.
- Do not commit, push, deploy, delete data, or use destructive cleanup without explicit approval.
- Preserve pre-existing dirty worktree changes.
