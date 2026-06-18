---
name: avio-diff-explainer
description: Explain an Avio git diff by grouping files, identifying risk, and separating unrelated changes.
---

# Avio Diff Explainer

Use to explain current local changes in `/opt/avio-dev`.

## Workflow

1. List changed files with `git status --short`.
2. Group changes by `source`, `test`, `docs`, `migration`, `generated build`, `runtime data`, or `diagnostic artifact`.
3. Explain each file in simple language.
4. Identify unrelated changes and pre-existing dirty files.
5. Identify risky changes and required checks.
6. Suggest non-destructive rollback options or exact files to review.

## Rules

- Do not run destructive cleanup commands.
- Do not hide dirty state.
- Do not claim runtime is green unless checks were run and passed.
