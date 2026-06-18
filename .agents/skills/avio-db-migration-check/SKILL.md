---
name: avio-db-migration-check
description: Check Avio schema/data changes for migration, backfill, rollback, and tenant data risk.
policy:
  allow_implicit_invocation: false
---

# Avio DB Migration Check

Use only when the user explicitly asks about schema changes, data migrations, backfills, or database release safety.

## Workflow

1. Identify schema or data change.
2. Check whether migration and/or backfill is needed.
3. Check rollback plan.
4. Check tenant data risk.
5. Require explicit user approval before destructive operations.

## Rules

- Do not delete DB rows, drop columns/tables, truncate data, or mutate prod data without exact approval.
- Do not run write-smoke against prod tenants `1` or `3`.
- Report dev evidence and prod evidence separately.
