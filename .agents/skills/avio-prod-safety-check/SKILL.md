---
name: avio-prod-safety-check
description: Verify Avio production identity and enforce prod-readonly safety before prod observations.
policy:
  allow_implicit_invocation: false
---

# Avio Prod Safety Check

Use only when the user explicitly asks for prod observation, prod checks, or prod deployment safety.

## Workflow

1. Verify prod identity:
   - IP includes `195.133.15.7`.
   - `pwd` is `/opt/avio`.
   - compose project is `avio`.
2. Treat prod as read-only by default.
3. Never run write-smoke on tenants `1` or `3`.
4. Sanitize logs before reporting.
5. Separate dev evidence from prod evidence.

## Rules

- If identity does not match, stop and say it is not prod.
- Do not edit prod unless the user explicitly asks for a prod fix/deploy.
- Do not delete data, reset state, or rotate tokens without exact approval.
