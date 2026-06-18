# Release Hygiene

Use these rules before release work, deploy preparation, build artifacts, generated files, and any task that may touch multiple scopes.

## Git, Dirty Worktree & Release Hygiene

- Before changes, record `git status --short`.
- If the worktree is already dirty, do not treat those changes as yours and do not revert them.
- Do not mix unrelated changes in one logical block. Documentation, tests, runtime refactor, UI, and deploy scripts should be separated at least in the final report, and preferably by commits/PRs when commits are requested.
- Do not deploy from an unknown dirty state.
- Before deploy, the diff must be understandable: what is new, what pre-existed, which files are generated, and which files were manually edited.
- Generated artifacts, backups, screenshots, dumps, and temporary smoke files must not enter git without an explicit reason.
- If diagnostic artifacts are needed, keep them outside tracked release scope or remove them after checks.
- Before any deploy/release report, separately list migrations, env changes, data backfill/cleanup, restart requirements, and rollback plan if any.

## Worktree Cleanliness Rules

- Before changes, classify expected files as `source`, `test`, `docs`, `migration`, `generated build`, `runtime data`, or `diagnostic artifact`.
- In the final report, group changes by those categories.
- Do not run generators or builds blindly.
- If a command creates assets, snapshots, dumps, logs, exports, or cache, first check `.gitignore` and expected output paths.
- Runtime/raw data must not appear as untracked release files.
- For Avito/dialog exports, use only `/data/tenants/{tenant_id}/uploads/dialogs/` or `dialogs/`; `dialogs/` must remain ignored and outside release scope.
- SPA build artifacts in `apps/api/static/spa/client/` are allowed only when UI changed and the build is part of the release.
- Old hashed SPA assets deleted by a new build must be either part of an intentional UI diff or listed in accepted deletions for release guard.
- After any command that may create files, run `git status --short --untracked-files=all`.
- Separate generated/runtime artifacts from source changes.
- Do not add new untracked directories to release accidentally.
- If an untracked directory is runtime/diagnostic output, add a precise `.gitignore` pattern.
- If it is a new source module, include it in the relevant release slice/pathspec.
- Never use destructive cleanup commands to make status look clean. If a file is not yours or its purpose is unclear, report it as a risk/question instead of deleting it.

## Release Scope Guard Rules

Before `python scripts/release_scope_guard.py --strict`, ensure:

- Raw exports and local diagnostics are ignored or removed.
- All modified/untracked source paths are covered by release slice manifests.
- Tracked deletions are restored or listed in `docs/release/<date>/accepted-tracked-deletions.txt`.
- `uncovered_dirty_paths=0`.
- `local_generated_candidates=0`.
- `unaccepted=0`.

Run:

```bash
python scripts/release_scope_guard.py
```

Use `--strict` only when preparing a release candidate or prod approval.

## Generated Artifacts Rules

- Build artifacts are not proof that source changed correctly; explain why they are included.
- Screenshots, logs, dumps, exports, and smoke outputs are diagnostic artifacts unless explicitly required for release.
- Do not commit secrets, tokens, raw payloads, customer messages, phone numbers, or private IDs in generated files.
- If generated files are unavoidable, state the command that created them and why they belong in the diff.

## Rollback Expectations

For release/deploy work, the final report should include:

- What can be reverted by git.
- What needs data rollback or manual cleanup.
- What services need restart.
- What env or migration changes must be undone.
- How to validate rollback with prod-readonly checks.
