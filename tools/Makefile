.PHONY: diag diag-verbose

# Run post-deploy diagnostics locally.
diag:
	AVIO_URL="$(AVIO_URL)" ADMIN_TOKEN="$(ADMIN_TOKEN)" ./scripts/diag.sh

# Run diagnostics with verbose Python logging.
diag-verbose:
	DIAG_VERBOSE=1 AVIO_URL="$(AVIO_URL)" ADMIN_TOKEN="$(ADMIN_TOKEN)" ./scripts/diag.sh

.PHONY: migrate

# Apply database migrations and display critical table schemas.
migrate:
	./scripts/migrate.sh
