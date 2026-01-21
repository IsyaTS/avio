-- Auth: tenants, users, tokens, sessions

CREATE TABLE IF NOT EXISTS auth_tenants (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  tenant_id BIGINT NOT NULL UNIQUE REFERENCES auth_tenants(id) ON DELETE CASCADE,
  is_verified BOOLEAN NOT NULL DEFAULT FALSE,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS user_tokens (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash TEXT NOT NULL,
  token_type TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  used_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  request_ip TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_tokens_hash ON user_tokens(token_hash);
CREATE INDEX IF NOT EXISTS idx_user_tokens_user ON user_tokens(user_id);

CREATE TABLE IF NOT EXISTS user_sessions (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  session_id_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  user_agent TEXT,
  ip TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_sessions_hash ON user_sessions(session_id_hash);
CREATE INDEX IF NOT EXISTS idx_user_sessions_user ON user_sessions(user_id);
