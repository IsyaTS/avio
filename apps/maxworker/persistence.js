'use strict';

const fs = require('fs');
const path = require('path');

function tenantSessionDir(baseDir, tenant) {
  const cleanedTenant = String(tenant).replace(/[^a-zA-Z0-9_-]/g, '');
  return path.join(String(baseDir || ''), `tenant-${cleanedTenant}`);
}

function sessionMetadataPath(sessionDir) {
  return path.join(String(sessionDir || ''), 'avio-session.json');
}

function persistSessionMetadata(state) {
  if (!state || !state.sessionDir) return;
  fs.mkdirSync(state.sessionDir, { recursive: true });
  const payload = {
    tenant: Number(state.tenant || 0),
    callback_url: String(state.callbackUrl || '').trim(),
    webhook_token: String(state.webhookToken || '').trim(),
    account: state.account && typeof state.account === 'object' ? state.account : {},
    last_status: String(state.status || '').trim(),
    auth_probe_failures: Number(state.authProbeFailures || 0),
    restore_grace_until: Number(state.restoreGraceUntil || 0),
    last_inbound_at: Number(state.lastInboundAt || 0),
    last_outbound_at: Number(state.lastOutboundAt || 0),
    last_chat_id: String(state.lastChatId || '').trim(),
    updated_at: Date.now(),
  };
  fs.writeFileSync(sessionMetadataPath(state.sessionDir), JSON.stringify(payload, null, 2));
}

function readSessionMetadata(sessionDir) {
  try {
    const raw = fs.readFileSync(sessionMetadataPath(sessionDir), 'utf8');
    const payload = JSON.parse(String(raw || '{}'));
    return payload && typeof payload === 'object' ? payload : {};
  } catch (_err) {
    return {};
  }
}

function listStoredTenants(baseDir) {
  try {
    const entries = fs.readdirSync(String(baseDir || ''), { withFileTypes: true });
    return entries
      .filter((entry) => entry.isDirectory() && /^tenant-[a-zA-Z0-9_-]+$/.test(entry.name))
      .map((entry) => Number.parseInt(entry.name.replace('tenant-', ''), 10))
      .filter((tenant) => Number.isFinite(tenant) && tenant > 0);
  } catch (_err) {
    return [];
  }
}

module.exports = {
  listStoredTenants,
  persistSessionMetadata,
  readSessionMetadata,
  sessionMetadataPath,
  tenantSessionDir,
};
