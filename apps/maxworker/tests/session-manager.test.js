'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { MaxPersonalSessionManager, STATUS } = require('../session-manager');
const { buildTextEchoKey } = require('../events');

function createManager() {
  const manager = new MaxPersonalSessionManager();
  manager.cfg.sessionsDir = fs.mkdtempSync(path.join(os.tmpdir(), 'maxworker-tests-'));
  return manager;
}

test('send uses idempotency dedupe for repeated keys', async () => {
  const manager = createManager();
  manager.cfg.mockMode = true;
  const state = manager._getOrCreate(101);
  state.status = STATUS.AUTHORIZED;

  const first = await manager.send({
    tenant: 101,
    to: 'chat-1',
    text: 'hello',
    idempotency_key: 'dup-1',
  });
  const second = await manager.send({
    tenant: 101,
    to: 'chat-1',
    text: 'hello',
    idempotency_key: 'dup-1',
  });

  assert.equal(first.status, 200);
  assert.equal(second.status, 200);
  assert.equal(second.body.duplicate, true);
});

test('ingestInbound handles session.authorized lifecycle event', async () => {
  const manager = createManager();
  const state = manager._getOrCreate(202);
  state.status = STATUS.WAITING_QR;

  const result = await manager.ingestInbound({
    tenant: 202,
    event: 'session.authorized',
    account: { id: 'acc-202', name: 'MAX 202' },
  });

  assert.equal(result.status, 200);
  assert.equal(state.status, STATUS.AUTHORIZED);
  assert.equal(state.account.account_id, 'acc-202');
  assert.equal(state.account.display_name, 'MAX 202');
});

test('ingestInbound marks manager outgoing when fromSelf without echo', async () => {
  const manager = createManager();
  const state = manager._getOrCreate(303);
  state.callbackUrl = 'http://callback.local';
  manager._pushWebhook = async (_st, payload) => {
    assert.equal(payload.manager, true);
    assert.equal(payload.out, true);
    assert.equal(payload.origin, 'max_personal:manager');
    return true;
  };

  const result = await manager.ingestInbound({
    tenant: 303,
    chat_id: 'chat-303',
    message_id: 'm-303',
    text: 'manual manager reply',
    from_self: true,
  });
  assert.equal(result.status, 200);
});

test('ingestInbound promotes waiting auth session to authorized on real message', async () => {
  const manager = createManager();
  const state = manager._getOrCreate(306);
  state.status = STATUS.AUTHORIZING;
  state.qrId = 'qr-306';
  state.qrPngDataUrl = 'data:image/png;base64,abc';
  state.callbackUrl = 'http://callback.local';
  manager._pushWebhook = async () => true;

  const result = await manager.ingestInbound({
    tenant: 306,
    chat_id: 'chat-306',
    message_id: 'm-306',
    text: 'hello',
    from_self: false,
  });

  assert.equal(result.status, 200);
  assert.equal(state.status, STATUS.AUTHORIZED);
  assert.equal(state.qrId, null);
  assert.equal(state.qrPngDataUrl, null);
});

test('ingestInbound suppresses recent outbound echo even without from_self flag', async () => {
  const manager = createManager();
  const state = manager._getOrCreate(304);
  const now = Date.now();
  state.sentEchoText.set(buildTextEchoKey(304, 'chat-304', 'manual manager reply'), {
    sentAt: now,
    expiresAt: now + 30_000,
  });
  let pushed = false;
  manager._pushWebhook = async () => {
    pushed = true;
    return true;
  };

  const result = await manager.ingestInbound({
    tenant: 304,
    chat_id: 'chat-304',
    message_id: 'm-304',
    text: 'manual manager reply',
    from_self: false,
    ts: now + 500,
  });

  assert.equal(result.status, 200);
  assert.equal(result.body.suppressed, 'self_echo');
  assert.equal(pushed, false);
});

test('ingestInbound dedupes repeated DOM text with different synthetic ids', async () => {
  const manager = createManager();
  const state = manager._getOrCreate(307);
  state.callbackUrl = 'http://callback.local';
  let pushed = 0;
  manager._pushWebhook = async () => {
    pushed += 1;
    return true;
  };

  const first = await manager.ingestInbound({
    tenant: 307,
    chat_id: 'chat-307',
    message_id: 'dom-1',
    text: 'same client text',
    from_self: false,
  });
  const second = await manager.ingestInbound({
    tenant: 307,
    chat_id: 'chat-307',
    message_id: 'dom-2',
    text: 'same client text',
    from_self: false,
    ts: Date.now() + 500,
  });

  assert.equal(first.status, 200);
  assert.equal(second.status, 200);
  assert.equal(second.body.duplicate, true);
  assert.equal(second.body.reason, 'text_window');
  assert.equal(pushed, 1);
});

test('ingestInbound forwards manager origin for self DOM messages', async () => {
  const manager = createManager();
  const state = manager._getOrCreate(308);
  state.callbackUrl = 'http://callback.local';
  let captured = null;
  manager._pushWebhook = async (_st, payload) => {
    captured = payload;
    return true;
  };

  const result = await manager.ingestInbound({
    tenant: 308,
    chat_id: 'chat-308',
    message_id: 'dom-self-1',
    text: 'manual manager text',
    from_self: true,
  });

  assert.equal(result.status, 200);
  assert.equal(result.body.kind, 'manager_outgoing');
  assert.equal(captured.manager, true);
  assert.equal(captured.out, true);
  assert.equal(captured.origin, 'max_personal:manager');
});

test('ingestInbound upgrades sender match to self message', async () => {
  const manager = createManager();
  const state = manager._getOrCreate(305);
  state.callbackUrl = 'http://callback.local';
  state.account = { display_name: 'MAX account 305' };
  manager._pushWebhook = async (_st, payload) => {
    assert.equal(payload.manager, true);
    assert.equal(payload.origin, 'max_personal:manager');
    return true;
  };

  const result = await manager.ingestInbound({
    tenant: 305,
    chat_id: 'chat-305',
    message_id: 'm-305',
    text: 'reply from account owner',
    display_name: 'MAX account 305',
    from_self: false,
  });

  assert.equal(result.status, 200);
  assert.equal(result.body.kind, 'manager_outgoing');
});

test('restoreSessions hydrates stored metadata for tenant session', async () => {
  const manager = createManager();
  manager.cfg.mockMode = true;
  const sessionDir = path.join(manager.cfg.sessionsDir, 'tenant-404');
  fs.mkdirSync(sessionDir, { recursive: true });
  fs.writeFileSync(
    path.join(sessionDir, 'avio-session.json'),
    JSON.stringify({
      tenant: 404,
      callback_url: 'http://callback.local/webhook/max_personal?tenant=404',
      webhook_token: 'secret-404',
      account: { display_name: 'MAX 404' },
      last_status: 'authorized',
    })
  );

  await manager.restoreSessions();

  const state = manager._getOrCreate(404);
  assert.equal(state.status, STATUS.AUTHORIZED);
  assert.equal(state.callbackUrl, 'http://callback.local/webhook/max_personal?tenant=404');
  assert.equal(state.webhookToken, 'secret-404');
  assert.equal(state.account.display_name, 'MAX 404');
});

test('expired QR does not immediately reauth a previously authorized session', async () => {
  const manager = createManager();
  manager.cfg.mockMode = true;
  const state = manager._getOrCreate(606);
  state.status = STATUS.WAITING_QR;
  state.lastPersistedStatus = STATUS.AUTHORIZED;
  state.qrId = 'qr-606';
  state.qrPngDataUrl = 'data:image/png;base64,abc';
  state.qrExpiresAt = Date.now() - 1000;

  const result = await manager.getQr(606);

  assert.equal(result.ok, false);
  assert.equal(result.error, 'session_restore_pending');
  assert.equal(state.status, STATUS.STALE);
  assert.equal(state.lastError, 'qr_expired_after_authorized_session');
});

test('connect without force reuses live authorized session', async () => {
  const manager = createManager();
  manager.cfg.mockMode = true;
  const state = manager._getOrCreate(707);
  state.status = STATUS.AUTHORIZED;
  state.account = { display_name: 'MAX 707' };

  const result = await manager.startSession({
    tenant: 707,
    callbackUrl: 'http://callback.local',
    force: false,
  });

  assert.equal(result.ok, true);
  assert.equal(result.reused, true);
  assert.equal(result.status, STATUS.AUTHORIZED);
  assert.equal(state.account.display_name, 'MAX 707');
});

test('connect without force restarts non-authorized browser session for QR flow', async () => {
  const manager = createManager();
  manager.cfg.mockMode = true;
  const state = manager._getOrCreate(808);
  state.status = STATUS.AUTHORIZING;
  state.browserRef = { context: { close: async () => {} }, page: { close: async () => {} } };

  const result = await manager.startSession({
    tenant: 808,
    callbackUrl: 'http://callback.local',
    force: false,
  });

  assert.equal(result.ok, true);
  assert.equal(state.status, STATUS.WAITING_QR);
  assert.ok(state.qrPngDataUrl);
  assert.equal(state.lastError, null);
});

test('_pushWebhook includes worker auth token header', async () => {
  const manager = createManager();
  manager.cfg.authToken = 'worker-auth-token';
  const state = manager._getOrCreate(505);
  state.callbackUrl = 'http://callback.local/webhook/max_personal?tenant=505';
  state.webhookToken = 'event-secret-505';

  const originalFetch = global.fetch;
  let capturedHeaders = null;
  global.fetch = async (_url, options) => {
    capturedHeaders = options?.headers || null;
    return { status: 200 };
  };

  try {
    const pushed = await manager._pushWebhook(state, { tenant: 505 });
    assert.equal(pushed, true);
    assert.ok(capturedHeaders);
    assert.equal(capturedHeaders['X-Webhook-Token'], 'event-secret-505');
    assert.equal(capturedHeaders['X-Auth-Token'], 'worker-auth-token');
  } finally {
    global.fetch = originalFetch;
  }
});
