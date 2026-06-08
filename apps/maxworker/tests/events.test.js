'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildInboundTextKey,
  buildMessageKey,
  buildTextEchoKey,
  classifyMessage,
  extractInboundCandidates,
  normalizeInboundPayload,
} = require('../events');

test('normalize inbound payload maps aliases', () => {
  const payload = normalizeInboundPayload({
    chatId: 'chat-1',
    messageId: 'msg-1',
    fromSelf: true,
    message: 'hello',
    userId: 77,
    displayName: 'Manager',
  });
  assert.equal(payload.chatId, 'chat-1');
  assert.equal(payload.messageId, 'msg-1');
  assert.equal(payload.fromSelf, true);
  assert.equal(payload.text, 'hello');
  assert.equal(payload.userId, '77');
  assert.equal(payload.displayName, 'Manager');
});

test('normalize inbound payload handles common outgoing aliases safely', () => {
  assert.equal(
    normalizeInboundPayload({ chatId: 'chat-1', text: 'manager', fromMe: true }).fromSelf,
    true
  );
  assert.equal(
    normalizeInboundPayload({ chatId: 'chat-1', text: 'manager', isMine: true }).fromSelf,
    true
  );
  assert.equal(
    normalizeInboundPayload({ chatId: 'chat-1', text: 'manager', direction: 'outgoing' }).fromSelf,
    true
  );
  assert.equal(
    normalizeInboundPayload({ chatId: 'chat-1', text: 'manager', author: { isMe: true } }).fromSelf,
    true
  );
  assert.equal(
    normalizeInboundPayload({ chatId: 'chat-1', text: 'client', fromSelf: 'false' }).fromSelf,
    false
  );
});

test('classify self echo', () => {
  const sentEcho = new Map();
  const sentEchoText = new Map();
  const key = buildMessageKey(1, 'chat-1', 'msg-1');
  sentEcho.set(key, Date.now() + 60_000);
  const kind = classifyMessage(
    { tenant: 1, chatId: 'chat-1', messageId: 'msg-1', fromSelf: true },
    sentEcho,
    sentEchoText,
    60
  );
  assert.equal(kind, 'self_echo');
});

test('classify self echo by text fingerprint when provider message id changes', () => {
  const sentEcho = new Map();
  const sentEchoText = new Map();
  const key = buildTextEchoKey(1, 'chat-1', 'hello world');
  sentEchoText.set(key, { sentAt: Date.now(), expiresAt: Date.now() + 60_000 });
  const kind = classifyMessage(
    {
      tenant: 1,
      chatId: 'chat-1',
      messageId: 'other-msg-id',
      fromSelf: false,
      text: 'hello world',
      ts: Date.now(),
    },
    sentEcho,
    sentEchoText,
    60
  );
  assert.equal(kind, 'self_echo');
});

test('does not classify delayed same-text inbound as self echo', () => {
  const sentEcho = new Map();
  const sentEchoText = new Map();
  const sentAt = Date.now();
  const key = buildTextEchoKey(1, 'chat-1', 'hello world');
  sentEchoText.set(key, { sentAt, expiresAt: sentAt + 60_000 });
  const kind = classifyMessage(
    {
      tenant: 1,
      chatId: 'chat-1',
      messageId: 'other-msg-id',
      fromSelf: false,
      text: 'hello world',
      ts: sentAt + 90_000,
    },
    sentEcho,
    sentEchoText,
    60
  );
  assert.equal(kind, 'inbound');
});

test('classify manager outgoing when fromSelf without echo key', () => {
  const kind = classifyMessage(
    { tenant: 1, chatId: 'chat-1', messageId: 'msg-2', fromSelf: true },
    new Map(),
    new Map(),
    60
  );
  assert.equal(kind, 'manager_outgoing');
});

test('build inbound text key is stable across synthetic message ids', () => {
  const first = buildInboundTextKey(101, 'chat-1', 'Здравствуйте');
  const second = buildInboundTextKey(101, 'chat-1', 'Здравствуйте');
  const other = buildInboundTextKey(101, 'chat-1', 'Другой текст');

  assert.equal(first, second);
  assert.notEqual(first, other);
});

test('extract inbound candidates from nested payload', () => {
  const payload = {
    updates: [
      {
        chat_id: 'chat-42',
        id: 'm-42',
        text: 'Здравствуйте',
        sender_id: 'u-9',
        out: false,
      },
    ],
  };
  const list = extractInboundCandidates(payload);
  assert.equal(Array.isArray(list), true);
  assert.equal(list.length, 1);
  assert.equal(list[0].chatId, 'chat-42');
  assert.equal(list[0].messageId, 'm-42');
  assert.equal(list[0].text, 'Здравствуйте');
  assert.equal(list[0].fromSelf, false);
});

test('extract inbound candidates preserves outgoing manager marker from websocket payloads', () => {
  const list = extractInboundCandidates({
    payload: {
      dialog_id: 'chat-99',
      id: 'm-out',
      text: 'Ручной ответ менеджера',
      author: { isMe: true, id: 'manager-1' },
    },
  });
  assert.equal(list.length, 1);
  assert.equal(list[0].fromSelf, true);
  assert.equal(list[0].sourceDebug.inferred_from_self, true);
});

test('extract inbound candidates detects compact MAX own-message websocket shape', () => {
  const list = extractInboundCandidates({
    event: {
      chatId: '93267442',
      unread: false,
      message: 'Тест менеджера',
      ttl: 0,
      mark: 1,
      prevMessageId: 'prev-1',
    },
  });
  assert.equal(list.length, 1);
  assert.equal(list[0].chatId, '93267442');
  assert.equal(list[0].text, 'Тест менеджера');
  assert.equal(list[0].fromSelf, true);
  assert.equal(list[0].sourceDebug.max_compact_outgoing, true);
});

test('extract inbound candidates does not treat unread compact MAX payload as own message', () => {
  const list = extractInboundCandidates({
    event: {
      chatId: '93267442',
      unread: true,
      message: 'Тест клиента',
      ttl: 0,
      mark: 1,
      prevMessageId: 'prev-1',
    },
  });
  assert.equal(list.length, 1);
  assert.equal(list[0].fromSelf, false);
  assert.equal(list[0].sourceDebug.max_compact_outgoing, false);
});

test('normalize inbound payload extracts nested text objects', () => {
  const payload = normalizeInboundPayload({
    chatId: 'chat-2',
    messageId: 'msg-2',
    text: { text: 'здравствуйте' },
  });
  assert.equal(payload.text, 'здравствуйте');
});

test('normalize inbound payload ignores numeric pseudo names', () => {
  const payload = normalizeInboundPayload({
    chatId: '93267442',
    messageId: 'msg-3',
    text: 'тест',
    userId: '93267442',
    username: '93267442',
    displayName: '93267442',
  });
  assert.equal(payload.username, '');
  assert.equal(payload.displayName, '');
});

test('extract inbound candidates prefers chat title as display name', () => {
  const list = extractInboundCandidates({
    updates: [
      {
        chat_id: '93267442',
        message_id: 'm-99',
        text: 'привет',
        chat: { title: 'Айдар' },
        sender_id: '93267442',
      },
    ],
  });
  assert.equal(list.length, 1);
  assert.equal(list[0].displayName, 'Айдар');
});

test('extract inbound candidates supports nested chat.id shape', () => {
  const list = extractInboundCandidates({
    updates: [
      {
        event: 'message.new',
        message: {
          id: 'm-chat-id',
          text: 'Привет из chat.id',
          chat: { id: 'chat-nested-1' },
          sender: { id: 'u-nested-1', username: 'nested_user', name: 'Nested User' },
        },
      },
    ],
  });
  assert.equal(list.length, 1);
  assert.equal(list[0].chatId, 'chat-nested-1');
  assert.equal(list[0].messageId, 'm-chat-id');
  assert.equal(list[0].text, 'Привет из chat.id');
});
