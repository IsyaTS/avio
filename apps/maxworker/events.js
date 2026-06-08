'use strict';

function _pickText(value) {
  if (typeof value === 'string') return value;
  if (!value || typeof value !== 'object') return '';
  const nested = value.text || value.body || value.message || value.caption || '';
  return typeof nested === 'string' ? nested : '';
}

function normalizeText(value) {
  return String(_pickText(value) || '').trim();
}

const HUMAN_NAME_RE = /[A-Za-zА-Яа-яЁё]/;
const TRUE_FLAG_VALUES = new Set(['1', 'true', 'yes', 'y', 'on', 'out', 'outgoing', 'sent', 'self', 'own', 'mine', 'right', 'me']);
const FALSE_FLAG_VALUES = new Set(['0', 'false', 'no', 'n', 'off', 'in', 'incoming', 'received', 'left', 'client']);
const SELF_DIRECTION_KEYS = [
  'direction',
  'dir',
  'message_direction',
  'messageDirection',
  'side',
  'flow',
  'source',
  'origin',
];
const SELF_FLAG_KEYS = [
  'from_self',
  'fromSelf',
  'from_me',
  'fromMe',
  'is_from_me',
  'isFromMe',
  'outgoing',
  'out',
  'is_outgoing',
  'isOutgoing',
  'own',
  'is_own',
  'isOwn',
  'mine',
  'is_mine',
  'isMine',
  'self',
  'is_self',
  'isSelf',
  'me',
  'is_me',
  'isMe',
];
const SELF_NESTED_KEYS = ['sender', 'author', 'from', 'owner', 'creator', 'user', 'actor'];

function _coerceBooleanFlag(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value !== 'string') return false;
  const normalized = value.trim().toLowerCase();
  if (!normalized) return false;
  if (TRUE_FLAG_VALUES.has(normalized)) return true;
  if (FALSE_FLAG_VALUES.has(normalized)) return false;
  return false;
}

function _isExplicitFalse(value) {
  if (value === false) return true;
  if (typeof value === 'number') return value === 0;
  if (typeof value !== 'string') return false;
  const normalized = value.trim().toLowerCase();
  return FALSE_FLAG_VALUES.has(normalized);
}

function _looksLikeMaxCompactOutgoing(value) {
  if (!value || typeof value !== 'object') return false;
  const hasCompactMessageShape =
    Object.prototype.hasOwnProperty.call(value, 'chatId') &&
    Object.prototype.hasOwnProperty.call(value, 'message') &&
    Object.prototype.hasOwnProperty.call(value, 'mark');
  if (!hasCompactMessageShape) return false;

  // MAX web websocket sometimes sends messages from the connected account as:
  // { chatId, message, unread:false, mark, prevMessageId, ... } without a
  // sender/fromMe flag. Inbound client messages keep unread=true or do not use
  // this own-message mark shape.
  const unreadIsFalse =
    Object.prototype.hasOwnProperty.call(value, 'unread') && _isExplicitFalse(value.unread);
  const hasOwnChainMarker =
    Object.prototype.hasOwnProperty.call(value, 'prevMessageId') ||
    Object.prototype.hasOwnProperty.call(value, 'ttl');
  return unreadIsFalse && hasOwnChainMarker;
}

function _objectSelfFlag(value, depth = 0) {
  if (!value || typeof value !== 'object' || depth > 2) return false;
  if (_looksLikeMaxCompactOutgoing(value)) return true;
  for (const key of SELF_FLAG_KEYS) {
    if (Object.prototype.hasOwnProperty.call(value, key) && _coerceBooleanFlag(value[key])) {
      return true;
    }
  }
  for (const key of SELF_DIRECTION_KEYS) {
    if (Object.prototype.hasOwnProperty.call(value, key) && _coerceBooleanFlag(value[key])) {
      return true;
    }
  }
  for (const key of SELF_NESTED_KEYS) {
    if (_objectSelfFlag(value[key], depth + 1)) return true;
  }
  return false;
}

function _readDebugValue(value, key) {
  if (!value || typeof value !== 'object') return undefined;
  if (!Object.prototype.hasOwnProperty.call(value, key)) return undefined;
  const raw = value[key];
  if (typeof raw === 'boolean' || typeof raw === 'number') return raw;
  if (typeof raw === 'string') return raw.slice(0, 64);
  return undefined;
}

function _sourceDebug(obj) {
  if (!obj || typeof obj !== 'object') return null;
  const flags = {};
  for (const key of [...SELF_FLAG_KEYS, ...SELF_DIRECTION_KEYS, 'unread', 'mark', 'ttl', 'prevMessageId']) {
    const value = _readDebugValue(obj, key);
    if (value !== undefined) flags[key] = value;
  }
  const nested = {};
  for (const key of SELF_NESTED_KEYS) {
    const value = obj[key];
    if (!value || typeof value !== 'object') continue;
    const nestedFlags = {};
    for (const nestedKey of [...SELF_FLAG_KEYS, ...SELF_DIRECTION_KEYS, 'id', 'username', 'name']) {
      const nestedValue = _readDebugValue(value, nestedKey);
      if (nestedValue !== undefined) nestedFlags[nestedKey] = nestedValue;
    }
    if (Object.keys(nestedFlags).length) nested[key] = nestedFlags;
  }
  return {
    keys: Object.keys(obj).slice(0, 40),
    flags,
    nested,
    max_compact_outgoing: _looksLikeMaxCompactOutgoing(obj),
    inferred_from_self: _objectSelfFlag(obj),
  };
}

function _sanitizeHumanName(value, { chatId = '', userId = '' } = {}) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (!HUMAN_NAME_RE.test(raw)) return '';
  if (/^(max|max_personal)\s*:/.test(raw.toLowerCase())) return '';
  if (chatId && raw === String(chatId).trim()) return '';
  if (userId && raw === String(userId).trim()) return '';
  return raw;
}

function _hashText(value) {
  const source = String(value || '');
  let h = 5381;
  for (let i = 0; i < source.length; i += 1) {
    h = ((h << 5) + h + source.charCodeAt(i)) >>> 0;
  }
  return h.toString(16);
}

function buildInboundTextKey(tenant, chatId, text) {
  const t = String(tenant || '').trim();
  const c = String(chatId || '').trim();
  const normalizedText = normalizeText(text).toLowerCase();
  if (!t || !c || !normalizedText) return '';
  return `${t}:${c}:text:${_hashText(normalizedText)}`;
}

function buildMessageKey(tenant, chatId, messageId) {
  const t = String(tenant || '').trim();
  const c = String(chatId || '').trim();
  const m = String(messageId || '').trim();
  if (!t || !c || !m) return '';
  return `${t}:${c}:${m}`;
}

function buildTextEchoKey(tenant, chatId, text) {
  const t = String(tenant || '').trim();
  const c = String(chatId || '').trim();
  const normalizedText = normalizeText(text).toLowerCase();
  if (!t || !c || !normalizedText) return '';
  return `${t}:${c}:${_hashText(normalizedText)}`;
}

function _readEchoEntry(rawValue) {
  if (!rawValue) return null;
  if (typeof rawValue === 'object') {
    return {
      expiresAt: Number(rawValue.expiresAt || 0),
      sentAt: Number(rawValue.sentAt || 0),
    };
  }
  return {
    expiresAt: Number(rawValue || 0),
    sentAt: 0,
  };
}

function normalizeInboundPayload(payload = {}) {
  const chatId = String(payload.chat_id || payload.chatId || payload.peer || '').trim();
  const messageId = String(payload.message_id || payload.messageId || payload.id || '').trim();
  const fromSelf = _objectSelfFlag(payload);
  const text = normalizeText(payload.text || payload.body || payload.message || '');
  const userId = payload.user_id || payload.userId || null;
  const rawUsername = payload.username ? String(payload.username).trim() : '';
  const rawDisplayName = payload.display_name
    ? String(payload.display_name).trim()
    : payload.displayName
    ? String(payload.displayName).trim()
    : '';
  const username = _sanitizeHumanName(rawUsername, { chatId, userId });
  const displayName = _sanitizeHumanName(rawDisplayName, { chatId, userId });
  const ts = Number(payload.ts || payload.timestamp || Date.now());
  const fallbackMessageId = _hashText(
    `${chatId}|${text}|${userId || ''}|${ts || Date.now()}|${fromSelf ? '1' : '0'}`
  );
  return {
    chatId,
    messageId: messageId || `auto-${fallbackMessageId}`,
    fromSelf,
    text,
    userId: userId !== null && userId !== undefined ? String(userId) : '',
    username,
    displayName,
    ts: Number.isFinite(ts) ? ts : Date.now(),
  };
}

function _walk(value, visit, depth = 0) {
  if (depth > 8 || value === null || value === undefined) return;
  if (Array.isArray(value)) {
    for (const item of value) _walk(item, visit, depth + 1);
    return;
  }
  if (typeof value !== 'object') return;
  visit(value);
  for (const key of Object.keys(value)) {
    _walk(value[key], visit, depth + 1);
  }
}

function extractInboundCandidates(payload = {}) {
  const candidates = [];
  _walk(payload, (obj) => {
    const text = normalizeText(
      obj.text ||
        obj.body ||
        obj.message ||
        obj.caption ||
        obj.content?.text ||
        obj.payload?.text ||
        obj.last_message?.text
    );
    if (!text) return;
    const chatId = String(
      obj.chat_id ||
        obj.chatId ||
        obj.peer ||
        obj.peer_id ||
        obj.peerId ||
        obj.dialog_id ||
        obj.dialogId ||
        (obj.dialog && typeof obj.dialog === 'object' ? obj.dialog.id : '') ||
        (obj.chat && typeof obj.chat === 'object' ? obj.chat.id : '') ||
        obj.conversation_id ||
        obj.conversationId ||
        obj.conversationID ||
        obj.thread_id ||
        obj.threadId ||
        ''
    ).trim();
    if (!chatId) return;
    const candidate = normalizeInboundPayload({
      chat_id: chatId,
      message_id: obj.message_id || obj.messageId || obj.id || obj.mid || '',
      from_self: _objectSelfFlag(obj),
      text,
      user_id:
        obj.user_id ||
        obj.userId ||
        obj.sender_id ||
        obj.senderId ||
        obj.author_id ||
        obj.authorId ||
        obj.sender?.id ||
        obj.author?.id ||
        '',
      username:
        obj.username ||
        obj.sender_username ||
        obj.login ||
        obj.sender?.username ||
        obj.author?.username ||
        '',
      display_name:
        obj.display_name ||
        obj.displayName ||
        obj.chat_name ||
        obj.chat?.title ||
        obj.dialog?.title ||
        obj.peer_name ||
        obj.title ||
        obj.name ||
        obj.sender_name ||
        obj.sender?.name ||
        obj.author?.name ||
        '',
      ts: obj.ts || obj.timestamp || obj.created_at || obj.createdAt || Date.now(),
    });
    candidate.sourceDebug = _sourceDebug(obj);
    candidates.push(candidate);
  });

  const unique = new Map();
  for (const c of candidates) {
    const key = `${c.chatId}|${c.messageId}|${c.ts}|${c.text}`;
    if (!unique.has(key)) unique.set(key, c);
  }
  return Array.from(unique.values());
}

function isLikelySelfEcho(message, sentEchoMap, sentEchoTextMap, _ttlSeconds = 900) {
  const now = Date.now();
  const key = buildMessageKey(message.tenant, message.chatId, message.messageId);
  if (key) {
    const cached = _readEchoEntry(sentEchoMap.get(key));
    if (cached) {
      if (!cached.expiresAt || cached.expiresAt <= now) {
        sentEchoMap.delete(key);
      } else {
        return true;
      }
    }
  }
  const textKey = buildTextEchoKey(message.tenant, message.chatId, message.text);
  if (!textKey) return false;
  const cachedText = _readEchoEntry(sentEchoTextMap.get(textKey));
  if (!cachedText) return false;
  if (!cachedText.expiresAt || cachedText.expiresAt <= now) {
    sentEchoTextMap.delete(textKey);
    return false;
  }
  if (message.fromSelf) return true;
  const messageTs = Number(message.ts || 0);
  const sentAt = Number(cachedText.sentAt || 0);
  if (!sentAt) return false;
  if (messageTs && messageTs + 2000 < sentAt) return false;
  if (messageTs && messageTs - sentAt > 30_000) return false;
  return true;
}

function classifyMessage(message, sentEchoMap, sentEchoTextMap, ttlSeconds = 900) {
  if (isLikelySelfEcho(message, sentEchoMap, sentEchoTextMap, ttlSeconds)) return 'self_echo';
  if (!message.fromSelf) return 'inbound';
  return 'manager_outgoing';
}

module.exports = {
  buildInboundTextKey,
  buildMessageKey,
  buildTextEchoKey,
  classifyMessage,
  extractInboundCandidates,
  isLikelySelfEcho,
  normalizeInboundPayload,
  normalizeText,
};
