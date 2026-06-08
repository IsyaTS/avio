'use strict';

const fs = require('fs/promises');
const path = require('path');
const os = require('os');
const { selectors } = require('./selectors');

function _baseWebUrl() {
  return String(process.env.MAX_PERSONAL_WEB_URL || 'https://web.max.ru').replace(/\/+$/, '');
}

function _chatUrl(chatId) {
  const value = String(chatId || '').trim();
  if (!value) {
    throw new Error('chat_required');
  }
  return `${_baseWebUrl()}/${encodeURIComponent(value)}`;
}

async function _waitForMessageInput(page, timeoutMs = 15000) {
  const startedAt = Date.now();
  const timeout = Math.max(1000, Number(timeoutMs || 15000));
  while (Date.now() - startedAt < timeout) {
    for (const selector of selectors.messageInput) {
      const locator = page.locator(selector).first();
      try {
        await locator.waitFor({ state: 'visible', timeout: 800 });
        return locator;
      } catch (_err) {
        // keep scanning candidates
      }
    }
    await page.waitForTimeout(150).catch(() => undefined);
  }
  return null;
}

async function _inputContainsText(inputLocator, textValue) {
  const probe = String(textValue || '').trim();
  if (!probe) return false;
  try {
    const value = await inputLocator.inputValue();
    if (String(value || '').includes(probe)) return true;
  } catch (_err) {
    // contenteditable branch
  }
  try {
    const content = await inputLocator.textContent();
    if (String(content || '').includes(probe)) return true;
  } catch (_err) {
    // ignore
  }
  return false;
}

async function _waitForTextDelivered(page, inputLocator, textValue, timeoutMs = 5000) {
  const probe = String(textValue || '').trim();
  if (!probe) return true;
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const stillTyped = await _inputContainsText(inputLocator, probe);
    if (!stillTyped) return true;
    await page.waitForTimeout(120).catch(() => undefined);
  }
  return false;
}

async function _openChat(page, chatId) {
  const targetUrl = _chatUrl(chatId);
  await page.goto(targetUrl, {
    timeout: 20000,
    waitUntil: 'domcontentloaded',
  });
  const input = await _waitForMessageInput(page, 15000);
  if (!input) {
    throw new Error('input_not_found');
  }
  return input;
}

async function _clickSend(page) {
  for (const selector of selectors.sendButton) {
    const button = page.locator(selector).first();
    try {
      await button.waitFor({ state: 'visible', timeout: 1000 });
      await button.click({ timeout: 3000 }).catch(() => undefined);
      return true;
    } catch (_err) {
      // continue
    }
  }
  await page.keyboard.press('Enter').catch(() => undefined);
  return true;
}

async function sendTextViaUi(browserRef, to, text) {
  if (!browserRef || !browserRef.page) {
    throw new Error('session_unavailable');
  }
  const page = browserRef.page;
  const textValue = String(text || '').trim();
  if (!textValue) {
    throw new Error('empty_text');
  }

  const input = await _openChat(page, to);
  await input.click({ timeout: 3000 }).catch(() => undefined);
  let filled = false;
  try {
    await input.fill(textValue);
    filled = true;
  } catch (_err) {
    filled = false;
  }
  if (!filled) {
    await page.keyboard.type(textValue, { delay: 5 });
  }

  await _clickSend(page);
  const delivered = await _waitForTextDelivered(page, input, textValue, 5000);
  if (!delivered) {
    throw new Error('text_not_delivered');
  }
  return true;
}

async function _clickAttachmentButton(page) {
  for (const selector of selectors.attachmentButton) {
    const button = page.locator(selector).first();
    try {
      await button.waitFor({ state: 'visible', timeout: 1000 });
      await button.click({ timeout: 3000 }).catch(() => undefined);
      return true;
    } catch (_err) {
      // continue
    }
  }
  return false;
}

async function _setInputFile(page, selector, filePath) {
  try {
    await page.setInputFiles(selector, filePath, { timeout: 1500 });
    return true;
  } catch (_err) {
    return false;
  }
}

async function _inputHasSelectedFile(page, filePath) {
  const expectedName = path.basename(String(filePath || '').trim());
  if (!expectedName) return false;
  for (const selector of selectors.fileInput) {
    const locator = page.locator(selector).first();
    try {
      const selected = await locator.evaluate((node, expected) => {
        if (!node || !node.files || !node.files.length) return false;
        const names = Array.from(node.files)
          .map((item) => String((item && item.name) || '').trim())
          .filter(Boolean);
        if (!names.length) return false;
        return names.includes(expected);
      }, expectedName);
      if (selected) {
        return true;
      }
    } catch (_err) {
      // keep scanning candidates
    }
  }
  return false;
}

async function _attachFileViaChooser(page, filePath) {
  if (!page || typeof page.waitForEvent !== 'function') return false;

  const clicked = await _clickAttachmentButton(page);
  if (!clicked) return false;

  await page.waitForTimeout(120).catch(() => undefined);
  const chooserPromise = Promise.resolve()
    .then(() => page.waitForEvent('filechooser', { timeout: 2400 }))
    .catch(() => null);
  for (const selector of selectors.attachmentFileAction || []) {
    const item = page.locator(selector).first();
    try {
      await item.waitFor({ state: 'visible', timeout: 600 });
      await item.click({ timeout: 1500 }).catch(() => undefined);
      break;
    } catch (_err) {
      // keep scanning menu actions
    }
  }
  try {
    const chooser = await chooserPromise;
    if (!chooser || typeof chooser.setFiles !== 'function') return false;
    await chooser.setFiles(filePath);
    return true;
  } catch (_err) {
    return false;
  }
}

async function _waitForAttachmentPrepared(page, filePath) {
  const fileName = path.basename(String(filePath || '').trim());
  const startedAt = Date.now();
  while (Date.now() - startedAt < 7000) {
    const selectedInInput = await _inputHasSelectedFile(page, filePath);
    if (selectedInInput) return true;

    if (fileName) {
      const quotedTextNode = page.locator(`text="${fileName}"`).first();
      try {
        await quotedTextNode.waitFor({ state: 'visible', timeout: 250 });
        return true;
      } catch (_err) {
        // keep polling
      }
    }

    for (const selector of selectors.attachmentPreview) {
      const node = page.locator(selector).first();
      try {
        await node.waitFor({ state: 'visible', timeout: 350 });
        return true;
      } catch (_err) {
        // try next selector
      }
    }
    if (fileName) {
      const textNode = page.locator(`text=${fileName}`).first();
      try {
        await textNode.waitFor({ state: 'visible', timeout: 350 });
        return true;
      } catch (_err) {
        // keep polling
      }
    }
    await page.waitForTimeout(150).catch(() => undefined);
  }
  return false;
}

async function _waitForAttachmentCleared(page, filePath) {
  const fileName = path.basename(String(filePath || '').trim());
  const startedAt = Date.now();
  while (Date.now() - startedAt < 7000) {
    let previewVisible = false;
    for (const selector of selectors.attachmentPreview) {
      const node = page.locator(selector).first();
      try {
        const vis = await node.isVisible({ timeout: 250 });
        if (vis) {
          previewVisible = true;
          break;
        }
      } catch (_err) {
        // keep checking
      }
    }
    if (!previewVisible && fileName) {
      try {
        const byName = page.locator(`text=${fileName}`).first();
        const vis = await byName.isVisible({ timeout: 250 });
        previewVisible = Boolean(vis);
      } catch (_err) {
        // ignore
      }
    }
    if (!previewVisible) return true;
    await page.waitForTimeout(150).catch(() => undefined);
  }
  return false;
}

async function _waitForFileDelivered(page, filePath) {
  const fileName = path.basename(String(filePath || '').trim());
  if (!fileName) return false;
  const startedAt = Date.now();
  while (Date.now() - startedAt < 8000) {
    const byQuotedName = page.locator(`text="${fileName}"`).first();
    try {
      const visible = await byQuotedName.isVisible({ timeout: 250 });
      if (visible) return true;
    } catch (_err) {
      // keep polling
    }
    const stillSelected = await _inputHasSelectedFile(page, filePath);
    if (!stillSelected) {
      // input is cleared and file is no longer pending; allow a soft-success
      // even when MAX web does not render filename text in the viewport.
      return true;
    }
    await page.waitForTimeout(180).catch(() => undefined);
  }
  return false;
}

async function _attachFileViaUi(browserRef, to, filePath) {
  if (!browserRef || !browserRef.page) {
    throw new Error('session_unavailable');
  }
  const page = browserRef.page;
  await _openChat(page, to);

  let attached = false;
  attached = await _attachFileViaChooser(page, filePath);
  if (!attached) {
    for (const selector of selectors.fileInput) {
      if (await _setInputFile(page, selector, filePath)) {
        attached = true;
        break;
      }
    }
  }
  if (!attached) {
    await _clickAttachmentButton(page);
    for (const selector of selectors.fileInput) {
      if (await _setInputFile(page, selector, filePath)) {
        attached = true;
        break;
      }
    }
  }
  if (!attached) {
    throw new Error('attachment_input_not_found');
  }

  const prepared = await _waitForAttachmentPrepared(page, filePath);
  if (!prepared) {
    throw new Error('attachment_not_prepared');
  }
  await page.waitForTimeout(500).catch(() => undefined);
  await _clickSend(page);
  const cleared = await _waitForAttachmentCleared(page, filePath);
  const delivered = await _waitForFileDelivered(page, filePath);
  if (!delivered) {
    throw new Error('attachment_not_delivered');
  }
  if (!cleared) {
    await page.waitForTimeout(400).catch(() => undefined);
  }
  return true;
}

async function _attachmentPlan(attachments) {
  return _attachmentPlanWithOptions(attachments, {
    fetchRemote: false,
    timeoutMs: 15000,
    maxBytes: 15 * 1024 * 1024,
  });
}

function _sanitizeFilename(value, fallback = 'attachment') {
  const raw = String(value || '').trim();
  if (!raw) return fallback;
  const base = path.basename(raw).replace(/[^\w.\-]+/g, '_');
  if (!base || base === '.' || base === '..') return fallback;
  return base;
}

function _extensionFromType(type, mime, urlPathname) {
  const fromPath = path.extname(String(urlPathname || '').trim());
  if (fromPath) return fromPath;
  const mimeNorm = String(mime || '').trim().toLowerCase();
  if (mimeNorm.includes('png')) return '.png';
  if (mimeNorm.includes('gif')) return '.gif';
  if (mimeNorm.includes('webp')) return '.webp';
  if (mimeNorm.includes('pdf')) return '.pdf';
  if (mimeNorm.includes('jpeg') || mimeNorm.includes('jpg')) return '.jpg';
  if (mimeNorm.includes('heic')) return '.heic';
  if (String(type || '').trim().toLowerCase() === 'image') return '.jpg';
  return '.bin';
}

async function _downloadRemoteAttachment(urlValue, blob, options) {
  const timeoutMs = Math.max(1000, Number(options.timeoutMs || 15000));
  const maxBytes = Math.max(1_000_000, Number(options.maxBytes || 15 * 1024 * 1024));
  let parsed;
  try {
    parsed = new URL(String(urlValue || '').trim());
  } catch (_err) {
    return null;
  }
  if (!['http:', 'https:'].includes(parsed.protocol)) {
    return null;
  }

  if (typeof fetch !== 'function') {
    return null;
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(parsed.toString(), {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
    });
    if (!response || !response.ok) {
      return null;
    }

    const contentLength = Number(response.headers.get('content-length') || 0);
    if (Number.isFinite(contentLength) && contentLength > maxBytes) {
      return null;
    }

    const arrayBuffer = await response.arrayBuffer();
    const bytes = Buffer.from(arrayBuffer);
    if (!bytes.length || bytes.length > maxBytes) {
      return null;
    }

    const type = String(blob.type || '').trim().toLowerCase();
    const mime = String(blob.mime || blob.mime_type || blob.mimetype || response.headers.get('content-type') || '')
      .trim()
      .toLowerCase();
    const fileExt = _extensionFromType(type, mime, parsed.pathname);
    const providedName = _sanitizeFilename(blob.name || blob.filename || blob.title || '', '');
    const baseName = providedName || `${type || 'attachment'}_${Date.now()}`;
    const normalizedName = path.extname(baseName) ? baseName : `${baseName}${fileExt}`;
    const tmpDir = path.join(os.tmpdir(), 'avio-maxworker-remote');
    await fs.mkdir(tmpDir, { recursive: true });
    const filePath = path.join(
      tmpDir,
      `${Date.now()}_${Math.random().toString(36).slice(2, 10)}_${_sanitizeFilename(normalizedName)}`
    );
    await fs.writeFile(filePath, bytes);
    return filePath;
  } catch (_err) {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function _attachmentPlanWithOptions(attachments, options = {}) {
  if (!Array.isArray(attachments)) return { files: [], lines: [], tempFiles: [] };
  const files = [];
  const lines = [];
  const tempFiles = [];
  const fetchRemote = Boolean(options.fetchRemote);
  const tenantsRoot = String(process.env.MAX_PERSONAL_TENANTS_DIR || '/data/tenants').trim();

  const resolveLocalPathCandidates = (rawPath) => {
    const value = String(rawPath || '').trim();
    if (!value) return [];
    const uniq = new Set();
    const push = (candidate) => {
      const normalized = String(candidate || '').trim();
      if (!normalized) return;
      if (uniq.has(normalized)) return;
      uniq.add(normalized);
    };

    push(value);
    if (value.startsWith('/app/data/tenants/')) {
      push(path.join(tenantsRoot, value.slice('/app/data/tenants/'.length)));
    } else if (value.startsWith('/opt/avio-dev/data/tenants/')) {
      push(path.join(tenantsRoot, value.slice('/opt/avio-dev/data/tenants/'.length)));
    } else if (value.startsWith('/data/tenants/')) {
      push(path.join(tenantsRoot, value.slice('/data/tenants/'.length)));
    }
    return Array.from(uniq);
  };

  for (const blob of attachments) {
    if (!blob || typeof blob !== 'object') continue;
    const localPath = String(
      blob.path || blob.file_path || blob.local_path || blob.absolute_path || ''
    ).trim();
    if (localPath) {
      const candidates = resolveLocalPathCandidates(localPath);
      let uploadedLocal = false;
      for (const candidate of candidates) {
        try {
          const stat = await fs.stat(candidate);
          if (stat && stat.isFile()) {
            files.push(candidate);
            uploadedLocal = true;
            break;
          }
        } catch (_err) {
          // try next candidate
        }
      }
      if (uploadedLocal) {
        continue;
      }
    }
    const type = String(blob.type || '').trim().toLowerCase();
    const name = String(blob.name || blob.filename || blob.title || '').trim();
    const url = String(blob.url || blob.link || '').trim();
    if (!url) continue;
    if (fetchRemote) {
      const downloaded = await _downloadRemoteAttachment(url, blob, options);
      if (downloaded) {
        files.push(downloaded);
        tempFiles.push(downloaded);
        continue;
      }
    }
    if (name) {
      lines.push(`${name}: ${url}`);
      continue;
    }
    if (type === 'image') {
      lines.push(`Фото: ${url}`);
      continue;
    }
    lines.push(`Файл: ${url}`);
  }
  return { files, lines, tempFiles };
}

async function sendText(session, payload, cfg) {
  const chatId = String(payload.to || '').trim();
  if (!chatId) {
    return { ok: false, error: 'chat_required', retryable: false };
  }
  const text = String(payload.text || '').trim();
  const attachmentPlan = await _attachmentPlanWithOptions(payload.attachments, {
    fetchRemote: Boolean(cfg && cfg.fetchRemoteAttachments),
    timeoutMs: Number((cfg && cfg.remoteAttachmentTimeoutMs) || 15000),
    maxBytes: Number((cfg && cfg.remoteAttachmentMaxBytes) || 15 * 1024 * 1024),
  });
  const attachmentFiles = Array.isArray(attachmentPlan.files) ? attachmentPlan.files : [];
  const attachmentLines = Array.isArray(attachmentPlan.lines) ? attachmentPlan.lines : [];
  const tempFiles = Array.isArray(attachmentPlan.tempFiles) ? attachmentPlan.tempFiles : [];
  if (!text && !attachmentFiles.length && !attachmentLines.length) {
    return { ok: false, error: 'empty_text', retryable: false };
  }
  if (cfg.killSwitch) {
    return { ok: false, error: 'kill_switch', retryable: false };
  }
  if (!cfg.outboundEnabled) {
    return { ok: false, error: 'outbound_disabled', retryable: false };
  }
  if (!session) {
    return { ok: false, error: 'session_not_authorized', retryable: true };
  }
  if (['reauth_required', 'disconnected', 'error'].includes(String(session.status || ''))) {
    return { ok: false, error: 'session_not_authorized', retryable: true };
  }
  if (cfg.mockMode) {
    return {
      ok: true,
      message_id: `mock-${Date.now()}`,
      chat_id: chatId,
    };
  }
  if (!session.browserRef || !session.browserRef.page) {
    return { ok: false, error: 'session_unavailable', retryable: true };
  }

  try {
    let sentFiles = 0;
    let sentText = false;
    const sendTextWithFiles = Boolean(cfg && cfg.sendTextWithAttachments);
    if (text && sendTextWithFiles) {
      await sendTextViaUi(session.browserRef, payload.to, text);
      sentText = true;
    }
    for (const filePath of attachmentFiles) {
      await _attachFileViaUi(session.browserRef, payload.to, filePath);
      sentFiles += 1;
    }
    if (text && (!attachmentFiles.length || sendTextWithFiles)) {
      if (!sentText) {
        await sendTextViaUi(session.browserRef, payload.to, text);
        sentText = true;
      }
    }
    for (const line of attachmentLines) {
      await sendTextViaUi(session.browserRef, payload.to, line);
      sentText = true;
    }
    return {
      ok: true,
      message_id: `ui-${Date.now()}`,
      chat_id: chatId,
      sent_files: sentFiles,
      sent_text: sentText,
    };
  } catch (err) {
    const reason = err && err.message ? String(err.message) : 'send_failed';
    const retryable = [
      'session_unavailable',
      'input_not_found',
      'attachment_not_prepared',
      'text_not_delivered',
    ].includes(reason);
    return { ok: false, error: reason, retryable };
  } finally {
    for (const filePath of tempFiles) {
      try {
        await fs.rm(filePath, { force: true });
      } catch (_err) {
        // ignore temp cleanup errors
      }
    }
  }
}

module.exports = {
  _attachmentPlan,
  _attachmentPlanWithOptions,
  _chatUrl,
  _openChat,
  _waitForMessageInput,
  sendText,
};
