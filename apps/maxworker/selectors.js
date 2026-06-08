'use strict';

/**
 * Selector presets for MAX web UI automation.
 * Keep this isolated so selector updates do not affect session logic.
 */
const selectors = Object.freeze({
  qrCanvas: [
    '[data-testid="qr-code"] canvas',
    '[data-testid="auth-qr"] canvas',
    'canvas[aria-label*="QR"]',
  ],
  qrImage: [
    '[data-testid="qr-code"] img',
    '[data-testid="auth-qr"] img',
    'img[aria-label*="QR"]',
    'img[alt*="QR"]',
  ],
  qrSvg: [
    '[data-testid="qr-code"] svg',
    '[data-testid="auth-qr"] svg',
    'svg[aria-label*="QR"]',
  ],
  qrText: [
    '[data-testid="qr-code"][data-qr]',
    '[data-testid="auth-qr"][data-qr]',
    '[data-testid*="qr"] code',
  ],
  accountTitle: [
    '[data-testid="profile-name"]',
    '[data-testid="account-name"]',
    '.profile-name',
  ],
  chatList: [
    '[data-testid="chat-list"]',
    '[role="list"][data-testid*="chat"]',
  ],
  messageInput: [
    '[data-testid="composer-input"]',
    '[data-testid="composer"] [data-lexical-editor="true"]',
    '[data-testid="composer"] [role="textbox"]',
    '[data-lexical-editor="true"]',
    '[role="textbox"][aria-placeholder*="Сообщ"]',
    '[contenteditable][data-testid*="input"]',
    'textarea[data-testid*="input"]',
  ],
  sendButton: [
    '[data-testid="composer-send"]',
    'button[aria-label*="Отправить"]',
    'button[data-testid*="send"]',
    'button[aria-label*="Send"]',
  ],
  attachmentButton: [
    '[data-testid="composer"] [data-testid="composer-attach"]',
    '[data-testid="composer"] [data-testid*="attach"]',
    '[data-testid="composer-attach"]',
    '[data-testid="composer"] button[aria-label*="Загрузить файл"]',
    '[data-testid="composer"] button[aria-label*="Прикреп"]',
    '[data-testid="composer"] button[aria-label*="Attach"]',
    'button[aria-label*="Загрузить файл"]',
    'button[aria-label*="Прикреп"]',
    'button[aria-label*="Attach"]',
  ],
  attachmentFileAction: [
    'button[aria-label="Файл"]',
    'button[aria-label*="Файл"]',
    '[role="menuitem"][aria-label="Файл"]',
    '[role="menuitem"][aria-label*="Файл"]',
    'button:has-text("Файл")',
  ],
  fileInput: [
    'input[type="file"][data-testid*="attach"]',
    '[data-testid="composer"] input[type="file"]',
    'footer input[type="file"]',
    'form input[type="file"]',
  ],
  attachmentPreview: [
    '[data-testid*="attachment-preview"]',
    '[data-testid*="attach-preview"]',
    '[data-testid*="composer-attachment"]',
    '[data-testid*="uploaded-file"]',
    '[data-testid*="media-preview"]',
  ],
});

module.exports = {
  selectors,
};
