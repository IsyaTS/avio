'use strict';

const fs = require('fs');
const { tenantSessionDir } = require('./persistence');

function cleanupProfileLocks(sessionDir) {
  for (const name of ['SingletonLock', 'SingletonCookie', 'SingletonSocket']) {
    try {
      fs.rmSync(`${String(sessionDir || '')}/${name}`, { force: true });
    } catch (_err) {
      // ignore stale lock cleanup failures
    }
  }
}

async function launchBrowserSession({ tenant, sessionDir, headless, timeoutMs }) {
  let playwright;
  try {
    playwright = require('playwright');
  } catch (_err) {
    throw new Error('playwright_not_installed');
  }

  const chromium = playwright.chromium;
  if (!chromium || typeof chromium.launchPersistentContext !== 'function') {
    throw new Error('playwright_chromium_unavailable');
  }

  fs.mkdirSync(sessionDir, { recursive: true });
  cleanupProfileLocks(sessionDir);

  const context = await chromium.launchPersistentContext(sessionDir, {
    headless: !!headless,
    viewport: { width: 1360, height: 900 },
    timeout: Number(timeoutMs || 30000),
    locale: 'ru-RU',
    timezoneId: 'Europe/Moscow',
    userAgent:
      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    args: ['--disable-blink-features=AutomationControlled'],
  });

  await context.addInitScript(() => {
    const override = (obj, key, value) => {
      try {
        Object.defineProperty(obj, key, {
          configurable: true,
          get: () => value,
        });
      } catch (_err) {
        // ignore fingerprint override failures
      }
    };

    override(Navigator.prototype, 'webdriver', false);
    override(Navigator.prototype, 'platform', 'Linux x86_64');
    override(Navigator.prototype, 'language', 'ru-RU');
    override(Navigator.prototype, 'languages', ['ru-RU', 'ru', 'en-US', 'en']);
    override(Navigator.prototype, 'hardwareConcurrency', 8);
    override(Navigator.prototype, 'plugins', [
      { name: 'Chrome PDF Plugin' },
      { name: 'Chrome PDF Viewer' },
      { name: 'Native Client' },
    ]);
  });

  let page = context.pages()[0] || null;
  if (!page) {
    page = await context.newPage();
  }

  return {
    tenant,
    context,
    page,
  };
}

async function openWebApp(browserRef, startUrl, timeoutMs) {
  if (!browserRef || !browserRef.page || !startUrl) return;
  try {
    await browserRef.page.goto(String(startUrl), {
      timeout: Number(timeoutMs || 30000),
      waitUntil: 'domcontentloaded',
    });
  } catch (_err) {
    // keep session alive even if initial navigation failed
  }
}

async function _findFirstElement(page, selectorCandidates, timeoutMs) {
  const selectors = Array.isArray(selectorCandidates) ? selectorCandidates : [];
  if (!selectors.length) return { node: null, selector: null };
  const totalTimeout = Math.max(250, Number(timeoutMs || 1500));
  const deadline = Date.now() + totalTimeout;
  while (Date.now() < deadline) {
    for (const selector of selectors) {
      try {
        const node = await page.$(selector);
        if (node) return { node, selector };
      } catch (_err) {
        // continue
      }
    }
    try {
      await page.waitForTimeout(120);
    } catch (_err) {
      // ignore
    }
  }
  return { node: null, selector: null };
}

async function probeAuthorized(browserRef, selectors, timeoutMs) {
  if (!browserRef || !browserRef.page) return false;
  const page = browserRef.page;
  const hasQrCopy = await _hasQrLoginCopy(page);
  if (hasQrCopy) return false;
  const [{ node: chatNode }, { node: inputNode }] = await Promise.all([
    _findFirstElement(page, selectors.chatList || [], timeoutMs || 1500),
    _findFirstElement(page, selectors.messageInput || [], timeoutMs || 1500),
  ]);
  return Boolean(chatNode && inputNode);
}

async function _hasQrLoginCopy(page) {
  try {
    return await page.evaluate(() => {
      const text = String(document.body?.innerText || '').toLowerCase();
      return (
        text.includes('войдите в max по qr-коду') ||
        text.includes('вход по qr-коду') ||
        text.includes('наведите камеру на qr-код') ||
        text.includes('войти по qr') ||
        text.includes('qr-код') ||
        text.includes('qr код') ||
        text.includes('сканируйте qr') ||
        text.includes('откройте max')
      );
    });
  } catch (_err) {
    return false;
  }
}

async function _readQrFromSelectors(page, selectorCandidates, kind, timeoutMs) {
  const selectors = Array.isArray(selectorCandidates) ? selectorCandidates : [];
  const timeout = Math.max(250, Number(timeoutMs || 1500));
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeout) {
    for (const selector of selectors) {
      try {
        const result = await page.$$eval(
          selector,
          (nodes, type) => {
            const rank = (node) => {
              const rect = node.getBoundingClientRect();
              const style = window.getComputedStyle(node);
              if (
                style.display === 'none' ||
                style.visibility === 'hidden' ||
                Number(style.opacity || '1') === 0
              ) {
                return null;
              }
              const width = Number(rect.width || 0);
              const height = Number(rect.height || 0);
              if (width < 110 || height < 110) return null;
              const ratio = height > 0 ? width / height : 0;
              if (ratio < 0.75 || ratio > 1.25) return null;
              const attrs = `${node.id || ''} ${node.className || ''} ${node.getAttribute?.('data-testid') || ''} ${node.getAttribute?.('aria-label') || ''} ${node.getAttribute?.('alt') || ''}`.toLowerCase();
              const qrHint =
                attrs.includes('qr') || attrs.includes('auth') || attrs.includes('login');
              const area = width * height;
              return {
                area,
                qrHint,
                html: String(node.outerHTML || ''),
                src: type === 'img' ? String(node.getAttribute('src') || '') : '',
                data:
                  type === 'canvas' && typeof node.toDataURL === 'function'
                    ? node.toDataURL('image/png')
                    : '',
              };
            };
            const ranked = [];
            for (const node of nodes || []) {
              const info = rank(node);
              if (!info) continue;
              ranked.push(info);
            }
            ranked.sort((a, b) => {
              const hintDiff = Number(Boolean(b.qrHint)) - Number(Boolean(a.qrHint));
              if (hintDiff !== 0) return hintDiff;
              return Number(b.area || 0) - Number(a.area || 0);
            });
            return ranked[0] || null;
          },
          kind
        );
        if (result) {
          if (kind === 'img' && result.src && String(result.src).trim()) {
            const src = String(result.src).trim();
            const looksLikeQrSrc =
              src.startsWith('data:image/') ||
              src.startsWith('blob:') ||
              /qr|auth|login/i.test(src);
            if (!looksLikeQrSrc) {
              continue;
            }
            return {
              qrPngDataUrl: src,
              qrSvg: null,
              qrText: null,
            };
          }
          if (kind === 'svg' && result.html) {
            return {
              qrPngDataUrl: `data:image/svg+xml;utf8,${encodeURIComponent(String(result.html))}`,
              qrSvg: String(result.html),
              qrText: null,
            };
          }
          if (kind === 'canvas' && result.data) {
            return {
              qrPngDataUrl: String(result.data),
              qrSvg: null,
              qrText: null,
            };
          }
        }
      } catch (_err) {
        // keep scanning selectors
      }
    }
    try {
      await page.waitForTimeout(120);
    } catch (_err) {
      // ignore
    }
  }
  return null;
}

async function _readQrByElementScreenshot(page, timeoutMs) {
  const timeout = Math.max(250, Number(timeoutMs || 1500));
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeout) {
    let selector = null;
    try {
      selector = await page.evaluate(() => {
        const viewportW = window.innerWidth || 0;
        const viewportH = window.innerHeight || 0;
        const visible = (node) => {
          if (!node || typeof node.getBoundingClientRect !== 'function') return null;
          const rect = node.getBoundingClientRect();
          const style = window.getComputedStyle(node);
          if (
            style.display === 'none' ||
            style.visibility === 'hidden' ||
            Number(style.opacity || '1') === 0
          ) {
            return null;
          }
          const width = Number(rect.width || 0);
          const height = Number(rect.height || 0);
          if (width < 150 || height < 150 || width > 520 || height > 520) return null;
          const ratio = height > 0 ? width / height : 0;
          if (ratio < 0.8 || ratio > 1.2) return null;
          if (rect.left < 0 || rect.top < 0 || rect.right > viewportW || rect.bottom > viewportH) {
            return null;
          }
          const attrs = `${node.id || ''} ${node.className || ''} ${node.getAttribute?.('data-testid') || ''} ${node.getAttribute?.('aria-label') || ''} ${node.getAttribute?.('alt') || ''}`.toLowerCase();
          const hasQrHint = attrs.includes('qr') || attrs.includes('auth') || attrs.includes('login');
          const tag = String(node.tagName || '').toLowerCase();
          const hasQrChild = Boolean(
            node.querySelector?.('canvas,svg,img,[data-testid*="qr" i],[aria-label*="qr" i]')
          );
          const centrality =
            Math.abs(rect.left + width / 2 - viewportW / 2) +
            Math.abs(rect.top + height / 2 - viewportH / 2);
          return {
            tag,
            hasQrHint,
            hasQrChild,
            area: width * height,
            centrality,
          };
        };
        const candidates = [];
        const nodes = Array.from(
          document.querySelectorAll('canvas,svg,img,[data-testid*="qr" i],[aria-label*="qr" i],div')
        );
        nodes.forEach((node, index) => {
          const info = visible(node);
          if (!info) return;
          if (!info.hasQrHint && !info.hasQrChild && info.tag === 'div') return;
          candidates.push({ index, ...info });
        });
        candidates.sort((a, b) => {
          const qrDiff =
            Number(Boolean(b.hasQrHint || b.hasQrChild)) -
            Number(Boolean(a.hasQrHint || a.hasQrChild));
          if (qrDiff !== 0) return qrDiff;
          const centralDiff = Number(a.centrality || 0) - Number(b.centrality || 0);
          if (centralDiff !== 0) return centralDiff;
          return Number(b.area || 0) - Number(a.area || 0);
        });
        if (!candidates.length) return null;
        return `__qr_candidate_${candidates[0].index}`;
      });
    } catch (_err) {
      selector = null;
    }

    if (selector) {
      try {
        await page.evaluate((marker) => {
          const nodes = Array.from(
            document.querySelectorAll('canvas,svg,img,[data-testid*="qr" i],[aria-label*="qr" i],div')
          );
          const index = Number(String(marker || '').replace('__qr_candidate_', ''));
          const node = nodes[index];
          if (node) node.setAttribute('data-avio-qr-candidate', '1');
        }, selector);
        const handle = await page.$('[data-avio-qr-candidate="1"]');
        if (handle) {
          const buffer = await handle.screenshot({ type: 'png' });
          if (buffer && buffer.length > 1000) {
            return {
              qrPngDataUrl: `data:image/png;base64,${buffer.toString('base64')}`,
              qrSvg: null,
              qrText: null,
            };
          }
        }
      } catch (_err) {
        // keep polling until timeout
      } finally {
        try {
          await page.evaluate(() => {
            document
              .querySelectorAll('[data-avio-qr-candidate="1"]')
              .forEach((node) => node.removeAttribute('data-avio-qr-candidate'));
          });
        } catch (_err) {
          // ignore cleanup failure
        }
      }
    }
    try {
      await page.waitForTimeout(150);
    } catch (_err) {
      // ignore
    }
  }
  return null;
}

async function readQrSnapshot(browserRef, selectors, timeoutMs) {
  if (!browserRef || !browserRef.page) return { qrPngDataUrl: null, qrSvg: null, qrText: null };
  const page = browserRef.page;
  const hasQrCopy = await _hasQrLoginCopy(page);

  const imageSnapshot = await _readQrFromSelectors(
    page,
    selectors.qrImage || [],
    'img',
    timeoutMs || 1500
  );
  if (imageSnapshot) return imageSnapshot;

  const svgSnapshot = await _readQrFromSelectors(
    page,
    selectors.qrSvg || [],
    'svg',
    timeoutMs || 1500
  );
  if (svgSnapshot) return svgSnapshot;

  const canvasSnapshot = await _readQrFromSelectors(
    page,
    selectors.qrCanvas || [],
    'canvas',
    timeoutMs || 1500
  );
  if (canvasSnapshot) return canvasSnapshot;

  if (hasQrCopy) {
    const screenshotSnapshot = await _readQrByElementScreenshot(page, timeoutMs || 1500);
    if (screenshotSnapshot) return screenshotSnapshot;

    const { node: qrTextNode } = await _findFirstElement(page, selectors.qrText || [], timeoutMs || 900);
    if (qrTextNode) {
      try {
        const qrTextValue = await qrTextNode.evaluate((el) => {
          const attr = el.getAttribute('data-qr');
          if (attr && String(attr).trim()) return String(attr).trim();
          const txt = el.textContent || '';
          return String(txt).trim();
        });
        if (qrTextValue) {
          return { qrPngDataUrl: null, qrSvg: null, qrText: String(qrTextValue) };
        }
      } catch (_err) {
        // continue
      }
    }
  }

  return { qrPngDataUrl: null, qrSvg: null, qrText: null };
}

async function readAccountIdentity(browserRef, selectors, timeoutMs) {
  if (!browserRef || !browserRef.page) return {};
  const page = browserRef.page;
  const { selector } = await _findFirstElement(
    page,
    selectors.accountTitle || [],
    timeoutMs || 1000
  );
  if (!selector) return {};
  try {
    const text = await page.$eval(selector, (el) => String(el.textContent || '').trim());
    if (!text) return {};
    return {
      display_name: text,
      account_id: '',
      username: '',
      phone: '',
    };
  } catch (_err) {
    return {};
  }
}

async function closeBrowserSession(browserRef) {
  if (!browserRef) return;
  try {
    if (browserRef.page && typeof browserRef.page.close === 'function') {
      await browserRef.page.close();
    }
  } catch (_err) {
    // ignore
  }
  try {
    if (browserRef.context && typeof browserRef.context.close === 'function') {
      await browserRef.context.close();
    }
  } catch (_err) {
    // ignore
  }
}

async function attachInboundTap(browserRef, onPayload) {
  if (!browserRef || !browserRef.page || typeof onPayload !== 'function') {
    return () => {};
  }
  let stopped = false;
  const cleanups = [];
  const safeEmit = (payload, meta = {}) => {
    if (stopped || !payload) return;
    try {
      onPayload(payload, meta);
    } catch (_err) {
      // ignore callback errors
    }
  };

  try {
    const context = browserRef.context;
    const page = browserRef.page;
    const cdp = await context.newCDPSession(page);
    await cdp.send('Network.enable');
    const wsHandler = (evt) => {
      const raw = String(evt?.response?.payloadData || '').trim();
      if (!raw || raw.length < 2) return;
      let parsed = null;
      try {
        parsed = JSON.parse(raw);
      } catch (_err) {
        return;
      }
      safeEmit(parsed, { source: 'ws' });
    };
    cdp.on('Network.webSocketFrameReceived', wsHandler);
    cleanups.push(() => {
      try {
        cdp.off('Network.webSocketFrameReceived', wsHandler);
      } catch (_err) {
        // ignore
      }
      void Promise.resolve(cdp.detach()).catch(() => undefined);
    });
  } catch (_err) {
    // optional path
  }

  try {
    const page = browserRef.page;
    const bindingName = '__avioMaxDomInbound';
    try {
      await page.exposeBinding(bindingName, async (_source, payload) => {
        safeEmit(payload, { source: 'dom' });
      });
    } catch (_err) {
      // binding may already exist for this page lifecycle
    }
    await page.evaluate((name) => {
      if (window.__avioMaxDomTapInstalled) return;
      window.__avioMaxDomTapInstalled = true;
      const sent = new Set();
      const maxSeen = 250;

      const pickChatId = () => {
        try {
          const url = new URL(window.location.href);
          const fromParams =
            url.searchParams.get('chat') ||
            url.searchParams.get('dialog') ||
            url.searchParams.get('peer') ||
            url.searchParams.get('conversation');
          if (fromParams && String(fromParams).trim()) return String(fromParams).trim();
          const path = String(url.pathname || '');
          const m = path.match(/(?:chat|dialog|peer|conversation)\/([^/?#]+)/i);
          if (m && m[1]) return String(m[1]).trim();
        } catch (_err) {
          // ignore
        }
        return '';
      };

      const readText = (node) => {
        const raw = String(node?.innerText || node?.textContent || '').trim();
        if (!raw) return '';
        if (raw.length > 4096) return '';
        return raw;
      };

      const findBubbleRoot = (node) => {
        let current = node;
        let best = node;
        for (let depth = 0; current && depth < 7; depth += 1) {
          const klass = String(current?.className || '').toLowerCase();
          const testId = String(current?.getAttribute?.('data-testid') || '').toLowerCase();
          const hasMessageMarker =
            current?.getAttribute?.('data-message-id') ||
            current?.getAttribute?.('data-id') ||
            current?.dataset?.messageId ||
            current?.dataset?.id ||
            klass.includes('message') ||
            klass.includes('msg') ||
            klass.includes('bubble') ||
            testId.includes('message') ||
            testId.includes('msg');
          if (hasMessageMarker) best = current;
          current = current.parentElement;
        }
        return best || node;
      };

      const getNodeDebug = (node) => {
        try {
          const root = findBubbleRoot(node);
          const rect = root?.getBoundingClientRect?.();
          const style = window.getComputedStyle ? window.getComputedStyle(root) : null;
          return {
            root_class: String(root?.className || '').slice(0, 160),
            root_testid: String(root?.getAttribute?.('data-testid') || ''),
            left: rect ? Math.round(rect.left) : null,
            right: rect ? Math.round(rect.right) : null,
            width: rect ? Math.round(rect.width) : null,
            viewport: window.innerWidth || null,
            margin_left: style ? String(style.marginLeft || '') : '',
            margin_right: style ? String(style.marginRight || '') : '',
            align_self: style ? String(style.alignSelf || '') : '',
          };
        } catch (_err) {
          return {};
        }
      };

      const readMessageId = (node, idx) => {
        const fromAttr =
          node?.getAttribute?.('data-message-id') ||
          node?.getAttribute?.('data-id') ||
          node?.dataset?.messageId ||
          node?.dataset?.id ||
          node?.dataset?.avioMaxMessageId ||
          node?.id ||
          '';
        if (fromAttr && String(fromAttr).trim()) return String(fromAttr).trim();
        const generated = `dom-${Date.now()}-${idx}`;
        try {
          if (node?.dataset) node.dataset.avioMaxMessageId = generated;
        } catch (_err) {
          // ignore
        }
        return generated;
      };

      const isFromSelf = (node) => {
        let current = node;
        for (let depth = 0; current && depth < 7; depth += 1) {
          const klass = String(current?.className || '').toLowerCase();
          const aria = String(current?.getAttribute?.('aria-label') || '').toLowerCase();
          const testId = String(current?.getAttribute?.('data-testid') || '').toLowerCase();
          const dir = String(current?.getAttribute?.('data-direction') || '').toLowerCase();
          const dataOwn = String(current?.getAttribute?.('data-own') || '').toLowerCase();
          let style = null;
          try {
            style = window.getComputedStyle ? window.getComputedStyle(current) : null;
          } catch (_err) {
            style = null;
          }
          const marginLeft = String(style?.marginLeft || '').toLowerCase();
          const marginRight = String(style?.marginRight || '').toLowerCase();
          const alignSelf = String(style?.alignSelf || '').toLowerCase();
          const justifySelf = String(style?.justifySelf || '').toLowerCase();
          if (
            klass.includes('out') ||
            klass.includes('self') ||
            klass.includes('own') ||
            klass.includes('sent') ||
            klass.includes('right') ||
            klass.includes('mine') ||
            klass.includes('me-') ||
            aria.includes('outgoing') ||
            aria.includes('исход') ||
            testId.includes('out') ||
            testId.includes('sent') ||
            dir === 'out' ||
            dir === 'outgoing' ||
            dataOwn === 'true' ||
            dataOwn === '1' ||
            marginLeft === 'auto' ||
            marginLeft === 'auto auto' ||
            alignSelf === 'flex-end' ||
            justifySelf === 'end' ||
            justifySelf === 'flex-end'
          ) {
            return true;
          }
          if (marginLeft === '0px' && marginRight === 'auto') {
            return false;
          }
          current = current.parentElement;
        }
        try {
          const root = findBubbleRoot(node);
          const rect = root?.getBoundingClientRect?.();
          const viewport = window.innerWidth || 0;
          if (rect && rect.width > 0 && viewport > 0 && rect.left > viewport * 0.42) return true;
          if (rect && rect.width > 0 && viewport > 0 && rect.right > viewport * 0.78) {
            return true;
          }
        } catch (_err) {
          // geometry is best-effort only
        }
        return false;
      };

      const emitNode = (node, idx) => {
        const chatId = pickChatId();
        if (!chatId) return;
        const text = readText(node);
        if (!text) return;
        const messageId = readMessageId(node, idx);
        const key = `${chatId}|${messageId}|${text}`;
        if (sent.has(key)) return;
        sent.add(key);
        if (sent.size > maxSeen) {
          const first = sent.values().next().value;
          if (first) sent.delete(first);
        }
        const payload = {
          chat_id: chatId,
          message_id: messageId,
          text,
          from_self: isFromSelf(node),
          dom_debug: getNodeDebug(node),
          ts: Date.now(),
          type: 'message',
        };
        try {
          window[name](payload);
        } catch (_err) {
          // ignore callback failures
        }
      };

      const collect = () => {
        const selectors = [
          '[data-message-id]',
          '[data-testid*="message"]',
          '[data-testid*="msg"]',
          '[class*="message"]',
        ];
        let index = 0;
        for (const selector of selectors) {
          const nodes = document.querySelectorAll(selector);
          for (const node of nodes) {
            emitNode(node, index);
            index += 1;
          }
        }
      };

      const observer = new MutationObserver(() => {
        try {
          collect();
        } catch (_err) {
          // ignore
        }
      });
      observer.observe(document.body || document.documentElement, {
        childList: true,
        subtree: true,
      });

      const timer = window.setInterval(() => {
        try {
          collect();
        } catch (_err) {
          // ignore
        }
      }, 1500);

      window.__avioMaxDomTapStop = () => {
        try {
          observer.disconnect();
        } catch (_err) {
          // ignore
        }
        try {
          window.clearInterval(timer);
        } catch (_err) {
          // ignore
        }
      };

      collect();
    }, bindingName);
    cleanups.push(() => {
      void browserRef.page
        .evaluate(() => {
          try {
            if (typeof window.__avioMaxDomTapStop === 'function') {
              window.__avioMaxDomTapStop();
            }
            window.__avioMaxDomTapStop = null;
            window.__avioMaxDomTapInstalled = false;
          } catch (_err) {
            // ignore
          }
        })
        .catch(() => undefined);
    });
  } catch (_err) {
    // optional path
  }

  const responseHandler = async (response) => {
    if (stopped || !response) return;
    const url = String(response.url?.() || '').toLowerCase();
    if (
      !url.includes('message') &&
      !url.includes('chat') &&
      !url.includes('dialog') &&
      !url.includes('event') &&
      !url.includes('updates')
    ) {
      return;
    }
    const headers = response.headers?.() || {};
    const ctype = String(headers['content-type'] || headers['Content-Type'] || '').toLowerCase();
    if (!ctype.includes('json')) return;
    try {
      const data = await response.json();
      if (data && (Array.isArray(data) || typeof data === 'object')) {
        safeEmit(data, { source: 'http', url });
      }
    } catch (_err) {
      // ignore parse errors
    }
  };
  browserRef.page.on('response', responseHandler);
  cleanups.push(() => {
    try {
      browserRef.page.off('response', responseHandler);
    } catch (_err) {
      // ignore
    }
  });

  return () => {
    stopped = true;
    while (cleanups.length) {
      const fn = cleanups.pop();
      try {
        fn();
      } catch (_err) {
        // ignore
      }
    }
  };
}

module.exports = {
  launchBrowserSession,
  openWebApp,
  probeAuthorized,
  readAccountIdentity,
  readQrSnapshot,
  attachInboundTap,
  closeBrowserSession,
  tenantSessionDir,
};
