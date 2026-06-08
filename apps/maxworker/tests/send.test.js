'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs/promises');
const os = require('os');
const path = require('path');

const { _chatUrl, _openChat, sendText } = require('../send');
const { STATUS } = require('../state');

test('_chatUrl builds direct MAX chat route', () => {
  assert.equal(_chatUrl('93267442'), 'https://web.max.ru/93267442');
  assert.equal(_chatUrl('chat:abc'), 'https://web.max.ru/chat%3Aabc');
});

test('_openChat navigates to chat-specific route and waits for composer', async () => {
  const waitCalls = [];
  const input = {
    waitFor: async (params) => {
      waitCalls.push(params);
    },
  };
  const page = {
    gotoCalls: [],
    locator: () => ({
      first: () => input,
    }),
    goto: async function (url, params) {
      this.gotoCalls.push({ url, params });
    },
    waitForTimeout: async () => undefined,
  };

  const result = await _openChat(page, '93267442');

  assert.equal(result, input);
  assert.equal(page.gotoCalls.length, 1);
  assert.equal(page.gotoCalls[0].url, 'https://web.max.ru/93267442');
  assert.equal(waitCalls.length >= 1, true);
});

test('sendText uses chat route before typing', async () => {
  const clicked = [];
  const filled = [];
  const typed = [];
  const buttons = [];
  const input = {
    waitFor: async () => undefined,
    click: async () => {
      clicked.push('input');
    },
    fill: async (value) => {
      filled.push(value);
    },
  };
  const button = {
    waitFor: async () => undefined,
    click: async () => {
      buttons.push('send');
    },
  };
  const page = {
    gotoCalls: [],
    locator: (selector) => ({
      first: () => (selector.includes('send') ? button : input),
    }),
    goto: async function (url, params) {
      this.gotoCalls.push({ url, params });
    },
    waitForTimeout: async () => undefined,
    keyboard: {
      type: async (value) => {
        typed.push(value);
      },
      press: async () => undefined,
    },
  };

  const result = await sendText(
    {
      status: STATUS.AUTHORIZED,
      browserRef: { page },
    },
    {
      tenant: 101,
      to: '93267442',
      text: 'hello max',
    },
    {
      killSwitch: false,
      outboundEnabled: true,
      mockMode: false,
    }
  );

  assert.equal(result.ok, true);
  assert.equal(page.gotoCalls.length, 1);
  assert.equal(page.gotoCalls[0].url, 'https://web.max.ru/93267442');
  assert.deepEqual(clicked, ['input']);
  assert.deepEqual(filled, ['hello max']);
  assert.deepEqual(typed, []);
  assert.deepEqual(buttons, ['send']);
});

test('sendText allows attachment-only payload and sends attachment links', async () => {
  const filled = [];
  const input = {
    waitFor: async () => undefined,
    click: async () => undefined,
    fill: async (value) => {
      filled.push(value);
    },
  };
  const button = {
    waitFor: async () => undefined,
    click: async () => undefined,
  };
  const page = {
    locator: (selector) => ({
      first: () => (selector.includes('send') ? button : input),
    }),
    goto: async () => undefined,
    waitForTimeout: async () => undefined,
    keyboard: {
      type: async () => undefined,
      press: async () => undefined,
    },
  };

  const result = await sendText(
    {
      status: STATUS.AUTHORIZED,
      browserRef: { page },
    },
    {
      tenant: 101,
      to: '93267442',
      text: '',
      attachments: [
        { type: 'image', url: 'https://cdn.local/photo.jpg' },
        { type: 'file', name: 'price.pdf', url: 'https://cdn.local/price.pdf' },
      ],
    },
    {
      killSwitch: false,
      outboundEnabled: true,
      mockMode: false,
    }
  );

  assert.equal(result.ok, true);
  assert.deepEqual(filled, ['Фото: https://cdn.local/photo.jpg', 'price.pdf: https://cdn.local/price.pdf']);
});

test('sendText downloads remote image and uploads as file when enabled', async () => {
  const prevFetch = global.fetch;
  global.fetch = async () => ({
    ok: true,
    headers: {
      get: (name) => {
        const key = String(name || '').toLowerCase();
        if (key === 'content-type') return 'image/jpeg';
        if (key === 'content-length') return '12';
        return null;
      },
    },
    arrayBuffer: async () => Buffer.from('mock-jpeg-12'),
  });

  const filled = [];
  const setInputCalls = [];
  const input = {
    waitFor: async () => undefined,
    click: async () => undefined,
    fill: async (value) => {
      filled.push(value);
    },
  };
  const button = {
    waitFor: async () => undefined,
    click: async () => undefined,
  };
  const page = {
    locator: (selector) => ({
      first: () => (selector.includes('send') ? button : input),
    }),
    goto: async () => undefined,
    waitForTimeout: async () => undefined,
    waitForEvent: async () => {
      throw new Error('no_filechooser');
    },
    setInputFiles: async (selector, localPath) => {
      setInputCalls.push({ selector, localPath });
      if (!selector.includes('input[type="file"]')) {
        throw new Error('selector_not_supported');
      }
    },
    keyboard: {
      type: async () => undefined,
      press: async () => undefined,
    },
  };

  try {
    const result = await sendText(
      {
        status: STATUS.AUTHORIZED,
        browserRef: { page },
      },
      {
        tenant: 101,
        to: '93267442',
        text: '',
        attachments: [{ type: 'image', url: 'https://cdn.local/photo.jpg' }],
      },
      {
        killSwitch: false,
        outboundEnabled: true,
        mockMode: false,
        fetchRemoteAttachments: true,
        remoteAttachmentTimeoutMs: 5000,
        remoteAttachmentMaxBytes: 1024 * 1024,
      }
    );

    assert.equal(result.ok, true);
    assert.equal(setInputCalls.length >= 1, true);
    assert.equal(filled.length, 0);
    assert.match(String(setInputCalls[0].localPath || ''), /\.jpg$/);
  } finally {
    global.fetch = prevFetch;
  }
});

test('sendText allows send when session is authorizing but browser page is alive', async () => {
  const filled = [];
  const input = {
    waitFor: async () => undefined,
    click: async () => undefined,
    fill: async (value) => {
      filled.push(value);
    },
  };
  const button = {
    waitFor: async () => undefined,
    click: async () => undefined,
  };
  const page = {
    locator: (selector) => ({
      first: () => (selector.includes('send') ? button : input),
    }),
    goto: async () => undefined,
    waitForTimeout: async () => undefined,
    keyboard: {
      type: async () => undefined,
      press: async () => undefined,
    },
  };

  const result = await sendText(
    {
      status: STATUS.AUTHORIZING,
      browserRef: { page },
    },
    {
      tenant: 101,
      to: '93267442',
      text: 'hello from authorizing',
    },
    {
      killSwitch: false,
      outboundEnabled: true,
      mockMode: false,
    }
  );

  assert.equal(result.ok, true);
  assert.deepEqual(filled, ['hello from authorizing']);
});

test('sendText uploads local attachment file when path is provided', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'maxworker-send-'));
  const filePath = path.join(tempDir, 'catalog.pdf');
  await fs.writeFile(filePath, Buffer.from('dummy-pdf'));

  const setInputCalls = [];
  const input = {
    waitFor: async () => undefined,
    click: async () => undefined,
    fill: async () => undefined,
  };
  const button = {
    waitFor: async () => undefined,
    click: async () => undefined,
  };
  const page = {
    locator: (selector) => ({
      first: () => (selector.includes('send') ? button : input),
    }),
    goto: async () => undefined,
    waitForTimeout: async () => undefined,
    waitForEvent: async () => {
      throw new Error('no_filechooser');
    },
    setInputFiles: async (selector, localPath) => {
      setInputCalls.push({ selector, localPath });
      if (!selector.includes('input[type="file"]')) {
        throw new Error('selector_not_supported');
      }
    },
    keyboard: {
      type: async () => undefined,
      press: async () => undefined,
    },
  };

  const result = await sendText(
    {
      status: STATUS.AUTHORIZED,
      browserRef: { page },
    },
    {
      tenant: 101,
      to: '93267442',
      text: '',
      attachments: [{ type: 'document', path: filePath, filename: 'catalog.pdf' }],
    },
    {
      killSwitch: false,
      outboundEnabled: true,
      mockMode: false,
    }
  );

  assert.equal(result.ok, true);
  assert.equal(setInputCalls.length >= 1, true);
  assert.equal(setInputCalls[0].localPath, filePath);

  await fs.rm(tempDir, { recursive: true, force: true });
});

test('sendText remaps /app/data/tenants attachment path to MAX worker tenants dir', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'maxworker-remap-'));
  const tenantsRoot = path.join(tempDir, 'tenants-root');
  const targetDir = path.join(tenantsRoot, '101');
  await fs.mkdir(targetDir, { recursive: true });
  const targetFile = path.join(targetDir, 'catalog.pdf');
  await fs.writeFile(targetFile, Buffer.from('dummy-pdf'));

  const prevTenantsDir = process.env.MAX_PERSONAL_TENANTS_DIR;
  process.env.MAX_PERSONAL_TENANTS_DIR = tenantsRoot;

  const setInputCalls = [];
  const input = {
    waitFor: async () => undefined,
    click: async () => undefined,
    fill: async () => undefined,
  };
  const button = {
    waitFor: async () => undefined,
    click: async () => undefined,
  };
  const page = {
    locator: (selector) => ({
      first: () => (selector.includes('send') ? button : input),
    }),
    goto: async () => undefined,
    waitForTimeout: async () => undefined,
    waitForEvent: async () => {
      throw new Error('no_filechooser');
    },
    setInputFiles: async (selector, localPath) => {
      setInputCalls.push({ selector, localPath });
      if (!selector.includes('input[type="file"]')) {
        throw new Error('selector_not_supported');
      }
    },
    keyboard: {
      type: async () => undefined,
      press: async () => undefined,
    },
  };

  const result = await sendText(
    {
      status: STATUS.AUTHORIZED,
      browserRef: { page },
    },
    {
      tenant: 101,
      to: '93267442',
      text: '',
      attachments: [
        {
          type: 'document',
          path: '/app/data/tenants/101/catalog.pdf',
          filename: 'catalog.pdf',
        },
      ],
    },
    {
      killSwitch: false,
      outboundEnabled: true,
      mockMode: false,
    }
  );

  assert.equal(result.ok, true);
  assert.equal(setInputCalls.length >= 1, true);
  assert.equal(setInputCalls[0].localPath, targetFile);

  if (prevTenantsDir === undefined) delete process.env.MAX_PERSONAL_TENANTS_DIR;
  else process.env.MAX_PERSONAL_TENANTS_DIR = prevTenantsDir;
  await fs.rm(tempDir, { recursive: true, force: true });
});

test('sendText uploads via filechooser when available', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'maxworker-chooser-'));
  const filePath = path.join(tempDir, 'catalog.pdf');
  await fs.writeFile(filePath, Buffer.from('dummy-pdf'));

  const chooserCalls = [];
  const input = {
    waitFor: async () => undefined,
    click: async () => undefined,
    fill: async () => undefined,
  };
  const button = {
    waitFor: async () => undefined,
    click: async () => undefined,
  };
  const page = {
    locator: (selector) => ({
      first: () => (selector.includes('send') ? button : input),
    }),
    goto: async () => undefined,
    waitForTimeout: async () => undefined,
    waitForEvent: async (eventName) => {
      assert.equal(eventName, 'filechooser');
      return {
        setFiles: async (localPath) => {
          chooserCalls.push(localPath);
        },
      };
    },
    setInputFiles: async () => {
      throw new Error('should_not_use_set_input_files');
    },
    keyboard: {
      type: async () => undefined,
      press: async () => undefined,
    },
  };

  const result = await sendText(
    {
      status: STATUS.AUTHORIZED,
      browserRef: { page },
    },
    {
      tenant: 101,
      to: '93267442',
      text: '',
      attachments: [{ type: 'document', path: filePath, filename: 'catalog.pdf' }],
    },
    {
      killSwitch: false,
      outboundEnabled: true,
      mockMode: false,
    }
  );

  assert.equal(result.ok, true);
  assert.deepEqual(chooserCalls, [filePath]);
  await fs.rm(tempDir, { recursive: true, force: true });
});
