const fs = require('fs/promises');
const path = require('path');
const {
  initAuthCreds,
  BufferJSON,
} = require('@whiskeysockets/baileys');

const ensureDir = async (dir) => {
  await fs.mkdir(dir, { recursive: true });
};

const readJsonIfExists = async (file) => {
  try {
    const raw = await fs.readFile(file, 'utf8');
    return JSON.parse(raw, BufferJSON.reviver);
  } catch (err) {
    if (err && err.code === 'ENOENT') {
      return null;
    }
    throw err;
  }
};

const writeJson = async (file, data) => {
  const payload = JSON.stringify(data, BufferJSON.replacer, 2);
  await fs.writeFile(file, payload, 'utf8');
};

async function createAuthStore(rootDir, logger) {
  await ensureDir(rootDir);
  const credsPath = path.join(rootDir, 'creds.json');
  const keysPath = path.join(rootDir, 'keys.json');

  let creds = await readJsonIfExists(credsPath);
  if (!creds) {
    creds = initAuthCreds();
    await writeJson(credsPath, creds);
  }

  let keys = (await readJsonIfExists(keysPath)) || {};

  const keyStore = {
    get: async (type, ids) => {
      const stored = keys[type] || {};
      const results = {};
      for (const id of ids) {
        if (stored[id]) {
          results[id] = stored[id];
        }
      }
      return results;
    },
    set: async (data) => {
      for (const type of Object.keys(data || {})) {
        if (!keys[type]) {
          keys[type] = {};
        }
        Object.assign(keys[type], data[type]);
      }
      await writeJson(keysPath, keys);
    },
    clear: async () => {
      keys = {};
      await writeJson(keysPath, keys);
    },
  };

  const saveCreds = async () => {
    await writeJson(credsPath, creds);
  };

  return {
    state: {
      creds,
      keys: keyStore,
    },
    saveCreds,
  };
}

module.exports = { createAuthStore };
