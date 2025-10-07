(function () {
  function extractVersion(src) {
    if (typeof src !== 'string') return 'unknown';
    try {
      const url = new URL(src, window.location.origin);
      const versionParam = url.searchParams.get('v');
      return versionParam || 'unknown';
    } catch (error) {
      try {
        const match = src.match(/[?&]v=([^&]+)/);
        if (match && match[1]) return match[1];
      } catch (_) {
        /* noop */
      }
      return 'unknown';
    }
  }

  const currentScript = document.currentScript;
  const scriptVersion = extractVersion(currentScript && currentScript.getAttribute('src'));
  const bootStartedAt = Date.now();
  window.__EXPORT_BIND_OK__ = false;
  window.__EXPORT_BOOT_TS__ = bootStartedAt;

  document.addEventListener('DOMContentLoaded', () => {
    let button = document.getElementById('export-download');
    if (!button) {
      console.info('[boot.js] export button missing; version=%s started=%s', scriptVersion, new Date(bootStartedAt).toISOString());
      return;
    }

    if (button.dataset && button.dataset.bound) {
      const clone = button.cloneNode(true);
      clone.dataset.bound = '';
      button.replaceWith(clone);
      button = clone;
    }

    button.dataset.bound = '';

    console.info('[boot.js] ready version=%s started=%s', scriptVersion, new Date(bootStartedAt).toISOString());
  });
})();
