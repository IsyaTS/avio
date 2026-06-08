const { chromium } = require('playwright');
(async () => {
  const sessionDir = '/workspace/tmp/max101copy.XGFphe';
  const context = await chromium.launchPersistentContext(sessionDir, {
    headless: false,
    viewport: { width: 1440, height: 960 },
    locale: 'ru-RU',
    timezoneId: 'Europe/Moscow',
    userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    args: ['--disable-blink-features=AutomationControlled'],
  });
  const page = context.pages()[0] || await context.newPage();
  await page.goto('https://web.max.ru/', { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(8000);
  console.log('URL', page.url());
  console.log('TITLE', await page.title());
  const summary = await page.evaluate(() => {
    const pick = (nodes) => Array.from(nodes).slice(0, 25).map((el) => ({
      tag: el.tagName,
      text: String(el.innerText || el.textContent || '').trim().slice(0, 120),
      href: el.getAttribute('href'),
      role: el.getAttribute('role'),
      testid: el.getAttribute('data-testid'),
      aria: el.getAttribute('aria-label'),
      cls: typeof el.className === 'string' ? el.className : '',
    }));
    return {
      bodyText: String(document.body?.innerText || '').slice(0, 2000),
      links: pick(document.querySelectorAll('a[href]')),
      buttons: pick(document.querySelectorAll('button')),
      inputs: pick(document.querySelectorAll('input, textarea, [contenteditable="true"]')),
      navs: pick(document.querySelectorAll('[role="listitem"], [role="option"], [role="link"], [role="button"]')),
    };
  });
  console.log(JSON.stringify(summary, null, 2));
  await page.screenshot({ path: '/workspace/tmp/max101-inspect.png', fullPage: true });
  await context.close();
})();
