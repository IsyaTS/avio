// Lightweight follow-up init for cases when client-settings.js init fails.
// Render/add/save rules using data from #client-settings-state.
(function followupsInlineInit() {
  const onReady = (fn) => {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', fn, { once: true });
    } else {
      fn();
    }
  };

  onReady(() => {
  const stateNode = document.getElementById('client-settings-state');
  let state = {};
  try {
    state = stateNode ? JSON.parse(stateNode.textContent || '{}') : {};
  } catch (e) {
    state = {};
  }

  const urls = (state && state.urls) || {};
  const tenant = state.tenant;
  const key = state.key || '';

  const dom = {
    container: document.getElementById('followup-rules'),
    addBtn: document.getElementById('followup-add'),
    saveBtn: document.getElementById('followup-save'),
    message: document.getElementById('followup-message'),
  };

  function setStatus(msg, variant) {
    if (!dom.message) return;
    dom.message.className = `status-text ${variant || 'muted'}`.trim();
    dom.message.textContent = msg || '';
  }

  function render(rules) {
    if (!dom.container) return;
    dom.container.innerHTML = '';
    if (!Array.isArray(rules) || !rules.length) {
      const hint = document.createElement('div');
      hint.className = 'hint';
      hint.textContent = 'Правил пока нет. Добавьте новое.';
      dom.container.appendChild(hint);
      return;
    }
    rules.forEach((rule) => {
      const card = document.createElement('div');
      card.className = 'surface stack';
      card.dataset.type = 'followup-card';
      card.style.gap = '10px';

      const row = document.createElement('div');
      row.style.display = 'grid';
      row.style.gridTemplateColumns = 'repeat(auto-fit, minmax(160px, 1fr))';
      row.style.gap = '10px';

      const makeLabel = (text) => {
        const label = document.createElement('label');
        label.className = 'stack';
        label.style.gap = '4px';
        const span = document.createElement('span');
        span.className = 'label';
        span.textContent = text;
        label.appendChild(span);
        return label;
      };

      const channelLabel = makeLabel('Канал');
      const select = document.createElement('select');
      select.className = 'followup-channel';
      ['any', 'telegram', 'avito', 'whatsapp'].forEach((ch) => {
        const opt = document.createElement('option');
        opt.value = ch;
        opt.textContent = ch === 'any' ? 'Любой' : ch;
        if ((rule.channel || 'any') === ch) opt.selected = true;
        select.appendChild(opt);
      });
      channelLabel.appendChild(select);
      row.appendChild(channelLabel);

      const delayLabel = makeLabel('Задержка, мин');
      const delayInput = document.createElement('input');
      delayInput.type = 'number';
      delayInput.min = '1';
      delayInput.value = rule.delay_minutes != null ? String(rule.delay_minutes) : '10';
      delayInput.className = 'followup-delay';
      delayLabel.appendChild(delayInput);
      row.appendChild(delayLabel);

      const attemptsLabel = makeLabel('Макс. попыток');
      const attemptsInput = document.createElement('input');
      attemptsInput.type = 'number';
      attemptsInput.min = '0';
      attemptsInput.value = rule.max_attempts != null ? String(rule.max_attempts) : '1';
      attemptsInput.className = 'followup-attempts';
      attemptsLabel.appendChild(attemptsInput);
      row.appendChild(attemptsLabel);

      const activeLabel = document.createElement('label');
      activeLabel.style.display = 'flex';
      activeLabel.style.alignItems = 'center';
      activeLabel.style.gap = '8px';
      const activeInput = document.createElement('input');
      activeInput.type = 'checkbox';
      activeInput.className = 'followup-active';
      activeInput.checked = rule.active !== false;
      activeLabel.appendChild(activeInput);
      activeLabel.appendChild(document.createTextNode('Активно'));
      row.appendChild(activeLabel);

      card.appendChild(row);

      const textLabel = makeLabel('Текст сообщения');
      const textarea = document.createElement('textarea');
      textarea.className = 'textarea followup-text';
      textarea.rows = 3;
      textarea.value = rule.text || '';
      textLabel.appendChild(textarea);
      card.appendChild(textLabel);

      const actions = document.createElement('div');
      actions.style.display = 'flex';
      actions.style.gap = '10px';
      const del = document.createElement('button');
      del.type = 'button';
      del.className = 'btn btn--secondary';
      del.textContent = 'Удалить';
      del.addEventListener('click', () => card.remove());
      actions.appendChild(del);
      card.appendChild(actions);

      dom.container.appendChild(card);
    });
  }

  function collect() {
    if (!dom.container) return [];
    return Array.from(dom.container.querySelectorAll('[data-type="followup-card"]'))
      .map((card) => {
        const channel = (card.querySelector('.followup-channel') || {}).value || 'any';
        const delay = Number.parseInt((card.querySelector('.followup-delay') || {}).value || '0', 10) || 0;
        const attempts = Number.parseInt((card.querySelector('.followup-attempts') || {}).value || '1', 10) || 0;
        const active = !!(card.querySelector('.followup-active') || { checked: true }).checked;
        const text = (card.querySelector('.followup-text') || {}).value || '';
        return { channel, delay_minutes: delay, max_attempts: attempts, active, text };
      })
      .filter((rule) => rule.delay_minutes > 0 && rule.text.trim());
  }

  async function load() {
    const url = urls.get_followups || (tenant ? `/client/${tenant}/follow-ups` : '');
    if (!url || !dom.container) return;
    try {
      const resp = await fetch(key ? `${url}?k=${encodeURIComponent(key)}` : url);
      if (!resp.ok) throw new Error('Не удалось загрузить правила');
      const data = await resp.json();
      render(Array.isArray(data.rules) ? data.rules : []);
    } catch (err) {
      render([]);
      setStatus(err.message || 'Ошибка загрузки', 'alert');
    }
  }

  async function save() {
    const rules = collect();
    if (!rules.length) {
      setStatus('Добавьте хотя бы одно правило', 'alert');
      return;
    }
    const url = urls.save_followups || (tenant ? `/client/${tenant}/follow-ups` : '');
    if (!url) {
      setStatus('Endpoint не задан', 'alert');
      return;
    }
    try {
      const resp = await fetch(key ? `${url}?k=${encodeURIComponent(key)}` : url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rules }),
      });
      if (!resp.ok) {
        const detail = await resp.text();
        throw new Error(detail || 'Не удалось сохранить');
      }
      setStatus('Сохранено', 'muted');
    } catch (err) {
      setStatus(err.message || 'Ошибка сохранения', 'alert');
    }
  }

  if (dom.addBtn) {
    dom.addBtn.addEventListener('click', (e) => {
      e.preventDefault();
      const current = collect();
      current.push({ channel: 'any', delay_minutes: 10, max_attempts: 1, active: true, text: '' });
      render(current);
    });
  }

  if (dom.saveBtn) {
    dom.saveBtn.addEventListener('click', (e) => {
      e.preventDefault();
      save();
    });
  }

  load();
  });
})();
