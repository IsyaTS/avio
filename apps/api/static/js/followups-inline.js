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

  function normalizeFactKey(raw, fallback) {
    const cleaned = String(raw || '')
      .toLowerCase()
      .trim()
      .replace(/\s+/g, '_')
      .replace(/[^\w\u0400-\u04ff_]+/g, '');
    return cleaned || fallback;
  }

  function buildFactOptions(rules) {
    const seen = new Map();
    (Array.isArray(rules) ? rules : []).forEach((rule, idx) => {
      const capture = rule && rule.capture;
      if (!capture || !capture.key) return;
      const label = String(capture.label || rule.text || capture.key || `Факт ${idx + 1}`).trim();
      if (!seen.has(capture.key)) {
        seen.set(capture.key, label);
      }
    });
    return Array.from(seen.entries()).map(([key, label]) => ({ key, label }));
  }

  function conditionPresetFrom(condition) {
    if (!condition) return 'custom';
    const op = String(condition.op || 'eq');
    const value = typeof condition.value === 'string'
      ? condition.value
      : Array.isArray(condition.value)
      ? condition.value.join(', ')
      : '';
    if (op === 'eq' && value === 'yes') return 'yes';
    if (op === 'eq' && value === 'no') return 'no';
    if (op === 'neq' && value === 'yes') return 'not_yes';
    if (op === 'neq' && value === 'no') return 'not_no';
    if (op === 'exists') return 'exists';
    if (op === 'not_exists') return 'not_exists';
    return 'custom';
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
    const factOptions = buildFactOptions(rules);
    rules.forEach((rule, index) => {
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

      const conditionRaw = Array.isArray(rule.condition) ? rule.condition[0] : rule.condition;
      const condition = conditionRaw && typeof conditionRaw === 'object' ? conditionRaw : null;
      const conditionKey = condition && condition.key ? String(condition.key) : '';
      const conditionOp = condition && condition.op ? String(condition.op) : 'eq';
      const conditionMode = condition && condition.key ? 'conditional' : 'always';
      const conditionValueRaw = condition && condition.value != null
        ? Array.isArray(condition.value) ? condition.value.join(', ') : String(condition.value)
        : '';
      const preset = conditionPresetFrom(condition);
      const matchedFact = factOptions.find((opt) => opt.key === conditionKey);
      const factSelectValue = matchedFact
        ? matchedFact.key
        : conditionKey
        ? '__custom__'
        : factOptions.length
        ? factOptions[0].key
        : '__custom__';

      const conditionBlock = document.createElement('div');
      conditionBlock.className = 'stack';
      conditionBlock.style.gap = '10px';
      const conditionTitle = document.createElement('span');
      conditionTitle.className = 'label';
      conditionTitle.textContent = 'Условие отправки';
      conditionBlock.appendChild(conditionTitle);

      const conditionModeLabel = makeLabel('Режим');
      const conditionModeSelect = document.createElement('select');
      conditionModeSelect.className = 'followup-condition-mode';
      [
        { value: 'always', label: 'Всегда' },
        { value: 'conditional', label: 'По условию' },
      ].forEach((optData) => {
        const opt = document.createElement('option');
        opt.value = optData.value;
        opt.textContent = optData.label;
        if (conditionMode === optData.value) opt.selected = true;
        conditionModeSelect.appendChild(opt);
      });
      conditionModeLabel.appendChild(conditionModeSelect);
      conditionBlock.appendChild(conditionModeLabel);

      const conditionFields = document.createElement('div');
      conditionFields.style.display = conditionMode === 'conditional' ? 'grid' : 'none';
      conditionFields.style.gridTemplateColumns = 'repeat(auto-fit, minmax(160px, 1fr))';
      conditionFields.style.gap = '10px';

      const conditionFactLabel = makeLabel('Факт');
      const conditionFactSelect = document.createElement('select');
      conditionFactSelect.className = 'followup-condition-fact';
      factOptions.forEach((optData) => {
        const opt = document.createElement('option');
        opt.value = optData.key;
        opt.textContent = optData.label;
        if (factSelectValue === optData.key) {
          opt.selected = true;
        }
        conditionFactSelect.appendChild(opt);
      });
      const customFactOption = document.createElement('option');
      customFactOption.value = '__custom__';
      customFactOption.textContent = 'Свой факт...';
      if (factSelectValue === '__custom__') {
        customFactOption.selected = true;
      }
      conditionFactSelect.appendChild(customFactOption);
      conditionFactLabel.appendChild(conditionFactSelect);
      conditionFields.appendChild(conditionFactLabel);

      const conditionKeyLabel = makeLabel('Название факта');
      const conditionKeyInput = document.createElement('input');
      conditionKeyInput.type = 'text';
      conditionKeyInput.className = 'followup-condition-custom-key';
      conditionKeyInput.value = conditionKey;
      conditionKeyLabel.appendChild(conditionKeyInput);
      conditionKeyLabel.style.display = factSelectValue === '__custom__' ? 'block' : 'none';
      conditionFields.appendChild(conditionKeyLabel);

      const conditionPresetLabel = makeLabel('Что должно быть');
      const conditionPresetSelect = document.createElement('select');
      conditionPresetSelect.className = 'followup-condition-preset';
      [
        { value: 'yes', label: 'Ответ "Да"' },
        { value: 'no', label: 'Ответ "Нет"' },
        { value: 'not_yes', label: 'Не "Да" (или нет ответа)' },
        { value: 'not_no', label: 'Не "Нет" (или нет ответа)' },
        { value: 'exists', label: 'Есть ответ/значение' },
        { value: 'not_exists', label: 'Нет ответа/значения' },
        { value: 'custom', label: 'Другое условие' },
      ].forEach((optData) => {
        const opt = document.createElement('option');
        opt.value = optData.value;
        opt.textContent = optData.label;
        if (preset === optData.value) {
          opt.selected = true;
        }
        conditionPresetSelect.appendChild(opt);
      });
      conditionPresetLabel.appendChild(conditionPresetSelect);
      conditionFields.appendChild(conditionPresetLabel);

      const conditionCustomRow = document.createElement('div');
      conditionCustomRow.style.display = preset === 'custom' ? 'grid' : 'none';
      conditionCustomRow.style.gridTemplateColumns = 'repeat(auto-fit, minmax(160px, 1fr))';
      conditionCustomRow.style.gap = '10px';

      const conditionOpLabel = makeLabel('Оператор');
      const conditionOpSelect = document.createElement('select');
      conditionOpSelect.className = 'followup-condition-op';
      [
        { value: 'eq', label: '=' },
        { value: 'neq', label: '≠' },
        { value: 'exists', label: 'есть' },
        { value: 'not_exists', label: 'нет' },
        { value: 'in', label: 'в списке' },
        { value: 'not_in', label: 'не в списке' },
      ].forEach((optData) => {
        const opt = document.createElement('option');
        opt.value = optData.value;
        opt.textContent = optData.label;
        if (conditionOp === optData.value) {
          opt.selected = true;
        }
        conditionOpSelect.appendChild(opt);
      });
      conditionOpLabel.appendChild(conditionOpSelect);
      conditionCustomRow.appendChild(conditionOpLabel);

      const conditionValueLabel = makeLabel('Значение');
      const conditionValueInput = document.createElement('input');
      conditionValueInput.type = 'text';
      conditionValueInput.className = 'followup-condition-value';
      conditionValueInput.placeholder = 'значения через запятую';
      conditionValueInput.value = conditionValueRaw || '';
      conditionValueLabel.appendChild(conditionValueInput);
      conditionCustomRow.appendChild(conditionValueLabel);

      const updateConditionValueVisibility = () => {
        const op = conditionOpSelect.value;
        conditionValueLabel.style.display = ['exists', 'not_exists'].includes(op) ? 'none' : 'block';
      };
      updateConditionValueVisibility();

      const updateConditionVisibility = () => {
        const enabled = conditionModeSelect.value === 'conditional';
        conditionFields.style.display = enabled ? 'grid' : 'none';
        conditionCustomRow.style.display = enabled && conditionPresetSelect.value === 'custom' ? 'grid' : 'none';
      };
      updateConditionVisibility();

      conditionModeSelect.addEventListener('change', updateConditionVisibility);
      conditionFactSelect.addEventListener('change', () => {
        conditionKeyLabel.style.display = conditionFactSelect.value === '__custom__' ? 'block' : 'none';
      });
      conditionPresetSelect.addEventListener('change', () => {
        updateConditionVisibility();
        updateConditionValueVisibility();
      });
      conditionOpSelect.addEventListener('change', updateConditionValueVisibility);

      conditionBlock.appendChild(conditionFields);
      conditionBlock.appendChild(conditionCustomRow);
      card.appendChild(conditionBlock);

      const capture = rule.capture && typeof rule.capture === 'object' ? rule.capture : null;
      const captureBlock = document.createElement('div');
      captureBlock.className = 'stack';
      captureBlock.style.gap = '10px';
      const captureToggleLabel = document.createElement('label');
      captureToggleLabel.style.display = 'flex';
      captureToggleLabel.style.alignItems = 'center';
      captureToggleLabel.style.gap = '8px';
      const captureToggle = document.createElement('input');
      captureToggle.type = 'checkbox';
      captureToggle.className = 'followup-capture-enabled';
      captureToggle.checked = !!capture;
      captureToggleLabel.appendChild(captureToggle);
      captureToggleLabel.appendChild(document.createTextNode('Сохранять ответ клиента'));
      captureBlock.appendChild(captureToggleLabel);

      const captureFields = document.createElement('div');
      captureFields.style.display = captureToggle.checked ? 'block' : 'none';
      captureFields.className = 'stack';
      captureFields.style.gap = '10px';

      const captureLabel = makeLabel('Название факта');
      const captureLabelInput = document.createElement('input');
      captureLabelInput.type = 'text';
      captureLabelInput.className = 'followup-capture-label';
      captureLabelInput.placeholder = 'Например: Заказ оформлен';
      captureLabelInput.value = capture && capture.label ? String(capture.label) : '';
      captureLabel.appendChild(captureLabelInput);
      captureFields.appendChild(captureLabel);

      const captureKeyDetails = document.createElement('details');
      captureKeyDetails.className = 'surface';
      captureKeyDetails.style.padding = '10px';
      const captureKeySummary = document.createElement('summary');
      captureKeySummary.textContent = 'Технический ключ';
      captureKeySummary.style.cursor = 'pointer';
      captureKeyDetails.appendChild(captureKeySummary);

      const captureKeyBody = document.createElement('div');
      captureKeyBody.className = 'stack';
      captureKeyBody.style.gap = '6px';
      captureKeyBody.style.marginTop = '10px';

      const captureKeyLabel = makeLabel('Технический ключ');
      const captureKeyInput = document.createElement('input');
      captureKeyInput.type = 'text';
      captureKeyInput.className = 'followup-capture-key';
      const fallbackLabel = String(rule.text || `Факт ${index + 1}`);
      const fallbackKey = normalizeFactKey(captureLabelInput.value || fallbackLabel, `fact_${index + 1}`);
      captureKeyInput.value = capture && capture.key ? String(capture.key) : fallbackKey;
      captureKeyLabel.appendChild(captureKeyInput);
      captureKeyBody.appendChild(captureKeyLabel);

      const captureKeyHint = document.createElement('div');
      captureKeyHint.className = 'status-text muted';
      captureKeyHint.textContent = 'Используется в условиях. Меняйте только если понимаете последствия.';
      captureKeyBody.appendChild(captureKeyHint);

      captureKeyDetails.appendChild(captureKeyBody);
      captureFields.appendChild(captureKeyDetails);

      const captureDetails = document.createElement('details');
      captureDetails.className = 'surface';
      captureDetails.style.padding = '10px';
      const captureSummary = document.createElement('summary');
      captureSummary.textContent = 'Синонимы ответов';
      captureSummary.style.cursor = 'pointer';
      captureDetails.appendChild(captureSummary);

      const captureSynonyms = document.createElement('div');
      captureSynonyms.style.display = 'grid';
      captureSynonyms.style.gridTemplateColumns = 'repeat(auto-fit, minmax(160px, 1fr))';
      captureSynonyms.style.gap = '10px';
      captureSynonyms.style.marginTop = '10px';

      const captureYesLabel = makeLabel('Ответы "Да"');
      const captureYesInput = document.createElement('textarea');
      captureYesInput.className = 'textarea followup-capture-yes';
      captureYesInput.rows = 2;
      captureYesInput.value = Array.isArray(capture && capture.yes) ? capture.yes.join('\n') : '';
      captureYesLabel.appendChild(captureYesInput);
      captureSynonyms.appendChild(captureYesLabel);

      const captureNoLabel = makeLabel('Ответы "Нет"');
      const captureNoInput = document.createElement('textarea');
      captureNoInput.className = 'textarea followup-capture-no';
      captureNoInput.rows = 2;
      captureNoInput.value = Array.isArray(capture && capture.no) ? capture.no.join('\n') : '';
      captureNoLabel.appendChild(captureNoInput);
      captureSynonyms.appendChild(captureNoLabel);

      captureDetails.appendChild(captureSynonyms);
      captureFields.appendChild(captureDetails);

      captureToggle.addEventListener('change', () => {
        captureFields.style.display = captureToggle.checked ? 'block' : 'none';
      });

      captureBlock.appendChild(captureFields);
      card.appendChild(captureBlock);

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
      .map((card, index) => {
        const channel = (card.querySelector('.followup-channel') || {}).value || 'any';
        const delay = Number.parseInt((card.querySelector('.followup-delay') || {}).value || '0', 10) || 0;
        const attempts = Number.parseInt((card.querySelector('.followup-attempts') || {}).value || '1', 10) || 0;
        const active = !!(card.querySelector('.followup-active') || { checked: true }).checked;
        const text = (card.querySelector('.followup-text') || {}).value || '';
        const conditionMode = (card.querySelector('.followup-condition-mode') || {}).value || 'always';
        const conditionFact = (card.querySelector('.followup-condition-fact') || {}).value || '__custom__';
        const conditionCustomKey = (card.querySelector('.followup-condition-custom-key') || {}).value || '';
        const conditionPreset = (card.querySelector('.followup-condition-preset') || {}).value || 'custom';
        const conditionOp = (card.querySelector('.followup-condition-op') || {}).value || 'eq';
        const conditionValue = (card.querySelector('.followup-condition-value') || {}).value || '';
        const captureEnabled = !!(card.querySelector('.followup-capture-enabled') || { checked: false }).checked;
        const captureLabel = (card.querySelector('.followup-capture-label') || {}).value || '';
        const captureKey = (card.querySelector('.followup-capture-key') || {}).value || '';
        const captureYes = (card.querySelector('.followup-capture-yes') || {}).value || '';
        const captureNo = (card.querySelector('.followup-capture-no') || {}).value || '';
        const parseTokens = (value) => String(value || '')
          .split(/\n|,/)
          .map((token) => token.trim())
          .filter(Boolean);
        const payload = { channel, delay_minutes: delay, max_attempts: attempts, active, text };
        if (conditionMode === 'conditional') {
          const resolvedKey = (conditionFact === '__custom__' ? conditionCustomKey : conditionFact).trim();
          if (resolvedKey) {
            if (conditionPreset === 'yes') {
              payload.condition = { key: resolvedKey, op: 'eq', value: 'yes' };
            } else if (conditionPreset === 'no') {
              payload.condition = { key: resolvedKey, op: 'eq', value: 'no' };
            } else if (conditionPreset === 'not_yes') {
              payload.condition = { key: resolvedKey, op: 'neq', value: 'yes' };
            } else if (conditionPreset === 'not_no') {
              payload.condition = { key: resolvedKey, op: 'neq', value: 'no' };
            } else if (conditionPreset === 'exists') {
              payload.condition = { key: resolvedKey, op: 'exists' };
            } else if (conditionPreset === 'not_exists') {
              payload.condition = { key: resolvedKey, op: 'not_exists' };
            } else {
              const cond = { key: resolvedKey, op: conditionOp };
              if (!['exists', 'not_exists'].includes(conditionOp)) {
                if (String(conditionValue || '').trim()) {
                  cond.value = String(conditionValue || '').trim();
                  payload.condition = cond;
                }
              } else {
                payload.condition = cond;
              }
            }
          }
        }
        if (captureEnabled) {
          const fallbackLabel = text || `Факт ${index + 1}`;
          const resolvedLabel = String(captureLabel || '').trim() || fallbackLabel;
          const resolvedKey = String(captureKey || '').trim()
            || normalizeFactKey(resolvedLabel, `fact_${index + 1}`);
          if (resolvedKey) {
            payload.capture = {
              key: resolvedKey,
              yes: parseTokens(captureYes),
              no: parseTokens(captureNo),
              label: resolvedLabel,
            };
          }
        }
        return payload;
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
