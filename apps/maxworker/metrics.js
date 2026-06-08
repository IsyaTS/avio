'use strict';

function sanitizeLabel(value) {
  return String(value ?? '')
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\n/g, ' ');
}

class MetricsRegistry {
  constructor() {
    this.counters = new Map();
    this.gauges = new Map();
  }

  inc(name, labels = {}, by = 1) {
    const key = this._key(name, labels);
    this.counters.set(key, (this.counters.get(key) || 0) + Number(by || 1));
  }

  set(name, labels = {}, value = 0) {
    const key = this._key(name, labels);
    this.gauges.set(key, Number(value || 0));
  }

  _key(name, labels) {
    const normalized = Object.keys(labels || {})
      .sort()
      .map((k) => `${k}=${sanitizeLabel(labels[k])}`)
      .join(',');
    return `${name}|${normalized}`;
  }

  _lineFromKey(rawKey, value) {
    const [name, labelChunk = ''] = String(rawKey).split('|', 2);
    if (!labelChunk) return `${name} ${value}`;
    const labels = labelChunk
      .split(',')
      .filter(Boolean)
      .map((entry) => {
        const [k, v = ''] = entry.split('=', 2);
        return `${k}="${sanitizeLabel(v)}"`;
      })
      .join(',');
    return `${name}{${labels}} ${value}`;
  }

  render() {
    const lines = [];
    for (const [key, value] of this.gauges.entries()) {
      lines.push(this._lineFromKey(key, value));
    }
    for (const [key, value] of this.counters.entries()) {
      lines.push(this._lineFromKey(key, value));
    }
    return `${lines.join('\n')}\n`;
  }
}

module.exports = {
  MetricsRegistry,
};
