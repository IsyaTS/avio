'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { STATUS, createSessionState, transition, markSeen } = require('../state');

test('state transition allows waiting_qr -> authorized', () => {
  const state = createSessionState(101);
  const ok1 = transition(state, STATUS.WAITING_QR, 'start');
  const ok2 = transition(state, STATUS.AUTHORIZED, 'authorized');
  assert.equal(ok1, true);
  assert.equal(ok2, true);
  assert.equal(state.status, STATUS.AUTHORIZED);
});

test('state transition blocks illegal jump', () => {
  const state = createSessionState(102);
  const ok = transition(state, STATUS.STALE, 'illegal');
  assert.equal(ok, false);
  assert.equal(state.status, STATUS.IDLE);
});

test('state transition allows idle -> reauth_required', () => {
  const state = createSessionState(103);
  const ok = transition(state, STATUS.REAUTH_REQUIRED, 'restore');
  assert.equal(ok, true);
  assert.equal(state.status, STATUS.REAUTH_REQUIRED);
});

test('markSeen deduplicates active key', () => {
  const mapRef = new Map();
  const first = markSeen(mapRef, 'a:b:c', 60, 1000);
  const second = markSeen(mapRef, 'a:b:c', 60, 1001);
  assert.equal(first.duplicate, false);
  assert.equal(second.duplicate, true);
});
