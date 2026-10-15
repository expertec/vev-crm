import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createMasmenuConfirmationHandler } from '../services/masmenuConfirmation.mjs';

const key = 'fixture-key-for-tests-only-1234567890';
const origin = 'https://menus.example.com';
const payload = {
  menuId: '12345678-1234-4234-8234-123456789012', slug: 'fixture-menu',
  restaurantName: 'Fixture Restaurant', whatsapp: '+528000000001',
  menuUrl: `${origin}/menu/fixture-menu`,
};

function database() {
  const records = new Map();
  let queue = Promise.resolve();
  return {
    records,
    failSentWrite: false,
    collection(name) {
      return { doc: id => ({
        key: `${name}/${id}`,
        set: async (value, options) => {
          if (this.failSentWrite && value.status === 'sent') throw new Error('fixture-db-error');
          records.set(`${name}/${id}`, options?.merge ? { ...records.get(`${name}/${id}`), ...value } : value);
        },
      }) };
    },
    runTransaction(callback) {
      const run = queue.then(async () => {
        const writes = [];
        const result = await callback({
          get: async ref => ({ exists: records.has(ref.key), data: () => records.get(ref.key) }),
          set: (ref, value) => writes.push([ref.key, value]),
        });
        for (const [id, value] of writes) records.set(id, value);
        return result;
      });
      queue = run.catch(() => {});
      return run;
    },
  };
}

function setup({ connected = true, sendError = false, env: overrideEnv = {} } = {}) {
  const db = database();
  const sends = [];
  let time = 1800000000000;
  const handler = createMasmenuConfirmationHandler({
    db, isConnected: () => connected, now: () => time,
    env: { MASMENU_CONFIRMATION_API_KEY: key, MASMENU_PUBLIC_APP_URL: origin, ...overrideEnv },
    sendText: async (phone, text) => {
      sends.push({ phone, text });
      if (sendError) throw new Error('fixture-socket-error');
      return { success: true };
    },
  });
  return {
    db, sends, advance: ms => { time += ms; },
    async call(body = payload, authorization = `Bearer ${key}`) {
      const response = { statusCode: 200, headers: {}, body: null };
      const res = {
        set(name, value) { response.headers[name] = value; return res; },
        status(value) { response.statusCode = value; return res; },
        json(value) { response.body = value; return response; },
      };
      await handler({ body, get: name => name === 'authorization' ? authorization : undefined }, res);
      return response;
    },
  };
}

test('CRM refuses missing configuration and invalid authentication without sending', async () => {
  const disabled = setup({ env: { MASMENU_CONFIRMATION_API_KEY: '' } });
  assert.equal((await disabled.call()).statusCode, 503);
  assert.equal(disabled.sends.length, 0);
  const enabled = setup();
  for (const header of ['', 'Bearer wrong-key']) assert.equal((await enabled.call(payload, header)).statusCode, 401);
  assert.equal(enabled.db.records.size, 0);
});

test('CRM pins the public URL and rejects arbitrary links, numbers and menu identifiers', async () => {
  const crm = setup();
  for (const body of [null, { ...payload, menuId: '../foreign' }, { ...payload, whatsapp: 'not-a-phone' },
    { ...payload, menuUrl: 'http://localhost:3000/menu/fixture-menu' },
    { ...payload, menuUrl: 'https://evil.example/menu/fixture-menu' },
    { ...payload, menuUrl: `${payload.menuUrl}?token=forbidden` },
    { ...payload, slug: '../foreign' }]) {
    assert.equal((await crm.call(body)).statusCode, 400);
  }
  assert.equal(crm.sends.length, 0);
});

test('CRM sends the fixed confirmation and records successful delivery', async () => {
  const crm = setup();
  const result = await crm.call({ ...payload, text: 'ignored arbitrary content', sender: 'ignored tenant' });
  assert.equal(result.body.status, 'sent');
  assert.equal(crm.sends[0].phone, payload.whatsapp.slice(1));
  assert.ok(crm.sends[0].text.includes(payload.menuUrl));
  assert.ok(crm.sends[0].text.includes(payload.restaurantName));
  assert.ok(!crm.sends[0].text.includes('ignored arbitrary content'));
  assert.ok([...crm.db.records.values()].some(record => record.status === 'sent'));
});

test('two simultaneous confirmations reserve a single delivery', async () => {
  const crm = setup();
  const results = await Promise.all([crm.call(), crm.call()]);
  assert.equal(crm.sends.length, 1);
  assert.equal(results.filter(result => result.body.status === 'sent').length, 1);
  assert.ok(results.some(result => result.body.code === 'CONFIRMATION_PROCESSING' || result.body.status === 'already_sent'));
});

test('replaying a completed request acknowledges it without a second message', async () => {
  const crm = setup();
  await crm.call();
  assert.equal((await crm.call()).body.status, 'already_sent');
  assert.equal(crm.sends.length, 1);
});

test('a disconnected primary session does not reserve or send a delivery', async () => {
  const crm = setup({ connected: false });
  assert.equal((await crm.call()).body.code, 'SESSION_NOT_CONNECTED');
  assert.equal(crm.sends.length, 0);
  assert.equal(crm.db.records.size, 0);
});

test('uncertain socket outcomes are never automatically resent', async () => {
  const crm = setup({ sendError: true });
  assert.equal((await crm.call()).body.code, 'CONFIRMATION_UNCERTAIN');
  assert.equal((await crm.call()).body.code, 'CONFIRMATION_UNCERTAIN');
  assert.equal(crm.sends.length, 1);
});

test('a database error after sending preserves a non-repeatable uncertain delivery', async () => {
  const crm = setup();
  crm.db.failSentWrite = true;
  assert.equal((await crm.call()).body.code, 'CONFIRMATION_UNCERTAIN');
  crm.db.failSentWrite = false;
  await crm.call();
  assert.equal(crm.sends.length, 1);
});

test('persistent per-menu limits reject rapid requests and excess daily recipients', async () => {
  const crm = setup();
  await crm.call();
  assert.equal((await crm.call({ ...payload, whatsapp: '+528000000002' })).statusCode, 429);
  crm.advance(61000);
  assert.equal((await crm.call({ ...payload, whatsapp: '+528000000002' })).body.status, 'sent');
  crm.advance(61000);
  assert.equal((await crm.call({ ...payload, whatsapp: '+528000000003' })).body.status, 'sent');
  crm.advance(61000);
  assert.equal((await crm.call({ ...payload, whatsapp: '+528000000004' })).statusCode, 429);
});

test('per-recipient limits also apply across different menus', async () => {
  const crm = setup();
  await crm.call();
  const otherMenu = { ...payload, menuId: '22345678-1234-4234-8234-123456789012' };
  assert.equal((await crm.call(otherMenu)).statusCode, 429);
  crm.advance(61000);
  assert.equal((await crm.call(otherMenu)).body.status, 'sent');
});
