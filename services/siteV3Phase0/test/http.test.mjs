import test from 'node:test';
import assert from 'node:assert/strict';
import { createSiteV3Handlers } from '../http.mjs';
import { BIZ, billing, fixture, foreign, harness, owner, prepared } from './helpers.mjs';

async function invoke(handler, identity, body = {}, params = {}) {
  const res = { status(code) { this.code = code; return this; }, json(body) { return { status: this.code, body }; } };
  return handler({ ...identity, params: { negocioId: BIZ, ...params }, body }, res);
}
test('HTTP creation requires verified session/tenant and returns stable safe errors', async () => {
  const h = harness(), api = createSiteV3Handlers(h.service);
  assert.equal((await invoke(api.create, {})).status, 401);
  assert.equal((await invoke(api.create, foreign)).status, 403);
  assert.equal((await invoke(api.create, owner)).status, 200);
  assert.equal((await invoke(api.create, owner)).body.data.duplicate, true);
});
test('HTTP edit/process/images/activation apply authorization before domain effects', async () => {
  const h = await prepared(), api = createSiteV3Handlers(h.service), before = h.store.snapshot();
  for (const [name, body] of [['edit', { brief: fixture('business-brief') }], ['process', { expectedInputRevision: 2 }], ['images', { imageRequest: fixture('image-request') }], ['activate', {}]]) {
    assert.equal((await invoke(api[name], foreign, body)).status, 403);
    assert.equal((await invoke(api[name], {}, body)).status, 401);
  }
  assert.deepEqual(h.store.snapshot(), before);
  assert.equal((await invoke(api.activate, billing)).body.error.code, 'VERIFIED_PAYMENT_ADAPTER_REQUIRED');
});
test('HTTP request cannot grant capabilities or spoof tenant through body', async () => {
  const h = await prepared(), api = createSiteV3Handlers(h.service);
  assert.equal((await invoke(api.process, owner, { expectedInputRevision: 2, negocioId: 'other' })).status, 422);
  assert.equal((await invoke(api.activate, billing, { paid: true, plan: 'premium' })).status, 422);
});
test('HTTP duplicate processing returns same durable job and no private evidence', async () => {
  const h = await prepared(), api = createSiteV3Handlers(h.service);
  const a = await invoke(api.process, owner, { expectedInputRevision: 2 }), b = await invoke(api.process, owner, { expectedInputRevision: 2 });
  assert.equal(a.body.data.job.id, b.body.data.job.id);
  assert.equal(b.body.data.duplicate, true);
  const status = await invoke(api.status, owner, {}, { jobId: a.body.data.job.id });
  assert.deepEqual(Object.keys(status.body.data).sort(), ['attempts', 'jobId', 'revisionId', 'state']);
});
test('HTTP adapter returns no stack or exception text from unexpected failures', async () => {
  const api = createSiteV3Handlers({ createProject: () => { throw new Error('private provider details'); } });
  assert.deepEqual(await invoke(api.create, owner), { status: 500, body: { success: false, error: { code: 'INTERNAL_ERROR' } } });
});
test('HTTP missing payload fields are validation errors rather than internal failures', async () => {
  const h = await prepared(), api = createSiteV3Handlers(h.service);
  for (const method of ['edit', 'confirm', 'process', 'images', 'approve', 'publish']) assert.equal((await invoke(api[method], owner, {})).status, 422);
});
