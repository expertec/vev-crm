import test from 'node:test';
import assert from 'node:assert/strict';
import { describeAiPage, editAiPage, registerAiPageEditorRoutes } from '../services/aiPageEditor.js';
const original = { version: 'ai-page-v1', title: 'FC inoxidables', description: 'Acero', html: '<header class="nw-digital-card"><div class="nw-brand"><img src="https://storage.googleapis.com/b/logo.png" alt="FC"></div></header><main><section class="nw-hero"><h1>Fabricación <span class="accent">en acero</span></h1><a class="cta" href="https://wa.me/5211234567890"><svg viewBox="0 0 24 24"><path d="M1 2"/></svg> Cotizar ahora</a><img src="https://storage.googleapis.com/b/hero.png"></section><section><h2>Galería</h2><div class="nw-gallery-grid"><figure class="photo"><img src="https://storage.googleapis.com/b/photo.png"><figcaption>Trabajo real</figcaption></figure></div></section><video controls src="https://storage.googleapis.com/b/video.mp4"></video></main>', css: '.accent{color:red}.photo{border-radius:12px}', assets: [{ url: 'https://storage.googleapis.com/b/hero.png' }], videos: ['https://storage.googleapis.com/b/video.mp4'] };
const uploaded = 'https://firebasestorage.googleapis.com/v0/b/b/o/fc%2Fsite-editor%2Fnew.png?alt=media';
test('edits actual text and buttons preserving nested design, icons, video and escaped content', async () => {
  const fields = describeAiPage(original).fields;
  const title = fields.find((f) => f.value === 'Fabricación');
  const button = fields.find((f) => f.value === 'Cotizar ahora');
  const link = fields.find((f) => f.type === 'link');
  const { page } = await editAiPage(original, { changes: { [title.id]: '<script>alert(1)</script>', [button.id]: 'Pedir presupuesto', [link.id]: 'https://wa.me/5219999999999' } });
  assert.ok(page.html.includes('&lt;script&gt;'));
  assert.ok(page.html.includes('<span class="accent">en acero</span>'));
  assert.ok(page.html.includes('Pedir presupuesto'));
  assert.ok(page.html.includes('<svg'));
  assert.ok(page.html.includes('<video controls'));
  assert.equal(page.css, original.css);
  assert.deepEqual(page.videos, original.videos);
});
test('replaces repeated photos and logo, expands own image CSP without changing palette or hash', async () => {
  const page = { ...original, inputHash: 'delivery-checkpoint', palette: ['#112233'], html: original.html + '<img src="https://storage.googleapis.com/b/logo.png">' };
  const logo = describeAiPage(page).fields.find((f) => f.label === 'Logo');
  const result = await editAiPage(page, { changes: { [logo.id]: uploaded } }, { validateImage: async (url) => url === uploaded });
  assert.equal(result.newLogo, uploaded);
  assert.equal(result.page.html.split(uploaded.replaceAll('&', '&amp;')).length - 1, 2);
  assert.equal(result.page.inputHash, page.inputHash);
  assert.deepEqual(result.page.palette, page.palette);
  assert.ok(describeAiPage(result.page, uploaded).document.includes('img-src https://storage.googleapis.com https://firebasestorage.googleapis.com'));
});
test('adds photos using existing gallery card without copying old captions, rejects unsafe inputs', async () => {
  const result = await editAiPage(original, { photos: [{ url: uploaded, caption: 'Nueva fabricación' }] }, { validateImage: async () => true });
  assert.equal(result.page.html.split('class="photo"').length - 1, 2);
  assert.ok(result.page.html.includes('Nueva fabricación'));
  const link = describeAiPage(original).fields.find((f) => f.type === 'link');
  await assert.rejects(editAiPage(original, { changes: { [link.id]: 'javascript:alert(1)' } }), /enlace/);
  await assert.rejects(editAiPage(original, { changes: { 'text-fake': 'a' } }), /campo/);
  const image = describeAiPage(original).fields.find((f) => f.type === 'image');
  await assert.rejects(editAiPage(original, { changes: { [image.id]: 'https://attacker.example/x.jpg' } }), /Sube/);
});
test('can add a logo to a page originally using a wordmark', async () => {
  const page = { ...original, html: original.html.replace('<img src="https://storage.googleapis.com/b/logo.png" alt="FC">', '<strong class="nw-wordmark">FC inoxidables</strong>') };
  const logo = describeAiPage(page).fields.find((f) => f.label === 'Logo');
  assert.equal(logo.value, '');
  const result = await editAiPage(page, { changes: { [logo.id]: uploaded } }, { validateImage: async () => true });
  assert.equal(result.newLogo, uploaded);
  assert.ok(result.page.html.includes('class="nw-wordmark"'));
});
function harness() {
  let data = { sampleGenerator: 'ai-page-v1', schema: { aiPage: original, brand: { logo: 'https://storage.googleapis.com/b/logo.png' } }, slug: 'fc' }, writes = 0;
  const handlers = {};
  const ref = { get: async () => ({ data: () => data }) };
  const db = { collection: () => ({ doc: (id) => { assert.equal(id, 'fc'); return ref; } }), runTransaction: async (callback) => callback({ get: ref.get, update: (_ref, patch) => { writes++; for (const [key, value] of Object.entries(patch)) { const bits = key.split('.'); let target = data; bits.slice(0, -1).forEach((b) => { target = target[b] ||= {}; }); target[bits.at(-1)] = value; } } }) };
  registerAiPageEditorRoutes({ get: (path, h) => { handlers.get = h; }, post: (path, h) => { handlers.post = h; } }, { db, bucket: { name: 'b', file: () => ({ getMetadata: async () => [{ contentType: 'image/png', size: 100 }] }) }, authenticate: async (req) => req.auth === false ? null : { negocioId: 'fc' } });
  return { get data() { return data; }, get writes() { return writes; }, async call(method, body, auth = true) { let result, status = 200; const res = { setHeader() {}, status(s) { status = s; return this; }, json(value) { result = value; return this; } }; await handlers[method]({ body, auth }, res); return { status, result }; } };
}
test('authenticated preview does not write; saves only ai page and supports optimistic restore', async () => {
  const h = harness(), initial = (await h.call('get')).result;
  const field = initial.fields.find((f) => f.value === 'Cotizar ahora');
  const body = { revision: initial.revision, changes: { [field.id]: 'Solicitar cotización' }, action: 'preview' };
  assert.equal((await h.call('post', body)).status, 200); assert.equal(h.writes, 0);
  assert.equal((await h.call('get', undefined, false)).status, 401);
  const saved = await h.call('post', { ...body, action: 'save' });
  assert.equal(saved.status, 200); assert.equal(h.writes, 1);
  assert.equal(h.data.slug, 'fc'); assert.ok(h.data.schema.aiPagePrevious);
  assert.equal((await h.call('post', { ...body, action: 'save' })).status, 409);
  const restored = await h.call('post', { revision: saved.result.revision, action: 'restore' });
  assert.equal(restored.status, 200); assert.deepEqual(h.data.schema.aiPage, original);
});
test('rejects other tenant uploads and edits during generation or disabled access', async () => {
  const h = harness(), initial = (await h.call('get')).result, image = initial.fields.find((f) => f.type === 'image');
  const body = { action: 'save', revision: initial.revision, changes: { [image.id]: uploaded.replace('fc%2F', 'other%2F') } };
  assert.equal((await h.call('post', body)).status, 400);
  h.data.aiSampleJob = { state: 'running' };
  assert.equal((await h.call('post', { ...body, changes: {} })).status, 409);
  h.data.siteTemporarilyDisabled = true;
  assert.equal((await h.call('get')).status, 403);
  assert.equal(h.writes, 0);
});
