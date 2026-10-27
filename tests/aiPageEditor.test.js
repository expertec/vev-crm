import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import sharp from 'sharp';
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
  const handlers = {}, uploads = [];
  const ref = { get: async () => ({ data: () => data }) };
  const db = { collection: () => ({ doc: (id) => { assert.equal(id, 'fc'); return ref; } }), runTransaction: async (callback) => callback({ get: ref.get, update: (_ref, patch) => { writes++; for (const [key, value] of Object.entries(patch)) { const bits = key.split('.'); let target = data; bits.slice(0, -1).forEach((b) => { target = target[b] ||= {}; }); target[bits.at(-1)] = value; } } }) };
  registerAiPageEditorRoutes({ get: (path, h) => { handlers.get = h; }, post: (path, h) => { handlers[path.endsWith('/upload') ? 'upload' : 'post'] = h; } }, { db, bucket: { name: 'b', file: (path) => ({ getMetadata: async () => [{ contentType: 'image/png', size: 100 }], save: async (buffer, options) => uploads.push({ path, buffer, options }), getSignedUrl: async () => [`https://storage.googleapis.com/b/${path}`] }) }, authenticate: async (req) => req.auth === false ? null : { negocioId: 'fc' } });
  return { uploads, get data() { return data; }, get writes() { return writes; }, async call(method, body, auth = true) { let result, status = 200; const res = { setHeader() {}, status(s) { status = s; return this; }, json(value) { result = value; return this; } }; await handlers[method]({ body, auth }, res); return { status, result }; }, async upload(buffer, auth = true) {
    const boundary = 'test-image-boundary';
    const body = Buffer.concat([Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="image"; filename="photo.png"\r\nContent-Type: image/png\r\n\r\n`), buffer, Buffer.from(`\r\n--${boundary}--\r\n`)]);
    const req = Readable.from([body]); req.method = 'POST'; req.auth = auth; req.headers = { 'content-type': `multipart/form-data; boundary=${boundary}`, 'content-length': String(body.length) };
    let status = 200, result;
    await handlers.upload(req, { setHeader() {}, status(s) { status = s; return this; }, json(value) { result = value; } });
    return { status, result };
  } };
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

test('seventeen additional photos work without a legacy gallery and can be reordered and removed', async () => {
  const bare = { ...original, html: '<main><section><h1>Taller</h1></section></main>' };
  const items = Array.from({ length: 17 }, (_, i) => ({ url: `${uploaded}&image=${i}`, thumbnailUrl: `${uploaded}&thumb=${i}`, caption: i === 0 ? '<script>Test</script>' : `Trabajo ${i}` }));
  const edited = await editAiPage(bare, { extraGallery: { title: 'Nuestros trabajos', items } }, { validateImage: async () => true });
  assert.equal(edited.page.html, bare.html); assert.equal(edited.page.css, bare.css);
  const info = describeAiPage(edited.page);
  assert.equal(info.extraGallery.items.length, 17); assert.equal(info.extraGalleryLimit, 20);
  assert.equal((info.document.match(/class="nw-extra-slide"/g) || []).length, 17);
  assert.ok(info.document.includes('loading="lazy"')); assert.ok(info.document.includes('&lt;script&gt;Test&lt;/script&gt;'));
  assert.equal(info.document.includes('<script>Test'), false);
  assert.ok(info.document.indexOf('id="nw-extra-gallery"') < info.document.indexOf('</main>'));
  assert.match(info.document, /script-src &#39;none&#39;/);
  const reordered = await editAiPage(edited.page, { extraGallery: { title: 'Fotos', items: [items[16], items[0]] } });
  assert.equal(reordered.page.extraGallery.items[0].url, items[16].url);
  assert.equal(reordered.page.assets.filter(a => a.purpose === 'extra_gallery').length, 4);
  const removed = await editAiPage(reordered.page, { extraGallery: { title: 'Fotos', items: [] } });
  assert.equal(describeAiPage(removed.page).document.includes('id="nw-extra-gallery"'), false);
  assert.equal(removed.page.assets.some(a => a.purpose === 'extra_gallery'), false);
  await assert.rejects(editAiPage(bare, { extraGallery: { title: '', items: [...items, ...items] } }), /20 fotos/);
  await assert.rejects(editAiPage(bare, { extraGallery: { title: '', items: [{ url: 'javascript:alert(1)', caption: '' }] } }), /Sube/);
  await assert.rejects(editAiPage(bare, { extraGallery: { title: '', items } }), /Sube/);
});

test('authenticated image upload verifies real content, optimizes photos and thumbnails and confines storage to the client', async () => {
  const h = harness();
  const image = await sharp({ create: { width: 2200, height: 1400, channels: 3, background: '#aaddcc' } }).png().toBuffer();
  assert.equal((await h.upload(image, false)).status, 401); assert.equal(h.uploads.length, 0);
  assert.equal((await h.upload(Buffer.from('<svg onload="bad()"/>'))).status, 400); assert.equal(h.uploads.length, 0);
  assert.equal((await h.upload(Buffer.alloc(8 * 1024 * 1024 + 1))).status, 413); assert.equal(h.uploads.length, 0);
  const result = await h.upload(image);
  assert.equal(result.status, 200); assert.match(result.result.url, /b\/fc\/site-editor\//);
  assert.match(result.result.thumbnailUrl, /-thumb.webp$/); assert.equal(h.uploads.length, 2);
  for (const upload of h.uploads) {
    assert.match(upload.path, /^fc\/site-editor\//);
    assert.equal(upload.options.metadata.contentType, 'image/webp');
    const meta = await sharp(upload.buffer).metadata(); assert.equal(meta.format, 'webp');
    assert.ok(meta.width <= (upload.path.includes('-thumb') ? 240 : 1920));
  }
  assert.equal(h.writes, 0);
  h.data.websiteArchived = true;
  assert.equal((await h.upload(image)).status, 403);
});

test('extra gallery is previewed without publishing and is persisted and restored with the page revision', async () => {
  const h = harness(), initial = (await h.call('get')).result;
  const extraGallery = { title: 'Trabajos recientes', items: [{ url: uploaded, thumbnailUrl: uploaded, caption: 'Motor' }] };
  const preview = await h.call('post', { action: 'preview', revision: initial.revision, extraGallery });
  assert.equal(preview.status, 200); assert.equal(h.writes, 0);
  assert.match(preview.result.document, /id="nw-extra-gallery"/);
  const saved = await h.call('post', { action: 'save', revision: initial.revision, extraGallery });
  assert.equal(saved.status, 200); assert.deepEqual(h.data.schema.aiPage.extraGallery, extraGallery);
  const restored = await h.call('post', { action: 'restore', revision: saved.result.revision });
  assert.equal(restored.status, 200); assert.equal(h.data.schema.aiPage.extraGallery, undefined);
});

test('new services clone the existing design without retaining the previous service text or image', async () => {
  const page = { ...original, html: '<main><div class="nw-service-grid"><article class="nw-service-card"><svg><path d="M1 2"/></svg><h3>Frenos</h3><p>Consulta por ruidos</p></article><article class="nw-service-card"><img src="https://storage.googleapis.com/b/old.png"><h3>Motores</h3><p>Texto anterior</p></article></div></main>' };
  assert.equal(describeAiPage(page).canAddServices, true);
  const edited = await editAiPage(page, { services: [{ title: 'Aire acondicionado', description: 'Revisión del sistema de enfriamiento', imageUrl: uploaded }, { title: '<script>Servicio</script>', description: 'Solo texto' }] }, { validateImage: async (url) => url === uploaded });
  assert.equal(describeAiPage(edited.page).serviceCount, 4);
  assert.equal(edited.page.css, page.css);
  assert.ok(edited.page.html.includes('Aire acondicionado')); assert.ok(edited.page.html.includes('Revisión del sistema'));
  assert.ok(edited.page.html.includes('&lt;script&gt;Servicio&lt;/script&gt;'));
  assert.equal(edited.page.html.split('Texto anterior').length - 1, 1);
  assert.equal(edited.page.html.split('old.png').length - 1, 1);
  assert.ok(edited.page.assets.some((asset) => asset.url === uploaded));
  await assert.rejects(editAiPage(page, { services: [{ title: '', description: 'Texto' }] }), /nombre/);
  await assert.rejects(editAiPage(page, { services: [{ title: 'Nuevo', description: 'Texto', imageUrl: 'https://attacker.example/a.png' }] }), /foto/);
  await assert.rejects(editAiPage(page, { services: Array.from({ length: 13 }, () => ({ title: 'Nuevo', description: 'Texto' })) }), /12 servicios/);
  const featuredOnly = { ...page, html: '<main><div class="nw-service-grid"><article class="nw-service-card"><img src="https://storage.googleapis.com/b/old.png"><h3>Frenos</h3><p>Texto anterior</p></article></div></main>' };
  const plain = await editAiPage(featuredOnly, { services: [{ title: 'Sin foto', description: 'Nuevo servicio' }] });
  assert.equal(plain.page.html.split('old.png').length - 1, 1);
});
