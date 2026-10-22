import test from 'node:test';
import assert from 'node:assert/strict';
import { generateAiSample, normalizeAiSampleInput, sanitizeSamplePage, sanitizeSampleCss, samplePageDocument, aiSampleInputHash, publicImageUrl, requestSampleOpenAI } from '../services/aiSampleGenerator.js';
import { queueAiSampleJob, runAiSampleJob } from '../services/aiSampleJobs.js';
import { canGenerateAiSample, isAiSampleAvailable } from '../services/aiSampleAccess.js';
import { sampleHeroCss } from '../services/aiSampleVisuals.js';

const business = { companyInfo: 'Taller Norte', businessStory: 'Reparación y mantenimiento de bicicletas en Monterrey.', contactWhatsapp: '528112345678', slug: 'taller-norte', primaryColor: '#0066cc', aiImageCount: 1 };
const design = (count = 1) => ({
  title: 'Taller Norte', description: business.businessStory, palette: ['#0066cc', '#071b2e', '#ffffff'],
  html: `<header><span class="nw-wordmark">Taller Norte</span></header><main><section id="inicio" class="nw-hero">${count ? '<img class="nw-hero-media" src="{{IMAGE_0}}" alt="Bicicleta ilustrativa" width="1536" height="1024">' : ''}<div class="nw-hero-content"><h1>Tu bicicleta, lista para rodar</h1><p>Reparación y mantenimiento de bicicletas. Cuéntanos qué necesita tu bicicleta y solicita información sobre nuestros servicios.</p><a href="{{CONTACT_URL}}">Escríbenos por WhatsApp</a></div></section><section id="servicios"><h2>Cuida tu bicicleta</h2><p>Consulta las opciones de reparación y mantenimiento para tu bicicleta. Comparte el problema y te orientamos sobre la atención necesaria.</p>${Array.from({ length: Math.max(0, count - 1) }, (_, index) => `<img src="{{IMAGE_${index + 1}}}" alt="Detalle de bicicleta" width="600" height="400">`).join('')}</section><section id="proceso"><h2>Cuéntanos qué necesitas</h2><ol><li>Envía un mensaje explicando el problema.</li><li>Comparte fotografías para orientarte.</li><li>Consulta las opciones y acuerda la atención por WhatsApp.</li></ol></section><section id="preguntas"><h2>Antes de contactarnos</h2><details><summary>¿Qué información debo enviar?</summary><p>Describe tu bicicleta y el problema; puedes compartir fotografías.</p></details><details><summary>¿Cómo consulto el costo?</summary><p>Escríbenos para consultar las opciones según tu bicicleta.</p></details><details><summary>¿Dónde reciben las bicicletas?</summary><p>Consulta por WhatsApp el punto de atención.</p></details></section><section id="contacto"><h2>Hablemos de tu bicicleta</h2><p>Da el siguiente paso y consulta la reparación o mantenimiento que buscas.</p><a href="{{CONTACT_URL}}">Solicitar información</a></section></main>`,
  css: 'body{font-family:Arial;background:#fff;color:#17202a}main{max-width:1200px;margin:auto}section:not(.nw-hero){padding:32px 24px}img{max-width:100%}@media(min-width:800px){section:not(.nw-hero){padding:48px}}',
  imagePrompts: Array.from({ length: count }, (_, index) => `Bicicleta y herramientas de mantenimiento, composición ${index}`),
});

function apiStub(page = design()) {
  const calls = [];
  return {
    calls,
    async request(resource, body) {
      calls.push({ resource, body });
      return resource === 'responses'
        ? { status: 'completed', output: [{ content: [{ type: 'output_text', text: JSON.stringify(page) }] }] }
        : { data: [{ b64_json: Buffer.from('image fixture').toString('base64') }] };
    },
  };
}

function imageStore() {
  const images = new Map();
  return { images, get: async (key) => images.get(key), put: async (key) => { const url = `https://storage.googleapis.com/test/${key}.webp`; images.set(key, url); return url; } };
}

test('videos del chat: limita a dos, conserva firmas y reproduce sin scripts ni autoplay', async () => {
  const first = 'https://storage.googleapis.com/test/one.mp4?key=123&signature=abc';
  const second = 'https://storage.googleapis.com/test/two.mp4';
  const api = apiStub();
  const schema = await generateAiSample({ ...business, videoURLs: ['javascript:alert(1)', first, first, second, 'https://other.example/three.mp4'] }, { request: api.request, imageStore: imageStore() });
  assert.deepEqual(schema.aiPage.videos, [first, second]);
  assert.equal((schema.aiPage.html.match(/<video\b/g) || []).length, 2);
  assert.ok(schema.aiPage.html.indexOf('id="videos"') < schema.aiPage.html.indexOf('id="proceso"'));
  assert.match(schema.aiPage.html, /controls playsinline preload="metadata"/);
  assert.match(schema.aiPage.html, /key=123&amp;signature=abc/);
  assert.doesNotMatch(schema.aiPage.html, /autoplay|<script|other\.example|javascript:/);
  const document = samplePageDocument(schema.aiPage);
  assert.match(document, /media-src https:\/\/storage.googleapis.com;/);
  assert.match(document, /script-src &#39;none&#39;/);
  assert.doesNotMatch(document, /allow-scripts|allow-same-origin/);
  const sanitized = sanitizeSamplePage({ ...design(0), html: `${design(0).html}<video src="https://evil.example/video.mp4" autoplay onplay="alert(1)"></video>` }, { input: normalizeAiSampleInput(business) });
  assert.doesNotMatch(sanitized.html, /<video|evil\.example|onplay/);
  assert.notEqual(aiSampleInputHash(business), aiSampleInputHash({ ...business, videoURLs: [first] }));
});

test('sin videos no agrega una sección vacía ni habilita recursos multimedia', async () => {
  const api = apiStub();
  const schema = await generateAiSample(business, { request: api.request, imageStore: imageStore() });
  assert.doesNotMatch(schema.aiPage.html, /id="videos"|<video/);
  assert.match(samplePageDocument(schema.aiPage), /media-src &#39;none&#39;/);
});

test('al encolar recupera videos entrantes guardados en el chat para la muestra', async () => {
  const { db, ref, read } = memoryDb();
  const filters = [];
  const query = {
    where(field, op, value) { filters.push([field, op, value]); return this; },
    limit(count) { assert.equal(count, 2); return this; },
    async get() { return { docs: [{ data: () => ({ mediaUrl: 'https://storage.googleapis.com/test/chat.mp4' }) }] }; },
  };
  db.collection = (name) => {
    assert.equal(name, 'leads');
    return { doc: (id) => { assert.equal(id, 'lead-1'); return { collection: (name) => { assert.equal(name, 'messages'); return query; } }; } };
  };
  await queueAiSampleJob(db, ref, { ...business, leadId: 'lead-1' });
  assert.deepEqual(filters, [['mediaType', '==', 'video'], ['sender', '==', 'lead']]);
  assert.deepEqual(read().aiSampleJob.input.videoURLs, ['https://storage.googleapis.com/test/chat.mp4']);
});

test('la tarjeta digital tiene iconos confiables y llamadas reales sin permitir SVG o teléfonos ajenos', () => {
  const input = normalizeAiSampleInput(business);
  const page = { ...design(0), html: `${design(0).html}<nav><a href="{{PHONE_URL}}">{{ICON_PHONE}} Llamar</a><a href="{{CONTACT_URL}}">{{ICON_WHATSAPP}} WhatsApp {{ICON_ARROW}}</a><a href="tel:+999">Otro teléfono</a>{{ICON_UNKNOWN}}<svg onload="alert(1)"><script>alert(1)</script><image href="https://evil.example/a.svg"/></svg></nav>` };
  const result = sanitizeSamplePage(page, { input });
  assert.match(result.html, /href="tel:\+528112345678"/);
  assert.match(result.html, /class="nw-icon nw-icon-phone"[^>]*><svg/);
  assert.match(result.html, /class="nw-icon nw-icon-whatsapp"[^>]*><svg/);
  assert.doesNotMatch(result.html, /tel:\+999|onload|<script|evil.example|\{\{ICON_/);
  const document = samplePageDocument({ ...result, assets: [] });
  assert.match(document, /script-src &#39;none&#39;/);
});

test('conserva animaciones y movimiento reducido, con CTA legible para paletas claras y oscuras', () => {
  const dark = sanitizeSampleCss(sampleHeroCss(['#000000', '#ffffff', '#008800']));
  const light = sanitizeSampleCss(sampleHeroCss(['#ffffff', '#000000', '#008800']));
  assert.match(dark, /--nw-on-accent:#ffffff/);
  assert.match(light, /--nw-on-accent:#111111/);
  assert.match(dark, /prefers-reduced-motion:reduce/);
  assert.match(dark, /animation:none!important/);
  assert.match(dark, /@keyframes nw-rise/);
  assert.match(dark, /body:has\(\.nw-mobile-cta\)/, 'la barra fija reserva espacio para no tapar el pie');
});

test('usa la paleta confirmada del logo sin imponer azul en la portada y conserva referencias solo como contexto', async () => {
  const api = apiStub({ ...design(), html: `${design().html}<img src="{{LOGO}}" alt="Logo">` });
  const palette = ['#008800', '#FFFFFF', '#111111'];
  const reference = 'https://storage.googleapis.com/test/reference.jpg';
  const schema = await generateAiSample({ ...business, logoURL: 'https://storage.googleapis.com/test/logo.png', brandPalette: palette, referenceImageURLs: [reference] }, { request: api.request, imageStore: imageStore() });
  assert.deepEqual(schema.colors.palette, palette);
  assert.equal(schema.colors.primary, '#008800');
  assert.match(schema.aiPage.css, /--nw-hero-rgb:0,22,0/);
  assert.doesNotMatch(schema.aiPage.css, /rgba\(3,15,28|#071b2e/);
  assert.equal(schema.aiPage.assets.some((asset) => asset.url === reference), false);
  assert.ok(api.calls[0].body.input[0].content.some((item) => item.image_url === reference));
});

test('genera código e imágenes con los modelos acordados y reutiliza imágenes guardadas', async () => {
  const api = apiStub();
  const store = imageStore();
  let savedDesign;
  const schema = await generateAiSample(business, { request: api.request, imageStore: store, saveDesign: async (page) => { savedDesign = page; } });
  assert.equal(api.calls[0].body.model, 'gpt-6.1-sol');
  assert.equal(api.calls[0].body.store, false);
  assert.equal(api.calls[1].body.model, 'gpt-image-2.5-flare');
  assert.equal(schema.aiPage.assets[0].origin, 'ai_generated');
  assert.match(schema.aiPage.html, /https:\/\/wa.me\/528112345678/);
  assert.doesNotMatch(schema.aiPage.html, /\{\{/);
  await generateAiSample(business, { request: api.request, imageStore: store, savedDesign });
  assert.equal(api.calls.length, 2, 'un reintento con diseño guardado no vuelve a consumir la API');
});

test('conserva fotografías y logo, y siempre crea una portada basada en el material recibido', async () => {
  const photo = 'https://storage.googleapis.com/test/photo.jpg';
  const logo = 'https://storage.googleapis.com/test/logo.png';
  const api = apiStub({ ...design(2), html: `${design(2).html}<img src="{{LOGO}}" alt="Logo">`, imagePrompts: design(1).imagePrompts });
  const schema = await generateAiSample({ ...business, photoURLs: [photo], logoURL: logo, aiImageCount: 0 }, { request: api.request, imageStore: imageStore() });
  assert.equal(api.calls.length, 2);
  assert.equal(api.calls[1].resource, 'images/edits');
  assert.deepEqual(api.calls[1].body.images, [{ image_url: photo }]);
  assert.equal(schema.brand.logo, logo);
  assert.equal(schema.aiPage.assets[0].purpose, 'hero');
  assert.equal(schema.aiPage.assets[1].origin, 'uploaded');
  assert.match(schema.aiPage.html, /photo.jpg/);
  assert.equal(api.calls[0].body.input[0].content.filter((item) => item.type === 'input_image').length, 2);
  assert.equal(normalizeAiSampleInput({ ...business, photoURLs: [photo], aiImageCount: 50 }).aiImageCount, 3);
  assert.equal(normalizeAiSampleInput({ ...business, aiImageCount: 0 }).aiImageCount, 1);
});

test('permite activar retoques fieles dentro del presupuesto de tres operaciones', async () => {
  const photos = [0, 1, 2].map((index) => `https://storage.googleapis.com/test/photo-${index}.jpg`);
  const api = apiStub({ ...design(4), imagePrompts: design(3).imagePrompts });
  const schema = await generateAiSample({ ...business, photoURLs: photos, aiImageCount: 3, enhancePhotos: true }, { request: api.request, imageStore: imageStore() });
  const imageCalls = api.calls.filter((call) => call.resource === 'images/edits');
  assert.equal(imageCalls.length, 3);
  assert.deepEqual(imageCalls[0].body.images.map((item) => item.image_url), photos);
  assert.deepEqual(imageCalls[1].body.images, [{ image_url: photos[0] }]);
  assert.match(imageCalls[1].body.prompt, /Conserva exactamente la identidad/);
  assert.equal(schema.aiPage.assets[0].purpose, 'hero');
  assert.equal(schema.aiPage.assets.filter((asset) => asset.origin === 'ai_edited').length, 2);
  assert.deepEqual(schema.aiPage.assets.slice(1).flatMap((asset) => asset.sourceUrls), photos);
  assert.equal(schema.hero.backgroundImageUrl, schema.aiPage.assets[0].url);
});

test('por defecto conserva originales y genera hero más dos servicios distintos, reutilizando hero al cierre', async () => {
  const photo = 'https://storage.googleapis.com/test/photo.jpg';
  const api = apiStub({ ...design(4), imagePrompts: design(3).imagePrompts });
  const schema = await generateAiSample({ ...business, photoURLs: [photo], aiImageCount: 3, keyItems: ['Cambio de aceite', 'Transmisiones'] }, { request: api.request, imageStore: imageStore() });
  assert.equal(api.calls.filter((call) => call.resource === 'images/edits').length, 1);
  assert.equal(api.calls.filter((call) => call.resource === 'images/generations').length, 2);
  assert.equal(schema.aiPage.assets[1].url, photo);
  assert.equal(schema.aiPage.assets[1].origin, 'uploaded');
  const scenes = api.calls.filter((call) => call.resource === 'images/generations');
  assert.match(scenes[0].body.prompt, /Cambio de aceite/);
  assert.match(scenes[1].body.prompt, /Transmisiones/);
  assert.match(scenes[0].body.prompt, /detalle cercano/);
  assert.match(scenes[1].body.prompt, /encuadre más abierto/);
  assert.equal(api.calls.filter((call) => call.resource.startsWith('images/')).length, 3);
  const contact = schema.aiPage.html.split('id="contacto"')[1];
  assert.ok(contact.includes(schema.hero.backgroundImageUrl), 'reutiliza la misma imagen al cierre');
  assert.match(contact, /class="nw-contact-photo"/);
  assert.match(contact, /Imagen ilustrativa/);
  assert.equal(normalizeAiSampleInput({}).enhancePhotos, false);
});

test('comprueba que el diseño use todas las imágenes solicitadas antes de generarlas', async () => {
  const api = apiStub({ ...design(1), imagePrompts: design(3).imagePrompts });
  await assert.rejects(generateAiSample({ ...business, aiImageCount: 3 }, { request: api.request, imageStore: imageStore() }), /omitió imágenes/);
  assert.equal(api.calls.length, 1);
});

test('rechaza respuestas incompletas y cantidades incorrectas antes de gastar en imágenes', async () => {
  const api = apiStub(design(3));
  await assert.rejects(generateAiSample(business, { request: api.request, imageStore: imageStore() }), /límite/);
  assert.equal(api.calls.length, 1);
  await assert.rejects(generateAiSample(business, { request: async () => ({ status: 'incomplete' }) }), /no terminó/);
});

test('al cambiar el objetivo reutiliza los recursos visuales compatibles sin volver a generarlos', async () => {
  const initialApi = apiStub();
  const first = await generateAiSample(business, { request: initialApi.request, imageStore: imageStore() });
  const api = apiStub({ ...design(1), imagePrompts: [] });
  const schema = await generateAiSample({ ...business, businessObjective: 'Recibir consultas', reusableAiAssets: first.aiPage.assets }, { request: api.request });
  assert.equal(api.calls.length, 1);
  assert.equal(schema.aiPage.assets[0].url, first.aiPage.assets[0].url);
  assert.equal(aiSampleInputHash(business), aiSampleInputHash({ ...business, reusableAiAssets: first.aiPage.assets }));
});

test('pregunta los colores cuando no hay logo y evita gastar antes de tener esa respuesta', async () => {
  await assert.rejects(generateAiSample({ ...business, primaryColor: '' }, { request: async () => assert.fail('No debe consumir la API') }), /Qué colores/);
  const api = apiStub();
  const schema = await generateAiSample({ ...business, primaryColor: '', colorPreference: 'Propón una combinación' }, { request: api.request, imageStore: imageStore() });
  assert.deepEqual(schema.colors.palette, ['#0066cc', '#071b2e', '#ffffff']);
  assert.match(schema.aiPage.html, /nw-wordmark/);
});

test('rechaza una página vacía antes de consumir la generación de imágenes', async () => {
  const api = apiStub({ ...design(), html: '<main><h1>Hola</h1><img src="{{IMAGE_0}}"><a href="{{CONTACT_URL}}">Contactar</a></main>' });
  await assert.rejects(generateAiSample(business, { request: api.request, imageStore: imageStore() }), /página completa|necesita completar/);
  assert.equal(api.calls.length, 1);
});

test('elimina scripts, eventos, formularios, imágenes ajenas y enlaces peligrosos', () => {
  const input = normalizeAiSampleInput({ ...business, aiImageCount: 0 });
  const unsafe = { ...design(0), html: design(0).html + '<script>alert(1)</script><img src="https://evil.example/x" onerror="alert(1)"><form action="https://evil.example"><input name="password"></form><a href="javascript:alert(1)" onclick="alert(1)">Abrir</a><iframe src="https://evil.example"></iframe>' };
  const safe = sanitizeSamplePage(unsafe, { input });
  assert.doesNotMatch(safe.html, /script|onerror|onclick|<form|<input|<iframe|evil\.example|javascript:/);
  const doc = samplePageDocument({ ...safe, title: '</title><script>bad</script>', assets: [] });
  assert.match(doc, /Content-Security-Policy/);
  assert.doesNotMatch(doc, /<script>/);
  for (const css of ['@import "https://evil.example";', 'div{background:url(https://evil.example)}', 'div{color:r\\65 d}']) {
    const filtered = sanitizeSamplePage({ ...design(0), css }, { input }).css;
    assert.doesNotMatch(filtered, /evil|url\(|\\|@import/);
  }
  assert.throws(() => sanitizeSamplePage({ ...design(0), css: '</style><script>' }, { input }), /CSS/);
  assert.equal(publicImageUrl('http://127.0.0.1/logo'), '');
  assert.equal(publicImageUrl('https://localhost/logo'), '');
  assert.equal(publicImageUrl('https://169.254.169.254/secret'), '');
});

test('conserva el diseño responsive aunque incluya fuentes externas, fondos remotos o reglas CSS modernas', () => {
  const css = sanitizeSampleCss(`
    /* <main> branding */
    @import url("https://fonts.googleapis.com/css2?family=Inter");
    @font-face {font-family:Remote;src:url("https://evil.example/font.woff2")}
    @layer base {body{font-family:Inter,Arial,sans-serif;color:#123456}}
    .hero{background-image:url("https://evil.example/hero.jpg");background-color:#fff;display:grid}
    @container (min-width:600px){.hero{grid-template-columns:1fr 1fr}}
    @media (max-width:600px){.hero{display:block}}
    .bad{behavior:url(x);-moz-binding:url(y);background:u/**/rl(z);content:"\\2192"}
  `);
  assert.match(css, /@layer base/);
  assert.match(css, /@container/);
  assert.match(css, /@media/);
  assert.match(css, /background-color:#fff/);
  assert.match(css, /font-family:Inter,Arial,sans-serif/);
  assert.doesNotMatch(css, /@import|@font-face|url\(|evil\.example|behavior|-moz-binding|\\|<main>/);
});

test('un error de API queda visible y no se convierte en una muestra ficticia', async () => {
  await assert.rejects(requestSampleOpenAI('responses', {}, { apiKey: 'test', fetchImpl: async () => ({ ok: false, status: 403, json: async () => ({ error: { message: 'Model access denied' } }) }) }), /Model access denied/);
});

test('permisos: administrador y vendedor asignado; no clientes ni usuarios inactivos', () => {
  assert.equal(canGenerateAiSample({ uid: 'a', role: 'superAdmin' }), true);
  assert.equal(canGenerateAiSample({ uid: 'a', role: 'salesAgent' }, { assignedTo: 'a' }), true);
  assert.equal(canGenerateAiSample({ uid: 'b', role: 'salesAgent' }, { assignedTo: 'a' }), false);
  assert.equal(canGenerateAiSample({ uid: 'a', role: 'cliente' }), false);
  assert.equal(canGenerateAiSample({ uid: 'a', role: 'admin', active: false }), false);
  const data = { sampleGenerator: 'ai-page-v1', schema: { aiPage: { version: 'ai-page-v1' } } };
  assert.equal(isAiSampleAvailable(data), true);
  assert.equal(isAiSampleAvailable({ ...data, archived: true }), false);
  assert.equal(isAiSampleAvailable({ ...data, sampleGenerator: 'legacy' }), false);
});

function memoryDb() {
  let data = {};
  const merge = (patch) => {
    for (const [key, value] of Object.entries(patch)) {
      const parts = key.split('.');
      let target = data;
      for (const part of parts.slice(0, -1)) target = target[part] ||= {};
      target[parts.at(-1)] = structuredClone(value);
    }
  };
  const ref = { id: 'business-1', get: async () => ({ data: () => structuredClone(data) }), update: async (patch) => merge(patch) };
  const db = { runTransaction: async (callback) => callback({ get: ref.get, update: (_ref, patch) => merge(patch), set: (_ref, patch) => merge(patch) }) };
  return { db, ref, read: () => data };
}

test('el trabajo se guarda antes de enviar y una solicitud repetida no genera ni envía otra vez', async () => {
  const { db, ref, read } = memoryDb();
  let generated = 0;
  let delivered = 0;
  await queueAiSampleJob(db, ref, business);
  assert.equal((await queueAiSampleJob(db, ref, business)).duplicate, true);
  const options = {
    db, negocioRef: ref, bucket: {},
    generate: async (input) => { generated += 1; return { aiPage: { inputHash: aiSampleInputHash(input) } }; },
    sendReady: async () => { assert.ok(read().schema.aiPage); delivered += 1; },
  };
  await runAiSampleJob(options);
  assert.equal(read().aiSampleJob.state, 'sent');
  assert.equal((await queueAiSampleJob(db, ref, business)).ready, true);
  await runAiSampleJob(options);
  assert.equal(generated, 1);
  assert.equal(delivered, 1);
});

test('si falla la entrega, se conserva la página y el reintento no consume el generador', async () => {
  const { db, ref, read } = memoryDb();
  let generated = 0;
  const options = {
    db, negocioRef: ref, bucket: {}, logger: { warn() {} },
    generate: async (input) => { generated += 1; return { aiPage: { inputHash: aiSampleInputHash(input) } }; },
    sendReady: async () => { throw new Error('WhatsApp desconectado'); },
  };
  await queueAiSampleJob(db, ref, business);
  await runAiSampleJob(options);
  assert.equal(read().aiSampleJob.state, 'failed');
  assert.match(read().aiSampleJob.error, /WhatsApp/);
  await queueAiSampleJob(db, ref, business);
  await runAiSampleJob({ ...options, sendReady: async () => {} });
  assert.equal(generated, 1);
  assert.equal(read().aiSampleJob.state, 'sent');
});

test('una muestra se entrega sin Chrome ni revisión visual', async () => {
  const { db, ref, read } = memoryDb();
  let delivered = false;
  await queueAiSampleJob(db, ref, business);
  await runAiSampleJob({ db, negocioRef: ref, bucket: {}, logger: { warn() {} }, prepareBrowser: async () => assert.fail('No debe requerir Chrome'), generate: async (input) => ({ aiPage: { inputHash: aiSampleInputHash(input) } }), validateLayout: async () => assert.fail('No debe ejecutar revisión visual'), sendReady: async () => { delivered = true; } });
  assert.equal(delivered, true);
  assert.equal(read().aiSampleJob.state, 'sent');
  assert.ok(read().schema.aiPage);
});

test('si falla la generación o sus validaciones, la muestra no se envía', async () => {
  const { db, ref, read } = memoryDb();
  await queueAiSampleJob(db, ref, business);
  await runAiSampleJob({ db, negocioRef: ref, bucket: {}, logger: { warn() {} }, generate: async () => { throw new Error('La muestra no contiene un contacto de WhatsApp válido.'); }, sendReady: async () => assert.fail('No debe entregar') });
  assert.equal(read().aiSampleJob.state, 'failed');
  assert.match(read().aiSampleJob.error, /contacto de WhatsApp/);
  assert.equal(read().schema, undefined);
});
