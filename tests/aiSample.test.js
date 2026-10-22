import test from 'node:test';
import assert from 'node:assert/strict';
import { generateAiSample, normalizeAiSampleInput, sanitizeSamplePage, sanitizeSampleCss, samplePageDocument, aiSampleInputHash, publicImageUrl, requestSampleOpenAI } from '../services/aiSampleGenerator.js';
import { queueAiSampleJob, runAiSampleJob } from '../services/aiSampleJobs.js';
import { canGenerateAiSample, isAiSampleAvailable } from '../services/aiSampleAccess.js';

const business = { companyInfo: 'Taller Norte', businessStory: 'Reparación y mantenimiento de bicicletas en Monterrey.', contactWhatsapp: '528112345678', slug: 'taller-norte', aiImageCount: 1 };
const design = (count = 1) => ({
  title: 'Taller Norte', description: business.businessStory,
  html: `<main><h1>Tu bicicleta, lista para rodar</h1><p>Reparación y mantenimiento de bicicletas. Cuéntanos qué necesita tu bicicleta y solicita información sobre nuestros servicios.</p>${Array.from({ length: count }, (_, index) => `<img src="{{IMAGE_${index}}}" alt="Bicicleta ilustrativa" width="600" height="400">`).join('')}<a href="{{CONTACT_URL}}">Escríbenos por WhatsApp</a><section id="contacto">Contacto</section></main>`,
  css: 'body{font-family:Arial;background:#fff;color:#17202a}main{max-width:1000px;margin:auto;padding:24px}img{width:100%}@media(min-width:800px){main{padding:48px}}',
  imagePrompts: Array.from({ length: count }, (_, index) => `Bicicleta sobre fondo neutro, composición ${index}`),
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

test('conserva fotografías y logo; solo genera el material faltante', async () => {
  const photo = 'https://storage.googleapis.com/test/photo.jpg';
  const logo = 'https://storage.googleapis.com/test/logo.png';
  const api = apiStub({ ...design(1), html: `${design(1).html}<img src="{{LOGO}}" alt="Logo">`, imagePrompts: [] });
  const schema = await generateAiSample({ ...business, photoURLs: [photo], logoURL: logo, aiImageCount: 0 }, { request: api.request });
  assert.equal(api.calls.length, 1);
  assert.equal(schema.brand.logo, logo);
  assert.equal(schema.aiPage.assets[0].origin, 'uploaded');
  assert.match(schema.aiPage.html, /photo.jpg/);
  assert.equal(api.calls[0].body.input[0].content.filter((item) => item.type === 'input_image').length, 2);
  assert.equal(normalizeAiSampleInput({ ...business, photoURLs: [photo], aiImageCount: 50 }).aiImageCount, 2);
});

test('rechaza respuestas incompletas y cantidades incorrectas antes de gastar en imágenes', async () => {
  const api = apiStub(design(3));
  await assert.rejects(generateAiSample(business, { request: api.request, imageStore: imageStore() }), /límite/);
  assert.equal(api.calls.length, 1);
  await assert.rejects(generateAiSample(business, { request: async () => ({ status: 'incomplete' }) }), /no terminó/);
});

test('al cambiar textos o colores puede reutilizar las imágenes generadas sin volver a cobrarlas', async () => {
  const api = apiStub({ ...design(1), imagePrompts: [] });
  const reused = 'https://storage.googleapis.com/test/generated.webp';
  const schema = await generateAiSample({ ...business, primaryColor: '#ff5500', reusableAiImages: [reused] }, { request: api.request });
  assert.equal(api.calls.length, 1);
  assert.equal(schema.aiPage.assets[0].origin, 'ai_generated');
  assert.equal(schema.aiPage.assets[0].url, reused);
  assert.equal(aiSampleInputHash(business), aiSampleInputHash({ ...business, reusableAiImages: [reused] }));
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
    validateLayout: async () => { assert.equal(delivered, 0); return null; },
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
    validateLayout: async () => null,
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

test('una página que falla la revisión no se entrega', async () => {
  const { db, ref, read } = memoryDb();
  let delivered = false;
  await queueAiSampleJob(db, ref, business);
  await runAiSampleJob({ db, negocioRef: ref, bucket: {}, logger: { warn() {} }, generate: async () => ({}), validateLayout: async () => { throw new Error('Desbordamiento en móvil'); }, sendReady: async () => { delivered = true; } });
  assert.equal(delivered, false);
  assert.equal(read().aiSampleJob.state, 'failed');
  assert.equal(read().schema, undefined);
});
