import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import sharp from 'sharp';
import { registerSamplePackageRoutes, createSamplePackageSender } from '../services/samplePackages.js';

function harness() {
  const rows = new Map(), files = new Map(), routes = new Map();
  const db = { collection: (name) => ({
    doc: (id) => ({ id, path: `${name}/${id}`, get: async () => ({ data: () => rows.get(`${name}/${id}`) }), delete: async () => rows.delete(`${name}/${id}`) }),
    get: async () => ({ docs: [...rows].filter(([key]) => key.startsWith(`${name}/`)).map(([key, data]) => ({ id: key.split('/')[1], data: () => data })) }),
  }), batch: () => { const writes = []; return { set: (ref, data, options) => writes.push([ref, data, options]), commit: async () => writes.forEach(([ref, data, options]) => rows.set(ref.path, options?.merge ? { ...rows.get(ref.path), ...data } : data)) }; } };
  const bucket = { file: (path) => ({ save: async (buffer) => files.set(path, buffer), getSignedUrl: async () => [`https://storage.test/${path}`], delete: async () => files.delete(path) }) };
  const app = Object.fromEntries(['get', 'post', 'delete'].map((method) => [method, (url, ...handlers) => routes.set(`${method} ${url}`, handlers)]));
  registerSamplePackageRoutes(app, { db, bucket, readUser: async (req) => req.user, prepareAudio: async (buffer) => ({ buffer, duration: 3 }) });
  const dispatch = async (method, { id = 'new', form, user = { uid: 'admin', role: 'admin' } } = {}) => {
    const payload = form ? new Response(form) : null;
    const buffer = payload ? Buffer.from(await payload.arrayBuffer()) : Buffer.alloc(0);
    const req = Object.assign(Readable.from(buffer.length ? [buffer] : []), { user, params: { id }, headers: payload ? { 'content-type': payload.headers.get('content-type'), 'content-length': String(buffer.length) } : {} });
    return new Promise((resolve, reject) => {
      const res = { code: 200, status(code) { this.code = code; return this; }, json(body) { resolve({ status: this.code, body }); } };
      const handlers = routes.get(`${method} /api/crm/sample-agent/packages${method === 'get' ? '' : '/:id'}`);
      let index = 0;
      const next = () => { try { Promise.resolve(handlers[index++](req, res, next)).catch(reject); } catch (error) { reject(error); } };
      next();
    });
  };
  return { rows, files, db, bucket, dispatch };
}
const form = (price = '$990 MXN al año') => {
  const body = new FormData();
  for (const [key, value] of Object.entries({ name: 'Web negocio', price, included: 'Diseño adaptable\nWhatsApp', enabled: 'true', audioScript: 'Nuestro paquete cuesta 990 al año.' })) body.append(key, value);
  return body;
};

test('restringe administración y guarda imagen, voz y paquete principal con multipart', async () => {
  const h = harness();
  assert.equal((await h.dispatch('post', { user: null })).status, 401);
  assert.equal((await h.dispatch('post', { user: { role: 'salesAgent' } })).status, 403);
  const body = form();
  body.append('image', new Blob([await sharp({ create: { width: 2, height: 2, channels: 3, background: '#fff' } }).png().toBuffer()]), 'package.png');
  body.append('audio', new Blob(['recording']), 'package.webm');
  const saved = await h.dispatch('post', { form: body });
  assert.equal(saved.status, 200);
  const item = saved.body.package;
  assert.equal(h.files.size, 2);
  assert.match(item.image.storagePath, new RegExp(`^sample-packages/${item.id}/.*\\.webp$`));
  assert.equal(item.audio.duration, 3);
  assert.equal(h.rows.get('config/appConfig').sampleAgentWebPackageId, item.id);
  assert.equal((await h.dispatch('get')).body.packages.length, 1);
  assert.equal((await h.dispatch('post', { id: item.id, form: form('$1200 al año') })).status, 400);
  assert.equal(h.rows.get(`sampleAgentPackages/${item.id}`).price, '$990 MXN al año');
});

test('rechaza imágenes corruptas sin publicar el paquete', async () => {
  const h = harness(), body = form();
  body.append('image', new Blob(['not an image']), 'image.png');
  assert.equal((await h.dispatch('post', { form: body })).status, 400);
  assert.equal(h.rows.size, 0); assert.equal(h.files.size, 0);
});

test('envía imagen y voz sin texto duplicado; ante fallo de audio usa texto y conserva avances', async () => {
  const h = harness();
  const id = '12345678-1234-1234-1234-123456789abc';
  const item = { name: 'Web', price: '$990 al año', included: ['WhatsApp'], enabled: true, audioScript: 'Oferta grabada', image: { storagePath: `sample-packages/${id}/1234.webp` }, audio: { storagePath: `sample-packages/${id}/1234.ogg` } };
  h.rows.set(`sampleAgentPackages/${id}`, item);
  let sends = [], state = {};
  const make = (fail) => createSamplePackageSender({ db: h.db, bucket: h.bucket, sendImage: async () => sends.push('image'), sendAudio: async (_lead, _url, options) => { assert.equal(options.ptt, true); if (fail) throw Error('offline'); sends.push('audio'); }, sendText: async (_lead, text) => sends.push(text), logger: { warn() {} } });
  const args = () => ({ leadId: 'lead', packageId: id, isCurrent: async () => true, state, save: async (value) => { state = { ...value }; } });
  await make(false)(args()); assert.deepEqual(sends, ['image', 'audio']);
  sends = []; state = {};
  await make(true)(args()); await make(true)(args());
  assert.equal(sends.length, 2); assert.equal(sends[0], 'image'); assert.match(sends[1], /\$990 al año/);
  sends = []; state = {}; item.audio.storagePath = 'https://untrusted/audio.ogg';
  await make(false)(args()); assert.equal(sends.includes('audio'), false); assert.match(sends.at(-1), /WhatsApp/);
  item.enabled = false; sends = [];
  assert.equal(await make(false)(args()), null); assert.equal(sends.length, 0);
});
