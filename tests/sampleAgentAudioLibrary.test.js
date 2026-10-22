import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { canManageSampleAudios, prepareSampleRecordedAudio, registerSampleAgentAudioRoutes, createSampleFlowAudioSender } from '../services/sampleAgentAudioLibrary.js';

function harness() {
  const records = new Map(); const files = new Map(); const routes = new Map();
  let preparations = 0;
  const db = { collection: () => ({
    get: async () => ({ docs: [...records].map(([id, data]) => ({ id, data: () => data })) }),
    doc: (id) => ({ get: async () => ({ exists: records.has(id), data: () => records.get(id) }), set: async (data) => records.set(id, data), update: async (data) => records.set(id, { ...records.get(id), ...data }), delete: async () => records.delete(id) }),
  }) };
  const bucket = { file: (name) => ({ save: async (buffer) => files.set(name, buffer), getSignedUrl: async () => [`https://storage.googleapis.com/test/${name}`], delete: async () => files.delete(name) }) };
  const app = Object.fromEntries(['get', 'post', 'patch', 'delete'].map((method) => [method, (url, ...handlers) => routes.set(`${method} ${url}`, handlers)]));
  registerSampleAgentAudioRoutes(app, { db, bucket, readUser: async (req) => req.user, prepare: async (buffer) => { preparations++; return { buffer, duration: 3 }; }, logger: { warn() {} } });
  const dispatch = async (method, { user = { uid: 'admin', role: 'admin' }, moment = 'welcome', body = {}, form = null } = {}) => {
    const payload = form ? new Response(form) : null;
    const buffer = payload ? Buffer.from(await payload.arrayBuffer()) : Buffer.alloc(0);
    const req = Object.assign(Readable.from(buffer.length ? [buffer] : []), { user, params: { moment }, body, headers: payload ? { 'content-type': payload.headers.get('content-type'), 'content-length': String(buffer.length) } : {} });
    return new Promise((resolve, reject) => {
      const res = { code: 200, status(code) { this.code = code; return this; }, json(value) { resolve({ status: this.code, body: value }); } };
      const url = method === 'get' ? '/api/crm/sample-agent/audios' : '/api/crm/sample-agent/audios/:moment';
      const handlers = routes.get(`${method} ${url}`);
      let index = 0;
      const next = () => { try { Promise.resolve(handlers[index++](req, res, next)).catch(reject); } catch (error) { reject(error); } };
      next();
    });
  };
  return { db, bucket, records, files, dispatch, preparations: () => preparations };
}

function uploadForm(enabled = true) {
  const form = new FormData();
  form.append('audio', new Blob(['voice'], { type: 'audio/webm' }), 'recording.webm');
  form.append('title', 'Mi bienvenida'); form.append('script', 'Hola, vamos a preparar tu muestra.'); form.append('enabled', String(enabled));
  return form;
}

test('protege la biblioteca: solo administradores activos pueden subir o activar audios', async () => {
  assert.equal(canManageSampleAudios({ role: 'superAdmin' }), true);
  assert.equal(canManageSampleAudios({ role: 'admin', active: false }), false);
  const h = harness();
  assert.equal((await h.dispatch('post', { user: null })).status, 401);
  assert.equal((await h.dispatch('post', { user: { role: 'salesAgent' } })).status, 403);
  assert.equal((await h.dispatch('post', { moment: '../other' })).status, 400);
  assert.equal(h.preparations(), 0);
});

test('guarda la grabación multipart, permite escucharla, activarla y eliminarla del flujo', async () => {
  const h = harness();
  const saved = await h.dispatch('post', { form: uploadForm(false) });
  assert.equal(saved.status, 200);
  assert.equal(saved.body.audio.enabled, false);
  assert.equal(saved.body.audio.duration, 3);
  assert.match(saved.body.audio.storagePath, /^sample-agent-audio\/welcome\/[a-f0-9-]+\.ogg$/);
  assert.equal(h.files.size, 1);
  const list = await h.dispatch('get');
  assert.equal(list.body.audios[0].title, 'Mi bienvenida');
  assert.match(list.body.audios[0].url, /storage.googleapis.com/);
  assert.equal((await h.dispatch('patch', { body: { enabled: true } })).status, 200);
  assert.equal(h.records.get('welcome').enabled, true);
  assert.equal((await h.dispatch('delete')).status, 200);
  assert.equal(h.records.size, 0); assert.equal(h.files.size, 0);
});

test('no publica grabaciones sin archivo ni permite habilitar momentos que no tienen audio', async () => {
  const h = harness();
  assert.equal((await h.dispatch('post')).status, 400);
  assert.equal((await h.dispatch('patch', { body: { enabled: true } })).status, 404);
  assert.equal(h.files.size, 0);
});

test('solo envía audios activos desde el almacenamiento propio como notas de voz', async () => {
  const h = harness(); const sends = [];
  const send = createSampleFlowAudioSender({ db: h.db, bucket: h.bucket, sendAudio: async (...args) => sends.push(args) });
  assert.equal(await send({ leadId: 'lead', moment: 'welcome' }), false);
  await h.dispatch('post', { form: uploadForm(true) });
  assert.equal(await send({ leadId: 'lead', moment: 'welcome' }), true);
  assert.equal(sends[0][0], 'lead');
  assert.match(sends[0][1], /sample-agent-audio\/welcome/);
  assert.equal(sends[0][2].ptt, true); assert.equal(sends[0][2].forwarded, false);
  h.records.set('welcome', { enabled: true, storagePath: 'https://evil.example/audio.ogg' });
  assert.equal(await send({ leadId: 'lead', moment: 'welcome' }), false);
  assert.equal(sends.length, 1);
});

test('convierte una grabación real a OGG/Opus válido y rechaza archivos corruptos', async () => {
  const samples = 16_000;
  const wav = Buffer.alloc(44 + samples * 2);
  wav.write('RIFF', 0); wav.writeUInt32LE(wav.length - 8, 4); wav.write('WAVEfmt ', 8);
  wav.writeUInt32LE(16, 16); wav.writeUInt16LE(1, 20); wav.writeUInt16LE(1, 22); wav.writeUInt32LE(16_000, 24);
  wav.writeUInt32LE(32_000, 28); wav.writeUInt16LE(2, 32); wav.writeUInt16LE(16, 34); wav.write('data', 36); wav.writeUInt32LE(samples * 2, 40);
  const result = await prepareSampleRecordedAudio(wav);
  assert.equal(result.buffer.subarray(0, 4).toString(), 'OggS');
  assert.ok(result.duration > .9 && result.duration < 1.1);
  await assert.rejects(prepareSampleRecordedAudio(Buffer.from('not audio')));
});
