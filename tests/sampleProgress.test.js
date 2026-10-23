import test from 'node:test';
import assert from 'node:assert/strict';
import { registerSampleProgressRoutes, summarizeSampleProgress, manualSampleInput } from '../services/sampleProgress.js';

const now = Date.parse('2026-10-23T16:00:00Z');
const lead = (brief = {}, extra = {}) => ({ nombre: 'Sergio', telefono: '5213323193742', sampleAgent: { status: 'collecting', updatedAt: new Date(now), brief, ...extra } });
const complete = { companyName: 'Gran Valor', activity: 'Tequila artesanal', objective: 'Recibir pedidos', offerings: ['Blanco', 'Reposado'], customers: 'Público general', differentiators: '100% agave', serviceArea: 'Amatitán', logoChoice: 'none', colorPreference: 'Azul y crema', photosChoice: 'none' };

test('el avance refleja datos reales y señala el siguiente paso', () => {
  const empty = summarizeSampleProgress('a', lead(), now);
  assert.equal(empty.progress, 0);
  assert.match(empty.pending, /Cómo se llama/);
  const partial = summarizeSampleProgress('a', lead({ companyName: 'Gran Valor', activity: 'Tequila' }), now);
  assert.equal(partial.progress, 17);
  assert.match(partial.pending, /lograr/);
  const ready = summarizeSampleProgress('a', lead(complete), now);
  assert.equal(ready.progress, 90);
  assert.match(ready.pending, /Confirmar/);
  assert.equal(summarizeSampleProgress('a', lead(complete, { status: 'generating' }), now).progress, 95);
});

test('el semáforo distingue contexto esencial, detalles pendientes y datos suficientes', () => {
  const red = summarizeSampleProgress('a', lead({ companyName: 'Gran Valor', activity: 'Tequila' }), now);
  assert.equal(red.readiness.level, 'red');
  assert.match(red.readiness.reason, /objetivo.*productos/);
  const yellow = summarizeSampleProgress('a', lead({ ...complete, colorPreference: '' }), now);
  assert.equal(yellow.readiness.level, 'yellow');
  assert.match(yellow.readiness.reason, /colores/);
  const green = summarizeSampleProgress('a', lead({ ...complete, customers: '', differentiators: '' }), now);
  assert.equal(green.readiness.level, 'green');
  assert.ok(green.progress < 90);
  assert.equal(green.canResume, true);
  assert.equal(summarizeSampleProgress('a', lead(complete, { status: 'generating' }), now).canResume, false);
  assert.equal(summarizeSampleProgress('a', { ...lead(complete), humanControl: true }, now).canResume, false);
});

test('play comprueba permisos y estado antes de retomar y devuelve el avance actualizado', async () => {
  let handler; let resumed = 0;
  let current = { ...lead(complete, { status: 'paused' }), assignedTo: 'seller' };
  const ref = { get: async () => ({ exists: true, ref, data: () => current }) };
  registerSampleProgressRoutes({ get: () => {}, post: (_path, fn) => { handler = fn; } }, {
    db: { collection: () => ({ doc: () => ref }) }, readUser: async (req) => req.user,
    resume: async (id, user) => { assert.equal(id, 'lead'); assert.equal(user.uid, 'seller'); resumed++; current.sampleAgent.status = 'awaiting_confirmation'; },
  });
  const dispatch = async (user = { uid: 'seller', role: 'salesAgent' }) => {
    const res = { code: 200, status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } };
    await handler({ user, body: { leadId: 'lead' } }, res);
    return res;
  };
  assert.equal((await dispatch(null)).code, 401);
  assert.equal((await dispatch({ uid: 'other', role: 'salesAgent' })).code, 403);
  const result = await dispatch();
  assert.equal(result.code, 200);
  assert.equal(resumed, 1);
  assert.equal(result.body.row.status, 'awaiting_confirmation');
  current.humanControl = true;
  assert.equal((await dispatch()).code, 409);
  delete current.humanControl;
  current.sampleAgent.status = 'generating';
  assert.equal((await dispatch()).code, 409);
  assert.equal(resumed, 1);
});

test('un logo ilegible no completa el paso y no muestra 90% por el estado del asistente', () => {
  const row = summarizeSampleProgress('a', lead({ ...complete, logoChoice: 'provided', palette: ['#ffffff', '#000000', '#888888'], images: [{ index: 0, role: 'logo', description: 'Captura', logoStatus: 'original_required' }] }, { status: 'awaiting_confirmation', images: [{ url: 'https://storage.googleapis.com/own/images/logo.jpg' }] }), now);
  assert.ok(row.progress < 90);
  assert.equal(row.steps.find((step) => step.label === 'Logo o nombre').done, false);
  assert.match(row.pending, /original/);
});

test('destaca pausas, errores e inactividad sin perder los datos', () => {
  const row = summarizeSampleProgress('a', lead(complete, { status: 'paused', pauseReason: 'other_service_or_human_conversation', updatedAt: new Date(now - 7 * 3600_000) }), now);
  assert.equal(row.progress, 90);
  assert.equal(row.inactive, true);
  assert.equal(row.needsAttention, true);
  assert.match(row.pauseReason, /asesor/);
  assert.equal(summarizeSampleProgress('a', lead({}, { status: 'error', error: 'Falló el audio' }), now).needsAttention, true);
  assert.equal(summarizeSampleProgress('a', { ...lead(), lastMessageAt: new Date(now) }, now).inactive, false);
});

test('excluye entregados, cancelados, archivados y contactos sin proceso', () => {
  for (const item of [ {}, lead({}, { status: 'completed' }), { ...lead(), webSales: { sample: { sentAt: new Date(now) } } }, { ...lead(), archived: true }, { ...lead(), stopSequences: true }, { ...lead(), estado: 'no interesado' } ]) {
    assert.equal(summarizeSampleProgress('a', item, now), null);
  }
});

test('la ruta exige sesión y permisos y no expone contactos ajenos ni datos completos del lead', async () => {
  let handler; let queried = false;
  const docs = [
    { id: 'a', data: () => ({ ...lead({ companyName: 'Visible' }), assignedTo: 'sales', secret: 'private' }) },
    { id: 'b', data: () => ({ ...lead({ companyName: 'Ajeno' }), assignedTo: 'other' }) },
  ];
  const query = { where: () => query, orderBy: () => query, startAfter: () => query, limit: () => query, get: async () => { queried = true; return { docs }; } };
  registerSampleProgressRoutes({ get: (_path, fn) => { handler = fn; } }, { db: { collection: () => query }, readUser: async (req) => req.user });
  const dispatch = async (user, cursor = '') => {
    const res = { code: 200, status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } };
    await handler({ user, query: { cursor } }, res);
    return res;
  };
  assert.equal((await dispatch(null)).code, 401);
  assert.equal((await dispatch({ uid: 'x', role: 'guest' })).code, 403);
  assert.equal(queried, false);
  const result = await dispatch({ uid: 'sales', role: 'salesAgent' });
  assert.deepEqual(result.body.rows.map((row) => row.id), ['a']);
  assert.equal(result.body.rows[0].secret, undefined);
  assert.equal((await dispatch({ uid: 'admin', role: 'admin' })).body.rows.length, 2);
  assert.equal((await dispatch({ uid: 'admin', role: 'admin' }, 'invalid/cursor')).code, 400);
});

test('la generación manual acepta un brief parcial y omite un logo inutilizable sin inventar datos', () => {
  const current = lead({ companyName: 'Gran Valor', activity: 'Tequila artesanal', logoChoice: 'provided', palette: ['#285581', '#438DC0', '#EFE2CD'], images: [{ index: 0, role: 'logo', description: 'Captura', logoStatus: 'original_required' }] }, { images: [{ url: 'https://storage.googleapis.com/own/images/logo.jpg' }] });
  const input = manualSampleInput(current);
  assert.equal(input.companyInfo, 'Gran Valor');
  assert.equal(input.logoURL, '');
  assert.match(input.colorPreference, /#285581/);
  assert.deepEqual(input.keyItems, []);
  assert.equal(input.businessObjective, '');
  assert.equal(input.aiImageCount, 3);
  assert.equal(current.sampleAgent.brief.logoChoice, 'provided');
  assert.throws(() => manualSampleInput(lead()), /nombre o la actividad/);
  current.sampleAgent.images.push({ url: 'https://storage.googleapis.com/own/images/clean-logo.jpg' });
  current.sampleAgent.brief.images.push({ index: 1, role: 'logo', description: 'Nuevo logo legible', logoStatus: 'clean' });
  assert.equal(manualSampleInput(current).logoURL, current.sampleAgent.images[1].url);
});

function manualHarness(generateError = false) {
  let current = { ...lead({ companyName: 'Gran Valor', activity: 'Tequila' }, { status: 'paused' }), assignedTo: 'seller' };
  const requests = [], cancelled = [];
  let handler;
  const update = (_ref, patch) => { for (const [key, value] of Object.entries(patch)) { const parts = key.split('.'); let row = current; for (const part of parts.slice(0, -1)) row = row[part] ||= {}; row[parts.at(-1)] = value; } };
  const ref = { id: 'lead', get: async () => ({ exists: true, data: () => structuredClone(current) }) };
  let chain = Promise.resolve();
  const db = { collection: () => ({ doc: () => ref }), runTransaction(fn) { const result = chain.then(() => fn({ get: () => ref.get(), update })); chain = result.catch(() => {}); return result; } };
  registerSampleProgressRoutes({ get: () => {}, post: (_path, fn) => { handler = fn; } }, { db, readUser: async (req) => req.user,
    cancelSequences: async (id) => cancelled.push(id),
    generate: async (args) => { requests.push(args); if (generateError) throw Error('Error al solicitar la muestra'); return { negocioId: 'business', queued: true }; },
  });
  return { requests, cancelled, get current() { return current; }, set current(value) { current = value; }, async dispatch(user = { uid: 'seller', role: 'salesAgent' }) {
    const res = { code: 200, status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } };
    await handler({ user, body: { leadId: 'lead' } }, res);
    return res;
  } };
}

test('generar manualmente evita esperar la aprobación y bloquea clics duplicados', async () => {
  const h = manualHarness();
  const [first, second] = await Promise.all([h.dispatch(), h.dispatch()]);
  assert.deepEqual([first.code, second.code].sort(), [200, 409]);
  assert.equal(h.requests.length, 1);
  assert.deepEqual(h.cancelled, ['lead']);
  assert.equal(h.current.sampleAgent.manualRequestedBy, 'seller');
  assert.equal(h.current.sampleAgent.status, 'generating');
  assert.equal(h.current.sampleAgent.enabled, false);
  assert.equal(h.current.sampleAgent.deliveryPaused, false);
  assert.equal(h.current.sampleAgent.negocioId, 'business');
  assert.equal(h.requests[0].input.companyInfo, 'Gran Valor');
});

test('la generación manual respeta acceso, cancelación, atención humana y datos mínimos', async () => {
  const h = manualHarness();
  assert.equal((await h.dispatch(null)).code, 401);
  assert.equal((await h.dispatch({ uid: 'other', role: 'salesAgent' })).code, 403);
  h.current.humanControl = true;
  assert.equal((await h.dispatch()).code, 409);
  delete h.current.humanControl;
  h.current.stopSequences = true;
  assert.equal((await h.dispatch()).code, 409);
  delete h.current.stopSequences;
  h.current.sampleAgent.brief.activity = '';
  assert.equal((await h.dispatch()).code, 400);
  assert.equal(h.requests.length, 0);
});

test('si falla la solicitud manual deja un error visible y no reactiva el chat', async () => {
  const h = manualHarness(true);
  assert.equal((await h.dispatch()).code, 500);
  assert.equal(h.current.sampleAgent.status, 'error');
  assert.equal(h.current.sampleAgent.enabled, false);
  assert.equal(h.current.sampleAgent.deliveryPaused, true);
  assert.match(h.current.sampleAgent.error, /Error al solicitar/);
});
