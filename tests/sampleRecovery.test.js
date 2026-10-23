import test from 'node:test';
import assert from 'node:assert/strict';
import { registerSampleRecoveryRoutes, previousSampleFacts, assertSampleRecoveryAllowed, buildSampleRecoveryPreview, recoverySalesQualification } from '../services/sampleRecovery.js';
import { hasDeliveredSample } from '../services/sampleAgentReactivation.js';
import { normalizeConversationBrief } from '../services/sampleConversationAgent.js';

const user = { uid: 'admin', role: 'admin' };
const complete = () => normalizeConversationBrief({ companyName: 'Gran Valor', activity: 'Tequila artesanal', objective: 'Pedidos', offerings: ['Blanco'], customers: 'Público general', differentiators: '100% agave', serviceArea: 'Amatitán', logoChoice: 'none', colorPreference: 'Azul', photosChoice: 'none' });

function harness() {
  const rows = new Map(), routes = new Map(), sends = [], jobs = [], audios = [], cancellations = [];
  const snapshot = (ref) => ({ exists: rows.has(ref.path), ref, id: ref.id, data: () => structuredClone(rows.get(ref.path)) });
  const update = (ref, fields) => {
    const data = structuredClone(rows.get(ref.path) || {});
    for (const [key, value] of Object.entries(fields)) { const parts = key.split('.'); let target = data; for (const part of parts.slice(0, -1)) target = target[part] ||= {}; target[parts.at(-1)] = structuredClone(value); }
    rows.set(ref.path, data);
  };
  const doc = (path) => ({ path, id: path.split('/').at(-1), get: async () => snapshot(doc(path)), set: async (data) => rows.set(path, structuredClone(data)), update: async (patch) => update(doc(path), patch), collection: (name) => collection(`${path}/${name}`) });
  const collection = (path, filters = [], limit = Infinity) => ({ doc: (id) => doc(`${path}/${id}`),
    orderBy: () => collection(path, filters, limit), limit: (count) => collection(path, filters, count),
    where: (key, op, value) => collection(path, [...filters, [key, op, value]], limit),
    get: async () => ({ docs: [...rows.keys()].filter((key) => key.startsWith(`${path}/`) && key.split('/').length === path.split('/').length + 1).map((key) => snapshot(doc(key))).filter((snap) => filters.every(([key, op, value]) => op === '>=' ? snap.data()[key] >= value : op === '<=' ? snap.data()[key] <= value : snap.data()[key] === value)).slice(0, limit) }),
  });
  let chain = Promise.resolve();
  const db = { collection, runTransaction(fn) { const result = chain.then(() => fn({ get: async (ref) => snapshot(ref), update })); chain = result.catch(() => {}); return result; } };
  rows.set('leads/lead', { nombre: 'Sergio', telefono: '5213323193742', sampleLinkSentAt: new Date(), sampleLinkSentUrl: 'https://negociosweb.mx/gran-valor', lastMessageAt: new Date(), sampleAgent: { status: 'completed', version: 'old', images: [], brief: complete() } });
  rows.set('leads/lead/messages/1', { sender: 'lead', content: 'Quiero mejorar el diseño', mediaType: 'text', timestamp: new Date() });
  rows.set('sampleAgentAudios/reactivation', { enabled: true, script: 'Tenemos un nuevo diseño. ¿Quieres verlo?', storagePath: 'sample-agent-audio/reactivation/recording.ogg' });
  let business = { companyInfo: 'Gran Valor', businessStory: 'Tequila artesanal' };
  registerSampleRecoveryRoutes({ post: (path, fn) => routes.set(path.split('/').at(-1), fn) }, { db, readUser: async (req) => req.user, readBusiness: async () => business,
    extract: async () => ({ brief: complete(), action: 'confirm' }),
    sendAudio: async (args) => { assert.equal(await args.isCurrent(), true); audios.push(args); return true; },
    send: async (...args) => sends.push(args), generate: async (job) => { jobs.push(job); return { negocioId: 'business', queued: true }; }, cancelSequences: async (id) => cancellations.push(id),
  });
  return { rows, sends, jobs, audios, cancellations, setBusiness: (value) => { business = value; },
    async call(action, body = {}, as = user) {
      const res = { statusCode: 200, status(code) { this.statusCode = code; return this; }, json(data) { this.data = data; return this; } };
      await routes.get(action)({ body, user: as }, res);
      return res;
    },
  };
}

test('permite recuperar una muestra anterior pero respeta cancelaciones, compras y permisos', () => {
  assert.doesNotThrow(() => assertSampleRecoveryAllowed(user, { sampleAgent: { status: 'completed' } }));
  for (const lead of [{ stopSequences: true }, { humanControl: true }, { estado: 'cliente' }, { sampleAgent: { status: 'generating' } }]) assert.throws(() => assertSampleRecoveryAllowed(user, lead));
  assert.throws(() => assertSampleRecoveryAllowed({ uid: 'other', role: 'salesAgent' }, { assignedTo: 'owner' }), /acceso/);
});

test('la lista expone la calificación guardada y distingue cero de sin evaluar', async () => {
  assert.equal(recoverySalesQualification({}).score, null);
  assert.equal(recoverySalesQualification({ salesState: { leadScore: 0 } }).score, 0);
  assert.equal(recoverySalesQualification({ salesState: { leadScore: '' } }).score, null);
  assert.equal(recoverySalesQualification({ salesState: { leadScore: 'no válido' } }).score, null);
  const h = harness();
  const evaluatedAt = new Date('2026-10-23T15:00:00Z');
  h.rows.get('leads/lead').salesState = { leadScore: 87, qualification: { readyForSales: true, qualificationStatus: 'ready_for_sales' }, updatedAt: evaluatedAt };
  const result = await h.call('candidates', { from: '2020-01-01', to: '2030-01-01', kind: 'all' });
  assert.equal(result.statusCode, 200);
  assert.deepEqual(result.data.leads[0].salesQualification, { score: 87, readyForSales: true, status: 'ready_for_sales', evaluatedAt: evaluatedAt.getTime() });
});

test('recupera datos y material de una muestra anterior sin convertir HTML generado en hechos', async () => {
  const business = { companyInfo: 'Taller antiguo', businessStory: 'Mecánica', keyItems: ['Afinación'], logoURL: 'https://storage.googleapis.com/own/logo.png', photoURLs: ['https://storage.googleapis.com/own/car.jpg'], schema: { html: 'Reseñas inventadas' } };
  const preview = await buildSampleRecoveryPreview({ lead: {}, business, messages: [], extract: async ({ brief, images, message }) => {
    assert.equal(brief.companyName, 'Taller antiguo'); assert.equal(brief.activity, 'Mecánica');
    assert.equal(images.length, 2); assert.equal(message.previousSample.schema, undefined);
    return { brief: { ...complete(), companyName: brief.companyName, activity: brief.activity }, action: 'confirm' };
  } });
  assert.equal(preview.canGenerate, true);
  assert.equal(previousSampleFacts({}, business).schema, undefined);
});

test('preparar no envía; ejecutar usa solo el audio y la promoción elegidos y conserva el contexto', async () => {
  const h = harness();
  const prepared = await h.call('preview', { leadId: 'lead', mode: 'audio', promotion: 'Oferta: $990 al año, según condiciones compartidas.' });
  assert.equal(prepared.statusCode, 200);
  assert.equal(h.audios.length, 0); assert.equal(h.sends.length, 0); assert.equal(h.jobs.length, 0);
  const result = await h.call('execute', { leadId: 'lead', draftId: prepared.data.draftId });
  assert.equal(result.statusCode, 200);
  assert.equal(h.audios.length, 1); assert.equal(h.audios[0].moment, 'reactivation');
  assert.deepEqual(h.sends, [['lead', 'Oferta: $990 al año, según condiciones compartidas.']]);
  assert.equal(h.jobs.length, 0);
  const agent = h.rows.get('leads/lead').sampleAgent;
  assert.equal(agent.enabled, true); assert.equal(agent.brief.companyName, 'Gran Valor');
  assert.match(agent.recoveryOffer, /Tenemos un nuevo diseño/);
  assert.deepEqual(h.cancellations, ['lead']);
  assert.equal(hasDeliveredSample(h.rows.get('leads/lead')), false);
  await h.call('execute', { leadId: 'lead', draftId: prepared.data.draftId });
  assert.equal(h.audios.length, 1); assert.equal(h.sends.length, 1);
});

test('renovar una muestra entregada usa nueva generación, tres imágenes y dirección de diseño sin contactar antes', async () => {
  const h = harness();
  const prepared = await h.call('preview', { leadId: 'lead', mode: 'renew', designDirection: 'Destaca el tequila blanco, hero más visual.', deliveryMessage: 'Preparamos una nueva versión para {{negocio}}.' });
  assert.equal(prepared.statusCode, 200);
  const result = await h.call('execute', { leadId: 'lead', draftId: prepared.data.draftId });
  assert.equal(result.statusCode, 200); assert.equal(h.jobs.length, 1);
  assert.equal(h.jobs[0].input.aiImageCount, 3);
  assert.match(h.jobs[0].input.regenerationId, /^[a-f0-9-]{36}$/);
  assert.match(h.jobs[0].input.designDirection, /tequila blanco/);
  assert.equal(h.jobs[0].lead.sampleAgent.deliveryMessage, 'Preparamos una nueva versión para {{negocio}}.');
  assert.equal(h.audios.length, 0); assert.equal(h.sends.length, 0);
  assert.equal(result.data.row.status, 'generating');
  assert.equal(h.rows.get('leads/lead').sampleAgent.enabled, false);
  assert.equal((await h.call('execute', { leadId: 'lead', draftId: prepared.data.draftId })).statusCode, 409);
  assert.equal(h.jobs.length, 1);
});

test('rechaza vistas previas desactualizadas, otro usuario y cambios en el audio', async () => {
  const h = harness();
  assert.equal((await h.call('preview', { leadId: 'lead' }, null)).statusCode, 401);
  const prepared = await h.call('preview', { leadId: 'lead', mode: 'audio' });
  assert.equal((await h.call('execute', { leadId: 'lead', draftId: prepared.data.draftId }, { uid: 'other', role: 'admin' })).statusCode, 404);
  h.setBusiness({ companyInfo: 'Cambió la muestra' });
  assert.equal((await h.call('execute', { leadId: 'lead', draftId: prepared.data.draftId })).statusCode, 409);
  assert.equal(h.audios.length, 0);
  const updated = await h.call('preview', { leadId: 'lead', mode: 'audio' });
  h.rows.get('sampleAgentAudios/reactivation').storagePath = 'changed';
  assert.equal((await h.call('execute', { leadId: 'lead', draftId: updated.data.draftId })).statusCode, 409);
  assert.equal(h.audios.length, 0);
  assert.equal(h.rows.get('leads/lead').sampleAgent.enabled, false);
});
