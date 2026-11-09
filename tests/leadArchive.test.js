import test from 'node:test';
import assert from 'node:assert/strict';
import { createLeadArchiveService, inactivityArchiveReason, restoreArchivedLeadOnInbound, registerLeadArchiveRoutes } from '../services/leadArchive.js';
import { shouldBlockSequenceByLeadContext } from '../utils/sequenceTriggerGuards.js';

const NOW = Date.parse('2026-11-09T12:00:00-06:00'), OLD = new Date(NOW - 40 * 86400000);
const admin = { uid: 'admin', role: 'superAdmin', active: true };
const oldLead = extra => ({ nombre: 'Negocio', telefono: '528112345678', lastInboundAt: OLD, ...extra });
function setup(seed = {}) {
  let rows = new Map(Object.entries(structuredClone(seed))), writes = 0, time = NOW;
  const field = (row, key) => key.split('.').reduce((v, k) => v?.[k], row);
  const patch = (path, data) => {
    const row = structuredClone(rows.get(path) || {});
    for (const [key, value] of Object.entries(data)) {
      const parts = key.split('.'); let at = row;
      for (const p of parts.slice(0, -1)) at = at[p] ||= {};
      if (value === '__DELETE__') delete at[parts.at(-1)]; else at[parts.at(-1)] = structuredClone(value);
    }
    rows.set(path, row); writes++;
  };
  const snap = path => ({ id: path.split('/').at(-1), ref: doc(path), exists: rows.has(path), data: () => structuredClone(rows.get(path)) });
  const doc = path => ({ id: path.split('/').at(-1), path, get: async () => snap(path), collection: name => collection(`${path}/${name}`) });
  function collection(path, constraints = [], max = Infinity, order = '', dir = 'asc', cursor = '') {
    return { path, doc: id => doc(`${path}/${id}`), where: (key, op, value) => collection(path, [...constraints, [key, op, value]], max, order, dir, cursor),
      limit: count => collection(path, constraints, count, order, dir, cursor),
      orderBy: (key, direction = 'asc') => collection(path, constraints, max, key, direction, cursor),
      startAfter: id => collection(path, constraints, max, order, dir, id),
      get: async () => {
        let result = [...rows.keys()].filter(key => key.startsWith(`${path}/`) && key.split('/').length === path.split('/').length + 1)
          .filter(key => constraints.every(([f, op, v]) => op === '==' && field(rows.get(key), f) === v));
        if (order) result.sort((a, b) => {
          const av = order === '__name__' ? a.split('/').at(-1) : field(rows.get(a), order), bv = order === '__name__' ? b.split('/').at(-1) : field(rows.get(b), order);
          return (av < bv ? -1 : av > bv ? 1 : 0) * (dir === 'desc' ? -1 : 1);
        });
        if (cursor) result = result.filter(key => key.split('/').at(-1) > cursor);
        const docs = result.slice(0, max).map(snap); return { docs, size: docs.length, empty: !docs.length };
      } };
  }
  let chain = Promise.resolve();
  const db = { collection, runTransaction: callback => {
    const next = chain.then(async () => {
      const before = structuredClone([...rows]);
      try { return await callback({ get: ref => ref.get(), update: (ref, data) => patch(ref.path, data),
        set: (ref, data) => { rows.set(ref.path, structuredClone(data)); writes++; }, delete: ref => { rows.delete(ref.path); writes++; } }); }
      catch (error) { rows = new Map(before); throw error; }
    }); chain = next.catch(() => {}); return next;
  } };
  const service = createLeadArchiveService({ db, FieldValue: { delete: () => '__DELETE__' }, now: () => time, secret: 'test-secret' });
  return { db, service, get: path => rows.get(path), put: (path, data) => rows.set(path, structuredClone(data)), writes: () => writes, advance: ms => { time += ms; } };
}

test('vista previa solo lectura; un envío reciente del bot no reinicia los 30 días', async () => {
  const h = setup({ 'leads/old': oldLead({ lastMessageAt: new Date(NOW) }), 'leads/recent': oldLead({ lastInboundAt: new Date(NOW) }) });
  const preview = await h.service.preview({}, admin);
  assert.deepEqual(preview.candidates.map(r => r.id), ['old']); assert.equal(h.writes(), 0);
});
test('verifica el historial real aunque lastInboundAt sea antiguo; fechas desconocidas quedan fuera', async () => {
  const h = setup({ 'leads/old': oldLead(), 'leads/old/messages/reply': { sender: 'lead', timestamp: new Date(NOW) }, 'leads/unknown': {} });
  const preview = await h.service.preview({}, admin);
  assert.equal(preview.candidates.length, 0); assert.equal(preview.excluded['Sin fecha de respuesta confiable'], 1);
});
test('recupera la última respuesta de leads antiguos sin lastInboundAt', async () => {
  const h = setup({ 'leads/old': { nombre: 'Viejo' }, 'leads/old/messages/reply': { sender: 'lead', timestamp: OLD } });
  assert.equal((await h.service.preview({}, admin)).candidates.length, 1);
});
test('protege clientes, pagos, atención humana, muestras y seguimientos', () => {
  for (const extra of [{ estado: 'compro' }, { planDeposit: { status: 'paid' } }, { humanControl: true },
    { sampleAgent: { status: 'generating' } }, { followUp: { status: 'pending' } }, { unreadCount: 1 }]) {
    assert.ok(inactivityArchiveReason(oldLead(extra), { now: NOW }));
  }
});
test('comprueba tareas, recordatorios, negocios y pagos relacionados antes de archivar', async () => {
  for (const [path, data] of [
    ['tasks/t', { leadId: 'old', status: 'pendiente' }], ['tasks/t', { leadPhone: '528112345678', status: 'pendiente' }],
    ['PlanDepositRequests/p', { leadId: 'old', status: 'pending' }], ['Negocios/b', { leadId: 'old', status: 'Activo' }],
    ['Negocios/b', { leadId: 'old', aiSampleJob: { state: 'queued' } }], ['sequenceQueue/q', { leadId: 'old', status: 'pending', jobType: 'reminder' }],
  ]) {
    const h = setup({ 'leads/old': oldLead(), [path]: data });
    assert.equal((await h.service.preview({}, admin)).candidates.length, 0, path);
  }
});
test('archivado detiene las consultas de trabajo, cancela jobs e inbox y conserva historial', async () => {
  const h = setup({ 'leads/old': oldLead({ sampleAgent: { enabled: true, version: 'v1' }, secuenciasActivas: [{ trigger: 'WebPromo' }],
    sequenceRecoveredReason: 'missing_destination_resolved', sequenceBlockedReason: 'missing_destination', sequencePausedReason: 'sales_queue', nextSequenceRunAt: OLD }),
    'leads/old/messages/m': { sender: 'lead', timestamp: OLD, content: 'Conservar' },
    'leads/old/sampleAgentInbox/m': { state: 'pending' }, 'WelcomeRecoveryJobs/old': {}, 'ClosingJobs/old': {},
    'sequenceQueue/q': { leadId: 'old', status: 'pending' },
    'sequenceQueue/sent': { leadId: 'old', status: 'sent', jobType: 'reminder' } });
  const { candidates } = await h.service.preview({}, admin);
  const result = await h.service.commit({ tokens: [candidates[0].token] }, admin);
  assert.equal(result.results[0].archived, true);
  const lead = h.get('leads/old');
  assert.equal(lead.sampleAgent.enabled, false); assert.equal(lead.archived, true); assert.equal(lead.secuenciasActivas, null);
  for (const key of ['nextSequenceRunAt', 'sequenceRecoveredReason', 'sequenceBlockedReason', 'sequencePausedReason']) assert.equal(key in lead, false);
  assert.equal(h.get('leads/old/messages/m').content, 'Conservar'); assert.equal(h.get('sequenceQueue/q').status, 'canceled');
  assert.equal(h.get('sequenceQueue/sent').status, 'sent');
  assert.equal(h.get('leads/old/sampleAgentInbox/m').state, 'archived'); assert.equal(h.get('ClosingJobs/old'), undefined);
  assert.ok(h.get(`leads/old/archiveHistory/${lead.archiveMeta.historyId}`));
});
test('una respuesta, tarea o pago entre vista previa y ejecución evita el archivado', async () => {
  for (const [path, data] of [['leads/old/messages/new', { sender: 'lead', timestamp: new Date(NOW) }],
    ['tasks/new', { leadId: 'old', status: 'pending' }], ['PlanDepositRequests/new', { leadId: 'old', status: 'paid' }]]) {
    const h = setup({ 'leads/old': oldLead() }); const preview = await h.service.preview({}, admin);
    h.put(path, data); const result = await h.service.commit({ tokens: [preview.candidates[0].token] }, admin);
    assert.equal(result.results[0].archived, false); assert.equal(h.get('leads/old').archived, undefined);
  }
});
test('tokens falsificados o vencidos no autorizan cambios', async () => {
  const h = setup({ 'leads/old': oldLead() }); const preview = await h.service.preview({}, admin);
  await assert.rejects(h.service.commit({ tokens: [preview.candidates[0].token + 'x'] }, admin));
  h.advance(31 * 60000); await assert.rejects(h.service.commit({ tokens: [preview.candidates[0].token] }, admin)); assert.equal(h.writes(), 0);
});
test('solo administradores activos pueden ejecutar lotes; manual respeta al propietario', async () => {
  const h = setup({ 'leads/old': oldLead() });
  for (const user of [null, { uid: 'seller', role: 'salesAgent' }, { ...admin, active: false }]) await assert.rejects(h.service.preview({}, user));
  await assert.rejects(h.service.archiveOne('old', { uid: 'other', role: 'salesAgent' }, { manual: true })); assert.equal(h.writes(), 0);
});
test('paginación recorre todos los contactos sin repetir los primeros 100', async () => {
  const seed = Object.fromEntries(Array.from({ length: 103 }, (_, n) => [`leads/id${String(n).padStart(3, '0')}`, oldLead()]));
  const h = setup(seed); const page1 = await h.service.preview({}, admin), page2 = await h.service.preview({ cursor: page1.nextCursor }, admin);
  assert.equal(page1.candidates.length, 100); assert.equal(page2.candidates.length, 3); assert.equal(page2.nextCursor, null);
  assert.equal(new Set([...page1.candidates, ...page2.candidates].map(r => r.id)).size, 103);
});
test('desarchivar nunca revive secuencias ni agente y conserva paros manuales previos', async () => {
  const h = setup({ 'leads/old': oldLead({ stopSequences: true }) });
  await h.service.archiveOne('old', admin, { manual: true }); await h.service.restore('old', admin);
  const lead = h.get('leads/old'); assert.equal(lead.archived, false); assert.equal(lead.sampleAgent.enabled, false);
  assert.equal(lead.humanControl, true); assert.equal(lead.seqPaused, true); assert.equal(lead.stopSequences, true); assert.equal(lead.secuenciasActivas, null);
});
test('mensaje entrante nuevo restaura para atención manual; eco antiguo o duplicado no', async () => {
  const h = setup({ 'leads/old': oldLead() }); await h.service.archiveOne('old', admin, { manual: true });
  const leadRef = h.db.collection('leads').doc('old');
  assert.equal(await restoreArchivedLeadOnInbound({ db: h.db, leadRef, messageId: 'older', receivedAt: OLD }), false);
  h.put('leads/old/messages/duplicate', { sender: 'lead', timestamp: new Date(NOW + 1) });
  assert.equal(await restoreArchivedLeadOnInbound({ db: h.db, leadRef, messageId: 'duplicate', receivedAt: new Date(NOW + 1) }), false);
  assert.equal(await restoreArchivedLeadOnInbound({ db: h.db, leadRef, messageId: 'fresh', receivedAt: new Date(NOW + 2) }), true);
  assert.equal(h.get('leads/old').archived, false); assert.equal(h.get('leads/old').sampleAgent.enabled, false); assert.equal(h.get('leads/old').humanControl, true);
});
test('la ruta rechaza peticiones anónimas sin ejecutar el servicio', async () => {
  const routes = new Map(); let called = false, code, result;
  registerLeadArchiveRoutes({ post: (path, handler) => routes.set(path, handler) }, {
    service: { preview: () => { called = true; } }, readUser: async () => null });
  const res = { status: n => { code = n; return res; }, json: body => { result = body; } };
  await routes.get('/api/crm/lead-archive/preview')({ body: {} }, res);
  assert.equal(code, 401); assert.equal(called, false); assert.ok(result.error);
});
test('todas las marcas de archivado bloquean cualquier familia de secuencias', () => {
  for (const lead of [{ archived: true }, { isArchived: true }, { archivedAt: OLD }]) {
    for (const trigger of ['WebPromo', 'PlanRedes', 'NuevoLead', 'custom']) {
      assert.deepEqual(shouldBlockSequenceByLeadContext(lead, trigger), { blocked: true, reason: 'lead_archived' });
    }
  }
});
test('no archiva si hay un bloqueo de procesamiento reciente y acepta tareas completadas', async () => {
  const h = setup({ 'leads/locked': oldLead({ sequenceLock: new Date(NOW - 60000) }),
    'leads/done': oldLead(), 'tasks/t': { leadId: 'done', status: 'completada' } });
  assert.deepEqual((await h.service.preview({}, admin)).candidates.map(r => r.id), ['done']);
});
test('limita el tamaño del lote y las selecciones duplicadas son idempotentes', async () => {
  const h = setup({ 'leads/old': oldLead() }); const { candidates } = await h.service.preview({}, admin);
  await assert.rejects(h.service.commit({ tokens: Array(26).fill(candidates[0].token) }, admin));
  const result = await h.service.commit({ tokens: [candidates[0].token, candidates[0].token] }, admin);
  assert.equal(result.results.length, 1); assert.equal(result.results[0].archived, true);
  const repeat = await h.service.commit({ tokens: [candidates[0].token] }, admin); assert.equal(repeat.results[0].reason, 'Ya archivado');
});
