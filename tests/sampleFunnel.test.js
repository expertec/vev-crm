import test from 'node:test';
import assert from 'node:assert/strict';
import { classifySampleFunnel, sampleInterestEvidence, sampleDeliveryInHistory, sortSampleFunnel, registerSampleFunnelRoutes, recordProposalDelivery, recordProposalOpen } from '../services/sampleFunnel.js';

const NOW = Date.parse('2026-10-23T18:00:00Z'), DAY = 86400_000;
const customer = (text = 'Sí, quiero una muestra de mi negocio', at = NOW - 2 * DAY) => ({ sender: 'lead', content: text, timestamp: new Date(at), mediaType: 'text' });
const complete = { companyName: 'Taller Norte', activity: 'Mecánica', objective: 'Consultas', offerings: ['Afinación'], customers: 'Automovilistas', differentiators: 'No definidos', serviceArea: 'Mante', logoChoice: 'none', colorPreference: 'Verde', photosChoice: 'none' };
const lead = (patch = {}) => ({ nombre: 'Sergio', telefono: '521831', assignedTo: 'seller', lastMessageAt: new Date(NOW), ...patch });

test('la plantilla de campaña y el envío de formulario no califican por sí solos como interés real', () => {
  const generic = customer('Hola, tengo un negocio y quiero información sobre la página web de $990 al año.');
  const data = lead({ sampleAgent: { status: 'collecting', brief: {} }, salesState: { intent: 'wants_sample' }, formLinkSentAt: new Date(NOW) });
  assert.equal(classifySampleFunnel('a', data, [generic], NOW), null);
  assert.equal(classifySampleFunnel('a', data, [{ ...generic, timestamp: new Date(NOW - 3600_000) }], NOW).stage, 'process');
  assert.equal(sampleInterestEvidence(lead({ webSales: { sample: { formSentAt: new Date(NOW) } } }), []), null);
});

test('recupera interesados antiguos y nuestros seguimientos no reinician su antigüedad', () => {
  const messages = [customer(), { sender: 'business', content: '¿Seguimos con tu muestra?', timestamp: new Date(NOW) }];
  const row = classifySampleFunnel('a', lead(), messages, NOW);
  assert.equal(row.stage, 'interested');
  assert.equal(row.lastCustomerAt, NOW - 2 * DAY);
  assert.match(row.evidence.text, /quiero una muestra/);
  assert.equal(classifySampleFunnel('a', lead(), [customer(undefined, NOW - DAY)], NOW).stage, 'interested');
  assert.equal(classifySampleFunnel('a', lead(), [customer(undefined, NOW - DAY + 1)], NOW).stage, 'process');
});

test('considera consentimiento y archivos para la muestra sin confundir otros servicios', () => {
  const prompt = { sender: 'business', content: '¿Quieres que preparemos una muestra para tu negocio?', timestamp: new Date(NOW - 3 * DAY) };
  assert.ok(sampleInterestEvidence({}, [prompt, customer('Sí, claro')]));
  const files = [{ sender: 'business', content: 'Envíame tu logo y fotos para la página web.' }, { ...customer(''), mediaType: 'image' }];
  assert.match(sampleInterestEvidence({}, files).reason, /material/);
  assert.equal(sampleInterestEvidence({}, [{ sender: 'business', content: 'Envíame tu recibo de pago.' }, { ...customer(''), mediaType: 'image' }]), null);
});

test('las entregadas pasan a propuestas aunque se acaben de enviar, abrir o responder', () => {
  const data = lead({ sampleLinkSentAt: new Date(NOW - 2 * DAY), sampleLinkSentUrl: 'https://negociosweb.mx/taller' });
  const old = [customer(undefined, NOW - 3 * DAY)];
  const proposal = classifySampleFunnel('a', data, old, NOW);
  assert.equal(proposal.stage, 'proposals');
  assert.equal(proposal.pending, 'Sin apertura registrada');
  assert.equal(proposal.canRenew, true);
  assert.equal(proposal.canResume, false);
  assert.equal(classifySampleFunnel('a', { ...data, sampleLinkSentAt: new Date(NOW) }, old, NOW).stage, 'proposals');
  assert.equal(classifySampleFunnel('a', { ...data, linkOpenedAt: new Date(NOW) }, old, NOW).stage, 'proposals');
  assert.equal(classifySampleFunnel('a', data, [customer('Gracias, voy a revisarla', NOW)], NOW).stage, 'proposals');
  assert.equal(classifySampleFunnel('a', { ...data, sampleAgent: { status: 'generating' } }, old, NOW).stage, 'process');
  assert.equal(classifySampleFunnel('a', { ...data, sampleLinkSentUrl: 'https://app.negociosweb.mx/muestra/521831' }, old, NOW).stage, 'interested');
  assert.equal(classifySampleFunnel('a', lead({ sampleAgent: { status: 'generating' } }), [], NOW).stage, 'process');
  assert.equal(classifySampleFunnel('a', lead({ sampleAgent: { status: 'error', negocioId: 'failed-business', error: 'Sin saldo' } }), old, NOW).stage, 'process');
});

test('una muestra antigua enviada en el chat califica como entrega, pero el enlace del formulario no', () => {
  const messages = [customer(), { sender: 'business', content: 'Te comparto el enlace de tu muestra.', timestamp: new Date(NOW - 2 * DAY) }, { sender: 'business', content: 'https://negociosweb.mx/taller-norte', timestamp: new Date(NOW - 2 * DAY) }];
  assert.equal(classifySampleFunnel('old', lead(), messages, NOW).stage, 'proposals');
  assert.equal(sampleDeliveryInHistory(messages).url, 'https://negociosweb.mx/taller-norte');
  messages.at(-1).content = 'https://app.negociosweb.mx/muestra/521831';
  assert.equal(sampleDeliveryInHistory(messages), null);
  messages.at(-1).content = 'https://negociosweb.mx/precios';
  assert.equal(sampleDeliveryInHistory(messages), null);
});

test('excluye ventas y cancelaciones y conserva bloqueos humanos al decidir las acciones', () => {
  for (const patch of [{ estado: 'compro' }, { webSales: { stage: 'won' } }, { webSales: { closing: { depositPaidAt: new Date() } } }, { stopSequences: true }, { archived: true }]) {
    assert.equal(classifySampleFunnel('a', lead(patch), [customer()], NOW), null);
  }
  assert.equal(classifySampleFunnel('a', lead(), [customer(), customer('No me contactes', NOW - DAY), customer('Gracias', NOW - DAY + 1)], NOW), null);
  const controlled = classifySampleFunnel('a', lead({ humanControl: true, sampleAgent: { status: 'collecting', brief: complete } }), [customer()], NOW);
  assert.equal(controlled.canGenerate, false); assert.equal(controlled.canResume, false);
});

test('una recuperación recién enviada vuelve al proceso sin falsificar la última respuesta del cliente', () => {
  const data = lead({ sampleAgent: { status: 'collecting', regenerationRequestedAt: new Date(NOW - 1000), brief: complete } });
  const row = classifySampleFunnel('a', data, [customer()], NOW);
  assert.equal(row.stage, 'process');
  assert.equal(row.lastCustomerAt, NOW - 2 * DAY);
});

test('prioriza el día de interacción y después el score; los sin evaluar no se inventan ni se excluyen', () => {
  const rows = [
    { id: 'old', lastCustomerAt: NOW - DAY, qualification: { score: 99 } },
    { id: 'low', lastCustomerAt: NOW, qualification: { score: 10 } },
    { id: 'best', lastCustomerAt: NOW - 1000, qualification: { score: 90 } },
    { id: 'unknown', lastCustomerAt: NOW, qualification: { score: null } },
  ];
  assert.deepEqual(sortSampleFunnel(rows).map((row) => row.id), ['best', 'low', 'unknown', 'old']);
  assert.equal(sortSampleFunnel(rows, 'score')[0].id, 'old');
});

function harness(extra = {}) {
  const rows = new Map(), routes = new Map(), reactivations = [], renewals = [], cancellations = [];
  const snapshot = (ref) => ({ id: ref.id, ref, exists: rows.has(ref.path), data: () => structuredClone(rows.get(ref.path)) });
  const update = (ref, patch) => { const data = structuredClone(rows.get(ref.path) || {}); for (const [key, value] of Object.entries(patch)) { const parts = key.split('.'); let target = data; for (const part of parts.slice(0, -1)) target = target[part] ||= {}; target[parts.at(-1)] = structuredClone(value); } rows.set(ref.path, data); };
  const doc = (path) => ({ path, id: path.split('/').at(-1), get: async () => snapshot(doc(path)), update: async (patch) => update(doc(path), patch), set: async (data) => rows.set(path, structuredClone(data)), collection: (name) => collection(`${path}/${name}`) });
  const collection = (path, ordering = '', direction = 'desc', count = Infinity) => ({
    doc: (id) => doc(`${path}/${id}`), where: () => collection(path, ordering, direction, count), orderBy: (field, dir) => collection(path, field, dir, count), limit: (limit) => collection(path, ordering, direction, limit),
    get: async () => { let docs = [...rows.keys()].filter((key) => key.startsWith(`${path}/`) && key.split('/').length === path.split('/').length + 1).map((key) => snapshot(doc(key))); if (ordering) docs.sort((a, b) => (+new Date(a.data()[ordering]) - +new Date(b.data()[ordering])) * (direction === 'desc' ? -1 : 1)); return { docs: docs.slice(0, count) }; },
  });
  const db = { collection, runTransaction: async (fn) => fn({ get: async (ref) => snapshot(ref), update }) };
  registerSampleFunnelRoutes({ get: (url, fn) => routes.set(`get ${url.split('/').at(-1)}`, fn), post: (url, fn) => routes.set(`post ${url.split('/').at(-1)}`, fn) }, { db, readUser: async (req) => req.user,
    reactivate: async (...args) => { reactivations.push(args); return { success: true }; }, renew: async (...args) => { renewals.push(args); return { success: true }; }, cancelSequences: async (id) => cancellations.push(id), ...extra,
  });
  const add = (id, data = {}) => { rows.set(`leads/${id}`, lead(data)); rows.set(`leads/${id}/messages/1`, customer('Quiero una muestra', Date.now() - 2 * DAY)); };
  return { rows, db, reactivations, renewals, cancellations, add, async call(method, name, data = {}, user = { uid: 'seller', role: 'salesAgent' }) {
    const res = { code: 200, status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; }, set() {} };
    await routes.get(`${method} ${name}`)({ user, query: data, body: data }, res); return res;
  } };
}

test('la tabla comprueba acceso y pagina después de clasificar, filtrar y ordenar', async () => {
  const h = harness();
  for (let index = 0; index < 30; index++) h.add(`own-${index}`, { salesState: { leadScore: index } });
  h.add('foreign', { assignedTo: 'other' });
  assert.equal((await h.call('get', 'list', {}, null)).code, 401);
  const result = await h.call('get', 'list', { stage: 'interested', order: 'score' });
  assert.equal(result.body.total, 30); assert.equal(result.body.counts.interested, 30); assert.equal(result.body.rows.length, 25); assert.equal(result.body.hasMore, true);
  assert.equal(result.body.rows[0].id, 'own-29'); assert.equal(result.body.rows.some((row) => row.id === 'foreign'), false);
  const last = await h.call('get', 'list', { stage: 'interested', order: 'score', page: 1 });
  assert.equal(last.body.rows.length, 5); assert.equal(last.body.hasMore, false);
});

test('las pestañas y sus contadores no mezclan muestras entregadas, activas e interesados antiguos', async () => {
  const h = harness();
  h.add('active', { sampleAgent: { status: 'generating' } });
  h.add('cold');
  h.add('delivered', { sampleLinkSentAt: new Date(), sampleLinkSentUrl: 'https://negociosweb.mx/taller', linkOpenedAt: new Date() });
  for (const [stage, expected] of [['process', 'active'], ['interested', 'cold'], ['proposals', 'delivered']]) {
    const result = await h.call('get', 'list', { stage });
    assert.deepEqual(result.body.rows.map((row) => row.id), [expected]);
    assert.deepEqual(result.body.counts, { process: 1, interested: 1, proposals: 1 });
    assert.equal(result.body.total, 1);
  }
});

test('recordar muestra permite revisar el texto, conserva los datos y evita otro envío en 24 horas', async () => {
  const sent = [], h = harness({ send: async (...args) => sent.push(args) });
  h.add('pending', { sampleAgent: { status: 'paused', enabled: false, brief: complete, images: [{ url: 'foto' }], confirmationHash: 'approval', history: [{ role: 'customer', text: 'Mis servicios' }] } });
  const preview = await h.call('get', 'reminder', { leadId: 'pending' });
  assert.equal(preview.code, 200); assert.match(preview.body.message, /Taller Norte/); assert.match(preview.body.message, /Te gustaría/); assert.equal(sent.length, 0);
  const result = await h.call('post', 'reminder', { leadId: 'pending', ...preview.body });
  assert.equal(result.code, 200); assert.equal(sent.length, 1);
  const agent = h.rows.get('leads/pending').sampleAgent;
  assert.deepEqual(agent.brief, complete); assert.deepEqual(agent.images, [{ url: 'foto' }]);
  assert.equal(agent.confirmationHash, 'approval'); assert.equal(agent.enabled, true);
  assert.equal(agent.history.at(-1).text, preview.body.message); assert.equal(+agent.leaseUntil, 0);
  assert.equal(h.cancellations.length, 1);
  assert.equal((await h.call('post', 'reminder', { leadId: 'pending', ...preview.body })).code, 409);
  assert.equal(sent.length, 1);
  const list = await h.call('get', 'list', { stage: 'interested' });
  assert.equal(list.body.rows[0].canRemind, false); assert.ok(list.body.rows[0].lastReminderAt);
});

test('recordatorio rechaza contexto cambiado, contactos recientes, entregados, pagados y atención humana', async () => {
  const sent = [], h = harness({ send: async (...args) => sent.push(args) });
  h.add('pending');
  const preview = await h.call('get', 'reminder', { leadId: 'pending' });
  h.rows.get('leads/pending').lastInboundAt = new Date();
  assert.equal((await h.call('post', 'reminder', { leadId: 'pending', ...preview.body })).code, 409);
  for (const [id, patch] of Object.entries({ human: { humanControl: true }, stopped: { stopSequences: true }, paid: { webSales: { stage: 'won' } }, delivered: { sampleLinkSentAt: new Date(), sampleLinkSentUrl: 'https://negociosweb.mx/taller' }, recent: { lastInboundAt: new Date() }, busy: { sampleAgent: { leaseUntil: new Date(Date.now() + 60_000) } } })) {
    h.add(id, patch);
    assert.equal((await h.call('get', 'reminder', { leadId: id })).code, 409, id);
    assert.equal((await h.call('post', 'reminder', { leadId: id, message: 'Hola', revision: '' })).code, 409, id);
  }
  h.add('foreign', { assignedTo: 'other' });
  assert.equal((await h.call('get', 'reminder', { leadId: 'foreign' })).code, 403);
  assert.equal(sent.length, 0);
});

test('recordatorio libera el contacto si falla WhatsApp y no marca un envío inexistente', async () => {
  const h = harness({ send: async () => { throw new Error('WhatsApp desconectado'); } });
  h.add('pending', { sampleAgent: { status: 'paused', enabled: false, brief: complete } });
  const preview = await h.call('get', 'reminder', { leadId: 'pending' });
  const result = await h.call('post', 'reminder', { leadId: 'pending', ...preview.body });
  assert.equal(result.code, 500);
  const data = h.rows.get('leads/pending');
  assert.equal(data.sampleReminder, undefined); assert.equal(data.sampleAgent.enabled, false);
  assert.equal(data.sampleAgent.status, 'paused'); assert.equal(+data.sampleAgent.leaseUntil, 0);
});

test('un envío de recordatorio en curso bloquea un segundo clic', async () => {
  let release, started;
  const sending = new Promise(resolve => { started = resolve; });
  const h = harness({ send: async () => { started(); await new Promise(resolve => { release = resolve; }); } });
  h.add('pending');
  const preview = await h.call('get', 'reminder', { leadId: 'pending' });
  const first = h.call('post', 'reminder', { leadId: 'pending', ...preview.body });
  await sending;
  assert.equal((await h.call('post', 'reminder', { leadId: 'pending', ...preview.body })).code, 409);
  release(); assert.equal((await first).code, 200);
});

test('configurar promociones requiere administrador y generar otra propuesta requiere entrega real', async () => {
  const h = harness(); h.add('a');
  assert.equal((await h.call('post', 'settings', { renewalMessage: 'Nueva propuesta' })).code, 403);
  assert.equal((await h.call('post', 'settings', { renewalMessage: 'Nueva propuesta para {{negocio}}', promotion: '$990 al año', applyPromotionToRenewal: true }, { uid: 'admin', role: 'admin' })).code, 200);
  assert.equal((await h.call('post', 'action', { leadId: 'a', action: 'renew' })).code, 409);
  h.rows.get('leads/a').sampleLinkSentAt = new Date(Date.now() - 2 * DAY); h.rows.get('leads/a').sampleLinkSentUrl = 'https://negociosweb.mx/taller';
  assert.equal((await h.call('post', 'action', { leadId: 'a', action: 'renew' })).code, 200);
  assert.equal(h.renewals.length, 1); assert.equal(h.renewals[0][2].promotion, '$990 al año');
  h.rows.get('leads/a').webSales = { stage: 'won' };
  assert.equal((await h.call('post', 'action', { leadId: 'a', action: 'renew' })).code, 409);
  assert.equal(h.renewals.length, 1);
});

test('la reactivación usa el audio configurado y la intervención manual pausa agente y secuencias', async () => {
  const h = harness(); h.add('a', { sampleAgent: { enabled: true, version: 'old', status: 'collecting', brief: complete } });
  assert.equal((await h.call('post', 'action', { leadId: 'a', action: 'reactivate' })).code, 200);
  assert.equal(h.reactivations.length, 1);
  assert.equal((await h.call('post', 'action', { leadId: 'a', action: 'takeover' }, { uid: 'other', role: 'salesAgent' })).code, 403);
  assert.equal((await h.call('post', 'action', { leadId: 'a', action: 'takeover' })).code, 200);
  const current = h.rows.get('leads/a');
  assert.equal(current.sampleAgent.enabled, false); assert.equal(current.sampleAgent.deliveryPaused, true); assert.equal(current.sampleAgent.status, 'paused'); assert.notEqual(current.sampleAgent.version, 'old');
  assert.equal(current.salesBrainMode, 'copilot'); assert.deepEqual(h.cancellations, ['a']);
});

test('la clasificación se puede corregir sin perder la conversación ni reabrir clientes pagados', async () => {
  const h = harness(); h.add('a', { sampleAgent: { enabled: true, status: 'collecting', brief: complete } });
  assert.equal((await h.call('post', 'action', { leadId: 'a', action: 'exclude' })).code, 200);
  assert.equal(h.rows.get('leads/a').sampleAgent.enabled, false);
  assert.equal(classifySampleFunnel('a', h.rows.get('leads/a'), [customer()]), null);
  assert.equal((await h.call('post', 'include', { identity: 'a' })).code, 200);
  assert.ok(classifySampleFunnel('a', h.rows.get('leads/a'), [customer()]));
  h.rows.get('leads/a').webSales = { stage: 'won' };
  assert.equal((await h.call('post', 'include', { identity: 'a' })).code, 409);
});

test('cada propuesta conserva su propio envío y apertura sin borrar el historial anterior', async () => {
  const h = harness(); h.add('a', { linkOpenedAt: new Date(NOW - 10 * DAY), sampleAgent: { regenerationId: 'new', regenerationRequestedAt: new Date(NOW - DAY), status: 'generating' }, sampleProposalCycle: { id: 'old', sentAt: new Date(NOW - 20 * DAY), openedAt: new Date(NOW - 10 * DAY) } });
  const ref = h.db.collection('leads').doc('a');
  await recordProposalOpen(h.db, ref, 'new');
  assert.equal(h.rows.get('leads/a').sampleProposalCycle.id, 'old');
  await recordProposalDelivery(h.db, ref, 'new');
  assert.equal(h.rows.get('leads/a').sampleProposalCycle.openedAt, null);
  const before = h.rows.get('leads/a').linkOpenedAt;
  await recordProposalOpen(h.db, ref, 'new');
  const opened = h.rows.get('leads/a').sampleProposalCycle.openedAt;
  assert.ok(opened);
  await recordProposalDelivery(h.db, ref, 'new');
  assert.deepEqual(h.rows.get('leads/a').sampleProposalCycle.openedAt, opened);
  assert.deepEqual(h.rows.get('leads/a').linkOpenedAt, before);
});

test('edición manual verifica acceso, revisión y estado; guarda sin enviar ni generar muestras', async () => {
  const h = harness();
  h.add('manual', { sampleAgent: { version: 'v1', status: 'collecting', enabled: true, brief: complete, images: [] } });
  const initial = await h.call('get', 'context', { leadId: 'manual' });
  assert.equal(initial.code, 200);
  assert.equal((await h.call('get', 'context', { leadId: 'manual' }, { uid: 'other', role: 'salesAgent' })).code, 403);
  const fields = { ...complete, companyName: 'Coroinox', withoutLogo: true, logoImageIndex: '' };
  const payload = { leadId: 'manual', revision: initial.body.revision, fields };
  assert.equal((await h.call('post', 'context', { ...payload, revision: 'stale' })).code, 409);
  assert.equal((await h.call('post', 'context', payload)).code, 200);
  const saved = h.rows.get('leads/manual').sampleAgent;
  assert.equal(saved.brief.companyName, 'Coroinox'); assert.equal(saved.enabled, false); assert.equal(saved.status, 'paused'); assert.equal(saved.manualContextUpdatedBy, 'seller');
  assert.equal(h.renewals.length, 0); assert.equal(h.reactivations.length, 0);
  assert.equal((await h.call('post', 'context', payload)).code, 409);
  for (const patch of [{ status: 'generating' }, { status: 'collecting', leaseUntil: new Date(Date.now() + 60000) }]) {
    Object.assign(h.rows.get('leads/manual').sampleAgent, patch);
    const current = await h.call('get', 'context', { leadId: 'manual' });
    assert.equal((await h.call('post', 'context', { ...payload, revision: current.body.revision })).code, 409);
  }
});


test('recorte manual toma solo la imagen del contacto, guarda el logo y conserva la fuente', async () => {
  const calls = [];
  const original = 'https://storage.googleapis.com/own/images/screenshot.jpg';
  const cropped = 'https://storage.googleapis.com/own/sample-logos/cropped.png';
  const h = harness({ prepareLogo: async (args) => { calls.push(args); return cropped; } });
  h.add('crop', { sampleAgent: { version: 'v1', status: 'paused', brief: complete, images: [{ url: original }] } });
  const initial = await h.call('get', 'context', { leadId: 'crop' });
  const payload = { leadId: 'crop', revision: initial.body.revision, fields: { ...complete, withoutLogo: false, logoImageIndex: '' }, logoCrop: { imageIndex: 0, crop: [0.1, 0.3, 0.8, 0.4], url: 'https://attacker.example/ignored.jpg' } };
  assert.equal((await h.call('post', 'context', payload, { uid: 'other', role: 'salesAgent' })).code, 403);
  assert.equal(calls.length, 0);
  assert.equal((await h.call('post', 'context', payload)).code, 200);
  assert.deepEqual(calls, [{ url: original, crop: payload.logoCrop.crop }]);
  const saved = h.rows.get('leads/crop').sampleAgent;
  assert.equal(saved.images[0].url, cropped); assert.equal(saved.images[0].sourceUrl, original);
  assert.equal(saved.brief.logoChoice, 'provided'); assert.equal(saved.brief.images[0].logoStatus, 'clean');
  assert.equal(saved.enabled, false); assert.equal(h.renewals.length, 0); assert.equal(h.reactivations.length, 0);
  assert.equal((await h.call('get', 'context', { leadId: 'crop' })).body.images[0].url, original);
});

test('el recorte manual recupera del chat el logo que quedó fuera de las primeras doce fotos', async () => {
  const source = 'https://storage.googleapis.com/own/images/lona.jpg';
  const cropped = 'https://storage.googleapis.com/own/sample-logos/lona.png';
  const h = harness({ prepareLogo: async ({ url }) => { assert.equal(url, source); return cropped; } });
  h.add('late', { sampleAgent: { status: 'paused', brief: { ...complete, images: [] }, images: Array.from({ length: 12 }, (_, i) => ({ url: `https://storage.googleapis.com/own/images/shoe${i}.jpg` })) } });
  h.rows.set('leads/late/messages/2', { sender: 'lead', mediaType: 'image', mediaUrl: source, timestamp: new Date() });
  const initial = await h.call('get', 'context', { leadId: 'late' });
  const image = initial.body.images.find((item) => item.url === source);
  assert.ok(image); assert.equal(initial.body.images.length, 12);
  assert.equal(h.rows.get('leads/late').sampleAgent.images.some((item) => item.url === source), false);
  const saved = await h.call('post', 'context', { leadId: 'late', revision: initial.body.revision, fields: { ...complete, withoutLogo: false }, logoCrop: { imageIndex: image.index, crop: [0.1, 0.2, 0.8, 0.5] } });
  assert.equal(saved.code, 200);
  const agent = h.rows.get('leads/late').sampleAgent;
  assert.equal(agent.images[agent.brief.images.find((item) => item.role === 'logo').index].url, cropped);
});

test('puede guardar un logo subido completo sin recortarlo y rechaza archivos de otro contacto', async () => {
  const id = '01234567-89ab-4cde-8123-0123456789ab';
  const logo = 'https://storage.googleapis.com/own/images/manual-logo.webp';
  const bucket = { file: () => ({ getMetadata: async () => [{ contentType: 'image/webp', size: 1000, metadata: { leadId: 'upload' } }], getSignedUrl: async () => [logo] }) };
  const h = harness({ bucket, prepareLogo: async ({ url }) => { assert.equal(url, logo); return 'https://storage.googleapis.com/own/sample-logos/upload-crop.png'; } });
  h.add('upload', { sampleAgent: { status: 'paused', brief: complete, images: [] } });
  const initial = await h.call('get', 'context', { leadId: 'upload' });
  const saved = await h.call('post', 'context', { leadId: 'upload', revision: initial.body.revision, fields: { ...complete, withoutLogo: false }, logoUploadId: id });
  assert.equal(saved.code, 200);
  const agent = h.rows.get('leads/upload').sampleAgent;
  assert.equal(agent.brief.logoChoice, 'provided'); assert.equal(agent.images[0].url, logo);
  const updated = await h.call('get', 'context', { leadId: 'upload' });
  const recropped = await h.call('post', 'context', { leadId: 'upload', revision: updated.body.revision, fields: { ...complete, withoutLogo: false }, logoUploadId: id, logoCrop: { imageIndex: 1000, crop: [0.1, 0.2, 0.8, 0.5] } });
  assert.equal(recropped.code, 200);
  assert.equal(h.rows.get('leads/upload').sampleAgent.images[0].url, 'https://storage.googleapis.com/own/sample-logos/upload-crop.png');
  h.add('other', { sampleAgent: { status: 'paused', brief: complete } });
  const other = await h.call('get', 'context', { leadId: 'other' });
  assert.equal((await h.call('post', 'context', { leadId: 'other', revision: other.body.revision, fields: { ...complete, withoutLogo: false }, logoUploadId: id })).code, 400);
});

test('rechaza recortes inválidos y no sobrescribe cambios del cliente durante el recorte', async () => {
  let calls = 0;
  const h = harness({ prepareLogo: async () => {
    calls++; h.rows.get('leads/crop').sampleAgent.inboxRevision = 1;
    return 'https://storage.googleapis.com/own/sample-logos/cropped.png';
  } });
  const original = 'https://storage.googleapis.com/own/images/screenshot.jpg';
  h.add('crop', { sampleAgent: { version: 'v1', status: 'paused', brief: complete, images: [{ url: original }] } });
  const initial = await h.call('get', 'context', { leadId: 'crop' });
  const payload = { leadId: 'crop', revision: initial.body.revision, fields: { ...complete, withoutLogo: false, logoImageIndex: '' } };
  assert.equal((await h.call('post', 'context', { ...payload, logoCrop: { imageIndex: 99, crop: [0, 0, 0.5, 0.5] } })).code, 400);
  assert.equal((await h.call('post', 'context', { ...payload, logoCrop: { imageIndex: 0, crop: [-1, 0, 0.5, 0.5] } })).code, 400);
  assert.equal(calls, 0);
  assert.equal((await h.call('post', 'context', { ...payload, logoCrop: { imageIndex: 0, crop: [0, 0, 0.5, 0.5] } })).code, 409);
  assert.equal(h.rows.get('leads/crop').sampleAgent.images[0].url, original);
});
