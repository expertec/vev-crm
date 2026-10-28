import test from 'node:test';
import assert from 'node:assert/strict';
import { assertReactivationAllowed, buildReactivationPreview, registerSampleReactivationRoutes, hasDeliveredSample } from '../services/sampleAgentReactivation.js';
import { normalizeConversationBrief, sampleBriefHash } from '../services/sampleConversationAgent.js';

const user = { uid: 'admin', role: 'admin' };
const complete = () => normalizeConversationBrief({ companyName: 'Taller Norte', activity: 'Mecánica', objective: 'Más clientes', offerings: ['Afinación'], customers: 'Automovilistas', differentiators: 'No definidos', serviceArea: 'Mante', logoChoice: 'none', colorPreference: 'Verde', photosChoice: 'none' });

function harness(options = {}) {
  const rows = new Map(); const handlers = new Map(); const sends = []; const cancellations = [];
  const snap = (ref) => ({ id: ref.id, ref, exists: rows.has(ref.path), data: () => structuredClone(rows.get(ref.path)) });
  const patch = (ref, fields) => {
    const value = structuredClone(rows.get(ref.path) || {});
    for (const [key, item] of Object.entries(fields)) {
      const parts = key.split('.'); let target = value;
      for (const part of parts.slice(0, -1)) target = target[part] ||= {};
      target[parts.at(-1)] = structuredClone(item);
    }
    rows.set(ref.path, value);
  };
  const collection = (path, filters = [], ordering = '', count = Infinity) => ({
    doc: (id) => doc(`${path}/${id}`),
    where: (key, op, value) => collection(path, [...filters, [key, op, value]], ordering, count),
    orderBy: (key) => collection(path, filters, key, count),
    limit: (limit) => collection(path, filters, ordering, limit),
    get: async () => {
      let docs = [...rows.keys()].filter((key) => key.startsWith(`${path}/`) && key.split('/').length === path.split('/').length + 1).map((key) => snap(doc(key)));
      docs = docs.filter((d) => filters.every(([key, op, value]) => op === '>=' ? d.data()[key] >= value : d.data()[key] === value));
      if (ordering) docs.sort((a, b) => Number(b.data()[ordering]) - Number(a.data()[ordering]));
      return { docs: docs.slice(0, count) };
    },
  });
  const doc = (path) => ({ path, id: path.split('/').at(-1), get: async () => snap(doc(path)), set: async (value) => rows.set(path, structuredClone(value)), update: async (fields) => patch(doc(path), fields), collection: (name) => collection(`${path}/${name}`) });
  const db = { collection, runTransaction: async (fn) => fn({ get: async (ref) => snap(ref), update: patch }) };
  rows.set('leads/lead', { nombre: 'Sergio', telefono: '521831', lastMessageAt: new Date(), sampleAgent: { version: 'old', brief: {}, images: [] } });
  rows.set('leads/lead/messages/1', { sender: 'lead', content: 'Tengo un taller', timestamp: new Date(), mediaType: 'text' });
  const service = registerSampleReactivationRoutes({ post: (path, fn) => handlers.set(path.split('/').at(-1), fn) }, { db, readUser: async () => user, extract: async () => ({ brief: complete(), action: 'confirm' }), send: async (...args) => sends.push(args), cancelSequences: async (id) => cancellations.push(id), ...options });
  const call = async (action, body) => {
    let status = 200; let data;
    await handlers.get(action)({ body }, { status(code) { status = code; return this; }, json(value) { data = value; } });
    return { status, data };
  };
  return { rows, db, sends, cancellations, call, service };
}

test('recupera fotos, texto y audios; distingue cliente de negocio y solo pregunta lo pendiente', async () => {
  let transcriptions = 0; let persisted = '';
  const preview = await buildReactivationPreview({
    lead: {},
    messages: [
      { sender: 'business', content: '¿Te gustaría ofrecer urgencias 24/7?' },
      { sender: 'lead', content: 'No, solo afinación' },
      { sender: 'lead', content: 'Mi taller', mediaType: 'image', mediaUrl: 'https://storage.googleapis.com/test/photo.jpg' },
      { sender: 'lead', mediaType: 'audio', mediaUrl: 'https://storage.googleapis.com/test/audio.ogg', ref: { update: async (data) => { persisted = data.transcription; } } },
      { sender: 'lead', mediaType: 'video' },
    ],
    transcribe: async () => { transcriptions++; return 'Me gustaría verde'; },
    extract: async ({ history, images, message }) => {
      assert.equal(history[0].role, 'assistant'); assert.equal(history[1].role, 'customer');
      assert.equal(history[3].text, 'Me gustaría verde'); assert.equal(images.length, 1); assert.equal(message.reactivation, true);
      return { brief: { ...complete(), objective: '', photosChoice: 'provided', images: [{ index: 0, role: 'work', description: 'Taller' }] }, action: 'generate' };
    },
  });
  assert.equal(transcriptions, 1); assert.equal(persisted, 'Me gustaría verde');
  assert.equal(preview.status, 'collecting'); assert.match(preview.reply, /Qué te gustaría lograr/);
  assert.doesNotMatch(preview.reply, /urgencias|generando|Envíame/);
  assert.equal(preview.videoCount, 1);
});

test('un contacto que vuelve por campaña conserva sus datos sin afirmar que dejó una muestra pendiente', async () => {
  const messages = [
    { sender: 'lead', content: 'Persianas, cortinas y decoración' },
    { sender: 'business', content: 'Tu página por $790 al año. Estos son ejemplos de otras páginas.' },
    { sender: 'lead', content: 'Bienvenido a Decorstyle en Puebla' },
    { sender: 'lead', content: 'Hola, tengo un negocio y quiero información sobre la página web de $990 al año.' },
  ];
  const extract = async ({ message }) => {
    assert.equal(message.returningCampaign, true);
    return { brief: { ...complete(), companyName: 'Decorstyle', activity: 'Persianas, cortinas y decoración', serviceArea: 'Puebla' }, action: 'confirm' };
  };
  const first = await buildReactivationPreview({ lead: {}, messages, extract });
  assert.match(first.reply, /Ya nos habías contado de Decorstyle/);
  assert.doesNotMatch(first.reply, /retomamos|790/);
  assert.equal(first.brief.companyName, 'Decorstyle');
  assert.equal(first.status, 'awaiting_confirmation');
  const pending = await buildReactivationPreview({ lead: { sampleAgent: { brief: complete() } }, messages, extract });
  assert.match(pending.reply, /retomamos tu muestra/);
});

test('al retomar recupera un logo reciente aunque ya hubiera doce fotos almacenadas', async () => {
  const images = Array.from({ length: 12 }, (_, i) => ({ url: `https://storage.googleapis.com/test/shoe${i}.jpg` }));
  const recent = 'https://storage.googleapis.com/test/late-logo.jpg';
  await buildReactivationPreview({ lead: { sampleAgent: { images, brief: complete() } }, messages: [{ sender: 'lead', mediaType: 'image', mediaUrl: recent }], extract: async ({ images }) => {
    assert.equal(images.length, 12); assert.equal(images.at(-1).url, recent);
    return { brief: complete(), action: 'ask' };
  } });
});

test('no repite la solicitud de fotos recibidas y admite transcripciones existentes', async () => {
  const result = await buildReactivationPreview({ lead: {}, messages: [{ sender: 'lead', mediaType: 'audio', transcription: 'Sí, son mías' }, { sender: 'lead', mediaType: 'image', mediaUrl: 'https://storage.googleapis.com/test/a.jpg' }], transcribe: async () => assert.fail('No debe retranscribir'), extract: async () => ({ brief: { ...complete(), photosChoice: 'pending', images: [{ index: 0, role: 'work', description: 'Producto' }] }, action: 'ask' }) });
  assert.match(result.reply, /Ya tengo 1 imagen/); assert.doesNotMatch(result.reply, /Envíame/);
});

test('excluye permisos ajenos, cancelados, atención humana y muestras enviadas', () => {
  for (const lead of [{ humanControl: true }, { estado: 'No interesado' }, { archived: true }, { sampleLinkSentAt: new Date(), sampleLinkSentUrl: 'https://negociosweb.mx/taller-norte' }, { sampleAgent: { status: 'generating' } }]) assert.throws(() => assertReactivationAllowed(user, lead));
  assert.throws(() => assertReactivationAllowed({ uid: 'other', role: 'salesAgent' }, { assignedTo: 'owner' }), /acceso/);
});

test('enviar un formulario o restaurar una invitación no equivale a entregar la página', async () => {
  const h = harness({ generate: async () => ({ negocioId: 'business', queued: true }) });
  Object.assign(h.rows.get('leads/lead'), {
    sampleLinkSentAt: new Date(), webLinkSentAt: new Date(),
    sampleLinkSentUrl: 'https://app.negociosweb.mx/muestra/5212216544471',
    sampleFlow: { source: 'crm_restore_sample' },
  });
  const result = await h.service.activate('lead', user);
  assert.equal(result.generated, true);
  assert.equal(h.sends.length, 1);
  assert.equal(hasDeliveredSample({ sampleLinkSentAt: new Date() }), false);
  assert.equal(hasDeliveredSample({ webLinkSentAt: new Date() }), false);
});

test('mantiene el bloqueo cuando hay evidencia de entrega de una muestra real', () => {
  for (const lead of [
    { webSales: { sample: { sentAt: new Date() } } },
    { sampleAgent: { status: 'completed' } },
    { siteSentAt: new Date() },
    { sampleLinkSentAt: new Date(), sampleLinkSentUrl: 'https://negociosweb.mx/sam' },
    { sampleLinkSentAt: new Date(), sampleLinkSentUrl: 'https://negociosweb.mx/m/c9NG2UICVYaO' },
  ]) {
    assert.equal(hasDeliveredSample(lead), true);
    assert.throws(() => assertReactivationAllowed(user, lead), /ya recibió/);
  }
});

test('la vista previa no contacta ni activa, y el envío conserva contexto y aprobación sin duplicados', async () => {
  const h = harness();
  h.rows.set('leads/lead/sampleAgentInbox/old', { state: 'pending', receivedAt: new Date(Date.now() - 10000) });
  const preview = await h.call('preview', { leadId: 'lead' });
  assert.equal(preview.status, 200); assert.equal(h.sends.length, 0); assert.notEqual(h.rows.get('leads/lead').sampleAgent.enabled, true);
  const body = { leadId: 'lead', draftId: preview.data.draftId };
  assert.equal((await h.call('send', body)).status, 200);
  assert.equal(h.sends.length, 1); assert.deepEqual(h.cancellations, ['lead']);
  const agent = h.rows.get('leads/lead').sampleAgent;
  assert.equal(agent.brief.companyName, 'Taller Norte'); assert.equal(agent.enabled, true);
  assert.equal(agent.confirmationHash, sampleBriefHash(agent.brief));
  assert.equal(agent.history.at(-1).text, preview.data.reply);
  assert.equal(h.rows.get('leads/lead/sampleAgentInbox/old').state, 'done');
  assert.equal((await h.call('send', body)).data.duplicate, true); assert.equal(h.sends.length, 1);
});

test('un mensaje nuevo o un bloqueo invalida la vista previa antes de enviar', async () => {
  for (const change of ['message', 'human']) {
    const h = harness(); const preview = await h.call('preview', { leadId: 'lead' });
    if (change === 'message') h.rows.set('leads/lead/messages/2', { sender: 'lead', content: 'Cambia los colores', timestamp: new Date() });
    else h.rows.get('leads/lead').humanControl = true;
    assert.equal((await h.call('send', { leadId: 'lead', draftId: preview.data.draftId })).status, 409);
    assert.equal(h.sends.length, 0);
  }
});

test('un error de envío no se reintenta automáticamente y deja el asistente pausado', async () => {
  const h = harness({ send: async () => { throw Error('WhatsApp no disponible'); } });
  const preview = await h.call('preview', { leadId: 'lead' });
  const body = { leadId: 'lead', draftId: preview.data.draftId };
  assert.equal((await h.call('send', body)).status, 500);
  assert.equal(h.rows.get('leads/lead').sampleAgent.enabled, false);
  assert.equal((await h.call('send', body)).status, 409);
});

test('filtra por mensajes del cliente aunque nuestra respuesta sea posterior al rango', async () => {
  const h = harness();
  const from = new Date(Date.now() - 86400_000), to = new Date(Date.now() - 3600_000);
  h.rows.get('leads/lead/messages/1').timestamp = new Date(Date.now() - 7200_000);
  h.rows.set('leads/lead/messages/2', { sender: 'business', content: 'Hola', timestamp: new Date() });
  const result = await h.call('candidates', { from, to });
  assert.equal(result.status, 200); assert.equal(result.data.leads.length, 1);
});

test('respeta una cancelación detectada en el historial y no guarda una reactivación', async () => {
  const h = harness({ extract: async () => ({ brief: complete(), action: 'pause' }) });
  assert.equal((await h.call('preview', { leadId: 'lead' })).status, 409);
  assert.equal(h.sends.length, 0);
});

test('reutiliza transcripciones del asistente y no vuelve a llamar al transcriptor', async () => {
  const h = harness({ transcribe: async () => assert.fail('Ya estaba transcrito'), extract: async ({ history }) => {
    assert.equal(history.at(-1).text, 'Tengo un taller');
    return { brief: complete(), action: 'confirm' };
  } });
  const mediaUrl = 'https://storage.googleapis.com/test/voice.ogg';
  h.rows.set('leads/lead/messages/1', { sender: 'lead', mediaType: 'audio', mediaUrl, timestamp: new Date() });
  h.rows.set('leads/lead/sampleAgentInbox/voice', { mediaUrl, transcription: 'Tengo un taller', receivedAt: new Date(), state: 'done' });
  assert.equal((await h.call('preview', { leadId: 'lead' })).status, 200);
});

test('un mensaje recibido mientras se cancelan secuencias detiene el envío desactualizado', async () => {
  const h = harness({ cancelSequences: async () => { h.rows.get('leads/lead').sampleAgent.inboxRevision = 1; } });
  const preview = await h.call('preview', { leadId: 'lead' });
  assert.equal((await h.call('send', { leadId: 'lead', draftId: preview.data.draftId })).status, 409);
  assert.equal(h.sends.length, 0);
});

test('rechaza acceso sin sesión y vistas previas vencidas', async () => {
  const anonymous = harness({ readUser: async () => null });
  assert.equal((await anonymous.call('preview', { leadId: 'lead' })).status, 401);
  const h = harness(); const preview = await h.call('preview', { leadId: 'lead' });
  h.rows.get(`leads/lead/sampleAgentReactivations/${preview.data.draftId}`).expiresAt = new Date(0);
  assert.equal((await h.call('send', { leadId: 'lead', draftId: preview.data.draftId })).status, 409);
  assert.equal(h.sends.length, 0);
});


test('activar continúa inmediatamente y genera cuando el historial está completo, sin otro mensaje', async () => {
  const jobs = [];
  const h = harness({ generate: async (job) => { jobs.push(job); return { negocioId: 'business', queued: true }; } });
  const result = await h.service.activate('lead', user);
  assert.equal(jobs.length, 1);
  assert.equal(jobs[0].input.companyInfo, 'Taller Norte');
  assert.equal(jobs[0].input.enhancePhotos, false);
  assert.equal(result.generated, true);
  assert.equal(result.sampleAgent.status, 'generating');
  assert.equal(h.sends.length, 1);
  assert.match(h.sends[0][1], /tengo la información necesaria/);
  assert.doesNotMatch(h.sends[0][1], /¿Confirmas/);
});

test('play permite retomar para pedir confirmación aunque ya haya datos completos, sin generar', async () => {
  const h = harness({ generate: async () => assert.fail('Play debe pedir confirmar, no generar') });
  const result = await h.service.activate('lead', user, { generateWhenReady: false });
  assert.equal(result.generated, false);
  assert.equal(result.sampleAgent.enabled, true);
  assert.equal(result.sampleAgent.status, 'awaiting_confirmation');
  assert.equal(h.sends.length, 1);
  assert.match(h.sends[0][1], /¿Confirmas/);
  assert.deepEqual(h.cancellations, ['lead']);
});

test('activar con información incompleta pregunta solo lo pendiente y conserva las imágenes', async () => {
  const h = harness({ extract: async () => ({ brief: { ...complete(), objective: '', images: [{ index: 0, role: 'work', description: 'Foto del taller' }] }, action: 'ask' }), generate: async () => assert.fail('Faltan datos') });
  h.rows.set('leads/lead/messages/photo', { sender: 'lead', mediaType: 'image', mediaUrl: 'https://storage.googleapis.com/test/photo.jpg', timestamp: new Date() });
  const result = await h.service.activate('lead', user);
  assert.equal(result.generated, false);
  assert.equal(result.sampleAgent.images.length, 1);
  assert.equal(result.sampleAgent.status, 'collecting');
  assert.equal(h.sends.length, 1);
  assert.match(h.sends[0][1], /Qué te gustaría lograr/);
  assert.doesNotMatch(h.sends[0][1], /Envíame/);
});

test('dos clics simultáneos en activar comparten la operación y no duplican la muestra', async () => {
  let generated = 0;
  const h = harness({ generate: async () => { generated++; return { negocioId: 'business', queued: true }; } });
  const [first, second] = await Promise.all([h.service.activate('lead', user), h.service.activate('lead', user)]);
  assert.equal(first.generated, true); assert.equal(second.generated, true);
  assert.equal(generated, 1); assert.equal(h.sends.length, 1);
});

test('activar no genera automáticamente si hay audios pendientes de entender', async () => {
  const h = harness({ transcribe: async () => { throw Error('Inaudible'); }, generate: async () => assert.fail('Hay un audio pendiente') });
  h.rows.set('leads/lead/messages/audio', { sender: 'lead', mediaType: 'audio', mediaUrl: 'https://storage.googleapis.com/test/audio.ogg', timestamp: new Date() });
  const result = await h.service.activate('lead', user);
  assert.equal(result.generated, false);
  assert.match(h.sends[0][1], /¿Confirmas/);
});


test('un contacto de campaña sin primer mensaje descifrado puede activarse sin inventar un brief', async () => {
  const preview = await buildReactivationPreview({ lead: { etiquetas: ['MetaAds', 'MensajeNoDesencriptado'], metaAttribution: { trigger: 'LeadWhatsapp' } }, messages: [], extract: async () => assert.fail('No hay datos que interpretar') });
  assert.equal(preview.brief.companyName, '');
  assert.equal(preview.history.length, 0);
  assert.equal(preview.status, 'collecting');
  assert.match(preview.reply, /Cómo se llama tu negocio/);
  await assert.rejects(buildReactivationPreview({ lead: {}, messages: [] }), /No hay mensajes/);
});

test('iniciar con bienvenida envía el audio aunque esté activo, conserva historial y no manda retomamos', async () => {
  let audios = 0;
  const h = harness({
    loadAudioOptions: async () => [{ moment: 'welcome', script: '¡Hola! Soy Sergio. ¿Cómo se llama tu negocio?' }],
    sendFlowAudio: async ({ moment, isCurrent }) => { assert.equal(moment, 'welcome'); assert.equal(await isCurrent(), true); audios++; return true; },
    extract: async () => assert.fail('La bienvenida manual no debe esperar al modelo'),
  });
  const lead = h.rows.get('leads/lead');
  lead.sampleAgent.enabled = true;
  lead.sampleAgent.history = [{ role: 'customer', text: 'También lo posicionas en Google' }];
  lead.sampleAgent.brief = { companyName: 'Negocio previo' };
  const result = await h.service.startWelcome('lead', user);
  assert.equal(audios, 1); assert.equal(h.sends.length, 0);
  assert.equal(result.sampleAgent.brief.companyName, 'Negocio previo');
  assert.equal(result.sampleAgent.history[0].text, 'También lo posicionas en Google');
  assert.match(result.sampleAgent.history.at(-1).text, /Soy Sergio/);
  assert.doesNotMatch(result.sampleAgent.history.at(-1).text, /retomamos/);
  assert.deepEqual(result.sampleAgent.audioMomentsSent, ['welcome']);
  assert.equal((await h.service.startWelcome('lead', user)).duplicate, true);
  assert.equal(audios, 1);
});

test('la bienvenida manual no requiere mensajes previos y dos clics no duplican el audio', async () => {
  let audios = 0;
  const h = harness({ loadAudioOptions: async () => [{ moment: 'welcome', script: 'Hola, cuéntame de tu negocio.' }], sendFlowAudio: async () => { audios++; return true; } });
  h.rows.delete('leads/lead/messages/1');
  const results = await Promise.all([h.service.startWelcome('lead', user), h.service.startWelcome('lead', user)]);
  assert.equal(audios, 1);
  assert.equal(results[0].sampleAgent.enabled, true);
  assert.equal(results[1].sampleAgent.enabled, true);
  assert.equal(h.sends.length, 0);
});

test('la bienvenida manual avisa si falta la grabación y respeta atención humana', async () => {
  const h = harness();
  await assert.rejects(h.service.startWelcome('lead', user), /Graba y activa/);
  assert.equal(h.sends.length, 0);
  assert.notEqual(h.rows.get('leads/lead').sampleAgent.enabled, true);
  h.rows.get('leads/lead').humanControl = true;
  await assert.rejects(h.service.startWelcome('lead', user), /atención humana/);
});
