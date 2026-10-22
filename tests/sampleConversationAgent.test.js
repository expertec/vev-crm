import test from 'node:test';
import assert from 'node:assert/strict';
import { createSampleConversationAgent, normalizeConversationBrief, missingBriefQuestion, isExplicitSampleConfirmation, sampleInputFromConversation, extractConversationBrief, agentCanReply } from '../services/sampleConversationAgent.js';
import { shouldBlockSequenceByLeadContext } from '../utils/sequenceTriggerGuards.js';

const complete = () => normalizeConversationBrief({ companyName: 'Makiatto', activity: 'Venta de bocinas y amplificadores', objective: 'Cotizaciones por WhatsApp', offerings: ['Bocinas activas', 'Amplificadores'], customers: 'Músicos y organizadores', differentiators: 'Asesoría para elegir equipo', serviceArea: 'Monterrey', logoChoice: 'none', colorPreference: 'Verde y negro', photosChoice: 'none' });

function database() {
  const rows = new Map();
  const snapshot = (ref) => ({ id: ref.id, ref, exists: rows.has(ref.path), data: () => structuredClone(rows.get(ref.path)) });
  const update = (ref, patch) => {
    const row = structuredClone(rows.get(ref.path) || {});
    for (const [key, value] of Object.entries(patch)) {
      const keys = key.split('.'); let current = row;
      for (const part of keys.slice(0, -1)) current = current[part] ||= {};
      current[keys.at(-1)] = structuredClone(value);
    }
    rows.set(ref.path, row);
  };
  const collection = (path) => {
    const get = async (field, expected) => ({ docs: [...rows.keys()].filter((key) => key.startsWith(`${path}/`) && key.split('/').length === path.split('/').length + 1).map((key) => snapshot(document(key))).filter((snap) => !field || field.split('.').reduce((v, k) => v?.[k], snap.data()) === expected) });
    return { doc: (id) => document(`${path}/${id}`), get: () => get(), where: (field, _op, value) => ({ get: () => get(field, value), limit: () => ({ get: () => get(field, value) }) }) };
  };
  const document = (path) => ({ path, id: path.split('/').at(-1), get: async () => snapshot(document(path)), update: async (patch) => update(document(path), patch), collection: (name) => collection(`${path}/${name}`) });
  let chain = Promise.resolve();
  return {
    rows, collection,
    runTransaction(fn) {
      const work = chain.then(() => fn({ get: async (ref) => snapshot(ref), update, set: (ref, data) => rows.set(ref.path, structuredClone(data)) }));
      chain = work.catch(() => {}); return work;
    },
  };
}

function setup(extract, transcribe) {
  const db = database();
  const leadRef = db.collection('leads').doc('test');
  db.rows.set(leadRef.path, { telefono: '528112345678', sampleAgent: { enabled: true, version: 'v1', status: 'collecting', history: [], images: [], brief: {} } });
  const replies = []; const jobs = []; const errors = [];
  const agent = createSampleConversationAgent({ db, extract, transcribe, send: async (_id, text) => replies.push(text), generate: async (job) => { jobs.push(job); return { negocioId: 'business' }; }, logger: { warn: (...args) => errors.push(args) } });
  const receive = (text, id, extra = {}) => agent.receive({ leadRef, messageId: id, message: { content: text, mediaType: 'text', ...extra } });
  return { db, leadRef, agent, replies, jobs, errors, receive };
}

test('recopila por chat, confirma y genera una sola vez sin otra llamada de IA', async () => {
  let calls = 0;
  const s = setup(async () => { calls++; return { brief: complete(), action: 'generate', reply: 'Ya la genero' }; });
  await s.receive('Tengo un negocio de audio', '1');
  assert.match(s.replies[0], /¿Confirmas/);
  assert.equal(s.jobs.length, 0);
  await s.receive('sí, generar', '2');
  await s.receive('sí, generar', '2');
  assert.equal(s.jobs.length, 1);
  assert.equal(calls, 1);
  assert.equal(s.jobs[0].input.companyInfo, 'Makiatto');
  assert.match(s.jobs[0].input.businessStory, /Monterrey/);
  assert.equal((await s.leadRef.get()).data().sampleAgent.status, 'generating');
  await s.receive('¿Ya está?', '3');
  assert.equal(s.jobs.length, 1);
  assert.match(s.replies.at(-1), /sigue en preparación/);
});

test('las correcciones invalidan el resumen anterior antes de generar', async () => {
  const s = setup(async ({ message }) => ({ brief: { ...complete(), serviceArea: message.text.includes('Saltillo') ? 'Saltillo' : 'Monterrey' }, action: 'generate', reply: '' }));
  await s.receive('Datos de mi negocio', '1');
  await s.receive('Sí, pero estamos en Saltillo', '2');
  assert.equal(s.jobs.length, 0);
  assert.match(s.replies.at(-1), /Saltillo/);
  await s.receive('confirmo', '3');
  assert.match(s.jobs[0].input.businessStory, /Saltillo/);
});

test('un brief incompleto o sin colores no dispara la generación aunque el modelo lo ordene', async () => {
  const brief = { ...complete(), colorPreference: '' };
  const s = setup(async () => ({ brief, action: 'generate', reply: '' }));
  await s.receive('genera', '1');
  assert.equal(s.jobs.length, 0);
  assert.match(s.replies[0], /colores/);
  assert.match(missingBriefQuestion({ ...complete(), offerings: [] }), /productos o servicios/);
});

test('una referencia visual no se trata como foto de producto ni se envía al retoque', () => {
  const images = [{ url: 'https://storage.googleapis.com/test/logo.png' }, { url: 'https://storage.googleapis.com/test/screenshot.png' }, { url: 'https://storage.googleapis.com/test/speaker.jpg' }];
  const brief = normalizeConversationBrief({ ...complete(), logoChoice: 'provided', photosChoice: 'provided', palette: ['#008800', '#FFFFFF', '#111111'], images: [{ index: 0, role: 'logo', description: 'Logo verde' }, { index: 1, role: 'reference', description: 'Ejemplo de web' }, { index: 2, role: 'product', description: 'Bocina activa' }, { index: 999, role: 'product', description: 'No recibida' }] }, images);
  assert.equal(missingBriefQuestion(brief), '');
  const input = sampleInputFromConversation(brief, images, '528112345678');
  assert.deepEqual(input.photoURLs, [images[2].url]);
  assert.deepEqual(input.referenceImageURLs, [images[1].url]);
  assert.deepEqual(input.brandPalette, ['#008800', '#FFFFFF', '#111111']);
  assert.equal(input.logoURL, images[0].url);
});

test('pregunta por el contenido de una imagen que el modelo no clasificó', async () => {
  const s = setup(async () => ({ brief: complete(), action: 'confirm', reply: '' }));
  await s.receive('', 'image1', { mediaType: 'image', mediaUrl: 'https://storage.googleapis.com/test/image.jpg' });
  assert.match(s.replies[0], /representa la imagen/);
  assert.equal(s.jobs.length, 0);
});

test('duplicados y mensajes concurrentes conservan el contexto y no responden dos veces', async () => {
  let calls = 0;
  const s = setup(async ({ brief }) => { calls++; return { brief: normalizeConversationBrief({ ...brief, companyName: 'Audio Norte' }), action: 'ask', reply: '¿A qué se dedica?' }; });
  await Promise.all([s.receive('Hola', '1'), s.receive('Hola', '1'), s.receive('Audio Norte', '2')]);
  await s.agent.resume();
  assert.equal(calls, 2);
  assert.equal(s.replies.length, 2);
  assert.equal(s.errors.length, 0);
});

test('pausar durante una llamada de IA impide que se envíe una respuesta tardía', async () => {
  let release; let started;
  const waitStarted = new Promise((resolve) => { started = resolve; });
  const s = setup(async () => { started(); await new Promise((resolve) => { release = resolve; }); return { brief: complete(), action: 'confirm', reply: '' }; });
  const processing = s.receive('Hola', '1');
  await waitStarted;
  await s.leadRef.update({ 'sampleAgent.enabled': false, 'sampleAgent.version': 'v2' });
  release(); await processing;
  assert.equal(s.replies.length, 0);
  assert.equal(s.jobs.length, 0);
});

test('respeta atención humana y permite cancelar mientras se genera', async () => {
  const s = setup(async () => ({ brief: complete(), action: 'confirm', reply: '' }));
  await s.leadRef.update({ humanControl: true });
  await s.receive('Hola', '1');
  assert.equal(s.replies.length, 0);
  assert.equal(agentCanReply({ sampleAgent: { enabled: true }, stopSequences: true }), false);
  await s.leadRef.update({ humanControl: false, 'sampleAgent.status': 'generating' });
  await s.receive('no me envíes más mensajes', '2');
  assert.equal((await s.leadRef.get()).data().sampleAgent.enabled, false);
  assert.equal((await s.leadRef.get()).data().sampleAgent.deliveryPaused, true);
});

test('un fallo de IA no pierde el mensaje y los reintentos están limitados', async () => {
  const s = setup(async () => { throw new Error('API unavailable'); });
  await s.receive('Hola', '1');
  await s.agent.resume(); await s.agent.resume(); await s.agent.resume();
  assert.equal(s.errors.length, 3);
  assert.equal((await s.leadRef.get()).data().sampleAgent.status, 'error');
  assert.equal((await s.leadRef.get()).data().sampleAgent.enabled, false);
  assert.equal(s.replies.length, 0);
});

test('no confunde una corrección con permiso para generar', () => {
  assert.equal(isExplicitSampleConfirmation('sí, generar'), true);
  assert.equal(isExplicitSampleConfirmation('Si, pero cambia mi nombre'), false);
  assert.equal(isExplicitSampleConfirmation('hola'), false);
  assert.equal(shouldBlockSequenceByLeadContext({ sampleAgent: { enabled: true } }, 'NuevoLeadWeb').blocked, true);
  assert.equal(shouldBlockSequenceByLeadContext({}, 'NuevoLeadWeb').blocked, false);
});

test('usa visión y salida estructurada sin aceptar URLs inventadas en el brief', async () => {
  const images = [{ url: 'https://storage.googleapis.com/test/logo.png' }];
  const response = await extractConversationBrief({ message: { text: 'Mi logo' }, images }, async (resource, body) => {
    assert.equal(resource, 'responses'); assert.equal(body.store, false);
    assert.equal(body.text.format.strict, true);
    assert.equal(body.input[0].content[1].detail, 'high');
    return { output_text: JSON.stringify({ action: 'ask', reply: '¿Qué servicios ofreces?', brief: complete() }) };
  });
  assert.equal(response.brief.companyName, 'Makiatto');
});

test('transcribe una nota de voz y usa sus datos para continuar el brief', async () => {
  let transcriptions = 0;
  const s = setup(async ({ message }) => {
    assert.equal(message.text, 'Mi negocio se llama Audio Norte');
    return { brief: normalizeConversationBrief({ companyName: 'Audio Norte' }), action: 'ask', reply: '¿Qué productos ofreces?' };
  }, async ({ mediaUrl }) => { assert.match(mediaUrl, /voice.ogg/); transcriptions++; return 'Mi negocio se llama Audio Norte'; });
  await s.receive('', 'audio1', { mediaType: 'audio', mediaUrl: 'https://storage.googleapis.com/test/voice.ogg' });
  await s.receive('', 'audio1', { mediaType: 'audio', mediaUrl: 'https://storage.googleapis.com/test/voice.ogg' });
  assert.equal(transcriptions, 1);
  assert.equal(s.replies.length, 1);
  const agent = (await s.leadRef.get()).data().sampleAgent;
  assert.equal(agent.brief.companyName, 'Audio Norte');
  assert.equal(agent.history[0].text, 'Mi negocio se llama Audio Norte');
});

test('un reintento del brief reutiliza la transcripción y no vuelve a pagar por el audio', async () => {
  let calls = 0; let transcriptions = 0;
  const s = setup(async () => {
    if (++calls === 1) throw new Error('Temporary brief API error');
    return { brief: complete(), action: 'confirm', reply: '' };
  }, async () => { transcriptions++; return 'Datos completos de mi negocio'; });
  await s.receive('', 'audio1', { mediaType: 'audio', mediaUrl: 'https://storage.googleapis.com/test/voice.ogg' });
  await s.agent.resume();
  assert.equal(calls, 2);
  assert.equal(transcriptions, 1);
  assert.equal(s.replies.length, 1);
});

test('un audio fallido no cambia el brief, no confirma la muestra ni bloquea los siguientes textos', async () => {
  let calls = 0;
  const s = setup(async () => { calls++; return { brief: complete(), action: 'confirm', reply: '' }; }, async () => { throw new Error('Audio unreadable'); });
  await s.receive('Datos de mi negocio', '1');
  const previous = (await s.leadRef.get()).data().sampleAgent.brief;
  await s.receive('', 'audio1', { mediaType: 'audio', mediaUrl: 'https://storage.googleapis.com/test/voice.ogg' });
  const after = (await s.leadRef.get()).data().sampleAgent;
  assert.deepEqual(after.brief, previous);
  assert.equal(after.enabled, true);
  assert.equal(after.confirmationHash, '');
  assert.match(s.replies.at(-1), /reenviarlo más corto o escribir/);
  assert.equal(calls, 1);
  await s.receive('Sí, generar', '2');
  assert.equal(s.jobs.length, 0, 'un audio no entendido puede contener una corrección; hay que confirmar de nuevo');
  assert.match(s.replies.at(-1), /¿Confirmas/);
});

test('acepta confirmar por voz y detecta una corrección hablada sin generar antes de tiempo', async () => {
  let spoken = 'Sí, pero la zona es Saltillo';
  const s = setup(async ({ message }) => ({ brief: { ...complete(), serviceArea: message.text.includes('Saltillo') ? 'Saltillo' : 'Monterrey' }, action: 'generate', reply: '' }), async () => spoken);
  await s.receive('Datos completos', '1');
  await s.receive('', 'audio1', { mediaType: 'audio', mediaUrl: 'https://storage.googleapis.com/test/voice.ogg' });
  assert.equal(s.jobs.length, 0);
  assert.match(s.replies.at(-1), /Saltillo/);
  spoken = 'Sí, generar.';
  await s.receive('', 'audio2', { mediaType: 'audio', mediaUrl: 'https://storage.googleapis.com/test/voice2.ogg' });
  assert.equal(s.jobs.length, 1);
  assert.match(s.jobs[0].input.businessStory, /Saltillo/);
});

test('pausar mientras se transcribe evita respuestas tardías y otra llamada al modelo', async () => {
  let release; let started; let calls = 0;
  const waitStarted = new Promise((resolve) => { started = resolve; });
  const s = setup(async () => { calls++; return { brief: complete(), action: 'confirm', reply: '' }; }, async () => { started(); await new Promise((resolve) => { release = resolve; }); return 'Mi negocio es de audio'; });
  const processing = s.receive('', 'audio1', { mediaType: 'audio', mediaUrl: 'https://storage.googleapis.com/test/voice.ogg' });
  await waitStarted;
  await s.leadRef.update({ 'sampleAgent.enabled': false, 'sampleAgent.version': 'v2' });
  release(); await processing;
  assert.equal(calls, 0);
  assert.equal(s.replies.length, 0);
});
