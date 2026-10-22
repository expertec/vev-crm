import test from 'node:test';
import assert from 'node:assert/strict';
import { createSampleConversationAgent, normalizeConversationBrief, missingBriefQuestion, isExplicitSampleConfirmation, sampleInputFromConversation, extractConversationBrief, interpretSampleApproval, agentCanReply, briefSummary } from '../services/sampleConversationAgent.js';
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

function setup(extract, transcribe, options = {}) {
  const db = database();
  const leadRef = db.collection('leads').doc('test');
  db.rows.set(leadRef.path, { telefono: '528112345678', sampleAgent: { enabled: true, version: 'v1', status: 'collecting', history: [], images: [], brief: {} } });
  const replies = []; const jobs = []; const errors = [];
  const agent = createSampleConversationAgent({ db, extract, transcribe, debounceMs: 0, interpretApproval: async () => ({ intent: 'change', reply: '' }), send: async (_id, text) => replies.push(text), generate: async (job) => { jobs.push(job); return { negocioId: 'business' }; }, logger: { warn: (...args) => errors.push(args) }, ...options });
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

test('entrega la muestra lista con enlace solo y reintenta el enlace sin repetir el aviso', async () => {
  const messages = [];
  const url = 'https://negociosweb.mx/taller-norte';
  let generations = 0; let failed = false;
  const s = setup(async () => ({ brief: complete(), action: 'confirm', reply: '' }), undefined, {
    generate: async () => { generations++; return { ready: true, sampleUrl: url, negocioId: 'business' }; },
    send: async (_id, text) => {
      messages.push(text);
      if (text === url && !failed) { failed = true; throw Error('Temporary WhatsApp error'); }
    },
  });
  await s.receive('Datos del negocio', '1');
  await s.receive('sí, generar', '2');
  assert.match(messages[1], /siguiente mensaje/);
  assert.equal(messages[2], url);
  assert.notEqual((await s.leadRef.get()).data().sampleAgent.status, 'completed');
  await s.agent.resume();
  assert.deepEqual(messages.slice(2), [url, url]);
  assert.equal(generations, 1);
  assert.equal((await s.leadRef.get()).data().sampleAgent.status, 'completed');
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
  assert.equal(calls, 1);
  assert.equal(s.replies.length, 1);
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
  assert.equal(isExplicitSampleConfirmation('si apruebo'), true);
  assert.equal(isExplicitSampleConfirmation('ok genera la muestra'), true);
  assert.equal(isExplicitSampleConfirmation('sí apruebo pero cambia el nombre'), false);
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

test('acepta aprobaciones naturales conservando exactamente los datos del resumen', async () => {
  for (const text of ['si apruebo', 'ok genera la muestra', 'me gusta, puedes empezar', 'Sí, adelante con eso']) {
    const s = setup(async () => ({ brief: complete(), action: 'confirm', reply: '' }), undefined, {
      interpretApproval: async ({ brief, history, text: message }) => {
        assert.equal(message, text); assert.equal(brief.companyName, 'Makiatto'); assert.match(history.at(-1).text, /¿Confirmas/);
        return { intent: 'approve', reply: '' };
      },
    });
    await s.receive('Datos de mi negocio', '1');
    await s.receive(text, '2');
    assert.equal(s.jobs.length, 1, text);
    assert.equal(s.replies.filter((reply) => reply.includes('¿Confirmas')).length, 1, 'no repite el resumen');
    assert.equal(s.jobs[0].input.companyInfo, 'Makiatto');
  }
});

test('la aprobación funciona después de que Firestore reordena las claves de los mapas', async () => {
  const s = setup(async () => ({ brief: complete(), action: 'confirm', reply: '' }));
  await s.receive('Datos de mi negocio', '1');
  const stored = s.db.rows.get(s.leadRef.path);
  stored.sampleAgent.brief = Object.fromEntries(Object.entries(stored.sampleAgent.brief).sort(([a], [b]) => a.localeCompare(b)));
  s.db.rows.set(s.leadRef.path, stored);
  await s.receive('ok genera la muestra', '2');
  assert.equal(s.jobs.length, 1);
});

test('recupera una aprobación pendiente de la versión anterior sin pedir otro resumen', async () => {
  const s = setup(async () => ({ brief: complete(), action: 'confirm', reply: '' }));
  await s.receive('Datos de mi negocio', '1');
  await s.leadRef.update({ 'sampleAgent.confirmationHash': 'hash-con-el-orden-anterior' });
  await s.receive('sí, generar', '2');
  assert.equal(s.jobs.length, 1);
});

test('responde dudas antes de aprobar sin repetir el resumen ni generar', async () => {
  let next = 'question';
  const s = setup(async () => ({ brief: complete(), action: 'confirm', reply: '' }), undefined, {
    interpretApproval: async () => ({ intent: next, reply: 'Sí, la muestra tendrá un botón para solicitar cotizaciones por WhatsApp. ¿La preparo?' }),
  });
  await s.receive('Datos de mi negocio', '1');
  await s.receive('¿Va a tener botón de WhatsApp?', '2');
  assert.equal(s.jobs.length, 0);
  assert.equal(s.replies.filter((reply) => reply.includes('¿Confirmas')).length, 1);
  assert.equal((await s.leadRef.get()).data().sampleAgent.status, 'awaiting_confirmation');
  next = 'approve';
  await s.receive('Sí, me parece muy bien', '3');
  assert.equal(s.jobs.length, 1);
});

test('el intérprete recibe el contexto y devuelve solo la intención, sin reescribir el brief', async () => {
  const result = await interpretSampleApproval({ brief: complete(), history: [{ role: 'assistant', text: '¿Confirmas?' }], text: 'si apruebo' }, async (resource, body) => {
    assert.equal(resource, 'responses');
    const content = JSON.parse(body.input[0].content[0].text);
    assert.equal(content.text, 'si apruebo'); assert.equal(content.brief.companyName, 'Makiatto');
    assert.equal(body.text.format.strict, true);
    return { output_text: JSON.stringify({ intent: 'approve', reply: '' }) };
  });
  assert.equal(result.intent, 'approve');
});

test('un audio que cubre la pregunta reemplaza el texto y se usa una sola vez por momento', async () => {
  const sent = [];
  const s = setup(async () => ({ brief: normalizeConversationBrief({ companyName: 'Makiatto' }), action: 'ask', reply: '¿A qué se dedica?', audioMoment: 'business' }), undefined, {
    loadAudioOptions: async () => [{ moment: 'business', script: 'Cuéntame a qué se dedica tu negocio.' }],
    sendFlowAudio: async ({ moment }) => { sent.push(moment); return true; },
  });
  await s.receive('Hola', '1'); await s.receive('Hola', '1');
  await s.receive('Makiatto', '2'); await s.receive('Otra respuesta', '3');
  assert.deepEqual(sent, ['business']);
  assert.equal(s.replies.length, 2);
  assert.deepEqual((await s.leadRef.get()).data().sampleAgent.audioMomentsSent, ['business']);
});

test('la bienvenida genérica se usa una vez y acompaña la pregunta sin repetir su texto', async () => {
  const sent = [];
  const script = '¡Hola! Te voy a ayudar a preparar una muestra de tu página. Vamos paso a paso por aquí.';
  const s = setup(async () => ({ brief: normalizeConversationBrief({}), action: 'ask', reply: '¿Cómo se llama tu negocio y a qué se dedica?', audioMoment: '' }), undefined, {
    loadAudioOptions: async () => [{ moment: 'welcome', script }],
    sendFlowAudio: async ({ moment }) => { sent.push(moment); return true; },
  });
  await s.receive('Quiero una muestra', '1');
  assert.deepEqual(sent, ['welcome']);
  assert.deepEqual(s.replies, ['¿Cómo se llama tu negocio y a qué se dedica?']);
  assert.equal((await s.leadRef.get()).data().sampleAgent.history.at(-1).text, `${script}\n${s.replies[0]}`);
  await s.receive('Hola', '2');
  assert.deepEqual(sent, ['welcome']);
});

test('el audio de confirmación acompaña el resumen escrito una vez y permite aprobar', async () => {
  const sent = [];
  const s = setup(async () => ({ brief: complete(), action: 'confirm', reply: '', audioMoment: '' }), undefined, {
    loadAudioOptions: async () => [{ moment: 'confirmation', script: 'Te comparto por escrito los datos que reunimos. Revísalos y dime si están bien o quieres cambiar algo.' }],
    sendFlowAudio: async ({ moment }) => { sent.push(moment); return true; },
  });
  await s.receive('Datos completos', '1');
  assert.deepEqual(sent, ['confirmation']);
  assert.match(s.replies[0], /Así prepararé tu muestra/);
  assert.doesNotMatch(s.replies[0], /Te comparto por escrito/);
  await s.receive('sí, generar', '2');
  assert.equal(s.jobs.length, 1);
  assert.deepEqual(sent, ['confirmation']);
});

test('si falla la introducción de confirmación conserva el resumen escrito', async () => {
  const s = setup(async () => ({ brief: complete(), action: 'confirm', reply: '' }), undefined, {
    loadAudioOptions: async () => [{ moment: 'confirmation', script: 'Revisa los datos.' }],
    sendFlowAudio: async () => { throw new Error('Audio failed'); },
  });
  await s.receive('Datos completos', '1');
  assert.equal(s.replies.length, 1);
  assert.match(s.replies[0], /¿Confirmas/);
  assert.deepEqual((await s.leadRef.get()).data().sampleAgent.audioMomentsSent, []);
});

test('un fallo en el audio de respuesta conserva el texto y permite continuar el flujo', async () => {
  const s = setup(async () => ({ brief: complete(), action: 'confirm', reply: '' }), undefined, { loadAudioOptions: async () => [{ moment: 'generating', script: 'Voy a preparar la muestra.' }], sendFlowAudio: async () => { throw new Error('WhatsApp audio failed'); } });
  await s.receive('Datos de mi negocio', '1');
  assert.equal(s.replies.length, 1);
  assert.equal((await s.leadRef.get()).data().sampleAgent.status, 'awaiting_confirmation');
  await s.receive('sí, generar', '2');
  assert.equal(s.jobs.length, 1);
  assert.equal((await s.leadRef.get()).data().sampleAgent.enabled, true);
});

test('espera cuatro segundos de silencio y reúne las frases consecutivas en una sola respuesta', async () => {
  let time = 1_792_690_000_000; const calls = [];
  const s = setup(async ({ message }) => { calls.push(message); return { brief: complete(), action: 'confirm', reply: '' }; }, undefined, { debounceMs: 4000, now: () => time });
  await s.receive('Todo', '1'); time += 2000;
  await s.receive('Más ventas', '2'); time += 2000;
  await s.receive('Y clientes', '3');
  await s.agent.resume();
  assert.equal(calls.length, 0); assert.equal(s.replies.length, 0);
  time += 4000; await s.agent.resume();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].text, 'Todo\nMás ventas\nY clientes');
  assert.equal(calls[0].messages.length, 3);
  assert.equal(s.replies.length, 1);
  assert.equal([...s.db.rows.entries()].filter(([key, data]) => key.includes('sampleAgentInbox') && data.state === 'pending').length, 0);
});

test('descarta una pregunta en preparación si llega nueva información antes de enviarla', async () => {
  let time = 1_792_690_000_000; let release; let started; let calls = 0;
  const waitStarted = new Promise((resolve) => { started = resolve; });
  const s = setup(async ({ message }) => {
    if (++calls === 1) { started(); await new Promise((resolve) => { release = resolve; }); return { brief: complete(), action: 'ask', reply: '¿En qué zona atienden?' }; }
    assert.equal(message.text, 'Son ecológicos\nEn todo México');
    return { brief: { ...complete(), differentiators: 'Ecológicos', serviceArea: 'Todo México' }, action: 'confirm', reply: '' };
  }, undefined, { debounceMs: 4000, now: () => time });
  await s.receive('Son ecológicos', '1'); time += 4000;
  const processing = s.agent.resume(); await waitStarted;
  time += 100; await s.receive('En todo México', '2');
  release(); await processing;
  assert.equal(s.replies.length, 0);
  time += 4000; await s.agent.resume();
  assert.equal(s.replies.length, 1);
  assert.match(s.replies[0], /Todo México/);
  assert.doesNotMatch(s.replies[0], /¿En qué zona atienden/);
});

test('incluye el logo y su mensaje de aclaración en la misma tanda', async () => {
  let time = 1_792_690_000_000; let calls = 0;
  const image = 'https://storage.googleapis.com/test/logo.jpg';
  const s = setup(async ({ images, message }) => {
    calls++; assert.equal(images[0].url, image); assert.match(message.text, /Es ese/);
    return { brief: normalizeConversationBrief({ ...complete(), images: [{ index: 0, role: 'logo', description: 'Logo del negocio' }], logoChoice: 'provided', palette: ['#008800', '#FFFFFF', '#111111'] }, images), action: 'confirm', reply: '' };
  }, undefined, { debounceMs: 4000, now: () => time });
  await s.receive('', '1', { mediaType: 'image', mediaUrl: image });
  time += 2000; await s.receive('Es ese', '2');
  time += 4000; await s.agent.resume();
  assert.equal(calls, 1); assert.equal(s.replies.length, 1);
});

test('al día siguiente retoma el brief y la pregunta pendiente incluso con una nueva instancia del agente', async () => {
  const day1 = 1_792_690_000_000;
  const s = setup(async () => ({ brief: normalizeConversationBrief({ companyName: 'Una Mascota', activity: 'Productos para mascotas' }), action: 'ask', reply: '¿Qué quieres lograr con tu web?' }), undefined, { now: () => day1 });
  await s.receive('Mi negocio se llama Una Mascota', '1');
  const replies = []; let calls = 0;
  const resumed = createSampleConversationAgent({ db: s.db, debounceMs: 0, now: () => day1 + 26 * 3600_000, send: async (_id, text) => replies.push(text), generate: async () => { throw new Error('No debe generar todavía'); }, extract: async ({ brief, history, message }) => {
    calls++; assert.equal(brief.companyName, 'Una Mascota');
    assert.equal(history.at(-1).text, '¿Qué quieres lograr con tu web?');
    assert.equal(message.gapHours, 26);
    return { brief: normalizeConversationBrief({ ...brief, objective: 'Más distribuidores' }), action: 'ask', reply: '¿Qué productos quieres destacar para los distribuidores?' };
  } });
  await resumed.resume(); assert.equal(calls, 0, 'sin una respuesta del cliente no manda preguntas nuevas');
  await resumed.receive({ leadRef: s.leadRef, messageId: 'next-day', message: { content: 'Quiero más distribuidores', mediaType: 'text' } });
  assert.equal(calls, 1); assert.equal(replies.length, 1);
  assert.equal((await s.leadRef.get()).data().sampleAgent.brief.companyName, 'Una Mascota');
});

test('la bienvenida fija acompaña una pregunta específica cuando ya conocemos el nombre', async () => {
  let sent = 0;
  const s = setup(async ({ audioOptions }) => {
    assert.equal(audioOptions[0].moment, 'welcome');
    return { brief: normalizeConversationBrief({ companyName: 'Una Mascota' }), action: 'ask', reply: '¿Qué productos fabricas?', audioMoment: '' };
  }, undefined, { loadAudioOptions: async () => [{ moment: 'welcome', script: 'Hola, vamos paso a paso.' }], sendFlowAudio: async () => { sent++; return true; } });
  await s.receive('Hola, mi negocio es Una Mascota', '1');
  assert.equal(sent, 1); assert.deepEqual(s.replies, ['¿Qué productos fabricas?']);
});

test('conserva el orden de mensajes recibidos en el mismo milisegundo', async () => {
  let time = 1_792_690_000_000;
  const s = setup(async ({ message }) => {
    assert.equal(message.text, 'Sí\nPero cambia el nombre\nAhora es Mascota Verde');
    return { brief: { ...complete(), companyName: 'Mascota Verde' }, action: 'confirm', reply: '' };
  }, undefined, { debounceMs: 4000, now: () => time });
  await s.receive('Sí', 'z'); await s.receive('Pero cambia el nombre', 'a'); await s.receive('Ahora es Mascota Verde', 'b');
  time += 4000; await s.agent.resume();
  assert.equal(s.jobs.length, 0); assert.match(s.replies[0], /Mascota Verde/);
});


test('la bienvenida con pregunta tiene prioridad sobre negocio y no duplica la pregunta por texto', async () => {
  const sent = [];
  const welcome = 'Hola, soy Sergio. ¿Cómo se llama tu negocio y a qué se dedica?';
  const s = setup(async () => ({ brief: normalizeConversationBrief({}), action: 'ask', reply: '¿Cómo se llama tu negocio?', audioMoment: 'business' }), undefined, {
    loadAudioOptions: async () => [{ moment: 'welcome', script: welcome }, { moment: 'business', script: 'Cuéntame de tu negocio.' }],
    sendFlowAudio: async ({ moment }) => { sent.push(moment); return true; },
  });
  await s.receive('Quiero información sobre la página web', '1');
  assert.deepEqual(sent, ['welcome']); assert.equal(s.replies.length, 0);
  assert.equal((await s.leadRef.get()).data().sampleAgent.history.at(-1).text, welcome);
});

test('si falla la bienvenida fija conserva la pregunta por texto y permite continuar', async () => {
  const s = setup(async () => ({ brief: normalizeConversationBrief({}), action: 'ask', reply: '¿Cómo se llama tu negocio?' }), undefined, {
    loadAudioOptions: async () => [{ moment: 'welcome', script: 'Hola. ¿Cómo se llama tu negocio?' }],
    sendFlowAudio: async () => { throw Error('WhatsApp no disponible'); },
  });
  await s.receive('Hola', '1');
  assert.deepEqual(s.replies, ['¿Cómo se llama tu negocio?']);
});

test('la bienvenida fija acompaña el resumen cuando el primer mensaje ya trae toda la información', async () => {
  const sent = [];
  const s = setup(async () => ({ brief: complete(), action: 'confirm', reply: '' }), undefined, {
    loadAudioOptions: async () => [{ moment: 'welcome', script: 'Hola, te ayudo con tu muestra.' }, { moment: 'confirmation', script: 'Revisa el resumen.' }],
    sendFlowAudio: async ({ moment }) => { sent.push(moment); return true; },
  });
  await s.receive('Datos completos de mi negocio', '1');
  assert.deepEqual(sent, ['welcome']);
  assert.match(s.replies[0], /Así prepararé tu muestra/);
});


test('el resumen de doce fotos se mantiene breve y no enumera descripciones internas', () => {
  const brief = { ...complete(), images: Array.from({ length: 12 }, (_, index) => ({ index, role: 'work', description: 'Tinaco y calentador solar; uso en la web por confirmar. '.repeat(5) })) };
  const summary = briefSummary(brief);
  assert.ok(summary.length < 600);
  assert.match(summary, /hasta 3 de tus fotos/);
  assert.doesNotMatch(summary, /Tinaco|por confirmar|Diferenciadores|Objetivo/);
});

test('los colores exclusivos de un logo pendiente no completan los colores de la web', () => {
  const brief = { ...complete(), colorPreference: 'Amarillo y azul exclusivamente para el logo. Colores de la web sin definir.' };
  assert.match(missingBriefQuestion(brief), /colores.*página/);
});
