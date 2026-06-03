// services/hotLeadDetector.js
//
// Detector de "respuestas calientes": cuando un lead responde por WhatsApp,
// clasifica la intencion comercial, pausa el agente de reactivacion para que
// el bot no se meta en una conversacion de cierre, y si la intencion es alta
// crea una tarea para el vendedor + etiqueta el lead.
//
// Diseno:
//  - Funciona SIN IA (fallback por palabras clave) para que aporte valor aunque
//    no haya OPENAI_API_KEY. Si hay key, la IA refina la clasificacion.
//  - Nunca lanza: cualquier error se traga y se loggea. El pipeline de WhatsApp
//    no se debe romper por esto.
//
import admin from 'firebase-admin';
import { db } from '../firebaseAdmin.js';

const { FieldValue } = admin.firestore;

const HOT_TAG = 'RespuestaCaliente';
const TASK_SOURCE = 'ai_hot_reply';
const TASK_DEDUPE_HOURS = Math.max(1, Number(process.env.HOT_LEAD_TASK_DEDUPE_HOURS || 12));
const AI_MODEL = String(process.env.HOT_LEAD_AI_MODEL || 'gpt-4o-mini').trim() || 'gpt-4o-mini';
const AI_DISABLED = String(process.env.HOT_LEAD_AI || '').trim().toLowerCase() === 'off';

function cleanText(value = '', max = 600) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function normalizeForMatch(value = '') {
  return String(value || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase();
}

function firstName(value = '') {
  const raw = cleanText(value);
  return raw ? raw.split(' ')[0] : '';
}

const VALID_INTEREST = new Set(['hot', 'warm', 'cold', 'lost']);
const VALID_INTENT = new Set([
  'wants_price',
  'wants_examples',
  'ready_to_buy',
  'needs_time',
  'not_now',
  'no_interest',
  'question',
  'other',
]);

// ----------------------------- Fallback por keywords -----------------------------

const STOP_RE = /\b(no me interesa|ya no me interesa|ya no|no gracias|no quiero|no, gracias|deja de|dejen de|deja(r)? de escribir|no insistas|elimina(r|me)?|dar de baja|darme de baja|stop)\b/;
const HOT_RE = /(precio|costo|cuanto cuesta|cuanto vale|cuanto seria|cuanto es|cu[aá]nto|cotiza|cotizar|presupuesto|pagar|como pago|forma de pago|contratar|lo quiero|me interesa|estoy interesad|quiero (la|el|una|empezar|avanzar|contratar)|agendar|agenda|cita|llamada|comprar|factura|anticipo|deposito|transferencia|como empiezo|empezar|cuando podemos|listo para)/;
const WARM_RE = /(info|informacion|informaci[oó]n|ejemplo|ejemplos|muestra|portafolio|me puedes mandar|mandame|envia|env[ií]ame|ver|dudas|pregunta|me interesa saber|que incluye|como funciona)/;

function keywordClassify(text = '') {
  const t = normalizeForMatch(text);
  if (!t) {
    return { hot: false, interestLevel: 'cold', intent: 'other', summary: '', suggestedReply: '', source: 'keyword' };
  }
  if (STOP_RE.test(t)) {
    return { hot: false, interestLevel: 'lost', intent: 'no_interest', summary: 'El lead pide no continuar.', suggestedReply: '', source: 'keyword' };
  }
  if (HOT_RE.test(t)) {
    const intent = /(precio|costo|cuanto|cotiza|cotizar|presupuesto|pagar|pago)/.test(t)
      ? 'wants_price'
      : 'ready_to_buy';
    return { hot: true, interestLevel: 'hot', intent, summary: 'Mensaje con intencion de compra/precio.', suggestedReply: '', source: 'keyword' };
  }
  if (WARM_RE.test(t)) {
    return { hot: false, interestLevel: 'warm', intent: 'wants_examples', summary: 'El lead pide informacion o ejemplos.', suggestedReply: '', source: 'keyword' };
  }
  return { hot: false, interestLevel: 'cold', intent: 'other', summary: '', suggestedReply: '', source: 'keyword' };
}

// ----------------------------- Clasificador IA (OpenAI 3.x) -----------------------------

let cachedOpenAi = null;
let openAiUnavailable = false;

async function getOpenAi() {
  if (AI_DISABLED) return null;
  if (openAiUnavailable) return null;
  if (cachedOpenAi) return cachedOpenAi;
  if (!process.env.OPENAI_API_KEY) {
    openAiUnavailable = true;
    return null;
  }
  try {
    const { Configuration, OpenAIApi } = await import('openai');
    const configuration = new Configuration({ apiKey: process.env.OPENAI_API_KEY });
    cachedOpenAi = new OpenAIApi(configuration);
    return cachedOpenAi;
  } catch (error) {
    console.warn('[hot-lead] OpenAI no disponible:', error?.message || error);
    openAiUnavailable = true;
    return null;
  }
}

function buildBusinessContext(lead = {}) {
  const parts = [];
  if (lead?.giro) parts.push(`Giro: ${cleanText(lead.giro, 120)}`);
  if (lead?.negocio) parts.push(`Negocio: ${cleanText(lead.negocio, 120)}`);
  if (lead?.estado) parts.push(`Estado en CRM: ${cleanText(lead.estado, 60)}`);
  if (lead?.etapaNombre || lead?.etapa) parts.push(`Etapa: ${cleanText(lead.etapaNombre || lead.etapa, 60)}`);
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas.slice(0, 8).join(', ') : '';
  if (tags) parts.push(`Etiquetas: ${cleanText(tags, 160)}`);
  return parts.join(' | ');
}

function parseAiJson(raw = '') {
  if (!raw) return null;
  let text = String(raw).trim();
  text = text.replace(/^```(?:json)?/i, '').replace(/```$/i, '').trim();
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start === -1 || end === -1 || end <= start) return null;
  try {
    return JSON.parse(text.slice(start, end + 1));
  } catch {
    return null;
  }
}

function coerceClassification(parsed = {}) {
  const interestLevel = VALID_INTEREST.has(parsed.interestLevel) ? parsed.interestLevel : 'cold';
  const intent = VALID_INTENT.has(parsed.intent) ? parsed.intent : 'other';
  const hot = parsed.hot === true || interestLevel === 'hot';
  return {
    hot,
    interestLevel,
    intent,
    summary: cleanText(parsed.summary || '', 240),
    suggestedReply: cleanText(parsed.suggestedReply || '', 600),
    source: 'ai',
  };
}

async function aiClassify({ lead = {}, recentMessages = [], latestText = '' }) {
  const openai = await getOpenAi();
  if (!openai) return null;

  const history = recentMessages
    .slice(-12)
    .map((m) => `${m.sender === 'lead' ? 'Cliente' : 'Nosotros'}: ${cleanText(m.content || '', 300)}`)
    .filter((line) => line.length > (line.startsWith('Cliente: ') ? 9 : 9))
    .join('\n');

  const businessContext = buildBusinessContext(lead);
  const nombre = firstName(lead?.nombre || '') || 'el cliente';

  const system = [
    'Eres un asistente comercial para una agencia que vende paginas web, campanas de Meta Ads y software a la medida en Mexico.',
    'Clasificas la intencion comercial de la ULTIMA respuesta del cliente por WhatsApp.',
    'Responde SOLO con JSON valido, sin texto extra, con estas claves:',
    '{"interestLevel":"hot|warm|cold|lost","intent":"wants_price|wants_examples|ready_to_buy|needs_time|not_now|no_interest|question|other","hot":true|false,"summary":"resumen corto en espanol","suggestedReply":"un solo mensaje breve y natural en espanol de Mexico, sin emojis excesivos, sin links inventados, listo para enviar"}',
    'hot=true solo si el cliente muestra intencion real de avanzar, comprar, pedir precio o agendar.',
    'Si el cliente pide no continuar, interestLevel="lost" e intent="no_interest".',
  ].join('\n');

  const user = [
    businessContext ? `Contexto del lead: ${businessContext}` : '',
    `Nombre: ${nombre}`,
    history ? `Conversacion reciente:\n${history}` : '',
    `Ultima respuesta del cliente: "${cleanText(latestText, 500)}"`,
  ].filter(Boolean).join('\n\n');

  try {
    const response = await openai.createChatCompletion({
      model: AI_MODEL,
      temperature: 0.3,
      max_tokens: 320,
      messages: [
        { role: 'system', content: system },
        { role: 'user', content: user },
      ],
    });
    const content = response?.data?.choices?.[0]?.message?.content || '';
    const parsed = parseAiJson(content);
    if (!parsed) return null;
    return coerceClassification(parsed);
  } catch (error) {
    console.warn('[hot-lead] Error IA:', error?.response?.data?.error?.message || error?.message || error);
    return null;
  }
}

export async function classifyLeadReply({ lead = {}, recentMessages = [], latestText = '' } = {}) {
  const text = cleanText(latestText, 1000);
  const fallback = keywordClassify(text);
  const ai = await aiClassify({ lead, recentMessages, latestText: text });
  if (!ai) return fallback;

  // La IA manda, pero si el fallback detecto un STOP explicito lo respetamos.
  if (fallback.interestLevel === 'lost' && ai.interestLevel !== 'lost') {
    return { ...ai, interestLevel: 'lost', intent: 'no_interest', hot: false };
  }
  return ai;
}

// ----------------------------- Lectura de mensajes recientes -----------------------------

async function loadRecentMessages(leadRef, limit = 12) {
  try {
    const snap = await leadRef
      .collection('messages')
      .orderBy('timestamp', 'desc')
      .limit(limit)
      .get();
    return snap.docs
      .map((d) => d.data() || {})
      .reverse()
      .map((m) => ({ sender: m.sender === 'lead' ? 'lead' : 'business', content: cleanText(m.content || '', 400) }))
      .filter((m) => m.content);
  } catch (error) {
    console.warn('[hot-lead] No se pudieron leer mensajes recientes:', error?.message || error);
    return [];
  }
}

// ----------------------------- Tarea para el vendedor -----------------------------

function intentLabel(intent = '') {
  const map = {
    wants_price: 'pide precio',
    wants_examples: 'pide ejemplos/info',
    ready_to_buy: 'listo para avanzar',
    needs_time: 'pide tiempo',
    not_now: 'ahora no',
    no_interest: 'sin interes',
    question: 'tiene una duda',
    other: 'respondio',
  };
  return map[intent] || 'respondio';
}

async function hasRecentOpenTask(leadId) {
  try {
    const cutoff = new Date(Date.now() - TASK_DEDUPE_HOURS * 60 * 60 * 1000);
    const snap = await db
      .collection('tasks')
      .where('leadId', '==', String(leadId))
      .where('source', '==', TASK_SOURCE)
      .limit(10)
      .get();
    return snap.docs.some((d) => {
      const data = d.data() || {};
      const status = String(data.status || 'pendiente').toLowerCase();
      if (status === 'completada') return false;
      const createdMs = data.createdAt?.toMillis?.() || 0;
      return createdMs >= cutoff.getTime();
    });
  } catch (error) {
    console.warn('[hot-lead] No se pudo verificar tareas previas:', error?.message || error);
    return false;
  }
}

async function createHotLeadTask({ leadId, lead, classification }) {
  if (await hasRecentOpenTask(leadId)) {
    return { created: false, reason: 'dedupe' };
  }

  const nombre = cleanText(lead?.nombre || '', 120) || 'Lead';
  const assignedTo = String(lead?.assignedTo || process.env.HOT_LEAD_DEFAULT_ASSIGNEE || '').trim();
  const title = `🔥 ${nombre} ${intentLabel(classification.intent)} — contactar`;
  const descriptionParts = [
    classification.summary ? `Resumen IA: ${classification.summary}` : '',
    classification.suggestedReply ? `Respuesta sugerida: ${classification.suggestedReply}` : '',
    `Intencion: ${classification.intent} | Nivel: ${classification.interestLevel}`,
  ].filter(Boolean);

  await db.collection('tasks').add({
    title: cleanText(title, 180),
    description: cleanText(descriptionParts.join('\n'), 2000),
    status: 'pendiente',
    dueDate: '',
    assignedTo,
    assignedToName: assignedTo ? '' : 'Sin asignar',
    createdBy: 'system',
    createdByName: 'Detector IA',
    leadId: String(leadId),
    leadName: nombre,
    leadPhone: cleanText(lead?.telefono || '', 60),
    source: TASK_SOURCE,
    catalogItemId: '',
    catalogItemName: '',
    serviceId: '',
    templateId: '',
    active: true,
    aiIntent: classification.intent,
    aiInterestLevel: classification.interestLevel,
    createdAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  });

  return { created: true };
}

// ----------------------------- Orquestador (entry point) -----------------------------

/**
 * Procesa la respuesta de un lead. Nunca lanza.
 * @returns {Promise<object>} resultado de la clasificacion y acciones.
 */
export async function handleInboundLeadReply({ leadRef, leadId, leadData = {}, latestText = '' } = {}) {
  try {
    const text = cleanText(latestText, 1000);
    if (!text || !leadId) {
      return { ok: false, reason: 'no_text' };
    }
    const ref = leadRef || db.collection('leads').doc(String(leadId));
    const recentMessages = await loadRecentMessages(ref, 12);
    const classification = await classifyLeadReply({ lead: leadData, recentMessages, latestText: text });

    const leadPatch = {
      aiReply: {
        hot: Boolean(classification.hot),
        interestLevel: classification.interestLevel,
        intent: classification.intent,
        summary: classification.summary || '',
        suggestedReply: classification.suggestedReply || '',
        source: classification.source,
        model: classification.source === 'ai' ? AI_MODEL : 'keyword',
        lastText: cleanText(text, 400),
        classifiedAt: FieldValue.serverTimestamp(),
      },
    };

    let taskResult = { created: false };
    if (classification.hot) {
      leadPatch.etiquetas = FieldValue.arrayUnion(HOT_TAG);
      // Pausar SOLO el agente de reactivacion 24/7 para no pisar el cierre humano.
      leadPatch['aiFollowup.paused'] = true;
      taskResult = await createHotLeadTask({ leadId, lead: leadData, classification });
    }

    await ref.set(leadPatch, { merge: true });

    return {
      ok: true,
      classification,
      taskCreated: taskResult.created === true,
    };
  } catch (error) {
    console.warn('[hot-lead] handleInboundLeadReply error:', error?.message || error);
    return { ok: false, reason: 'error', error: String(error?.message || error) };
  }
}
