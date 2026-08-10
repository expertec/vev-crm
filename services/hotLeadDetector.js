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
import { buildSampleFormLink, hasLeadCompletedForm } from './leadReactivationService.js';
import {
  analyzeConversation,
  buildAcquisitionContext,
  runSalesBrainForInbound,
  resolveSalesBrainMode,
} from './salesBrain/index.js';
import { SALES_BRAIN_MODES } from './salesBrain/catalog.js';
import { ROUTING_STATUSES, updateRoutingAfterInbound } from './salesQueue/index.js';

const { FieldValue } = admin.firestore;

const AUTO_FORM_LINK_MODE = String(process.env.AUTO_FORM_LINK || '').trim().toLowerCase();
const AUTO_FORM_ENABLED = ['1', 'true', 'yes', 'on'].includes(AUTO_FORM_LINK_MODE);
const AUTO_FORM_RESERVATION_TTL_MS = Math.max(
  60_000,
  Number(process.env.AUTO_FORM_RESERVATION_TTL_MS || 6 * 60 * 60 * 1000)
);
const AUTO_FORM_RECENT_OUTBOUND_MS = Math.max(
  60_000,
  Number(process.env.AUTO_FORM_RECENT_OUTBOUND_MS || 30 * 60 * 1000)
);
const POSITIVE_INTENTS = new Set(['wants_information', 'wants_examples', 'wants_price', 'ready_to_buy', 'asks_how_to_start', 'question']);

function leadAlreadyGotFormLink(lead = {}) {
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas.map((t) => normalizeForMatch(t)) : [];
  return tags.includes('formlinksent') || Boolean(lead?.formLinkSentAt);
}

function toMillis(value) {
  if (!value) return 0;
  if (value instanceof Date) return value.getTime() || 0;
  if (typeof value?.toMillis === 'function') return value.toMillis() || 0;
  if (typeof value?.toDate === 'function') return value.toDate()?.getTime?.() || 0;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function hasActiveSequences(lead = {}) {
  if (lead?.hasActiveSequences === true) return true;
  return Array.isArray(lead?.secuenciasActivas)
    && lead.secuenciasActivas.some((item) => item?.completed !== true);
}

function cleanSavePath(value = '') {
  const path = String(value || '').trim();
  if (!path || path.length > 160) return '';
  if (!/^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*$/.test(path)) return '';
  return path;
}

async function applyPendingSequenceQuestionReply({ leadRef, leadData = {}, latestText = '', routing = null } = {}) {
  const latestSnap = await leadRef.get().catch(() => null);
  const currentLead = latestSnap?.exists ? { ...(latestSnap.data() || {}) } : leadData;
  const pending = currentLead?.sequenceQuestionPending && typeof currentLead.sequenceQuestionPending === 'object'
    ? currentLead.sequenceQuestionPending
    : null;
  if (!pending || pending.status !== 'waiting_for_reply') return { applied: false };

  const saveTo = cleanSavePath(pending.saveTo || '');
  const rawSaveTo = saveTo.startsWith('salesContext.')
    ? `salesContextRaw.${saveTo.slice('salesContext.'.length)}`
    : '';
  const requiresAgent = routing?.status === ROUTING_STATUSES.READY_FOR_AGENT || routing?.status === ROUTING_STATUSES.FOLLOWUP;
  const trigger = String(pending.trigger || '').trim();
  const index = Number.isFinite(Number(pending.index)) ? Number(pending.index) : null;

  const secuencias = Array.isArray(currentLead.secuenciasActivas)
    ? currentLead.secuenciasActivas.map((seq) => ({ ...seq }))
    : [];
  const nextSequences = secuencias.map((seq) => {
    if (String(seq?.trigger || '') !== trigger || Number(seq?.index) !== index) return seq;
    if (requiresAgent) return { ...seq, status: 'paused_for_agent', pausedAt: new Date().toISOString() };
    return {
      ...seq,
      status: 'running',
      index: Number(index || 0) + 1,
      startTime: new Date().toISOString(),
    };
  });

  const patch = {
    sequenceQuestionPending: FieldValue.delete(),
    secuenciasActivas: nextSequences,
    hasActiveSequences: nextSequences.some((seq) => seq?.completed !== true && seq?.status !== 'paused_for_agent'),
  };
  if (saveTo) {
    const pathParts = saveTo.split('.');
    const existingValue = pathParts.reduce((acc, key) => (
      acc && typeof acc === 'object' ? acc[key] : undefined
    ), currentLead);
    if (existingValue === undefined || existingValue === null || existingValue === '') {
      patch[saveTo] = latestText;
    }
  }
  if (rawSaveTo) patch[rawSaveTo] = latestText;
  if (requiresAgent) {
    patch.nextSequenceRunAt = FieldValue.delete();
  } else {
    patch.nextSequenceRunAt = new Date();
  }

  await leadRef.update(patch);
  return { applied: true, continued: !requiresAgent, saveTo };
}

function hasRecentHumanOrAutomationTouch(lead = {}, refMs = Date.now()) {
  const lastOutboundMs = Math.max(
    toMillis(lead?.lastOutboundAt),
    toMillis(lead?.formLinkSentAt),
    toMillis(lead?.autoFormLinkReservedAt)
  );
  return lastOutboundMs > 0 && (refMs - lastOutboundMs) < AUTO_FORM_RECENT_OUTBOUND_MS;
}

function hasFreshAutoFormReservation(lead = {}, refMs = Date.now()) {
  const reservedMs = toMillis(lead?.autoFormLinkReservedAt);
  if (!reservedMs) return false;
  if (lead?.autoFormLinkStatus === 'failed') return false;
  return (refMs - reservedMs) < AUTO_FORM_RESERVATION_TTL_MS;
}

function buildFormLinkMessage(lead = {}, url = '') {
  const nombre = firstName(lead?.nombre || '');
  const saludo = nombre ? `Hola ${nombre}, ` : 'Hola, ';
  return `${saludo}para armarte tu muestra de pagina GRATIS solo llena este formulario corto (toma 2 min) y yo te la preparo y te la mando por aqui: ${url}`;
}

// Decide si conviene mandar el formulario de muestra automaticamente.
function decideAutoFormLink(lead = {}, classification = {}) {
  if (!AUTO_FORM_ENABLED) return null;
  if (classification.automated === true) return null;
  if (classification.interestLevel === 'lost' || classification.intent === 'no_interest') return null;

  const positive = POSITIVE_INTENTS.has(classification.intent)
    || classification.interestLevel === 'hot'
    || classification.interestLevel === 'warm';
  if (!positive) return null;

  if (hasLeadCompletedForm(lead)) return null;
  if (leadAlreadyGotFormLink(lead)) return null;
  if (hasActiveSequences(lead)) return null;
  if (hasRecentHumanOrAutomationTouch(lead)) return null;
  if (hasFreshAutoFormReservation(lead)) return null;

  const url = buildSampleFormLink(lead);
  if (!url) return null;

  return { url, message: buildFormLinkMessage(lead, url) };
}

async function reserveAutoFormLink({ leadRef, leadId, leadData = {}, classification = {} } = {}) {
  const candidate = decideAutoFormLink(leadData, classification);
  if (!candidate || !leadRef) return null;

  const reserved = await db.runTransaction(async (tx) => {
    const snap = await tx.get(leadRef);
    if (!snap.exists) return null;
    const current = { id: snap.id, ...(snap.data() || {}) };
    const freshCandidate = decideAutoFormLink(current, classification);
    if (!freshCandidate) return null;

    tx.set(leadRef, {
      autoFormLinkReservedAt: FieldValue.serverTimestamp(),
      autoFormLinkStatus: 'reserved',
      autoFormLinkUrl: freshCandidate.url,
      autoFormLinkSourceMessageId: String(classification?.inputMessageId || ''),
    }, { merge: true });

    return freshCandidate;
  });

  if (reserved) {
    console.log(`[auto-form-link] reservado para ${leadId}`);
  }
  return reserved;
}

const HOT_TAG = 'RespuestaCaliente';
const AUTO_TAG = 'RespuestaAutomatica';
const TASK_SOURCE = 'ai_hot_reply';
const AUTO_TASK_SOURCE = 'ai_auto_responder';
const TASK_DEDUPE_HOURS = Math.max(1, Number(process.env.HOT_LEAD_TASK_DEDUPE_HOURS || 12));
const AI_MODEL = String(process.env.HOT_LEAD_AI_MODEL || 'gpt-4o-mini').trim() || 'gpt-4o-mini';
const AI_DISABLED = String(process.env.HOT_LEAD_AI || '').trim().toLowerCase() === 'off';
const HOT_LEAD_AI_ENABLED = ['1', 'true', 'yes', 'on'].includes(
  String(process.env.HOT_LEAD_AI || process.env.SALES_BRAIN_AI || '').trim().toLowerCase()
);
const HOT_LEAD_TASKS_ENABLED = ['1', 'true', 'yes', 'on'].includes(
  String(process.env.HOT_LEAD_TASKS || process.env.HOT_LEAD_CREATE_TASKS || '').trim().toLowerCase()
);

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
  'wants_information',
  'wants_price',
  'wants_examples',
  'ready_to_buy',
  'asks_how_to_start',
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

// Frases tipicas de bots / asistentes automaticos / contestadoras.
const AUTO_REPLY_RE = new RegExp([
  'mensaje automatico', 'respuesta automatica', 'es un mensaje automatico',
  'asistente virtual', 'soy (el|un|una|tu) asistente', 'asistente de ', 'soy (una|la) ia',
  'inteligencia artificial', 'soy un bot', 'bot de ',
  'gracias por (contactar|comunicarte|escribir|tu mensaje|tu interes)',
  'hemos recibido tu mensaje', 'tu mensaje (ha sido|fue) recibido', 'recibimos tu mensaje',
  'en breve (te|un) ', 'a la brevedad', 'uno de nuestros (asesores|agentes|representantes|ejecutivos)',
  'te atenderemos', 'te contactaremos', 'en cuanto (estemos|un asesor|un agente)',
  'horario de atencion', 'fuera de (nuestro )?horario', 'nuestro horario es',
  'para (una )?mejor atencion', 'marca la opcion', 'responde con el numero',
  'escribe el numero', 'selecciona una opcion', 'menu principal',
  'este numero no (recibe|atiende)', 'no se atienden llamadas',
].join('|'));

function detectAutomatedReplyByKeyword(text = '') {
  const t = normalizeForMatch(text);
  if (!t) return false;
  return AUTO_REPLY_RE.test(t);
}

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
    automated: parsed.automated === true,
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
    '{"interestLevel":"hot|warm|cold|lost","intent":"wants_price|wants_examples|ready_to_buy|needs_time|not_now|no_interest|question|other","hot":true|false,"automated":true|false,"summary":"resumen corto en espanol","suggestedReply":"un solo mensaje breve y natural en espanol de Mexico, sin emojis excesivos, sin links inventados, listo para enviar"}',
    'hot=true solo si el cliente muestra intencion real de avanzar, comprar, pedir precio o agendar.',
    'automated=true si la respuesta parece de un bot, asistente automatico o contestadora (texto generico, "gracias por contactarnos", "un asesor te atendera", menus, horarios), NO de una persona escribiendo en el momento.',
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
  const keywordAuto = detectAutomatedReplyByKeyword(text);

  const fallback = keywordClassify(text);
  fallback.automated = keywordAuto;

  if (!HOT_LEAD_AI_ENABLED) return fallback;

  const analysis = await analyzeConversation({
    lead,
    recentMessages,
    latestText: text,
    acquisitionContext: buildAcquisitionContext(lead),
  });
  if (!analysis) return fallback;

  const automated = analysis.automated === true || keywordAuto;
  const classification = {
    hot: Boolean(analysis.hot),
    interestLevel: VALID_INTEREST.has(analysis.interestLevel) ? analysis.interestLevel : fallback.interestLevel,
    intent: VALID_INTENT.has(analysis.intent) ? analysis.intent : fallback.intent,
    automated,
    summary: cleanText(analysis.summary || fallback.summary || '', 240),
    suggestedReply: '',
    source: analysis.source || 'keyword',
    model: analysis.model || (analysis.source === 'ai' ? AI_MODEL : 'keyword'),
    salesBrainAnalysis: analysis,
  };

  // Si keywords detectaron STOP explicito, se respeta sobre cualquier lectura IA.
  if (fallback.interestLevel === 'lost' && classification.interestLevel !== 'lost') {
    return {
      ...classification,
      interestLevel: 'lost',
      intent: 'no_interest',
      hot: false,
    };
  }

  return classification;
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
    wants_information: 'pide informacion',
    wants_price: 'pide precio',
    wants_examples: 'pide ejemplos/info',
    ready_to_buy: 'listo para avanzar',
    asks_how_to_start: 'pregunta como empezar',
    needs_time: 'pide tiempo',
    not_now: 'ahora no',
    no_interest: 'sin interes',
    question: 'tiene una duda',
    other: 'respondio',
  };
  return map[intent] || 'respondio';
}

async function hasRecentOpenTask(leadId, source = TASK_SOURCE) {
  try {
    const cutoff = new Date(Date.now() - TASK_DEDUPE_HOURS * 60 * 60 * 1000);
    const snap = await db
      .collection('tasks')
      .where('leadId', '==', String(leadId))
      .where('source', '==', source)
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

// Tarea de alerta cuando un bot/IA contesta por el lead (el cliente quiza no ve los mensajes).
async function createAutoResponderTask({ leadId, lead, sample }) {
  if (await hasRecentOpenTask(leadId, AUTO_TASK_SOURCE)) {
    return { created: false, reason: 'dedupe' };
  }

  const nombre = cleanText(lead?.nombre || '', 120) || 'Lead';
  const assignedTo = String(lead?.assignedTo || process.env.HOT_LEAD_DEFAULT_ASSIGNEE || '').trim();
  const description = [
    'Este numero respondio con lo que parece un bot / asistente automatico.',
    'Riesgo: el cliente real podria NO estar viendo tus mensajes.',
    'Sugerencia: intenta llamarle, mandar nota de voz, o pedir hablar con la persona encargada.',
    sample ? `Respuesta recibida: "${cleanText(sample, 240)}"` : '',
  ].filter(Boolean).join('\n');

  await db.collection('tasks').add({
    title: cleanText(`🤖 ${nombre}: parece bot/IA contestando — contactar directo`, 180),
    description: cleanText(description, 2000),
    status: 'pendiente',
    dueDate: '',
    assignedTo,
    assignedToName: assignedTo ? '' : 'Sin asignar',
    createdBy: 'system',
    createdByName: 'Detector IA',
    leadId: String(leadId),
    leadName: nombre,
    leadPhone: cleanText(lead?.telefono || '', 60),
    source: AUTO_TASK_SOURCE,
    catalogItemId: '',
    catalogItemName: '',
    serviceId: '',
    templateId: '',
    active: true,
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
export async function handleInboundLeadReply({
  leadRef,
  leadId,
  leadData = {},
  latestText = '',
  inputMessageId = '',
} = {}) {
  try {
    const text = cleanText(latestText, 1000);
    if (!text || !leadId) {
      return { ok: false, reason: 'no_text' };
    }
    const ref = leadRef || db.collection('leads').doc(String(leadId));
    const recentMessages = await loadRecentMessages(ref, 12);
    const classification = await classifyLeadReply({ lead: leadData, recentMessages, latestText: text });
    const salesBrainMode = resolveSalesBrainMode(leadData);

    const isAutomated = classification.automated === true;

    const leadPatch = {
      aiReply: {
        hot: Boolean(classification.hot) && !isAutomated,
        interestLevel: classification.interestLevel,
        intent: classification.intent,
        automated: isAutomated,
        summary: classification.summary || '',
        suggestedReply: classification.suggestedReply || '',
        source: classification.source,
        model: classification.model || (classification.source === 'ai' ? AI_MODEL : 'keyword'),
        lastText: cleanText(text, 400),
        classifiedAt: FieldValue.serverTimestamp(),
      },
    };

    let taskResult = { created: false };
    let autoTaskResult = { created: false };

    if (HOT_LEAD_TASKS_ENABLED && isAutomated) {
      // El que contesta es un bot/IA: NO es cliente real, NO marcar caliente.
      // Avisar al usuario para que contacte por otro canal.
      leadPatch.etiquetas = FieldValue.arrayUnion(AUTO_TAG);
      leadPatch.autoResponder = {
        detected: true,
        sample: cleanText(text, 300),
        lastDetectedAt: FieldValue.serverTimestamp(),
      };
      autoTaskResult = await createAutoResponderTask({ leadId, lead: leadData, sample: text });
    } else if (HOT_LEAD_TASKS_ENABLED && classification.hot) {
      leadPatch.etiquetas = FieldValue.arrayUnion(HOT_TAG);
      // Pausar SOLO el agente de reactivacion 24/7 para no pisar el cierre humano.
      leadPatch['aiFollowup.paused'] = true;
      taskResult = await createHotLeadTask({ leadId, lead: leadData, classification });
    }

    await ref.set(leadPatch, { merge: true });

    // A: ¿conviene mandar el formulario de muestra automaticamente?
    // Se reserva contra el lead fresco para evitar duplicados con mensajes simultaneos.
    let autoFormLink = null;
    if (!isAutomated && salesBrainMode !== SALES_BRAIN_MODES.COPILOT) {
      autoFormLink = await reserveAutoFormLink({
        leadRef: ref,
        leadId,
        leadData,
        classification: { ...classification, inputMessageId },
      });
    }

    let salesBrain = { ok: true, skipped: true, reason: 'mode_off' };
    if (salesBrainMode === SALES_BRAIN_MODES.COPILOT) {
      salesBrain = await runSalesBrainForInbound({
        leadRef: ref,
        leadId,
        leadData,
        latestText: text,
        inputMessageId,
        recentMessages,
        analysis: classification.salesBrainAnalysis,
      });
    }

    const routingResult = await updateRoutingAfterInbound({
      leadRef: ref,
      leadId,
      leadData,
      latestText: text,
      analysis: classification.salesBrainAnalysis || salesBrain.analysis || {},
      salesBrain,
    });

    const sequenceQuestion = await applyPendingSequenceQuestionReply({
      leadRef: ref,
      leadData,
      latestText: text,
      routing: routingResult.routing,
    }).catch((sequenceError) => {
      console.warn('[hot-lead] sequence question reply error:', sequenceError?.message || sequenceError);
      return { applied: false, error: String(sequenceError?.message || sequenceError) };
    });

    return {
      ok: true,
      classification,
      automated: isAutomated,
      taskCreated: taskResult.created === true,
      autoResponderTaskCreated: autoTaskResult.created === true,
      autoFormLink,
      salesBrain,
      routing: routingResult,
      sequenceQuestion,
    };
  } catch (error) {
    console.warn('[hot-lead] handleInboundLeadReply error:', error?.message || error);
    return { ok: false, reason: 'error', error: String(error?.message || error) };
  }
}
