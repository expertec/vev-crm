import { Configuration, OpenAIApi } from 'openai';
import {
  AWARENESS_LEVELS,
  FACT_KEYS,
  INTEREST_LEVELS,
  INTENTS,
  OBJECTIONS,
  SALES_BRAIN_ANALYSIS_VERSION,
  SALES_STAGES,
  SENTIMENTS,
  SIGNALS,
  enumOr,
  uniqueAllowed,
} from './catalog.js';

const AI_MODEL = String(process.env.SALES_BRAIN_AI_MODEL || process.env.HOT_LEAD_AI_MODEL || 'gpt-4o-mini').trim() || 'gpt-4o-mini';
const AI_DISABLED = String(process.env.SALES_BRAIN_AI || process.env.HOT_LEAD_AI || '').trim().toLowerCase() === 'off';
const MAX_HISTORY_MESSAGES = 12;

let cachedOpenAi = null;
let openAiUnavailable = false;

function cleanText(value = '', max = 1000) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function normalizeForMatch(value = '') {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
}

function firstName(value = '') {
  const raw = cleanText(value, 120);
  return raw ? raw.split(' ')[0] : '';
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

async function getOpenAi() {
  if (AI_DISABLED || openAiUnavailable) return null;
  if (cachedOpenAi) return cachedOpenAi;
  if (!process.env.OPENAI_API_KEY) {
    openAiUnavailable = true;
    return null;
  }
  try {
    const configuration = new Configuration({ apiKey: process.env.OPENAI_API_KEY });
    cachedOpenAi = new OpenAIApi(configuration);
    return cachedOpenAi;
  } catch (error) {
    console.warn('[SalesBrain] analysis:openai_unavailable', error?.message || error);
    openAiUnavailable = true;
    return null;
  }
}

const STOP_RE = /\b(no me interesa|ya no me interesa|no gracias|no quiero|deja de|dejen de|dejar de escribir|no insistas|elimina|dar de baja|stop)\b/i;
const PRICE_RE = /(precio|costo|cuanto cuesta|cuanto vale|cuanto seria|cuanto es|cu[aá]nto|cotiza|cotizar|presupuesto|pagar|pago)/i;
const PAYMENT_RE = /(pagar|pago|transferencia|deposito|dep[oó]sito|anticipo|tarjeta|link de pago|datos de pago|clabe|cuenta)/i;
const EXAMPLES_RE = /(ejemplo|ejemplos|muestra|portafolio|trabajos|casos|ver paginas|ver webs)/i;
const START_RE = /(como empiezo|como empezamos|como inicio|quiero empezar|quiero avanzar|lo quiero|contratar|anticipo|deposito|transferencia|datos de pago|link de pago)/i;
const INFO_RE = /(info|informacion|informaci[oó]n|que incluye|como funciona|me interesa saber|dudas|pregunta)/i;
const TRUST_RE = /(desconf|no confio|estafa|fraude|mala experiencia|me quedaron mal|otra agencia|sin resultados|no funciono|me fallaron)/i;
const TIME_RE = /(luego|despues|despu[eé]s|no tengo tiempo|mas tarde|la otra semana|pr[oó]ximo mes|ahorita no)/i;
const AUTO_REPLY_RE = /(mensaje automatico|respuesta automatica|asistente virtual|soy (el|un|una|tu) asistente|soy una ia|soy un bot|gracias por contactar|hemos recibido|en breve|horario de atencion|menu principal|selecciona una opcion)/i;

const BUSINESS_PATTERNS = [
  ['restaurant', /(restaurante|comida|taqueria|taquería|cafeteria|cafetería|bar|cocina|pizzeria|pizza)/i],
  ['barbershop', /(barberia|barbería|barbero|salon|salón|estetica|estética)/i],
  ['clinic', /(clinica|clínica|consultorio|doctor|dentista|medico|m[eé]dico|terapia)/i],
  ['real_estate', /(inmobiliaria|bienes raices|bienes raíces|renta|venta de casas|terrenos)/i],
  ['store', /(tienda|boutique|ropa|zapatos|ecommerce|e-commerce|productos)/i],
  ['service_business', /(servicio|servicios|plomeria|plomería|electricista|limpieza|taller|reparacion|reparación)/i],
];

function detectBusinessType(text = '') {
  for (const [value, pattern] of BUSINESS_PATTERNS) {
    if (pattern.test(text)) return value;
  }
  return null;
}

function detectPrimaryNeed(text = '') {
  const t = normalizeForMatch(text);
  if (/(mas clientes|conseguir clientes|atraer clientes|generar clientes|vender mas|ventas)/.test(t)) return 'more_customers';
  if (/(pagina web|sitio web|web|landing)/.test(t)) return 'website';
  if (/(publicidad|anuncios|facebook ads|meta ads|campana|campaña)/.test(t)) return 'advertising';
  if (/(sistema|software|crm|automatizar|automatizacion)/.test(t)) return 'software';
  return null;
}

function detectCustomerAcquisition(text = '') {
  const t = normalizeForMatch(text);
  if (/(recomendacion|recomendado|boca en boca|referido|clientes de siempre)/.test(t)) return 'recommendations';
  if (/(facebook|instagram|redes|tiktok|whatsapp)/.test(t)) return 'social_media';
  if (/(google|maps|busqueda)/.test(t)) return 'google';
  if (/(anuncios|publicidad|ads|campana|campaña)/.test(t)) return 'paid_ads';
  if (/(volante|lonas|local|calle)/.test(t)) return 'offline';
  return null;
}

function buildFallbackAnalysis({ lead = {}, latestText = '' } = {}) {
  const text = cleanText(latestText, 1000);
  const normalized = normalizeForMatch(text);
  const signals = ['answered'];
  let intent = 'other';
  let interestLevel = 'cold';
  let salesStage = 'discovery';
  let objection = 'none';
  let sentiment = 'neutral';
  let awareness = 'unknown';
  let hot = false;
  let automated = AUTO_REPLY_RE.test(normalized);

  if (automated) signals.push('automated_reply');
  if (STOP_RE.test(normalized)) {
    intent = 'no_interest';
    interestLevel = 'lost';
    salesStage = 'lost';
    signals.push('no_interest', 'stop_requested');
    sentiment = 'negative';
  } else if (START_RE.test(normalized)) {
    intent = 'ready_to_buy';
    interestLevel = 'hot';
    salesStage = 'closing';
    signals.push('ready_to_buy', 'asks_how_to_start', 'wants_to_buy');
    hot = true;
    awareness = 'product_aware';
  } else if (PRICE_RE.test(normalized)) {
    intent = 'wants_price';
    interestLevel = 'hot';
    salesStage = 'evaluation';
    signals.push('asked_price');
    hot = true;
    awareness = 'solution_aware';
  } else if (EXAMPLES_RE.test(normalized)) {
    intent = 'wants_examples';
    interestLevel = 'warm';
    salesStage = 'evaluation';
    signals.push('asked_examples');
    awareness = 'solution_aware';
  } else if (INFO_RE.test(normalized)) {
    intent = 'wants_information';
    interestLevel = 'warm';
    salesStage = 'education';
    awareness = 'problem_aware';
  }

  if (PAYMENT_RE.test(normalized)) signals.push('asked_payment_method');
  if (intent === 'question' || /\?/.test(text)) signals.push('commercial_question');

  if (TRUST_RE.test(normalized)) {
    objection = normalized.includes('agencia') || normalized.includes('experiencia') ? 'bad_previous_experience' : 'trust';
    sentiment = 'skeptical';
    signals.push('trust_objection');
    if (objection === 'bad_previous_experience') signals.push('previous_bad_agency_experience');
  } else if (PRICE_RE.test(normalized) && /(caro|mucho|precio|presupuesto)/.test(normalized)) {
    objection = 'price';
    signals.push('price_objection');
  } else if (TIME_RE.test(normalized)) {
    objection = 'time';
    sentiment = interestLevel === 'cold' ? 'neutral' : sentiment;
    signals.push('time_objection');
  }

  const businessType = detectBusinessType(text) || lead?.salesState?.businessType || null;
  const primaryNeed = detectPrimaryNeed(text) || lead?.salesState?.primaryNeed || null;
  const customerAcquisition = detectCustomerAcquisition(text);
  if (businessType) signals.push('business_identified');
  if (primaryNeed) signals.push('primary_need_identified');
  if (customerAcquisition) signals.push('customer_acquisition_identified');
  if (String(lead?.source || '').toLowerCase() === 'meta_ads' || lead?.lastMetaAttribution) signals.push('meta_ad');

  const facts = {};
  if (businessType && detectBusinessType(text)) {
    facts.businessType = { value: businessType, confidence: 0.9, source: 'explicit' };
  }
  if (primaryNeed && detectPrimaryNeed(text)) {
    facts.primaryNeed = { value: primaryNeed, confidence: 0.8, source: 'explicit' };
    facts.primaryGoal = { value: primaryNeed, confidence: 0.8, source: 'explicit' };
  }
  if (customerAcquisition) {
    facts.customerAcquisition = { value: customerAcquisition, confidence: 0.75, source: 'explicit' };
  }
  if (signals.includes('previous_bad_agency_experience')) {
    facts.previousAgency = { value: true, confidence: 0.85, source: 'explicit' };
    facts.previousBadExperience = { value: true, confidence: 0.85, source: 'explicit' };
  }

  return coerceAnalysis({
    interestLevel,
    intent,
    hot,
    automated,
    summary: text ? `Ultima respuesta: ${text.slice(0, 180)}` : '',
    businessType,
    primaryNeed,
    salesStage,
    awareness,
    objection,
    sentiment,
    signals,
    facts,
    source: 'keyword',
    model: 'keyword',
  });
}

function coerceFactEntry(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'object' && !Array.isArray(value)) {
    const entryValue = value.value;
    if (entryValue === null || entryValue === undefined || entryValue === '') return null;
    return {
      value: entryValue,
      confidence: Math.max(0, Math.min(1, Number(value.confidence || 0.6))),
      source: String(value.source || 'inferred') === 'explicit' ? 'explicit' : 'inferred',
    };
  }
  return { value, confidence: 0.6, source: 'inferred' };
}

function coerceFacts(rawFacts = {}) {
  const facts = {};
  if (!rawFacts || typeof rawFacts !== 'object' || Array.isArray(rawFacts)) return facts;
  for (const key of FACT_KEYS) {
    const entry = coerceFactEntry(rawFacts[key]);
    if (entry) facts[key] = entry;
  }
  return facts;
}

export function coerceAnalysis(parsed = {}) {
  const intent = enumOr(parsed.intent, INTENTS, 'other');
  const interestLevel = enumOr(parsed.interestLevel, INTEREST_LEVELS, 'cold');
  const signals = uniqueAllowed(parsed.signals, SIGNALS);
  const hot = parsed.hot === true || interestLevel === 'hot' || intent === 'ready_to_buy' || intent === 'asks_how_to_start';

  return {
    interestLevel,
    intent,
    hot,
    automated: parsed.automated === true || signals.includes('automated_reply'),
    summary: cleanText(parsed.summary || '', 420),
    businessType: cleanText(parsed.businessType || '', 80) || null,
    primaryNeed: cleanText(parsed.primaryNeed || '', 120) || null,
    salesStage: enumOr(parsed.salesStage, SALES_STAGES, 'discovery'),
    awareness: enumOr(parsed.awareness, AWARENESS_LEVELS, 'unknown'),
    objection: enumOr(parsed.objection, OBJECTIONS, 'none'),
    sentiment: enumOr(parsed.sentiment, SENTIMENTS, 'neutral'),
    signals,
    facts: coerceFacts(parsed.facts || {}),
    source: parsed.source === 'ai' ? 'ai' : 'keyword',
    model: cleanText(parsed.model || (parsed.source === 'ai' ? AI_MODEL : 'keyword'), 80),
    analysisVersion: SALES_BRAIN_ANALYSIS_VERSION,
  };
}

function buildBusinessContext(lead = {}) {
  const parts = [];
  if (lead?.giro) parts.push(`Giro: ${cleanText(lead.giro, 120)}`);
  if (lead?.negocio) parts.push(`Negocio: ${cleanText(lead.negocio, 120)}`);
  if (lead?.estado) parts.push(`Estado CRM: ${cleanText(lead.estado, 60)}`);
  if (lead?.etapaNombre || lead?.etapa) parts.push(`Etapa CRM: ${cleanText(lead.etapaNombre || lead.etapa, 80)}`);
  if (lead?.salesState) parts.push(`SalesState previo: ${JSON.stringify(lead.salesState).slice(0, 800)}`);
  if (lead?.conversationMemory?.summary) parts.push(`Memoria: ${cleanText(lead.conversationMemory.summary, 600)}`);
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas.slice(0, 10).join(', ') : '';
  if (tags) parts.push(`Etiquetas: ${cleanText(tags, 180)}`);
  return parts.join('\n');
}

function buildAcquisitionContextText(acquisitionContext = {}) {
  const parts = [];
  if (acquisitionContext.source) parts.push(`source=${cleanText(acquisitionContext.source, 80)}`);
  if (acquisitionContext.campaign) parts.push(`campaign=${cleanText(acquisitionContext.campaign, 160)}`);
  if (acquisitionContext.adset) parts.push(`adset=${cleanText(acquisitionContext.adset, 160)}`);
  if (acquisitionContext.ad) parts.push(`ad=${cleanText(acquisitionContext.ad, 160)}`);
  if (acquisitionContext.ctwaClid) parts.push(`ctwaClid=${cleanText(acquisitionContext.ctwaClid, 160)}`);
  return parts.join(' | ');
}

async function aiAnalyze({ lead = {}, recentMessages = [], latestText = '', acquisitionContext = {} } = {}) {
  const openai = await getOpenAi();
  if (!openai) return null;

  const history = recentMessages
    .slice(-MAX_HISTORY_MESSAGES)
    .map((m) => `${m.sender === 'lead' ? 'Cliente' : 'Nosotros'}: ${cleanText(m.content || '', 300)}`)
    .filter((line) => line.length > 12)
    .join('\n');

  const system = [
    'Eres un analizador comercial para un CRM de ventas por WhatsApp en Mexico.',
    'Tu unica tarea es ENTENDER la conversacion. No redactes respuesta de venta.',
    'Devuelve SOLO JSON valido con campos controlados.',
    'Campos:',
    '{"interestLevel":"hot|warm|cold|lost","intent":"wants_information|wants_price|wants_examples|ready_to_buy|asks_how_to_start|needs_time|not_now|no_interest|question|other","hot":true|false,"automated":true|false,"summary":"resumen comercial breve","businessType":"tipo de negocio o null","primaryNeed":"necesidad principal o null","salesStage":"new|discovery|education|evaluation|closing|won|lost","awareness":"unaware|problem_aware|solution_aware|product_aware|most_aware|unknown","objection":"none|price|trust|time|bad_previous_experience|needs_approval|not_ready|other","sentiment":"positive|neutral|skeptical|negative|confused","signals":["..."],"facts":{}}',
    `signals permitidas: ${SIGNALS.join(', ')}`,
    `facts permitidos: ${FACT_KEYS.join(', ')}`,
    'facts solo para hechos objetivos. Usa {"value":X,"confidence":0-1,"source":"explicit|inferred"}.',
    'Extrae contexto de dueños de negocio local en lenguaje simple: businessType, customerAcquisition, currentSituation, primaryGoal, painPoint, hasWebsite, runsAds, previousExperience.',
    'No uses jerga como funnel, ROAS, CAC, conversion o pipeline en el resumen ni en hechos.',
    'No guardes opiniones como facts. Objeciones y sentimiento van en objection/sentiment.',
    'automated=true si parece bot, asistente automatico, menu u horario.',
  ].join('\n');

  const user = [
    buildBusinessContext(lead),
    buildAcquisitionContextText(acquisitionContext) ? `Adquisicion: ${buildAcquisitionContextText(acquisitionContext)}` : '',
    `Nombre: ${firstName(lead?.nombre || '') || 'cliente'}`,
    history ? `Conversacion reciente:\n${history}` : '',
    `Ultima respuesta del cliente: "${cleanText(latestText, 800)}"`,
  ].filter(Boolean).join('\n\n');

  try {
    console.log('[SalesBrain] analysis:start');
    const response = await openai.createChatCompletion({
      model: AI_MODEL,
      temperature: 0.2,
      max_tokens: 520,
      messages: [
        { role: 'system', content: system },
        { role: 'user', content: user },
      ],
    });
    const content = response?.data?.choices?.[0]?.message?.content || '';
    const parsed = parseAiJson(content);
    if (!parsed) return null;
    console.log('[SalesBrain] analysis:complete');
    return coerceAnalysis({ ...parsed, source: 'ai', model: AI_MODEL });
  } catch (error) {
    console.warn('[SalesBrain] analysis:error', error?.response?.data?.error?.message || error?.message || error);
    return null;
  }
}

export async function analyzeConversation({
  lead = {},
  recentMessages = [],
  latestText = '',
  acquisitionContext = {},
} = {}) {
  const fallback = buildFallbackAnalysis({ lead, latestText });
  const ai = await aiAnalyze({ lead, recentMessages, latestText, acquisitionContext });
  if (!ai) return fallback;

  if (fallback.interestLevel === 'lost' && ai.interestLevel !== 'lost') {
    return {
      ...ai,
      automated: ai.automated || fallback.automated,
      interestLevel: 'lost',
      intent: 'no_interest',
      hot: false,
      signals: uniqueAllowed([...ai.signals, ...fallback.signals], SIGNALS),
    };
  }

  return {
    ...ai,
    automated: ai.automated || fallback.automated,
    signals: uniqueAllowed([...ai.signals, ...fallback.signals], SIGNALS),
  };
}
