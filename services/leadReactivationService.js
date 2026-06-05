import crypto from 'node:crypto';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import { Timestamp, FieldValue } from 'firebase-admin/firestore';

dayjs.extend(utc);
dayjs.extend(timezone);

const DEFAULT_TIMEZONE = String(process.env.CRM_TIMEZONE || 'America/Monterrey').trim() || 'America/Monterrey';
const DEFAULT_MIN_SILENCE_HOURS = Math.max(12, Number(process.env.AI_REACTIVATION_MIN_SILENCE_HOURS || 24));
const DEFAULT_BASE_DELAY_MINUTES = Math.max(1, Number(process.env.AI_REACTIVATION_BASE_DELAY_MINUTES || 3));
const DEFAULT_SPACING_SECONDS = Math.max(45, Number(process.env.AI_REACTIVATION_SPACING_SECONDS || 95));
const DEFAULT_MAX_TOUCHES = Math.max(1, Number(process.env.AI_REACTIVATION_MAX_TOUCHES || 6));
const DEFAULT_LIMIT_PER_RUN = Math.max(1, Number(process.env.AI_REACTIVATION_LIMIT_PER_RUN || 40));
const DEFAULT_CADENCE_HOURS = [24, 72, 168];
const DEFAULT_TARGET_STAGES = ['leads_nuevos', 'interesados_01', 'seguimiento'];
// Anti-baneo: horario habil, tope diario por numero y warm-up (arranque gradual).
const DEFAULT_SEND_WINDOW_START_HOUR = Math.min(23, Math.max(0, Math.floor(Number(process.env.AI_REACTIVATION_WINDOW_START ?? 9))));
const DEFAULT_SEND_WINDOW_END_HOUR = Math.min(24, Math.max(1, Math.floor(Number(process.env.AI_REACTIVATION_WINDOW_END ?? 20))));
const DEFAULT_DAILY_CAP = Math.max(1, Math.floor(Number(process.env.AI_REACTIVATION_DAILY_CAP || 60)));
const DEFAULT_WARMUP_ENABLED = String(process.env.AI_REACTIVATION_WARMUP || 'on').trim().toLowerCase() !== 'off';
const DEFAULT_WARMUP_START_CAP = Math.max(1, Math.floor(Number(process.env.AI_REACTIVATION_WARMUP_START_CAP || 20)));
const DEFAULT_WARMUP_DAILY_INCREMENT = Math.max(0, Math.floor(Number(process.env.AI_REACTIVATION_WARMUP_INCREMENT || 10)));
const LAST_WEEK_SOURCE = 'last-week-reactivation';
const ALWAYS_ON_SOURCE = 'always-on-reactivation';
const SETTINGS_COLLECTION = 'automationSettings';
const SETTINGS_DOC_ID = 'leadReactivation24x7';
const COUNTER_COLLECTION = 'automationCounters';
const LOCK_COLLECTION = 'automationLocks';
const LOCK_DOC_ID = 'leadReactivation24x7';
const LOCK_TTL_MS = 8 * 60 * 1000;

// Muestra YA generada (el lead lleno el form y tiene slug): reenvia el sitio.
const SAMPLE_READY_VARIANTS = [
  'Hola {{nombre}}, te reenvio tu muestra para que la veas con calma. Si te gusta, hoy mismo la dejamos lista: {{link}}',
  '{{nombre}}, echale un ojo otra vez a tu muestra y me dices si avanzamos: {{link}}',
  'Hola {{nombre}}, aqui esta de nuevo el enlace de tu muestra, cualquier ajuste lo vemos juntos: {{link}}',
];

// Form de muestra GRATIS enviado pero NO llenado: invita a completarlo (gancho real).
const FORM_INVITE_VARIANTS = [
  'Hola {{nombre}}, te habia mandado el formulario para hacerte tu muestra de pagina GRATIS y vi que aun no lo llenas. Son 2 minutos y con eso te la armo: {{link}}',
  '{{nombre}}, para hacerte tu muestra gratis solo necesito que llenes este formulario corto, asi la dejo a la medida de tu negocio: {{link}}',
  'Hola {{nombre}}, aun puedo hacerte tu muestra de pagina sin costo. Llena aqui tus datos y yo me encargo de todo lo demas: {{link}}',
];

// Biblioteca de "angulos" de seguimiento. Cada toque (cada dia) usa un angulo
// distinto para mantener al lead presente sin repetir el mismo texto: mejor
// conversion y mucho menor riesgo de baneo. Se rota por numero de toque.
// {{nombre}} = primer nombre | {{tema}} = lo que pidio | {{link}} = su muestra.
const FOLLOWUP_ANGLES = [
  {
    key: 'reintro',
    requiresLink: false,
    variants: [
      'Hola {{nombre}}, te escribo de NegociosWeb. Quedamos pendientes con {{tema}} y se que entre tantos mensajes a veces se traspapela. Lo retomamos?',
      'Hola {{nombre}}, no quiero que se te pase: te habia compartido {{tema}}. Sigo disponible para ayudarte a dejarlo listo cuando gustes.',
      'Hola {{nombre}}, paso a recordarte lo de {{tema}}. Se que pediste info a varios, por eso te doy seguimiento para no dejarte colgado.',
    ],
  },
  {
    key: 'muestra',
    requiresLink: true,
    variants: SAMPLE_READY_VARIANTS,
  },
  {
    key: 'beneficio',
    requiresLink: false,
    variants: [
      'Hola {{nombre}}, una pagina propia hace que tu negocio se vea mas serio y te lleguen clientes incluso mientras duermes. Te ayudo a tenerla?',
      '{{nombre}}, tener presencia en linea es lo que hace que te elijan a ti y no al de junto. Aun te puedo apoyar para dejar la tuya lista.',
    ],
  },
  {
    key: 'prueba_social',
    requiresLink: false,
    variants: [
      'Hola {{nombre}}, esta semana dejamos lista la pagina de otro negocio como el tuyo y ya esta recibiendo mensajes. Me encantaria hacer lo mismo por ti.',
      '{{nombre}}, varios clientes que al inicio dudaban hoy ya venden mas con su pagina. Te muestro como quedaria la tuya?',
    ],
  },
  {
    key: 'pregunta',
    requiresLink: false,
    variants: [
      'Hola {{nombre}}, para no insistir de mas: que te detiene? Es tema de precio, de tiempo o tienes alguna duda? Lo resolvemos rapido.',
      '{{nombre}}, que te sirve mas: que te mande precio, ejemplos o que platiquemos por llamada? Tu dime y le seguimos.',
    ],
  },
  {
    key: 'oferta',
    requiresLink: false,
    variants: [
      'Hola {{nombre}}, este mes tengo un espacio para dejar tu proyecto listo con condiciones especiales. Lo aprovechamos?',
      '{{nombre}}, si lo retomamos esta semana te puedo dar un mejor precio por iniciar ahora. Te paso los detalles?',
    ],
  },
  {
    key: 'escasez',
    requiresLink: false,
    variants: [
      'Hola {{nombre}}, voy cerrando los proyectos de este mes y queria apartarte un lugar antes de llenarme. Avanzamos?',
      '{{nombre}}, me quedan pocos espacios para entregar este mes. Si quieres te aparto el tuyo y arrancamos.',
    ],
  },
  {
    key: 'breakup',
    requiresLink: false,
    variants: [
      'Hola {{nombre}}, no quiero llenarte de mensajes, asi que cierro el seguimiento por ahora. Si mas adelante retomas {{tema}}, aqui estoy para apoyarte.',
      '{{nombre}}, te dejo de escribir por ahora para no molestar. Cuando quieras retomar {{tema}}, me mandas un mensaje y seguimos.',
    ],
  },
];

const FALLBACK_ANGLE_KEY = 'beneficio';

// Catalogo completo de los textos automatizados de reactivacion (para revision
// de copy / informe BI). Devuelve cada angulo del seguimiento diario y las
// variantes de muestra/formulario.
export function getReactivationMessageCatalog() {
  const angles = FOLLOWUP_ANGLES.map((a) => ({
    key: a.key,
    requiresLink: Boolean(a.requiresLink),
    variants: a.key === 'muestra'
      ? ['(dinamico: usa los textos de muestra/formulario de abajo)']
      : (Array.isArray(a.variants) ? a.variants : []),
  }));
  return {
    dailyAngles: angles,
    sampleReadyVariants: [...SAMPLE_READY_VARIANTS],
    formInviteVariants: [...FORM_INVITE_VARIANTS],
  };
}

function getSampleSiteBaseUrl() {
  return String(
    process.env.SAMPLE_SITE_BASE_URL
      || process.env.SITE_PUBLIC_BASE_URL
      || 'https://negociosweb.mx/site'
  ).replace(/\/+$/, '');
}

export function resolveSampleSlug(lead = {}) {
  const candidate = [
    lead?.slug,
    lead?.webSlug,
    lead?.siteSlug,
    lead?.briefWeb?.slug,
    lead?.schema?.slug,
  ].find((v) => String(v || '').trim());
  return String(candidate || '').trim();
}

export function buildSampleLink(lead = {}) {
  const slug = resolveSampleSlug(lead);
  if (!slug) return '';
  return `${getSampleSiteBaseUrl()}/${encodeURIComponent(slug)}`;
}

function resolveTema(contextKey = 'generic') {
  if (contextKey === 'web_sent') return 'tu pagina web';
  if (contextKey === 'sample_sent') return 'tu muestra';
  return 'la informacion que pediste';
}

function getSampleFormBaseUrl() {
  return String(
    process.env.SAMPLE_FORM_BASE_URL
      || process.env.PUBLIC_SAMPLE_FORM_URL
      || process.env.NEXT_PUBLIC_SITE_URL
      || 'https://negociosweb.mx'
  ).replace(/\/+$/, '');
}

function resolvePhoneDigits(lead = {}) {
  const fromPhone = String(lead?.telefono || '').replace(/\D/g, '');
  if (fromPhone.length >= 10) return fromPhone;
  const jid = String(lead?.resolvedJid || lead?.jid || '');
  const match = jid.match(/(\d{10,15})@/);
  if (match) return match[1];
  return fromPhone;
}

// Link al formulario de muestra GRATIS (lo que el lead debe llenar).
export function buildSampleFormLink(lead = {}) {
  const digits = resolvePhoneDigits(lead);
  if (!digits || digits.length < 10) return '';
  return `${getSampleFormBaseUrl()}/muestra/${encodeURIComponent(digits)}`;
}

// El lead ya lleno el formulario de muestra?
export function hasLeadCompletedForm(lead = {}) {
  const etapa = String(lead?.etapa || lead?.etapaNombre || '').toLowerCase();
  if (etapa === 'form_submitted') return true;
  const tags = Array.isArray(lead?.etiquetas)
    ? lead.etiquetas.map((t) => String(t || '').toLowerCase())
    : [];
  return tags.includes('formok') || tags.includes('formulariocompletado');
}

function hashToInt(value = '') {
  const digest = crypto.createHash('sha256').update(String(value || '')).digest('hex').slice(0, 8);
  return Number.parseInt(digest, 16) || 0;
}

function pickVariantIndex(seed = '', size = 1) {
  if (!Number.isFinite(size) || size <= 1) return 0;
  return hashToInt(seed) % size;
}

function cleanText(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function firstName(value = '') {
  const raw = cleanText(value);
  return raw ? raw.split(' ')[0] : '';
}

function renderTemplate(template = '', vars = {}) {
  let text = String(template || '')
    .replace(/\{\{nombre\}\}/g, vars.nombre || '')
    .replace(/\{\{tema\}\}/g, vars.tema || '')
    .replace(/\{\{link\}\}/g, vars.link || '');
  // Cuando no hay nombre, evitar "Hola , ..." o ", ..." al inicio.
  text = text.replace(/^Hola\s*,/i, 'Hola,');
  text = text.replace(/^\s*,\s*/, '');
  text = cleanText(text.replace(/\s([,.!?;:])/g, '$1'));
  if (text) text = text.charAt(0).toUpperCase() + text.slice(1);
  return text;
}

function asPositiveNumber(value, fallback = 0, min = 0) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, parsed);
}

function normalizeCadenceHours(input) {
  const values = Array.isArray(input)
    ? input
    : String(input || '')
      .split(/[,\s;]+/)
      .map((item) => item.trim())
      .filter(Boolean);

  const normalized = values
    .map((item) => Number(item))
    .filter((item) => Number.isFinite(item) && item >= 12)
    .map((item) => Math.floor(item));

  if (normalized.length === 0) return [...DEFAULT_CADENCE_HOURS];
  return normalized.slice(0, 12);
}

function normalizeStageToken(value = '') {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function normalizeTargetStages(input, fallback = DEFAULT_TARGET_STAGES) {
  const chunks = Array.isArray(input)
    ? input
    : String(input || '')
      .split(/[,\n;|]+/)
      .map((item) => item.trim())
      .filter(Boolean);

  const normalized = chunks
    .map((item) => normalizeStageToken(item))
    .filter(Boolean)
    .slice(0, 20);

  if (normalized.length === 0) return [...fallback];
  return [...new Set(normalized)];
}

function clone(value = null) {
  if (value === null || value === undefined) return null;
  return JSON.parse(JSON.stringify(value));
}

export function toMillis(value) {
  if (value === null || value === undefined || value === '') return 0;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (value instanceof Date) {
    const ms = value.getTime();
    return Number.isFinite(ms) ? ms : 0;
  }
  if (typeof value?.toMillis === 'function') {
    const ms = value.toMillis();
    return Number.isFinite(ms) ? ms : 0;
  }
  if (typeof value?.toDate === 'function') {
    const ms = value.toDate()?.getTime?.() || 0;
    return Number.isFinite(ms) ? ms : 0;
  }
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function safeStatus(value = '') {
  return cleanText(value).toLowerCase();
}

function isLeadArchived(lead = {}) {
  return lead?.isArchived === true || lead?.archived === true || Boolean(lead?.archivedAt);
}

function hasActiveSequences(lead = {}) {
  if (lead?.hasActiveSequences === true) return true;
  return Array.isArray(lead?.secuenciasActivas) && lead.secuenciasActivas.some((item) => item?.completed !== true);
}

function hasHardStop(lead = {}) {
  if (lead?.stopSequences === true || lead?.seqPaused === true) return true;
  const status = safeStatus(lead?.estado || '');
  if (['compro', 'cliente', 'ganado', 'closed_won', 'cerrado_ganado'].includes(status)) return true;

  const tags = Array.isArray(lead?.etiquetas)
    ? lead.etiquetas.map((item) => safeStatus(item))
    : [];
  return tags.includes('compro')
    || tags.includes('detenersecuencia')
    || tags.includes('stopsequences')
    || tags.includes('no_interesa')
    || tags.includes('nointeresa');
}

function hasReachableTarget(lead = {}) {
  const candidates = [
    String(lead?.resolvedJid || '').trim(),
    String(lead?.jid || '').trim(),
  ];
  if (candidates.some((item) => /@s\.whatsapp\.net$/i.test(item))) return true;

  const phoneDigits = String(lead?.telefono || '').replace(/\D/g, '');
  return phoneDigits.length >= 10;
}

function detectLeadContext(lead = {}) {
  const tags = Array.isArray(lead?.etiquetas)
    ? lead.etiquetas.map((item) => safeStatus(item))
    : [];
  const stage = safeStatus(lead?.etapa || lead?.etapaNombre || '');

  if (tags.includes('webenviada') || stage.includes('web_enviada') || stage.includes('web enviada')) {
    return 'web_sent';
  }
  if (tags.includes('muestralista') || stage.includes('muestra') || stage.includes('form_submitted')) {
    return 'sample_sent';
  }
  return 'generic';
}

function buildCampaignId(window) {
  return `last-week-${window.fromDate}_${window.toDate}`;
}

function serializeLead(docSnap) {
  return {
    id: docSnap.id,
    ...(docSnap.data() || {}),
  };
}

function getLeadActivityMs(lead = {}) {
  return Math.max(
    toMillis(lead?.lastInboundAt),
    toMillis(lead?.lastMessageAt),
    toMillis(lead?.fecha_creacion)
  );
}

function getLeadCreatedMs(lead = {}) {
  return toMillis(lead?.fecha_creacion);
}

const PRIORITY_MODES = new Set(['newest', 'oldest', 'stage']);

function normalizePriorityMode(value) {
  const safe = String(value || '').trim().toLowerCase();
  return PRIORITY_MODES.has(safe) ? safe : 'newest';
}

function clampInt(value, fallback, min, max) {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

function normalizeDateKey(value) {
  const safe = String(value || '').trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(safe) ? safe : '';
}

function resolveLeadStageKey(lead = {}) {
  const etapa = normalizeStageToken(lead?.etapa || lead?.etapaNombre || '');
  if (etapa) return etapa;
  return 'leads_nuevos';
}

function sortAlwaysOnCandidates(leads = [], targetStages = DEFAULT_TARGET_STAGES, priorityMode = 'newest') {
  const mode = normalizePriorityMode(priorityMode);
  const stageOrder = new Map();
  normalizeTargetStages(targetStages).forEach((stage, index) => {
    stageOrder.set(stage, index);
  });
  const defaultOrder = stageOrder.size + 1;

  const stageRank = (lead) => {
    const stage = resolveLeadStageKey(lead);
    return stageOrder.has(stage) ? stageOrder.get(stage) : defaultOrder;
  };

  return [...leads].sort((a, b) => {
    // Prioridad principal: lead mas nuevo (o mas viejo) por fecha de creacion.
    if (mode === 'newest' || mode === 'oldest') {
      const aCreated = getLeadCreatedMs(a);
      const bCreated = getLeadCreatedMs(b);
      if (aCreated !== bCreated) {
        return mode === 'newest' ? bCreated - aCreated : aCreated - bCreated;
      }
      // Empate por fecha: respeta el orden del embudo, luego actividad reciente.
      const stageDiff = stageRank(a) - stageRank(b);
      if (stageDiff !== 0) return stageDiff;
      return getLeadActivityMs(b) - getLeadActivityMs(a);
    }

    // mode === 'stage': comportamiento clasico (embudo primero).
    const stageDiff = stageRank(a) - stageRank(b);
    if (stageDiff !== 0) return stageDiff;
    return getLeadActivityMs(b) - getLeadActivityMs(a);
  });
}

function buildMessagePreview(message = '') {
  return cleanText(message).slice(0, 220);
}

function createDefaultSettings() {
  return {
    enabled: false,
    timezone: DEFAULT_TIMEZONE,
    minSilenceHours: DEFAULT_MIN_SILENCE_HOURS,
    baseDelayMinutes: DEFAULT_BASE_DELAY_MINUTES,
    spacingSeconds: DEFAULT_SPACING_SECONDS,
    maxTouches: DEFAULT_MAX_TOUCHES,
    limitPerRun: DEFAULT_LIMIT_PER_RUN,
    cadenceHours: [...DEFAULT_CADENCE_HOURS],
    targetStages: [...DEFAULT_TARGET_STAGES],
    priorityMode: 'newest',
    sendWindowStartHour: DEFAULT_SEND_WINDOW_START_HOUR,
    sendWindowEndHour: DEFAULT_SEND_WINDOW_END_HOUR,
    dailyCap: DEFAULT_DAILY_CAP,
    warmupEnabled: DEFAULT_WARMUP_ENABLED,
    warmupStartCap: DEFAULT_WARMUP_START_CAP,
    warmupDailyIncrement: DEFAULT_WARMUP_DAILY_INCREMENT,
    warmupStartDate: '',
    updatedAt: null,
    updatedBy: '',
    status: {
      lastRunAt: null,
      lastRunMode: '',
      lastRunSummary: null,
      lastError: '',
    },
  };
}

function normalizeSettingsInput(input = {}, previous = null) {
  const base = previous && typeof previous === 'object' ? previous : createDefaultSettings();
  const status = base?.status && typeof base.status === 'object'
    ? base.status
    : createDefaultSettings().status;

  return {
    enabled: input.enabled === undefined ? Boolean(base.enabled) : Boolean(input.enabled),
    timezone: cleanText(input.timezone || base.timezone || DEFAULT_TIMEZONE) || DEFAULT_TIMEZONE,
    minSilenceHours: Math.max(12, Math.floor(asPositiveNumber(input.minSilenceHours, base.minSilenceHours, 12))),
    baseDelayMinutes: Math.max(1, Math.floor(asPositiveNumber(input.baseDelayMinutes, base.baseDelayMinutes, 1))),
    spacingSeconds: Math.max(45, Math.floor(asPositiveNumber(input.spacingSeconds, base.spacingSeconds, 45))),
    maxTouches: Math.max(1, Math.floor(asPositiveNumber(input.maxTouches, base.maxTouches, 1))),
    limitPerRun: Math.max(1, Math.floor(asPositiveNumber(input.limitPerRun, base.limitPerRun, 1))),
    cadenceHours: normalizeCadenceHours(input.cadenceHours !== undefined ? input.cadenceHours : base.cadenceHours),
    targetStages: normalizeTargetStages(
      input.targetStages !== undefined ? input.targetStages : base.targetStages,
      DEFAULT_TARGET_STAGES
    ),
    priorityMode: normalizePriorityMode(input.priorityMode !== undefined ? input.priorityMode : base.priorityMode),
    sendWindowStartHour: clampInt(
      input.sendWindowStartHour !== undefined ? input.sendWindowStartHour : base.sendWindowStartHour,
      DEFAULT_SEND_WINDOW_START_HOUR, 0, 23
    ),
    sendWindowEndHour: clampInt(
      input.sendWindowEndHour !== undefined ? input.sendWindowEndHour : base.sendWindowEndHour,
      DEFAULT_SEND_WINDOW_END_HOUR, 1, 24
    ),
    dailyCap: Math.max(1, Math.floor(asPositiveNumber(input.dailyCap, base.dailyCap, 1))),
    warmupEnabled: input.warmupEnabled === undefined ? Boolean(base.warmupEnabled) : Boolean(input.warmupEnabled),
    warmupStartCap: Math.max(1, Math.floor(asPositiveNumber(input.warmupStartCap, base.warmupStartCap, 1))),
    warmupDailyIncrement: Math.max(0, Math.floor(asPositiveNumber(input.warmupDailyIncrement, base.warmupDailyIncrement, 0))),
    warmupStartDate: normalizeDateKey(input.warmupStartDate !== undefined ? input.warmupStartDate : base.warmupStartDate),
    updatedAt: base.updatedAt || null,
    updatedBy: String(base.updatedBy || ''),
    status: {
      lastRunAt: status.lastRunAt || null,
      lastRunMode: String(status.lastRunMode || ''),
      lastRunSummary: status.lastRunSummary || null,
      lastError: String(status.lastError || ''),
    },
  };
}

async function getDb(dbOverride = null) {
  if (dbOverride) return dbOverride;
  const { db } = await import('../firebaseAdmin.js');
  return db;
}

async function loadSettingsDoc(db) {
  const ref = db.collection(SETTINGS_COLLECTION).doc(SETTINGS_DOC_ID);
  const snap = await ref.get();
  const raw = snap.exists ? (snap.data() || {}) : {};
  const normalized = normalizeSettingsInput(raw, createDefaultSettings());
  return { ref, snap, settings: normalized };
}

async function acquireLock(db, lockId = LOCK_DOC_ID, ttlMs = LOCK_TTL_MS) {
  const ref = db.collection(LOCK_COLLECTION).doc(lockId);
  const nowMs = Date.now();
  const lockUntil = new Date(nowMs + Math.max(30_000, Number(ttlMs || LOCK_TTL_MS)));

  const acquired = await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const data = snap.exists ? (snap.data() || {}) : {};
    const lockedUntilMs = toMillis(data.lockedUntil);
    if (lockedUntilMs > nowMs) {
      return false;
    }

    tx.set(ref, {
      lockId,
      lockedAt: new Date(nowMs),
      lockedUntil: lockUntil,
      updatedAt: new Date(nowMs),
    }, { merge: true });
    return true;
  });

  return { acquired, ref };
}

async function releaseLock(ref) {
  if (!ref) return;
  await ref.set(
    {
      lockedUntil: new Date(0),
      updatedAt: new Date(),
    },
    { merge: true }
  ).catch(() => {});
}

export async function getLeadReactivationSettings({
  dbOverride = null,
} = {}) {
  const db = await getDb(dbOverride);
  const { settings } = await loadSettingsDoc(db);
  return settings;
}

export async function updateLeadReactivationSettings({
  enabled,
  timezone,
  minSilenceHours,
  baseDelayMinutes,
  spacingSeconds,
  maxTouches,
  limitPerRun,
  cadenceHours,
  targetStages,
  priorityMode,
  sendWindowStartHour,
  sendWindowEndHour,
  dailyCap,
  warmupEnabled,
  warmupStartCap,
  warmupDailyIncrement,
  warmupStartDate,
  updatedBy = '',
} = {}, {
  dbOverride = null,
} = {}) {
  const db = await getDb(dbOverride);
  const loaded = await loadSettingsDoc(db);

  const next = normalizeSettingsInput(
    {
      enabled,
      timezone,
      minSilenceHours,
      baseDelayMinutes,
      spacingSeconds,
      maxTouches,
      limitPerRun,
      cadenceHours,
      targetStages,
      priorityMode,
      sendWindowStartHour,
      sendWindowEndHour,
      dailyCap,
      warmupEnabled,
      warmupStartCap,
      warmupDailyIncrement,
      warmupStartDate,
    },
    loaded.settings
  );

  const payload = {
    ...next,
    updatedAt: Timestamp.now(),
    updatedBy: cleanText(updatedBy).slice(0, 120) || 'crm',
  };

  await loaded.ref.set(payload, { merge: true });
  return normalizeSettingsInput(payload, loaded.settings);
}

export function getPreviousCalendarWeekWindow({
  now = new Date(),
  timezone: tz = DEFAULT_TIMEZONE,
} = {}) {
  const base = dayjs(now).tz(tz);
  const startOfThisWeek = base.startOf('day').subtract((base.day() + 6) % 7, 'day');
  const start = startOfThisWeek.subtract(7, 'day');
  const endExclusive = startOfThisWeek;
  const endInclusive = endExclusive.subtract(1, 'day');

  const window = {
    timezone: tz,
    startDate: start.toDate(),
    endExclusiveDate: endExclusive.toDate(),
    fromDate: start.format('YYYY-MM-DD'),
    toDate: endInclusive.format('YYYY-MM-DD'),
    startIso: start.toISOString(),
    endExclusiveIso: endExclusive.toISOString(),
  };
  return {
    ...window,
    campaignId: buildCampaignId(window),
  };
}

export function getExplicitWindow({
  fromDate,
  toDate,
  timezone: tz = DEFAULT_TIMEZONE,
} = {}) {
  const safeFrom = cleanText(fromDate);
  const safeTo = cleanText(toDate);
  if (!safeFrom || !safeTo) {
    throw new Error('Debes indicar fromDate y toDate en formato YYYY-MM-DD.');
  }

  const start = dayjs.tz(`${safeFrom} 00:00:00`, tz);
  const endExclusive = dayjs.tz(`${safeTo} 00:00:00`, tz).add(1, 'day');
  if (!start.isValid() || !endExclusive.isValid()) {
    throw new Error('No se pudo interpretar el rango de fechas.');
  }
  if (endExclusive.isBefore(start)) {
    throw new Error('El rango de fechas es invalido.');
  }

  const window = {
    timezone: tz,
    startDate: start.toDate(),
    endExclusiveDate: endExclusive.toDate(),
    fromDate: start.format('YYYY-MM-DD'),
    toDate: endExclusive.subtract(1, 'day').format('YYYY-MM-DD'),
    startIso: start.toISOString(),
    endExclusiveIso: endExclusive.toISOString(),
  };
  return {
    ...window,
    campaignId: buildCampaignId(window),
  };
}

export function buildLeadFollowupVariant(lead = {}, {
  campaignId = '',
  contextKey = detectLeadContext(lead),
  touchIndex = 0,
} = {}) {
  const nombre = firstName(lead?.nombre || '');
  const safeTouch = Math.max(0, Math.floor(Number(touchIndex) || 0));
  let angle = FOLLOWUP_ANGLES[safeTouch % FOLLOWUP_ANGLES.length];

  // Resolver que "muestra" tiene el lead:
  //  - slug presente  -> muestra YA generada, reenviamos el sitio.
  //  - sin slug        -> el form de muestra GRATIS aun no se llena, reenviamos el form.
  const slug = resolveSampleSlug(lead);
  const sampleLink = slug ? buildSampleLink(lead) : '';
  const formLink = slug ? '' : buildSampleFormLink(lead);
  const link = sampleLink || formLink;

  let tema;
  if (slug || hasLeadCompletedForm(lead)) tema = resolveTema(contextKey);
  else if (formLink) tema = 'tu muestra de pagina gratis';
  else tema = resolveTema(contextKey);

  // Seleccion de variantes. El angulo "muestra" es dinamico segun el link disponible.
  let variants = angle.variants;
  if (angle.key === 'muestra') {
    if (sampleLink) {
      variants = SAMPLE_READY_VARIANTS;
    } else if (formLink) {
      variants = FORM_INVITE_VARIANTS;
    } else {
      angle = FOLLOWUP_ANGLES.find((a) => a.key === FALLBACK_ANGLE_KEY) || FOLLOWUP_ANGLES[0];
      variants = angle.variants;
    }
  } else if (angle.requiresLink && !link) {
    angle = FOLLOWUP_ANGLES.find((a) => a.key === FALLBACK_ANGLE_KEY) || FOLLOWUP_ANGLES[0];
    variants = angle.variants;
  }

  const seedBase = [
    campaignId,
    lead?.id || '',
    lead?.telefono || '',
    contextKey,
    angle.key,
    safeTouch,
  ].join('|');

  const variantIndex = pickVariantIndex(`${seedBase}:v`, variants.length);
  const message = renderTemplate(variants[variantIndex], { nombre, tema, link });

  return {
    contextKey,
    angleKey: angle.key,
    variationKey: `${angle.key}-t${safeTouch}-v${variantIndex}`,
    message: message.slice(0, 520),
  };
}

function evaluateLeadBase(lead = {}, {
  now = new Date(),
  minSilenceHours = DEFAULT_MIN_SILENCE_HOURS,
} = {}) {
  const createdMs = toMillis(lead?.fecha_creacion);
  const lastActivityMs = Math.max(getLeadActivityMs(lead), createdMs);
  const nowMs = toMillis(now) || Date.now();
  const minSilenceMs = Math.max(1, Number(minSilenceHours || DEFAULT_MIN_SILENCE_HOURS)) * 60 * 60 * 1000;

  if (!createdMs) return { eligible: false, reason: 'missing_created_at', createdMs, lastActivityMs };
  if (lead?.mergedInto) return { eligible: false, reason: 'merged_lead', createdMs, lastActivityMs };
  if (isLeadArchived(lead)) return { eligible: false, reason: 'archived', createdMs, lastActivityMs };
  if (hasHardStop(lead)) return { eligible: false, reason: 'hard_stop', createdMs, lastActivityMs };
  if (Number(lead?.unreadCount || 0) > 0) return { eligible: false, reason: 'has_unread_messages', createdMs, lastActivityMs };
  if (hasActiveSequences(lead)) return { eligible: false, reason: 'active_sequence', createdMs, lastActivityMs };
  if (!hasReachableTarget(lead)) return { eligible: false, reason: 'missing_target', createdMs, lastActivityMs };
  if ((nowMs - lastActivityMs) < minSilenceMs) return { eligible: false, reason: 'recent_activity', createdMs, lastActivityMs };

  return { eligible: true, reason: 'eligible', createdMs, lastActivityMs };
}

export function evaluateLeadForReactivation(lead = {}, {
  window,
  campaignId = window?.campaignId || '',
  now = new Date(),
  minSilenceHours = DEFAULT_MIN_SILENCE_HOURS,
} = {}) {
  const base = evaluateLeadBase(lead, { now, minSilenceHours });
  if (!base.eligible) return base;

  if (window?.startDate && base.createdMs < window.startDate.getTime()) {
    return { ...base, eligible: false, reason: 'outside_window' };
  }
  if (window?.endExclusiveDate && base.createdMs >= window.endExclusiveDate.getTime()) {
    return { ...base, eligible: false, reason: 'outside_window' };
  }

  const aiState = lead?.aiFollowup && typeof lead.aiFollowup === 'object' ? lead.aiFollowup : {};
  if (String(aiState?.lastCampaignId || '').trim() === String(campaignId || '').trim()) {
    return { ...base, eligible: false, reason: 'already_scheduled_in_campaign' };
  }

  return {
    ...base,
    contextKey: detectLeadContext(lead),
  };
}

export function evaluateLeadForAlwaysOn(lead = {}, {
  settings = createDefaultSettings(),
  now = new Date(),
} = {}) {
  const stageKey = resolveLeadStageKey(lead);
  const targetStages = normalizeTargetStages(settings?.targetStages, DEFAULT_TARGET_STAGES);
  if (!targetStages.includes(stageKey)) {
    return { eligible: false, reason: 'outside_target_stage', stageKey };
  }

  const cadenceHours = normalizeCadenceHours(settings?.cadenceHours);
  const aiState = lead?.aiFollowup && typeof lead.aiFollowup === 'object' ? lead.aiFollowup : {};
  if (aiState?.enabled === false || aiState?.paused === true) {
    return { eligible: false, reason: 'agent_paused' };
  }

  const touchCount = Math.max(0, Math.floor(asPositiveNumber(aiState?.touchCount, 0, 0)));
  const maxTouches = Math.max(1, Math.floor(asPositiveNumber(settings?.maxTouches, DEFAULT_MAX_TOUCHES, 1)));
  if (touchCount >= maxTouches) {
    return { eligible: false, reason: 'max_touches_reached' };
  }

  const requiredCadenceHours = cadenceHours[Math.min(touchCount, cadenceHours.length - 1)];
  const requiredSilenceHours = Math.max(
    asPositiveNumber(settings?.minSilenceHours, DEFAULT_MIN_SILENCE_HOURS, 12),
    requiredCadenceHours
  );
  const base = evaluateLeadBase(lead, { now, minSilenceHours: requiredSilenceHours });
  if (!base.eligible) return base;

  const nowMs = toMillis(now) || Date.now();
  const nextTouchMs = toMillis(aiState?.nextAiTouchAt);
  if (nextTouchMs && nextTouchMs > (nowMs - (15 * 60 * 1000))) {
    return { ...base, eligible: false, reason: 'waiting_next_touch' };
  }

  return {
    ...base,
    contextKey: detectLeadContext(lead),
    stageKey,
    touchCount,
    requiredSilenceHours,
  };
}

async function loadLeadsForWindow(db, window, { limit = 0 } = {}) {
  const collection = db.collection('leads');
  try {
    let q = collection
      .where('fecha_creacion', '>=', Timestamp.fromDate(window.startDate))
      .where('fecha_creacion', '<', Timestamp.fromDate(window.endExclusiveDate))
      .orderBy('fecha_creacion', 'asc');

    if (Number(limit) > 0) {
      q = q.limit(Number(limit));
    }

    const snap = await q.get();
    return {
      mode: 'range_query',
      queryError: '',
      leads: snap.docs.map(serializeLead),
    };
  } catch (error) {
    const scan = await collection.get();
    const filtered = scan.docs
      .map(serializeLead)
      .filter((lead) => {
        const createdMs = toMillis(lead?.fecha_creacion);
        return createdMs >= window.startDate.getTime() && createdMs < window.endExclusiveDate.getTime();
      })
      .sort((a, b) => toMillis(a?.fecha_creacion) - toMillis(b?.fecha_creacion));

    return {
      mode: 'full_scan_fallback',
      queryError: String(error?.message || error || ''),
      leads: Number(limit) > 0 ? filtered.slice(0, Number(limit)) : filtered,
    };
  }
}

async function loadLeadsForAlwaysOn(db, {
  limit = DEFAULT_LIMIT_PER_RUN,
  minSilenceHours = DEFAULT_MIN_SILENCE_HOURS,
} = {}) {
  const cutoffDate = new Date(Date.now() - (Math.max(12, Number(minSilenceHours || DEFAULT_MIN_SILENCE_HOURS)) * 60 * 60 * 1000));
  const collection = db.collection('leads');
  const expandedLimit = Math.max(300, Math.min(5000, Math.max(1, Number(limit || DEFAULT_LIMIT_PER_RUN)) * 25));

  try {
    const snap = await collection
      .where('lastMessageAt', '<=', cutoffDate)
      .orderBy('lastMessageAt', 'desc')
      .limit(expandedLimit)
      .get();

    const rows = snap.docs
      .map(serializeLead)
      .filter((lead) => getLeadActivityMs(lead) > 0 && getLeadActivityMs(lead) <= cutoffDate.getTime());

    return {
      mode: 'last_message_lte_cutoff_desc',
      queryError: '',
      leads: rows,
    };
  } catch (error) {
    const scan = await collection.get();
    const rows = scan.docs
      .map(serializeLead)
      .filter((lead) => getLeadActivityMs(lead) > 0 && getLeadActivityMs(lead) <= cutoffDate.getTime())
      .sort((a, b) => getLeadActivityMs(b) - getLeadActivityMs(a));

    return {
      mode: 'full_scan_fallback',
      queryError: String(error?.message || error || ''),
      leads: rows,
    };
  }
}

function computeDueAt({
  now = new Date(),
  index = 0,
  baseDelayMinutes = DEFAULT_BASE_DELAY_MINUTES,
  spacingSeconds = DEFAULT_SPACING_SECONDS,
  seed = '',
} = {}) {
  const safeBaseDelayMinutes = Math.max(1, Number(baseDelayMinutes || DEFAULT_BASE_DELAY_MINUTES));
  const safeSpacingSeconds = Math.max(45, Number(spacingSeconds || DEFAULT_SPACING_SECONDS));
  const jitterSeconds = 12 + pickVariantIndex(`${seed}:jitter`, 28);
  const totalMs = (safeBaseDelayMinutes * 60 * 1000)
    + (Math.max(0, Number(index || 0)) * safeSpacingSeconds * 1000)
    + (jitterSeconds * 1000);
  return new Date((toMillis(now) || Date.now()) + totalMs);
}

async function scheduleLeadJob(db, lead, plan, options = {}) {
  const now = options.now instanceof Date ? options.now : new Date();
  const source = cleanText(options.source || LAST_WEEK_SOURCE) || LAST_WEEK_SOURCE;
  const campaign = options.campaign && typeof options.campaign === 'object' ? options.campaign : {};
  const dueAt = computeDueAt({
    now,
    index: options.index,
    baseDelayMinutes: options.baseDelayMinutes,
    spacingSeconds: options.spacingSeconds,
    seed: `${campaign.id || source}:${lead.id}:${plan.variationKey}`,
  });

  const leadRef = db.collection('leads').doc(lead.id);
  const jobRef = db.collection('sequenceQueue').doc();
  const aiState = lead?.aiFollowup && typeof lead.aiFollowup === 'object' ? lead.aiFollowup : {};
  const nextAiState = {
    ...aiState,
    lastCampaignId: String(campaign.id || ''),
    lastCampaignSource: source,
    lastCampaignStatus: 'scheduled',
    lastScheduledAt: now,
    lastScheduledFor: dueAt,
    lastVariationKey: plan.variationKey,
    lastContextKey: plan.contextKey,
    lastMessagePreview: buildMessagePreview(plan.message),
    nextAiTouchAt: dueAt,
  };

  await jobRef.set({
    leadId: lead.id,
    status: 'pending',
    dueAt,
    payload: {
      type: 'texto',
      contenido: plan.message,
    },
    jobType: 'ai_followup',
    source,
    idx: 0,
    retry: 0,
    createdAt: Timestamp.fromDate(now),
    campaign: {
      id: String(campaign.id || ''),
      cohort: String(campaign.cohort || ''),
      timezone: String(campaign.timezone || ''),
      fromDate: String(campaign.fromDate || ''),
      toDate: String(campaign.toDate || ''),
      variationKey: plan.variationKey,
      contextKey: plan.contextKey,
    },
  });

  await leadRef.set({ aiFollowup: nextAiState }, { merge: true });

  return {
    jobId: jobRef.id,
    dueAt,
  };
}

function sanitizePlanItem(lead, evaluation, variant) {
  return {
    leadId: lead.id,
    nombre: cleanText(lead?.nombre || ''),
    telefono: cleanText(lead?.telefono || ''),
    estado: cleanText(lead?.estado || ''),
    etapa: cleanText(lead?.etapaNombre || lead?.etapa || ''),
    createdAt: evaluation.createdMs ? new Date(evaluation.createdMs).toISOString() : '',
    lastActivityAt: evaluation.lastActivityMs ? new Date(evaluation.lastActivityMs).toISOString() : '',
    contextKey: variant.contextKey,
    variationKey: variant.variationKey,
    message: variant.message,
  };
}

export async function runLastWeekLeadReactivation({
  commit = false,
  limit = 0,
  fromDate = '',
  toDate = '',
  timezone: tz = DEFAULT_TIMEZONE,
  now = new Date(),
  minSilenceHours = DEFAULT_MIN_SILENCE_HOURS,
  baseDelayMinutes = DEFAULT_BASE_DELAY_MINUTES,
  spacingSeconds = DEFAULT_SPACING_SECONDS,
  dbOverride = null,
} = {}) {
  const window = fromDate || toDate
    ? getExplicitWindow({ fromDate, toDate, timezone: tz })
    : getPreviousCalendarWeekWindow({ now, timezone: tz });

  const db = await getDb(dbOverride);
  const loaded = await loadLeadsForWindow(db, window, { limit });
  const eligible = [];
  const scheduled = [];
  const skipped = [];

  for (const lead of loaded.leads) {
    const evaluation = evaluateLeadForReactivation(lead, {
      window,
      campaignId: window.campaignId,
      now,
      minSilenceHours,
    });

    if (!evaluation.eligible) {
      skipped.push({
        leadId: lead.id,
        nombre: cleanText(lead?.nombre || ''),
        telefono: cleanText(lead?.telefono || ''),
        reason: evaluation.reason,
      });
      continue;
    }

    const variant = buildLeadFollowupVariant(lead, {
      campaignId: window.campaignId,
      contextKey: evaluation.contextKey,
    });
    const plan = sanitizePlanItem(lead, evaluation, variant);
    eligible.push(plan);

    if (commit) {
      const result = await scheduleLeadJob(db, lead, plan, {
        now,
        index: scheduled.length,
        baseDelayMinutes,
        spacingSeconds,
        source: LAST_WEEK_SOURCE,
        campaign: {
          id: window.campaignId,
          cohort: 'last_week',
          timezone: window.timezone,
          fromDate: window.fromDate,
          toDate: window.toDate,
        },
      });
      scheduled.push({
        ...plan,
        jobId: result.jobId,
        dueAt: result.dueAt.toISOString(),
      });
    }
  }

  return {
    commit: commit === true,
    source: LAST_WEEK_SOURCE,
    window: {
      campaignId: window.campaignId,
      timezone: window.timezone,
      fromDate: window.fromDate,
      toDate: window.toDate,
      startIso: window.startIso,
      endExclusiveIso: window.endExclusiveIso,
    },
    query: {
      mode: loaded.mode,
      queryError: loaded.queryError || '',
      loadedCount: loaded.leads.length,
    },
    summary: {
      eligibleCount: eligible.length,
      scheduledCount: scheduled.length,
      skippedCount: skipped.length,
      minSilenceHours: Number(minSilenceHours),
      baseDelayMinutes: Number(baseDelayMinutes),
      spacingSeconds: Number(spacingSeconds),
    },
    eligible,
    scheduled,
    skipped,
  };
}

export async function runAlwaysOnLeadReactivation({
  commit = false,
  limit = DEFAULT_LIMIT_PER_RUN,
  minSilenceHours = DEFAULT_MIN_SILENCE_HOURS,
  baseDelayMinutes = DEFAULT_BASE_DELAY_MINUTES,
  spacingSeconds = DEFAULT_SPACING_SECONDS,
  maxTouches = DEFAULT_MAX_TOUCHES,
  cadenceHours = DEFAULT_CADENCE_HOURS,
  targetStages = DEFAULT_TARGET_STAGES,
  priorityMode = 'newest',
  timezone: tz = DEFAULT_TIMEZONE,
  now = new Date(),
  dbOverride = null,
} = {}) {
  const db = await getDb(dbOverride);
  const loaded = await loadLeadsForAlwaysOn(db, { limit, minSilenceHours });
  const eligible = [];
  const scheduled = [];
  const skipped = [];
  const settings = normalizeSettingsInput({
    enabled: true,
    timezone: tz,
    minSilenceHours,
    baseDelayMinutes,
    spacingSeconds,
    maxTouches,
    limitPerRun: limit,
    cadenceHours,
    targetStages,
    priorityMode,
  }, createDefaultSettings());

  const campaignId = `always-on-${dayjs(now).tz(settings.timezone).format('YYYYMMDDHHmm')}`;
  const candidateLeads = sortAlwaysOnCandidates(loaded.leads, settings.targetStages, settings.priorityMode);

  for (const lead of candidateLeads) {
    if (eligible.length >= settings.limitPerRun) {
      skipped.push({
        leadId: lead.id,
        nombre: cleanText(lead?.nombre || ''),
        telefono: cleanText(lead?.telefono || ''),
        reason: 'limit_reached',
      });
      continue;
    }

    const evaluation = evaluateLeadForAlwaysOn(lead, {
      settings,
      now,
    });

    if (!evaluation.eligible) {
      skipped.push({
        leadId: lead.id,
        nombre: cleanText(lead?.nombre || ''),
        telefono: cleanText(lead?.telefono || ''),
        reason: evaluation.reason,
      });
      continue;
    }

    const variant = buildLeadFollowupVariant(lead, {
      campaignId,
      contextKey: evaluation.contextKey,
      touchIndex: evaluation.touchCount,
    });
    const plan = sanitizePlanItem(lead, evaluation, variant);
    eligible.push(plan);

    if (commit) {
      const result = await scheduleLeadJob(db, lead, plan, {
        now,
        index: scheduled.length,
        baseDelayMinutes: settings.baseDelayMinutes,
        spacingSeconds: settings.spacingSeconds,
        source: ALWAYS_ON_SOURCE,
        campaign: {
          id: campaignId,
          cohort: 'always_on',
          timezone: settings.timezone,
          fromDate: '',
          toDate: '',
        },
      });
      scheduled.push({
        ...plan,
        jobId: result.jobId,
        dueAt: result.dueAt.toISOString(),
      });
    }
  }

  return {
    commit: commit === true,
    source: ALWAYS_ON_SOURCE,
    window: {
      campaignId,
      timezone: settings.timezone,
      fromDate: '',
      toDate: '',
      startIso: '',
      endExclusiveIso: '',
    },
    query: {
      mode: loaded.mode,
      queryError: loaded.queryError || '',
      loadedCount: loaded.leads.length,
    },
    summary: {
      eligibleCount: eligible.length,
      scheduledCount: scheduled.length,
      skippedCount: skipped.length,
      minSilenceHours: Number(settings.minSilenceHours),
      baseDelayMinutes: Number(settings.baseDelayMinutes),
      spacingSeconds: Number(settings.spacingSeconds),
      maxTouches: Number(settings.maxTouches),
      limitPerRun: Number(settings.limitPerRun),
      cadenceHours: [...settings.cadenceHours],
      targetStages: [...settings.targetStages],
      priorityMode: settings.priorityMode,
    },
    eligible,
    scheduled,
    skipped,
  };
}

async function persistAutomationStatus(db, settings, {
  mode = '',
  summary = null,
  error = '',
} = {}) {
  const ref = db.collection(SETTINGS_COLLECTION).doc(SETTINGS_DOC_ID);
  await ref.set({
    ...settings,
    status: {
      lastRunAt: Timestamp.now(),
      lastRunMode: String(mode || ''),
      lastRunSummary: clone(summary),
      lastError: String(error || ''),
    },
  }, { merge: true });
}

// ----------------------------- Candado anti-baneo -----------------------------

function dateKeyInTz(now, tz) {
  return dayjs(now).tz(tz || DEFAULT_TIMEZONE).format('YYYY-MM-DD');
}

function localHourInTz(now, tz) {
  return dayjs(now).tz(tz || DEFAULT_TIMEZONE).hour();
}

// Horario habil: solo enviar entre startHour (incl.) y endHour (excl.) en la tz del CRM.
export function evaluateSendWindow(settings = {}, now = new Date()) {
  let start = clampInt(settings?.sendWindowStartHour, DEFAULT_SEND_WINDOW_START_HOUR, 0, 23);
  let end = clampInt(settings?.sendWindowEndHour, DEFAULT_SEND_WINDOW_END_HOUR, 1, 24);
  if (end <= start) {
    start = DEFAULT_SEND_WINDOW_START_HOUR;
    end = DEFAULT_SEND_WINDOW_END_HOUR;
  }
  const hour = localHourInTz(now, settings?.timezone);
  return { ok: hour >= start && hour < end, hour, start, end };
}

// Tope diario por numero, con warm-up (arranque gradual).
export function computeEffectiveDailyCap(settings = {}, todayKey = '') {
  const dailyCap = Math.max(1, Math.floor(asPositiveNumber(settings?.dailyCap, DEFAULT_DAILY_CAP, 1)));
  if (!settings?.warmupEnabled) {
    return { cap: dailyCap, dailyCap, warmupDay: 0, warmupActive: false };
  }
  const startCap = Math.max(1, Math.floor(asPositiveNumber(settings?.warmupStartCap, DEFAULT_WARMUP_START_CAP, 1)));
  const increment = Math.max(0, Math.floor(asPositiveNumber(settings?.warmupDailyIncrement, DEFAULT_WARMUP_DAILY_INCREMENT, 0)));
  const startDate = normalizeDateKey(settings?.warmupStartDate) || todayKey;
  const daysSinceStart = Math.max(0, dayjs(todayKey).diff(dayjs(startDate), 'day'));
  const warmupCap = startCap + (increment * daysSinceStart);
  const cap = Math.min(dailyCap, warmupCap);
  return { cap, dailyCap, warmupCap, warmupDay: daysSinceStart, warmupActive: cap < dailyCap };
}

function dailyCounterRef(db, dateKey) {
  return db.collection(COUNTER_COLLECTION).doc(`${SETTINGS_DOC_ID}:${dateKey}`);
}

async function getDailySentCount(db, dateKey) {
  const snap = await dailyCounterRef(db, dateKey).get();
  return snap.exists ? Math.max(0, Number(snap.data()?.count || 0)) : 0;
}

async function incrementDailySentCount(db, dateKey, amount) {
  const value = Math.floor(Number(amount) || 0);
  if (value <= 0) return;
  await dailyCounterRef(db, dateKey).set({
    dateKey,
    count: FieldValue.increment(value),
    updatedAt: Timestamp.now(),
  }, { merge: true });
}

export async function runLeadReactivationAutomationTick({
  force = false,
  dbOverride = null,
  now = new Date(),
} = {}) {
  const db = await getDb(dbOverride);
  const lock = await acquireLock(db, LOCK_DOC_ID, LOCK_TTL_MS);
  if (!lock.acquired) {
    return {
      ok: false,
      skipped: true,
      reason: 'lock_active',
    };
  }

  try {
    const loaded = await loadSettingsDoc(db);
    let settings = loaded.settings;
    if (!settings.enabled && !force) {
      return {
        ok: true,
        skipped: true,
        reason: 'disabled',
        settings,
      };
    }

    // Warm-up: fijar la fecha de arranque la primera vez que corre habilitado.
    const todayKey = dateKeyInTz(now, settings.timezone);
    if (settings.warmupEnabled && !settings.warmupStartDate) {
      await loaded.ref.set({ warmupStartDate: todayKey }, { merge: true }).catch(() => {});
      settings = { ...settings, warmupStartDate: todayKey };
    }

    // Capa 1: horario habil.
    const windowState = evaluateSendWindow(settings, now);
    if (!windowState.ok && !force) {
      await persistAutomationStatus(db, settings, {
        mode: 'always_on_tick',
        summary: { skippedReason: 'outside_send_window', ...windowState },
        error: '',
      });
      return {
        ok: true,
        skipped: true,
        reason: 'outside_send_window',
        window: windowState,
        settings,
      };
    }

    // Capa 2 + 3: tope diario por numero con warm-up.
    const capInfo = computeEffectiveDailyCap(settings, todayKey);
    const sentToday = await getDailySentCount(db, todayKey);
    const remaining = Math.max(0, capInfo.cap - sentToday);
    if (remaining <= 0 && !force) {
      await persistAutomationStatus(db, settings, {
        mode: 'always_on_tick',
        summary: { skippedReason: 'daily_cap_reached', sentToday, cap: capInfo.cap, warmupDay: capInfo.warmupDay },
        error: '',
      });
      return {
        ok: true,
        skipped: true,
        reason: 'daily_cap_reached',
        cap: capInfo,
        sentToday,
        settings,
      };
    }

    // Limite efectivo de esta corrida: el menor entre lo permitido por run y lo que queda del dia.
    const runLimit = force ? settings.limitPerRun : Math.min(settings.limitPerRun, remaining);

    const result = await runAlwaysOnLeadReactivation({
      commit: true,
      limit: runLimit,
      minSilenceHours: settings.minSilenceHours,
      baseDelayMinutes: settings.baseDelayMinutes,
      spacingSeconds: settings.spacingSeconds,
      maxTouches: settings.maxTouches,
      cadenceHours: settings.cadenceHours,
      targetStages: settings.targetStages,
      priorityMode: settings.priorityMode,
      timezone: settings.timezone,
      now,
      dbOverride: db,
    });

    // Registrar lo realmente programado contra el tope diario.
    const scheduledCount = Number(result?.summary?.scheduledCount || 0);
    if (scheduledCount > 0) {
      await incrementDailySentCount(db, todayKey, scheduledCount).catch(() => {});
    }

    await persistAutomationStatus(db, settings, {
      mode: 'always_on_tick',
      summary: {
        ...result.summary,
        antiBan: {
          window: windowState,
          dailyCap: capInfo.cap,
          dailyCapMax: capInfo.dailyCap,
          warmupDay: capInfo.warmupDay,
          warmupActive: capInfo.warmupActive,
          sentTodayBefore: sentToday,
          sentTodayAfter: sentToday + scheduledCount,
          runLimit,
        },
      },
      error: '',
    });

    return {
      ok: true,
      skipped: false,
      result,
      antiBan: {
        window: windowState,
        cap: capInfo,
        sentTodayBefore: sentToday,
        sentTodayAfter: sentToday + scheduledCount,
        runLimit,
      },
      settings,
    };
  } catch (error) {
    const loaded = await loadSettingsDoc(db);
    await persistAutomationStatus(db, loaded.settings, {
      mode: 'always_on_tick',
      summary: null,
      error: String(error?.message || error || ''),
    }).catch(() => {});

    return {
      ok: false,
      skipped: false,
      reason: 'error',
      error: String(error?.message || error || ''),
    };
  } finally {
    await releaseLock(lock.ref);
  }
}
