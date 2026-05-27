import crypto from 'node:crypto';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import { Timestamp } from 'firebase-admin/firestore';

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
const LAST_WEEK_SOURCE = 'last-week-reactivation';
const ALWAYS_ON_SOURCE = 'always-on-reactivation';
const SETTINGS_COLLECTION = 'automationSettings';
const SETTINGS_DOC_ID = 'leadReactivation24x7';
const LOCK_COLLECTION = 'automationLocks';
const LOCK_DOC_ID = 'leadReactivation24x7';
const LOCK_TTL_MS = 8 * 60 * 1000;

const OPENERS = [
  'Hola {{nombre}}, te escribo para dar seguimiento a tu pagina web.',
  'Hola {{nombre}}, regreso por aqui para retomar lo de tu pagina web.',
  'Hola {{nombre}}, sigo pendiente contigo sobre la web de tu negocio.',
  'Hola {{nombre}}, te mando un mensaje corto para retomar tu pagina web.',
];

const GENERIC_LINES = [
  'Todavia puedo ayudarte a dejarla lista de forma accesible y sin hacerte perder tiempo.',
  'Si sigues interesado, aun te puedo apoyar para sacarla rapido y bien hecha.',
  'La idea es ayudarte a avanzar sin complicarte el proceso.',
];

const SAMPLE_LINES = [
  'Quede pendiente sobre la muestra que te comparti y queria saber si la retomamos.',
  'Si la muestra que viste te gusto, puedo ayudarte a convertirla en tu pagina final.',
  'Vi que ya llevabas avance con la muestra y queria saber si lo seguimos.',
];

const WEB_LINES = [
  'Quede pendiente despues de la web que te comparti y queria saber si la retomamos.',
  'Si la propuesta que viste te sigue interesando, todavia puedo ayudarte a dejarla lista.',
  'Puedo ayudarte a ajustar la web que ya viste para que quede lista para vender.',
];

const CTAS = [
  'Si quieres, te envio opciones, ejemplos o precio por aqui.',
  'Si aun te interesa, hoy mismo te paso lo que sigue.',
  'Si te parece, te mando los siguientes pasos y te cotizo por aqui.',
  'Si gustas, te envio por aqui una opcion clara para avanzar.',
];

const CLOSES = [
  'Si por ahora lo quieres pausar, tambien dime y no te insisto.',
  'Si ya no va por ahora, solo avisame y cierro seguimiento.',
  'Si prefieres verlo despues, tambien te dejo de seguir por ahora sin problema.',
];

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

function renderTemplate(template = '', lead = {}) {
  const safeName = firstName(lead?.nombre || '') || '';
  const text = String(template || '').replace(/\{\{nombre\}\}/g, safeName);
  return cleanText(text.replace(/\s([,.!?;:])/g, '$1'));
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

function resolveMessagePool(contextKey = 'generic') {
  if (contextKey === 'web_sent') return WEB_LINES;
  if (contextKey === 'sample_sent') return SAMPLE_LINES;
  return GENERIC_LINES;
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

function resolveLeadStageKey(lead = {}) {
  const etapa = normalizeStageToken(lead?.etapa || lead?.etapaNombre || '');
  if (etapa) return etapa;
  return 'leads_nuevos';
}

function sortAlwaysOnCandidates(leads = [], targetStages = DEFAULT_TARGET_STAGES) {
  const stageOrder = new Map();
  normalizeTargetStages(targetStages).forEach((stage, index) => {
    stageOrder.set(stage, index);
  });
  const defaultOrder = stageOrder.size + 1;

  return [...leads].sort((a, b) => {
    const aStage = resolveLeadStageKey(a);
    const bStage = resolveLeadStageKey(b);
    const aOrder = stageOrder.has(aStage) ? stageOrder.get(aStage) : defaultOrder;
    const bOrder = stageOrder.has(bStage) ? stageOrder.get(bStage) : defaultOrder;
    if (aOrder !== bOrder) return aOrder - bOrder;
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
} = {}) {
  const seedBase = [
    campaignId,
    lead?.id || '',
    lead?.telefono || '',
    lead?.estado || '',
    lead?.etapa || '',
    contextKey,
  ].join('|');

  const openerIndex = pickVariantIndex(`${seedBase}:open`, OPENERS.length);
  const bodyPool = resolveMessagePool(contextKey);
  const bodyIndex = pickVariantIndex(`${seedBase}:body`, bodyPool.length);
  const ctaIndex = pickVariantIndex(`${seedBase}:cta`, CTAS.length);
  const closeIndex = pickVariantIndex(`${seedBase}:close`, CLOSES.length);

  const lines = [
    renderTemplate(OPENERS[openerIndex], lead),
    renderTemplate(bodyPool[bodyIndex], lead),
    renderTemplate(CTAS[ctaIndex], lead),
    renderTemplate(CLOSES[closeIndex], lead),
  ].filter(Boolean);

  let message = cleanText(lines.join(' '));
  if (!firstName(lead?.nombre || '')) {
    message = message.replace(/^Hola,\s*/i, 'Hola, ');
  }

  return {
    contextKey,
    variationKey: `o${openerIndex}-b${bodyIndex}-c${ctaIndex}-x${closeIndex}`,
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
  }, createDefaultSettings());

  const campaignId = `always-on-${dayjs(now).tz(settings.timezone).format('YYYYMMDDHHmm')}`;
  const candidateLeads = sortAlwaysOnCandidates(loaded.leads, settings.targetStages);

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
    const settings = loaded.settings;
    if (!settings.enabled && !force) {
      return {
        ok: true,
        skipped: true,
        reason: 'disabled',
        settings,
      };
    }

    const result = await runAlwaysOnLeadReactivation({
      commit: true,
      limit: settings.limitPerRun,
      minSilenceHours: settings.minSilenceHours,
      baseDelayMinutes: settings.baseDelayMinutes,
      spacingSeconds: settings.spacingSeconds,
      maxTouches: settings.maxTouches,
      cadenceHours: settings.cadenceHours,
      targetStages: settings.targetStages,
      timezone: settings.timezone,
      now,
      dbOverride: db,
    });

    await persistAutomationStatus(db, settings, {
      mode: 'always_on_tick',
      summary: result.summary,
      error: '',
    });

    return {
      ok: true,
      skipped: false,
      result,
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
