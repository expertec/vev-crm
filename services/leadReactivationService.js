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
const FOLLOWUP_SOURCE = 'last-week-reactivation';

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

function toDateKey(value) {
  return dayjs(value).format('YYYY-MM-DD');
}

function buildCampaignId(window) {
  return `last-week-${window.fromDate}_${window.toDate}`;
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

export function evaluateLeadForReactivation(lead = {}, {
  window,
  campaignId = window?.campaignId || '',
  now = new Date(),
  minSilenceHours = DEFAULT_MIN_SILENCE_HOURS,
} = {}) {
  const createdMs = toMillis(lead?.fecha_creacion);
  const lastActivityMs = Math.max(
    toMillis(lead?.lastInboundAt),
    toMillis(lead?.lastMessageAt),
    createdMs
  );
  const nowMs = toMillis(now) || Date.now();
  const minSilenceMs = Math.max(1, Number(minSilenceHours || DEFAULT_MIN_SILENCE_HOURS)) * 60 * 60 * 1000;
  const aiState = lead?.aiFollowup && typeof lead.aiFollowup === 'object' ? lead.aiFollowup : {};

  if (!createdMs) {
    return { eligible: false, reason: 'missing_created_at', createdMs, lastActivityMs };
  }
  if (window?.startDate && createdMs < window.startDate.getTime()) {
    return { eligible: false, reason: 'outside_window', createdMs, lastActivityMs };
  }
  if (window?.endExclusiveDate && createdMs >= window.endExclusiveDate.getTime()) {
    return { eligible: false, reason: 'outside_window', createdMs, lastActivityMs };
  }
  if (lead?.mergedInto) {
    return { eligible: false, reason: 'merged_lead', createdMs, lastActivityMs };
  }
  if (isLeadArchived(lead)) {
    return { eligible: false, reason: 'archived', createdMs, lastActivityMs };
  }
  if (hasHardStop(lead)) {
    return { eligible: false, reason: 'hard_stop', createdMs, lastActivityMs };
  }
  if (Number(lead?.unreadCount || 0) > 0) {
    return { eligible: false, reason: 'has_unread_messages', createdMs, lastActivityMs };
  }
  if (hasActiveSequences(lead)) {
    return { eligible: false, reason: 'active_sequence', createdMs, lastActivityMs };
  }
  if (!hasReachableTarget(lead)) {
    return { eligible: false, reason: 'missing_target', createdMs, lastActivityMs };
  }
  if (String(aiState?.lastCampaignId || '').trim() === String(campaignId || '').trim()) {
    return { eligible: false, reason: 'already_scheduled_in_campaign', createdMs, lastActivityMs };
  }
  if ((nowMs - lastActivityMs) < minSilenceMs) {
    return { eligible: false, reason: 'recent_activity', createdMs, lastActivityMs };
  }

  return {
    eligible: true,
    reason: 'eligible',
    createdMs,
    lastActivityMs,
    contextKey: detectLeadContext(lead),
  };
}

function serializeLead(docSnap) {
  return {
    id: docSnap.id,
    ...(docSnap.data() || {}),
  };
}

async function loadLeadsForWindow(db, window, { limit = 0 } = {}) {
  const collection = db.collection('leads');
  try {
    let query = collection
      .where('fecha_creacion', '>=', Timestamp.fromDate(window.startDate))
      .where('fecha_creacion', '<', Timestamp.fromDate(window.endExclusiveDate))
      .orderBy('fecha_creacion', 'asc');

    if (Number(limit) > 0) {
      query = query.limit(Number(limit));
    }

    const snap = await query.get();
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

function buildMessagePreview(message = '') {
  return cleanText(message).slice(0, 220);
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

async function scheduleLeadJob(db, lead, plan, window, options = {}) {
  const now = options.now instanceof Date ? options.now : new Date();
  const dueAt = computeDueAt({
    now,
    index: options.index,
    baseDelayMinutes: options.baseDelayMinutes,
    spacingSeconds: options.spacingSeconds,
    seed: `${window.campaignId}:${lead.id}:${plan.variationKey}`,
  });

  const leadRef = db.collection('leads').doc(lead.id);
  const jobRef = db.collection('sequenceQueue').doc();
  const aiState = lead?.aiFollowup && typeof lead.aiFollowup === 'object' ? lead.aiFollowup : {};
  const nextAiState = {
    ...aiState,
    lastCampaignId: window.campaignId,
    lastCampaignSource: FOLLOWUP_SOURCE,
    lastCampaignStatus: 'scheduled',
    lastScheduledAt: now,
    lastScheduledFor: dueAt,
    lastVariationKey: plan.variationKey,
    lastContextKey: plan.contextKey,
    lastMessagePreview: buildMessagePreview(plan.message),
    lastWindowStart: window.fromDate,
    lastWindowEnd: window.toDate,
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
    source: FOLLOWUP_SOURCE,
    idx: 0,
    retry: 0,
    createdAt: Timestamp.fromDate(now),
    campaign: {
      id: window.campaignId,
      cohort: 'last_week',
      timezone: window.timezone,
      fromDate: window.fromDate,
      toDate: window.toDate,
      variationKey: plan.variationKey,
      contextKey: plan.contextKey,
    },
  });

  await leadRef.set(
    {
      aiFollowup: nextAiState,
    },
    { merge: true }
  );

  return {
    jobId: jobRef.id,
    dueAt,
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
} = {}) {
  const window = fromDate || toDate
    ? getExplicitWindow({ fromDate, toDate, timezone: tz })
    : getPreviousCalendarWeekWindow({ now, timezone: tz });

  const { db } = await import('../firebaseAdmin.js');
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
    const plan = {
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

    eligible.push(plan);

    if (commit) {
      const result = await scheduleLeadJob(db, lead, plan, window, {
        now,
        index: scheduled.length,
        baseDelayMinutes,
        spacingSeconds,
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
    source: FOLLOWUP_SOURCE,
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
