// queue.js
import { db, admin } from './firebaseAdmin.js';
import {
  sendClipMessage,
  getWhatsAppSock,
  getConnectionStatus,
  markWhatsAppSendFailure,
  sendVideoNote,
  sendAudioMessage,
} from './whatsappService.js';
import { getBuiltinSequenceDefinition } from './services/salesQueue/welcomeSequence.js';
import { shouldBlockSequenceByLeadContext } from './utils/sequenceTriggerGuards.js';

const { FieldValue } = admin.firestore;
const { Timestamp } = admin.firestore;

const SEQUENCE_LOCK_TTL_MS = 2 * 60 * 1000; // 2 minutos
const MAX_SEQUENCE_BATCH = 25;
const MAX_DUE_SEQUENCE_STEPS_PER_RUN = Math.max(1, Number(process.env.MAX_DUE_SEQUENCE_STEPS_PER_RUN || 8));
const WA_UNAVAILABLE_RETRY_MS = Math.max(30_000, Number(process.env.WA_UNAVAILABLE_RETRY_MS || 120_000));

/* ----------------------------- utilidades ------------------------------ */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function createWhatsAppUnavailableError(message) {
  const err = new Error(message || 'WhatsApp no está conectado');
  err.code = 'WA_NOT_CONNECTED';
  err.isWaUnavailable = true;
  return err;
}

function isWhatsAppUnavailableError(err) {
  if (err?.code === 'WA_NOT_CONNECTED' || err?.isWaUnavailable === true) return true;
  const statusCode = Number(err?.output?.statusCode || err?.output?.payload?.statusCode || err?.statusCode);
  const msg = String(err?.message || err || '');
  return statusCode === 428 || /no hay conexión activa|socket de whatsapp no está conectado|whatsapp no está conectado|connection\s+closed|stream\s+errored|unavailableService/i.test(msg);
}

function firstName(full = '') {
  return String(full).trim().split(/\s+/)[0] || '';
}

function isLidJid(jid) {
  return /@lid$/i.test(String(jid || '').trim());
}

function isSendableJid(jid) {
  return /@s\.whatsapp\.net$/i.test(String(jid || '').trim());
}

function cleanLeadPhone(value) {
  const raw = String(value || '').trim();
  if (!raw || raw.includes('@')) return '';
  return raw.replace(/\D/g, '');
}

function toDateSafe(v) {
  if (!v) return null;
  if (v instanceof Date) return isNaN(+v) ? null : v;
  if (typeof v?.toDate === 'function') {
    const d = v.toDate();
    return isNaN(+d) ? null : d;
  }
  if (typeof v?.toMillis === 'function') return new Date(v.toMillis());
  const d = new Date(v);
  return isNaN(+d) ? null : d;
}

function sameMinute(a, b) {
  const da = toDateSafe(a);
  const db = toDateSafe(b);
  if (!da || !db) return false;
  return Math.abs(da.getTime() - db.getTime()) < 1000;
}

function getSampleSiteBaseUrl() {
  return String(
    process.env.SAMPLE_SITE_BASE_URL ||
      process.env.SITE_PUBLIC_BASE_URL ||
      'https://negociosweb.mx/site'
  ).replace(/\/+$/, '');
}

function getSampleFormBaseUrl() {
  return String(
    process.env.SAMPLE_FORM_BASE_URL
      || process.env.PUBLIC_SAMPLE_FORM_URL
      || process.env.NEXT_PUBLIC_SITE_URL
      || 'https://negociosweb.mx'
  ).replace(/\/+$/, '');
}

function resolveSampleSlug(lead = {}) {
  const candidate = [
    lead?.slug,
    lead?.webSlug,
    lead?.siteSlug,
    lead?.briefWeb?.slug,
    lead?.schema?.slug,
  ].find((v) => String(v || '').trim());
  return String(candidate || '').trim();
}

function buildLinkPagina(lead = {}) {
  const slug = resolveSampleSlug(lead);
  if (!slug) return '';
  return `${getSampleSiteBaseUrl()}/${encodeURIComponent(slug)}`;
}

function buildLinkMuestra(phone = '') {
  const safePhone = normalizePhoneForWA(phone);
  if (!safePhone) return '';
  return `${getSampleFormBaseUrl()}/muestra/${encodeURIComponent(safePhone)}`;
}

function replacePlaceholders(template, lead) {
  if (!template) return '';
  const telFromLead = cleanLeadPhone(lead?.telefono);
  const leadJid = extractJidFromLead(lead);
  const telFromJid = isSendableJid(leadJid) ? phoneFromJid(leadJid) : null;
  const tel = telFromLead || telFromJid || '';
  const nameFirst = firstName(lead.nombre || '');
  const linkPagina = buildLinkPagina(lead);
  const linkMuestra = buildLinkMuestra(tel);

  const resolveKey = (key) => {
    if (key === 'telefono') return tel;
    if (key === 'phone') return tel;
    if (key === 'nombre') return nameFirst;
    if (key === 'linkPagina' || key === 'link_pagina') return linkPagina;
    if (key === 'linkMuestra' || key === 'link_muestra') return linkMuestra;
    return lead[key] ?? '';
  };

  return String(template)
    .replace(/\{\{\s*(\w+)\s*\}\}/g, (_, key) => resolveKey(key))
    .replace(/\$\{\s*(\w+)\s*\}/g, (_, key) => resolveKey(key));
}

// --- helpers de teléfono/JID (MX requiere 521 para móviles con Baileys) ---
function toE164(num) {
  const raw = String(num || '').replace(/\D/g, '');
  if (/^\d{10}$/.test(raw)) return `+52${raw}`;
  if (/^52\d{10}$/.test(raw)) return `+${raw}`;
  if (/^521\d{10}$/.test(raw)) return `+${raw}`;
  return `+${raw}`;
}
function normalizeJid(jid) {
  if (!jid) return null;
  const trimmed = String(jid).trim();
  if (trimmed.includes('@')) {
    const [user, domain] = trimmed.split('@');
    const cleanUser = user.split(':')[0].replace(/\s+/g, '');
    return `${cleanUser}@${domain}`;
  }
  const num = normalizePhoneForWA(trimmed);
  return num ? `${num}@s.whatsapp.net` : null;
}
function normalizePhoneForWA(phone) {
  let num = String(phone || '').replace(/\D/g, '');
  // 52 + 10 → forzar 521 + 10
  if (num.length === 12 && num.startsWith('52') && !num.startsWith('521')) {
    return '521' + num.slice(2);
  }
  // 10 → 521 + 10
  if (num.length === 10) return '521' + num;
  // si ya viene 521…, dejarlo
  return num;
}
function e164ToJid(e164) {
  const digits = String(e164 || '').replace(/\D/g, '');
  return `${normalizePhoneForWA(digits)}@s.whatsapp.net`;
}

function phoneFromJid(jid) {
  const normalized = normalizeJid(jid);
  if (!normalized || !isSendableJid(normalized)) return null;
  const [user] = normalized.split('@');
  const cleanUser = user.split(':')[0].replace(/\D/g, '');
  if (!cleanUser) return null;
  return normalizePhoneForWA(cleanUser);
}

function extractJidFromLead(lead) {
  const candidates = [
    lead?.resolvedJid,
    lead?.jid,
    lead?.id,
    lead?.leadId
  ];

  for (const cand of candidates) {
    const normalized = normalizeJid(cand);
    if (!normalized) continue;
    if (isLidJid(normalized)) continue;
    if (isSendableJid(normalized)) return normalized;
  }

  return null;
}

function resolveLeadJidAndPhone(lead) {
  const phoneRaw = lead?.telefono || '';
  const normalizedPhoneFromLead = normalizePhoneForWA(cleanLeadPhone(phoneRaw));
  const jidCandidate = extractJidFromLead(lead);
  const normalizedPhone =
    normalizedPhoneFromLead ||
    phoneFromJid(jidCandidate) ||
    phoneFromJid(lead?.resolvedJid) ||
    null;

  if (jidCandidate) {
    return { jid: jidCandidate, phone: normalizedPhone };
  }

  if (normalizedPhone) {
    return { jid: `${normalizedPhone}@s.whatsapp.net`, phone: normalizedPhone };
  }

  console.error(`[resolveLeadJidAndPhone] ❌ No se pudo resolver JID ni teléfono para lead:`, {
    leadId: lead?.id,
    telefono: phoneRaw,
    jid: lead?.jid
  });

  return { jid: null, phone: null };
}

function hasSameTrigger(secuencias = [], trigger = '') {
  const next = String(trigger || '').toLowerCase();
  return Array.isArray(secuencias)
    && secuencias.some((s) => !s?.completed && String(s?.trigger || '').toLowerCase() === next);
}

function hasTriggerInHistory(history = [], trigger = '') {
  const next = String(trigger || '').toLowerCase();
  if (!next) return false;
  return Array.isArray(history)
    && history.some((t) => String(t || '').toLowerCase() === next);
}

const FORM_COMPLETED_BLOCKED_TRIGGERS = new Set([
  'leadweb',
  'nuevolead',
  'nuevoleadweb',
  'leadwhatsapp',
  'webpromo',
]);

function hasLeadCompletedForm(leadData = {}) {
  const etapa = String(leadData?.etapa || '').toLowerCase();
  if (etapa === 'form_submitted') return true;
  const tags = Array.isArray(leadData?.etiquetas)
    ? leadData.etiquetas.map((t) => String(t || '').toLowerCase())
    : [];
  return tags.includes('formok') || tags.includes('formulariocompletado');
}

function shouldStopTriggerAfterForm(trigger = '') {
  return FORM_COMPLETED_BLOCKED_TRIGGERS.has(String(trigger || '').toLowerCase());
}

/* ----------------------- normalización de tipos ------------------------ */
// Unifica variantes: 'videonota' | 'video_note' | 'video-note' | 'ptv' → 'videonota'
function normType(t = '') {
  return String(t).trim().toLowerCase().replace(/[_\s-]+/g, '');
}
const TYPE_MAP = {
  texto: 'texto',
  imagen: 'imagen',
  audio: 'audio',
  clip: 'audio',
  video: 'video',
  videonota: 'videonota',
  videonote: 'videonota',
  videoptv: 'videonota',
  ptv: 'videonota',
  question: 'texto'
};
function resolveType(raw) {
  const k = normType(raw);
  return TYPE_MAP[k] || k;
}

/* ---------------- persistencia uniforme de salientes ------------------- */
function messageDocIdFromWaId(waMessageId) {
  const clean = String(waMessageId || '')
    .trim()
    .replace(/[^\w.-]/g, '_');
  if (!clean) return null;
  return `wa_${clean}`;
}

async function persistOutgoing(leadId, {
  content = '',
  mediaType = 'text',
  mediaUrl = null,
  waMessageId = '',
  sequenceTrigger = '',
  sequenceStep = null,
}) {
  const now = new Date();
  const payload = {
    content,
    mediaType,
    mediaUrl,
    sender: 'business',
    timestamp: now,
    automationType: 'sequence',
    ...(waMessageId ? { waMessageId: String(waMessageId) } : {}),
    ...(sequenceTrigger ? { sequenceTrigger: String(sequenceTrigger) } : {}),
    ...(sequenceStep !== null && sequenceStep !== undefined ? { sequenceStep: Number(sequenceStep) } : {}),
  };

  const messagesRef = db.collection('leads').doc(leadId).collection('messages');
  const docId = messageDocIdFromWaId(waMessageId);
  if (docId) {
    await messagesRef.doc(docId).set(payload, { merge: true });
  } else if (sequenceTrigger && sequenceStep !== null && sequenceStep !== undefined) {
    const fallbackDocId = `seq_${String(sequenceTrigger).replace(/[^\w.-]/g, '_')}_${Number(sequenceStep)}`;
    await messagesRef.doc(fallbackDocId).set(payload, { merge: true });
  } else {
    await messagesRef.add(payload);
  }

  await db.collection('leads').doc(leadId).set(
    { lastMessageAt: now },
    { merge: true }
  );
}

async function sendWithRetry(sock, jid, message, opts = {}, attempts = 3) {
  let lastErr = null;
  for (let i = 0; i < attempts; i++) {
    try {
      return await sock.sendMessage(jid, message, opts);
    } catch (err) {
      lastErr = err;
      if (markWhatsAppSendFailure(err, sock)) {
        err.code = 'WA_NOT_CONNECTED';
        err.isWaUnavailable = true;
        throw err;
      }
      const msg = String(err?.message || err || '');
      const transient = /timed\s*out|timeout|socket|network|disconnected|aborted|closed/i.test(msg);
      if (!transient || i === attempts - 1) throw err;
      const backoff = (i + 1) * 3000;
      await sleep(backoff);
    }
  }
  throw lastErr;
}

/* ----------------------- definición de secuencias ----------------------- */
const _sequenceDefCache = new Map();
const _sequenceDefCacheTime = new Map();
const SEQUENCE_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutos
const TRIGGER_FALLBACK = {
  WebPromo: 'LeadWhatsapp',
  webpromo: 'LeadWhatsapp'
};

const isSeqCacheFresh = (key) => {
  const ts = _sequenceDefCacheTime.get(key);
  return typeof ts === 'number' && (Date.now() - ts) < SEQUENCE_CACHE_TTL_MS;
};
const setSeqCache = (key, value) => {
  _sequenceDefCache.set(key, value);
  _sequenceDefCacheTime.set(key, Date.now());
  return value;
};

async function getSequenceDefinition(trigger) {
  if (!trigger) return null;
  const key = String(trigger);
  if (_sequenceDefCache.has(key) && isSeqCacheFresh(key)) return _sequenceDefCache.get(key);

  let seqDoc = await db.collection('secuencias').doc(key).get();
  if (!seqDoc.exists) {
    const q = await db.collection('secuencias')
      .where('trigger', '==', key)
      .limit(1)
      .get();
    if (!q.empty) seqDoc = q.docs[0];
  }
  if (!seqDoc.exists) {
    const builtin = getBuiltinSequenceDefinition(key);
    if (builtin) {
      console.warn(`[getSequenceDefinition] No existe secuencias/${key}. Usando definicion integrada.`);
      return setSeqCache(key, builtin);
    }
    const fallback = TRIGGER_FALLBACK[key];
    if (fallback) {
      console.warn(`[getSequenceDefinition] No existe secuencias/${key}. Usando fallback → ${fallback}`);
      const fb = await getSequenceDefinition(fallback);
      if (fb) {
        const aliasDef = { ...fb, trigger: key, aliasOf: fallback };
        return setSeqCache(key, aliasDef);
      }
    }
    console.warn(`[getSequenceDefinition] No existe secuencias/${key}`);
    return setSeqCache(key, null);
  }
  const data = seqDoc.data() || {};
  const messages = Array.isArray(data.messages) ? data.messages : [];
  const def = { id: seqDoc.id, trigger: data.trigger || key, active: data.active !== false, messages };
  return setSeqCache(key, def);
}

function computeSequenceStepRun(trigger, startTime, index = 0) {
  const seq = _sequenceDefCache.get(trigger);
  if (!seq || !seq.messages || seq.messages.length === 0) return null;
  if (index == null || index >= seq.messages.length) return null;

  const start = toDateSafe(startTime);
  if (!start) return null;

  const msg = seq.messages[index];
  const delayMin = Number(msg?.delay || 0);
  return new Date(start.getTime() + delayMin * 60_000);
}

function computeNextRunForLead(secuencias = []) {
  let nextAt = null;
  for (const seq of secuencias) {
    if (!seq || seq.completed) continue;
    if (seq.status === 'waiting_for_reply' || seq.status === 'paused_for_agent') continue;
    const runAt = computeSequenceStepRun(seq.trigger, seq.startTime, Number(seq.index || 0));
    if (!runAt) continue;
    if (!nextAt || runAt < nextAt) nextAt = runAt;
  }
  return nextAt;
}

// helper: obtener lead
async function _getLead(leadId) {
  const snap = await db.collection('leads').doc(leadId).get();
  return snap.exists ? { id: snap.id, ...(snap.data() || {}) } : null;
}

/* -------------------- programar / cancelar secuencias ------------------- */
export async function scheduleSequenceForLead(leadId, trigger, startAt = new Date(), options = {}) {
  const leadRef = db.collection('leads').doc(leadId);
  const def = await getSequenceDefinition(trigger);
  const normalizedTrigger = String(trigger || '');
  const allowReschedule = options?.allowReschedule === true;
  const debug = options?.debug === true;
  const source = String(options?.source || 'unknown');

  if (!def || def.active === false || !def.messages || def.messages.length === 0) {
    if (debug) {
      console.warn(
        `[scheduleSequenceForLead] skip(no-definition) source=${source} lead=${leadId} trigger=${normalizedTrigger} hasDef=${Boolean(def)} active=${def?.active !== false} steps=${Array.isArray(def?.messages) ? def.messages.length : 0}`
      );
    }
    return 0;
  }

  const startIso = toDateSafe(startAt)?.toISOString?.() || new Date().toISOString();

  const scheduleResult = await db.runTransaction(async (tx) => {
    const leadSnap = await tx.get(leadRef);
    const leadData = leadSnap.exists ? leadSnap.data() || {} : {};

    // No duplicar trigger activo aunque lleguen múltiples eventos en paralelo.
    const secAct = Array.isArray(leadData.secuenciasActivas) ? [...leadData.secuenciasActivas] : [];
    const contextBlock = shouldBlockSequenceByLeadContext(leadData, normalizedTrigger);
    if (contextBlock.blocked) return `blocked:${contextBlock.reason}`;

    if (hasSameTrigger(secAct, normalizedTrigger)) return 'already-active';

    // Regla global: cada trigger se ejecuta una sola vez por lead (histórico).
    const history = Array.isArray(leadData.sequenceDeliveredTriggers)
      ? leadData.sequenceDeliveredTriggers
      : [];
    const sent = leadData.sequenceSentSteps && typeof leadData.sequenceSentSteps === 'object'
      ? leadData.sequenceSentSteps
      : {};
    const hadSentStepsForTrigger = Object.keys(sent).some((k) => k.startsWith(`${normalizedTrigger}:`));

    if (
      !allowReschedule
      && (
        hasTriggerInHistory(history, normalizedTrigger)
        || hadSentStepsForTrigger
      )
    ) {
      return 'already-scheduled';
    }

    const newSeq = {
      trigger: normalizedTrigger,
      startTime: startIso,
      index: 0,
      completed: false
    };
    secAct.push(newSeq);

    // Limpiar pasos enviados previos de este trigger.
    const sentPatch = { ...(leadData.sequenceSentSteps || {}) };
    Object.keys(sentPatch).forEach((k) => {
      if (k.startsWith(`${normalizedTrigger}:`)) delete sentPatch[k];
    });

    const nextAt = computeNextRunForLead(secAct);
    const payload = {
      secuenciasActivas: secAct,
      hasActiveSequences: true,
      sequenceSentSteps: sentPatch,
      // Historial simple para evitar reactivaciones automáticas repetidas.
      sequenceScheduledTriggers: FieldValue.arrayUnion(normalizedTrigger)
    };
    if (nextAt) payload.nextSequenceRunAt = nextAt;

    tx.set(leadRef, payload, { merge: true });
    tx.set(leadRef, { etiquetas: FieldValue.arrayUnion(normalizedTrigger) }, { merge: true });
    return 'scheduled';
  });

  if (scheduleResult !== 'scheduled') {
    if (String(scheduleResult || '').startsWith('blocked:')) {
      console.log(`[scheduleSequenceForLead] trigger '${normalizedTrigger}' bloqueado en ${leadId}: ${scheduleResult}`);
      return 0;
    }
    if (scheduleResult === 'already-scheduled') {
      console.log(`[scheduleSequenceForLead] trigger '${normalizedTrigger}' ya fue programado antes en ${leadId}, se omite.`);
      return 0;
    }
    console.log(`[scheduleSequenceForLead] trigger '${normalizedTrigger}' ya presente en ${leadId}, no se duplica.`);
    return 0;
  }

  if (debug) {
    console.log(
      `[scheduleSequenceForLead] scheduled source=${source} lead=${leadId} trigger=${normalizedTrigger} steps=${def.messages.length} allowReschedule=${allowReschedule}`
    );
  }

  return def.messages.length;
}

export async function cancelSequences(leadId, triggers = []) {
  if (!leadId || !Array.isArray(triggers) || triggers.length === 0) return 0;

  const leadRef = db.collection('leads').doc(leadId);
  const snap = await leadRef.get();
  if (!snap.exists) return 0;

  const data = snap.data() || {};
  const secAct = Array.isArray(data.secuenciasActivas) ? data.secuenciasActivas : [];
  const filtered = secAct.filter(s => !triggers.includes(s?.trigger));

  if (filtered.length === secAct.length) return 0;

  // limpiar pasos enviados de esos triggers
  const sent = { ...(data.sequenceSentSteps || {}) };
  Object.keys(sent).forEach(k => {
    if (triggers.some(t => k.startsWith(`${t}:`))) delete sent[k];
  });

  const nextAt = computeNextRunForLead(filtered);
  const patch = {
    secuenciasActivas: filtered,
    sequenceSentSteps: sent,
    hasActiveSequences: filtered.length > 0
  };
  if (nextAt) patch.nextSequenceRunAt = nextAt;
  else {
    patch.nextSequenceRunAt = FieldValue.delete();
    patch.sequenceSentSteps = FieldValue.delete();
  }
  await leadRef.set(patch, { merge: true });
  return secAct.length - filtered.length;
}

// 🔹 Cancelar TODO lo pendiente de un lead
export async function cancelAllSequences(leadId) {
  if (!leadId) return 0;

  const leadRef = db.collection('leads').doc(leadId);
  await leadRef.set({
    hasActiveSequences: false,
    secuenciasActivas: [],
    nextSequenceRunAt: FieldValue.delete(),
    sequenceSentSteps: FieldValue.delete(),
    sequenceLock: FieldValue.delete()
  }, { merge: true });
  return 1;
}

// 🔹 Pausar / reanudar por lead (manual o por UI)
export async function pauseSequences(leadId) {
  if (!leadId) return false;
  await db.collection('leads').doc(leadId).set({ seqPaused: true }, { merge: true });
  return true;
}
export async function resumeSequences(leadId) {
  if (!leadId) return false;
  await db.collection('leads').doc(leadId).set({ seqPaused: false }, { merge: true });
  return true;
}

/* -------------------------- entrega de mensajes ------------------------- */

async function deliverPayload(leadId, payload) {
  const leadSnap = await db.collection('leads').doc(leadId).get();
  if (!leadSnap.exists) throw new Error(`Lead no existe: ${leadId}`);

  const lead = { id: leadSnap.id, ...leadSnap.data() };

  // Paso "disparar secuencia": no envía mensaje, encadena otra secuencia.
  // Permite, p.ej., que WebEnviada arranque CierrePost al terminar.
  const stepTypeRaw = normType(payload?.type || '');
  if (stepTypeRaw === 'secuencia' || stepTypeRaw === 'trigger' || stepTypeRaw === 'disparar_secuencia') {
    const targetTrigger = String(payload?.contenido || payload?.trigger || '').trim();
    if (targetTrigger) {
      await scheduleSequenceForLead(leadId, targetTrigger, new Date(), { allowReschedule: true });
      await persistSystemMessage(leadId, `[sequence-chain] disparada secuencia '${targetTrigger}'`);
      console.log(`[SEQ] chain → lead=${leadId} dispara '${targetTrigger}'`);
    } else {
      console.warn(`[SEQ] chain sin trigger destino en lead=${leadId}`);
    }
    return;
  }

  const { jid, phone } = resolveLeadJidAndPhone(lead);
  if (!jid) throw new Error(`Lead sin JID ni teléfono: ${leadId}`);

  const rawType = (payload?.type || 'texto');
  const type = resolveType(rawType); // ⬅️ normalizado
  const contenido = payload?.contenido || payload?.message || '';
  const seconds = Number.isFinite(+payload?.seconds) ? +payload.seconds : null;
  const sequenceMeta = {
    sequenceTrigger: String(payload?.sequenceTrigger || '').trim(),
    sequenceStep: payload?.sequenceStep ?? null,
  };

  const sock = getWhatsAppSock();
  if (!sock) {
    throw createWhatsAppUnavailableError(`WhatsApp no está conectado (estado: ${getConnectionStatus() || 'desconocido'})`);
  }

  console.log(`[SEQ] dispatch → ${jid} type=${type} delay? (payload no incluye delay)`);
  switch (type) {
    case 'texto': {
      const text = replacePlaceholders(contenido, lead).trim();
      if (text) {
        const sent = await sendWithRetry(sock, jid, { text, linkPreview: false }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: text, mediaType: 'text', waMessageId: sent?.key?.id || '', ...sequenceMeta });
      }
      break;
    }

    case 'formulario': {
      const text = replacePlaceholders(contenido, lead).trim();
      if (text) {
        const sent = await sendWithRetry(sock, jid, { text, linkPreview: false }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: text, mediaType: 'text', waMessageId: sent?.key?.id || '', ...sequenceMeta });
      }
      break;
    }

   case 'audio': {
      const src = replacePlaceholders(contenido, lead).trim();

      const ptt = payload?.ptt === true || String(payload?.ptt).toLowerCase() === 'true' || true;
      const forwarded = payload?.forwarded === true || String(payload?.forwarded).toLowerCase() === 'true';

      if (src) {
        const audioSource = /^https?:/i.test(src) ? { url: src } : src;
        // sendAudioMessage permite jid o teléfono normalizado
        let sent = false;
        let lastErr = null;
        for (let i = 0; i < 3 && !sent; i++) {
          try {
            sent = await sendAudioMessage(jid, audioSource, { ptt, forwarded });
          } catch (err) {
            lastErr = err;
            const msg = String(err?.message || err || '');
            const transient = /timed\s*out|timeout|socket|network|disconnected|aborted/i.test(msg);
            if (!transient || i === 2) throw err;
            await sleep((i + 1) * 3000);
          }
        }
        await persistOutgoing(leadId, { content: '', mediaType: 'audio', mediaUrl: src, waMessageId: sent?.key?.id || '', ...sequenceMeta });
      }
      break;
    }



    case 'imagen': {
      const url = replacePlaceholders(contenido, lead).trim();
      if (url) {
        const sent = await sendWithRetry(sock, jid, { image: { url } }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: '', mediaType: 'image', mediaUrl: url, waMessageId: sent?.key?.id || '', ...sequenceMeta });
      }
      break;
    }

    case 'video': {
      const url = replacePlaceholders(contenido, lead).trim();
      if (url) {
        const sent = await sendWithRetry(sock, jid, { video: { url } }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: '', mediaType: 'video', mediaUrl: url, waMessageId: sent?.key?.id || '', ...sequenceMeta });
      }
      break;
    }

    case 'videonota': { // ← incluye 'video_note', 'video-note', 'ptv', etc. por normalización
      const url = replacePlaceholders(contenido, lead).trim();
      console.log(`[SEQ] videonota → ${jid} url=${url || '(vacío)'} seconds=${seconds ?? 'n/a'}`);
      if (url) {
        let sent = false;
        let lastErr = null;
        for (let i = 0; i < 3 && !sent; i++) {
          try {
            sent = await sendVideoNote(phone || jid, url, seconds);
          } catch (err) {
            lastErr = err;
            const msg = String(err?.message || err || '');
            const transient = /timed\s*out|timeout|socket|network|disconnected|aborted/i.test(msg);
            if (!transient || i === 2) throw err;
            await sleep((i + 1) * 3000);
          }
        }
        await persistOutgoing(leadId, { content: '', mediaType: 'video_note', mediaUrl: url, waMessageId: sent?.key?.id || '', ...sequenceMeta });
      }
      break;
    }

    default: {
      // fallback a texto
      const text = replacePlaceholders(contenido, lead).trim();
      if (text) {
        const sent = await sendWithRetry(sock, jid, { text, linkPreview: false }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: text, mediaType: 'text', waMessageId: sent?.key?.id || '', ...sequenceMeta });
      } else {
        console.warn(`[SEQ] tipo no soportado: ${rawType} (normalizado=${type})`);
      }
    }
  }
}

/* ----------------------------- procesar cola ---------------------------- */
/**
 * Procesa jobs pendientes cuya dueAt <= ahora.
 * Orden total: dueAt ASC, idx ASC, createdAt ASC.
 * Respeta:
 *  - Pausa por lead (seqPaused)
 *  - Paro duro por etiqueta (Compro / DetenerSecuencia / StopSequences)
 */
export async function processQueue({ batchSize = 100, shard = null } = {}) {
  const now = new Date();

  let q = db.collection('sequenceQueue')
    .where('status', '==', 'pending')
    .where('dueAt', '<=', now)
    .orderBy('dueAt', 'asc')
    .limit(batchSize);

  if (shard !== null) q = q.where('shard', '==', shard);

  const snap = await q.get();
  if (snap.empty) return 0;

  // caches para no golpear Firestore por job
  const leadCache = new Map();   // leadId -> leadData
  const stopCache = new Map();   // leadId -> 'paused' | 'stopped' | null

  // Orden determinista adicional por idx y createdAt
  const jobs = snap.docs
    .map(d => ({ id: d.id, ref: d.ref, ...d.data() }))
    .sort((a, b) => {
      const da = a.dueAt?.toMillis?.() ?? +new Date(a.dueAt);
      const dbt = b.dueAt?.toMillis?.() ?? +new Date(b.dueAt);
      if (da !== dbt) return da - dbt;
      if ((a.idx ?? 0) !== (b.idx ?? 0)) return (a.idx ?? 0) - (b.idx ?? 0);
      const ca = a.createdAt?.toMillis?.() ?? Number.MAX_SAFE_INTEGER;
      const cb = b.createdAt?.toMillis?.() ?? Number.MAX_SAFE_INTEGER;
      return ca - cb;
    });

  for (const job of jobs) {
    try {
      const isReminderJob = String(job?.jobType || '').toLowerCase() === 'reminder';

      // obtener estado del lead (cacheado)
      let lead = leadCache.get(job.leadId);
      if (!lead) {
        lead = await _getLead(job.leadId);
        leadCache.set(job.leadId, lead);
      }
      if (!lead) {
        // si el lead no existe, marca error y sigue
        await job.ref.update({
          status: 'error',
          processedAt: FieldValue.serverTimestamp(),
          error: 'Lead no existe'
        });
        continue;
      }

      // ¿pausado o parado?
      let stopState = stopCache.get(job.leadId);
      if (!stopState) {
        const etiquetas = Array.isArray(lead.etiquetas) ? lead.etiquetas : [];
        const hasHardStop =
          etiquetas.includes('Compro') ||
          etiquetas.includes('DetenerSecuencia') ||
          etiquetas.includes('StopSequences') ||
          lead.stopSequences === true;

        if (hasHardStop) stopState = 'stopped';
        else if (lead.seqPaused) stopState = 'paused';
        else stopState = null;

        stopCache.set(job.leadId, stopState);
      }

      if (!isReminderJob) {
        if (stopState === 'paused') {
          await job.ref.update({
            status: 'paused',
            processedAt: FieldValue.serverTimestamp()
          });
          continue;
        }

        if (stopState === 'stopped') {
          // marca este job como cancelado y borra el resto pendientes del lead
          await job.ref.update({
            status: 'canceled',
            processedAt: FieldValue.serverTimestamp(),
            error: 'Lead con stop flag/etiqueta'
          });
          // cancelar todo lo demás una sola vez por lead
          if (!lead._allCanceledOnce) {
            await cancelAllSequences(job.leadId).catch(() => {});
            lead._allCanceledOnce = true;
            leadCache.set(job.leadId, lead);
          }
          continue;
        }
      }

      // entrega normal
      await deliverPayload(job.leadId, job.payload);

      await job.ref.update({
        status: 'sent',
        processedAt: FieldValue.serverTimestamp()
      });

      const leadPatch = {
        lastMessageAt: FieldValue.serverTimestamp(),
        lastOutboundAt: FieldValue.serverTimestamp(),
      };
      const jobType = String(job?.jobType || '').toLowerCase();
      if (jobType === 'ai_followup') {
        leadPatch['aiFollowup.lastSentAt'] = FieldValue.serverTimestamp();
        leadPatch['aiFollowup.lastCampaignStatus'] = 'sent';
        leadPatch['aiFollowup.lastCampaignSource'] = String(job?.source || 'always-on-reactivation');
        leadPatch['aiFollowup.lastCampaignId'] = String(job?.campaign?.id || '');
        leadPatch['aiFollowup.lastVariationKey'] = String(job?.campaign?.variationKey || '');
        leadPatch['aiFollowup.lastContextKey'] = String(job?.campaign?.contextKey || '');
        leadPatch['aiFollowup.lastMessagePreview'] = String(job?.payload?.contenido || '').trim().slice(0, 220);
        leadPatch['aiFollowup.nextAiTouchAt'] = FieldValue.delete();
        leadPatch['aiFollowup.touchCount'] = FieldValue.increment(1);
      }

      const leadRef = db.collection('leads').doc(job.leadId);
      try {
        await leadRef.update(leadPatch);
      } catch {
        await leadRef.set(leadPatch, { merge: true });
      }

      await sleep(350);
    } catch (err) {
      const msg = String(err?.message || err);
      console.error(`[QUEUE] error job=${job.id}: ${msg}`);

      if (isWhatsAppUnavailableError(err)) {
        const retryCount = Number(job.retry || 0);
        await job.ref.update({
          status: 'pending',
          dueAt: new Date(Date.now() + WA_UNAVAILABLE_RETRY_MS),
          retry: retryCount + 1,
          error: msg,
          processedAt: FieldValue.serverTimestamp()
        });
        console.log(`[QUEUE] ↻ WhatsApp desconectado; job=${job.id} reprogramado en ${WA_UNAVAILABLE_RETRY_MS}ms`);
        continue;
      }

      // Reintento simple para errores transitorios de conexión/socket
      const transient = /socket|terminated|timed out|econn|network|disconnected|closed/i.test(msg);
      const retryCount = Number(job.retry || 0);

      if (transient && retryCount < 3) {
        const delayMs = (retryCount + 1) * 15000; // 15s, 30s, 45s
        await job.ref.update({
          status: 'pending',
          dueAt: new Date(Date.now() + delayMs),
          retry: retryCount + 1,
          error: msg,
          processedAt: FieldValue.serverTimestamp()
        });
        console.log(`[QUEUE] ↻ reprogramado job=${job.id} en ${delayMs}ms (retry=${retryCount + 1})`);
      } else {
        await job.ref.update({
          status: 'error',
          processedAt: FieldValue.serverTimestamp(),
          error: msg
        });
      }
    }
  }

  return jobs.length;
}

/* ------------------ nuevo motor de secuencias por lead ------------------ */

function normalizeSecuencias(raw) {
  if (!Array.isArray(raw)) return [];
  return raw
    .map(s => ({
      trigger: s?.trigger,
      startTime: s?.startTime || s?.start_time || s?.startedAt || s?.start || s?.createdAt,
      index: Number.isFinite(+s?.index) ? +s.index : 0,
      completed: !!s?.completed,
      status: String(s?.status || (s?.waitingForReply ? 'waiting_for_reply' : 'running')).trim() || 'running'
    }))
    .filter(s => !!s.trigger);
}

function shouldPauseAutomationForSales(lead = {}) {
  const queueStatus = String(lead?.queue?.status || '').trim();
  const routingStatus = String(lead?.routing?.status || lead?.salesBrainCurrent?.routing?.status || '').trim();
  const status = String(lead?.estado || '').trim().toLowerCase();
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas.map((tag) => String(tag || '').trim().toLowerCase()) : [];
  return queueStatus === 'claimed'
    || routingStatus === 'ready_for_agent'
    || status === 'compro'
    || status === 'cliente'
    || status === 'no interesado'
    || lead?.stopSequences === true
    || lead?.salesBrainHumanControl === true
    || lead?.humanControl === true
    || tags.includes('detenersecuencia')
    || tags.includes('stopsequences');
}

async function recordSequencePausedForAgent(leadRef, leadId, reason = 'sales_queue') {
  await leadRef.collection('salesActivity').add({
    type: 'sequence_paused',
    agentId: String((await leadRef.get()).data()?.salesOwner || ''),
    createdAt: FieldValue.serverTimestamp(),
    metadata: { reason },
  }).catch(() => {});
  await persistSystemMessage(leadId, `[sequence] pausada por atencion comercial: ${reason}`).catch(() => {});
}

async function takeSequenceLock(leadRef) {
  const nowMs = Date.now();
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(leadRef);
    if (!snap.exists) return { ok: false, data: null };
    const data = snap.data() || {};
    const lockTs = toDateSafe(data.sequenceLock);
    if (lockTs && (nowMs - lockTs.getTime()) < SEQUENCE_LOCK_TTL_MS) {
      return { ok: false, data };
    }
    tx.update(leadRef, { sequenceLock: Timestamp.now() });
    return { ok: true, data };
  });
}

async function releaseSequenceLock(leadRef) {
  await leadRef.set({ sequenceLock: FieldValue.delete() }, { merge: true }).catch(() => {});
}

async function persistSystemMessage(leadId, content) {
  try {
    await db.collection('leads').doc(leadId).collection('messages').add({
      sender: 'system',
      content,
      timestamp: new Date()
    });
  } catch (err) {
    console.warn('[persistSystemMessage] no se pudo guardar:', err?.message || err);
  }
}

async function disableLeadSequencesMissingTarget(leadRef, leadData = {}, reason = '') {
  const safeReason = String(reason || 'Lead sin JID ni teléfono').trim();
  await leadRef.set({
    hasActiveSequences: false,
    secuenciasActivas: [],
    nextSequenceRunAt: FieldValue.delete(),
    sequenceSentSteps: FieldValue.delete(),
    sequenceBlockedReason: 'missing_destination',
    sequenceBlockedDetail: safeReason,
    sequenceBlockedAt: Timestamp.now(),
    sequenceBlockedMeta: {
      jid: String(leadData?.jid || ''),
      resolvedJid: String(leadData?.resolvedJid || ''),
      telefono: cleanLeadPhone(leadData?.telefono || ''),
      lidJid: String(leadData?.lidJid || ''),
    },
  }, { merge: true });
}

export async function processLeadSequences(leadId) {
  const leadRef = db.collection('leads').doc(leadId);
  const lock = await takeSequenceLock(leadRef);
  if (!lock.ok) return { processed: 0, reason: 'locked' };

  let processed = 0;
  try {
    const snap = await leadRef.get();
    if (!snap.exists) return { processed: 0, reason: 'missing' };

    const data = { id: snap.id, ...(snap.data() || {}) };
    let secuencias = normalizeSecuencias(data.secuenciasActivas);
    let sentSteps = { ...(data.sequenceSentSteps || {}) };
    let deliveredHistory = Array.isArray(data.sequenceDeliveredTriggers)
      ? [...data.sequenceDeliveredTriggers]
      : [];
    const formCompleted = hasLeadCompletedForm(data);
    const blockedByContext = [];
    secuencias = secuencias.filter((seq) => {
      if (!seq || seq.completed) return false;
      const contextBlock = shouldBlockSequenceByLeadContext(data, seq.trigger);
      if (!contextBlock.blocked) return true;
      blockedByContext.push({ trigger: seq.trigger, reason: contextBlock.reason });
      return false;
    });

    if (blockedByContext.length > 0) {
      const nextAtAfterBlock = computeNextRunForLead(secuencias);
      const blockPatch = {
        secuenciasActivas: secuencias,
        hasActiveSequences: secuencias.length > 0,
        sequenceBlockedReason: blockedByContext[0]?.reason || 'sequence_context_block',
        sequenceBlockedAt: Timestamp.now(),
      };
      if (nextAtAfterBlock) blockPatch.nextSequenceRunAt = nextAtAfterBlock;
      else blockPatch.nextSequenceRunAt = FieldValue.delete();
      await leadRef.set(blockPatch, { merge: true });
      for (const blocked of blockedByContext) {
        await persistSystemMessage(
          leadId,
          `[sequence] bloqueada por contexto: ${blocked.trigger} (${blocked.reason})`
        ).catch(() => {});
      }
    }

    if (shouldPauseAutomationForSales(data)) {
      const paused = secuencias.map((seq) => (
        seq.completed || seq.status === 'waiting_for_reply'
          ? seq
          : { ...seq, status: 'paused_for_agent', pausedAt: new Date().toISOString() }
      ));
      await leadRef.set({
        secuenciasActivas: paused,
        hasActiveSequences: paused.some((seq) => seq?.completed !== true && seq?.status !== 'paused_for_agent'),
        nextSequenceRunAt: FieldValue.delete(),
        sequencePausedReason: 'sales_queue',
        sequencePausedAt: Timestamp.now(),
      }, { merge: true });
      await recordSequencePausedForAgent(leadRef, leadId, 'sales_queue');
      return { processed: 0, reason: 'paused_for_agent' };
    }

    if (!secuencias.length) {
      await leadRef.set({
        secuenciasActivas: [],
        nextSequenceRunAt: FieldValue.delete(),
        sequenceSentSteps: FieldValue.delete(),
        hasActiveSequences: false
      }, { merge: true });
      return { processed: 0, reason: 'empty' };
    }

    const destination = resolveLeadJidAndPhone(data);
    if (!destination?.jid) {
      console.warn(
        `[processLeadSequences] skip(missing-destination) lead=${leadId} telefono=${String(data?.telefono || '')} jid=${String(data?.jid || '')} resolvedJid=${String(data?.resolvedJid || '')}`
      );
      await disableLeadSequencesMissingTarget(
        leadRef,
        data,
        `Lead sin JID ni teléfono: ${leadId}`
      );
      await persistSystemMessage(leadId, '[sequence] pausada: destino WhatsApp no resoluble');
      return { processed: 0, reason: 'missing_destination' };
    }

    const now = new Date();

    let stopProcessingSequences = false;
    let dueStepsProcessedThisRun = 0;

    for (const seq of secuencias) {
      if (stopProcessingSequences) break;
      if (seq.completed) continue;
      if (seq.status === 'waiting_for_reply' || seq.status === 'paused_for_agent') continue;
      if (formCompleted && shouldStopTriggerAfterForm(seq.trigger)) {
        seq.completed = true;
        continue;
      }
      const def = await getSequenceDefinition(seq.trigger);
      if (!def || def.active === false || !def.messages || def.messages.length === 0) {
        console.warn(
          `[processLeadSequences] skip(invalid-definition) lead=${leadId} trigger=${String(seq.trigger || '')} hasDef=${Boolean(def)} active=${def?.active !== false} steps=${Array.isArray(def?.messages) ? def.messages.length : 0}`
        );
        seq.completed = true;
        continue;
      }

      while (
        !seq.completed
        && seq.status !== 'waiting_for_reply'
        && seq.status !== 'paused_for_agent'
        && dueStepsProcessedThisRun < MAX_DUE_SEQUENCE_STEPS_PER_RUN
      ) {
        const runAt = computeSequenceStepRun(seq.trigger, seq.startTime, seq.index);
        if (!runAt) {
          seq.completed = true;
          break;
        }
        if (runAt > now) break; // aún no vence

        const stepKey = `${seq.trigger}:${seq.index}`;
        if (sentSteps[stepKey]) {
          seq.index += 1;
          if (seq.index >= def.messages.length) seq.completed = true;
          continue;
        }

        const msg = def.messages[seq.index] || {};
        const payloadMeta = {
          sequenceTrigger: seq.trigger,
          sequenceStep: seq.index,
        };
        const stepType = normType(msg?.type || '');
        const isQuestionStep = stepType === 'question';
        const payload = isQuestionStep
          ? { ...msg, ...payloadMeta, type: 'texto', contenido: msg.contenido || msg.message || '' }
          : { ...msg, ...payloadMeta };
        await deliverPayload(leadId, payload);
        processed += 1;
        dueStepsProcessedThisRun += 1;
        sentSteps[stepKey] = Timestamp.now();
        if (!hasTriggerInHistory(deliveredHistory, seq.trigger)) {
          deliveredHistory.push(seq.trigger);
        }
        await persistSystemMessage(leadId, `[sequence:${seq.trigger}] step ${seq.index} enviado`);

        if (isQuestionStep && msg.waitForReply === true) {
          seq.status = 'waiting_for_reply';
          await leadRef.set({
            sequenceQuestionPending: {
              status: 'waiting_for_reply',
              trigger: seq.trigger,
              index: seq.index,
              saveTo: String(msg.saveTo || '').trim() || null,
              objective: String(msg.objective || '').trim() || null,
              askedAt: Timestamp.now(),
            },
          }, { merge: true });
          stopProcessingSequences = true;
          break;
        }

        seq.index += 1;
        if (seq.index >= def.messages.length) seq.completed = true;
      }

      if (dueStepsProcessedThisRun >= MAX_DUE_SEQUENCE_STEPS_PER_RUN) {
        console.warn(
          `[processLeadSequences] limite de pasos vencidos alcanzado lead=${leadId} max=${MAX_DUE_SEQUENCE_STEPS_PER_RUN}`
        );
        break;
      }
    }

    // limpiar completados
    secuencias = secuencias.filter(s => !s.completed);
    const nextAt = computeNextRunForLead(secuencias);

    const patch = {
      secuenciasActivas: secuencias,
      hasActiveSequences: secuencias.length > 0,
      sequenceSentSteps: sentSteps
    };
    if (deliveredHistory.length > 0 || Array.isArray(data.sequenceDeliveredTriggers)) {
      patch.sequenceDeliveredTriggers = deliveredHistory;
    }
    if (nextAt) patch.nextSequenceRunAt = nextAt;
    else {
      patch.nextSequenceRunAt = FieldValue.delete();
      patch.sequenceSentSteps = FieldValue.delete();
    }
    await leadRef.set(patch, { merge: true });
    return { processed, nextAt };
  } finally {
    await releaseSequenceLock(leadRef);
  }
}

export async function processSequenceLeadsBatch({ limit = MAX_SEQUENCE_BATCH } = {}) {
  const now = new Date();
  const snap = await db.collection('leads')
    .where('nextSequenceRunAt', '<=', now)
    .orderBy('nextSequenceRunAt', 'asc')
    .limit(limit)
    .get();

  if (snap.empty) return 0;

  let total = 0;
  for (const doc of snap.docs) {
    try {
      const res = await processLeadSequences(doc.id);
      total += res?.processed || 0;
    } catch (err) {
      const msg = String(err?.message || err || '');
      console.error('[processSequenceLeadsBatch] error:', msg);

      if (/Lead sin JID ni teléfono/i.test(msg)) {
        const leadData = doc.data() || {};
        await disableLeadSequencesMissingTarget(doc.ref, leadData, msg).catch(() => {});
        await persistSystemMessage(doc.id, '[sequence] desactivada automáticamente por destino inválido').catch(() => {});
      }
    }
  }
  return total;
}

export async function hydrateNextSequenceRun({ limit = 50 } = {}) {
  const snap = await db.collection('leads')
    .where('secuenciasActivas', '!=', null)
    .limit(limit)
    .get();

  if (snap.empty) return 0;
  let updated = 0;
  for (const doc of snap.docs) {
    const data = doc.data() || {};
    const secuencias = normalizeSecuencias(data.secuenciasActivas);
    if (!secuencias.length) continue;
    for (const seq of secuencias) {
      if (!_sequenceDefCache.has(seq.trigger) || !isSeqCacheFresh(seq.trigger)) {
        await getSequenceDefinition(seq.trigger);
      }
    }
    const nextAt = computeNextRunForLead(secuencias);
    if (!nextAt) continue;
    if (sameMinute(data.nextSequenceRunAt, nextAt)) continue;
    await doc.ref.set({ nextSequenceRunAt: nextAt }, { merge: true });
    updated += 1;
  }
  return updated;
}

export async function backfillMissingSequences({ limit = 50, trigger = null } = {}) {
  const snap = await db.collection('leads')
    .where('secuenciasActivas', '==', null)
    .limit(limit)
    .get();

  if (snap.empty) return 0;

  let updated = 0;
  for (const doc of snap.docs) {
    const trg = trigger || doc.data()?.trigger || 'NuevoLeadWeb';
    const seq = { trigger: trg, startTime: new Date().toISOString(), index: 0, completed: false };
    const nextAt = await (async () => {
      const def = await getSequenceDefinition(trg);
      if (!def) return null;
      _sequenceDefCache.set(trg, def);
      return computeSequenceStepRun(trg, seq.startTime, seq.index);
    })();

    await doc.ref.set({
      secuenciasActivas: [seq],
      nextSequenceRunAt: nextAt || new Date(),
      sequenceSentSteps: {}
    }, { merge: true });
    updated += 1;
  }
  return updated;
}

// alias opcional usado por scheduler
export const processDueSequenceJobs = processSequenceLeadsBatch;

export {
  normalizeJid,
  phoneFromJid,
  extractJidFromLead,
  resolveLeadJidAndPhone,
  computeSequenceStepRun,
  computeNextRunForLead,
  hasSameTrigger
};
