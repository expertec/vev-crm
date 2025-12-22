// queue.js
import { db, admin } from './firebaseAdmin.js';
import {
  sendClipMessage,
  getWhatsAppSock,
  sendVideoNote,
  sendAudioMessage,
} from './whatsappService.js';

const { FieldValue } = admin.firestore;
const { Timestamp } = admin.firestore;

const SEQUENCE_LOCK_TTL_MS = 2 * 60 * 1000; // 2 minutos
const MAX_SEQUENCE_BATCH = 25;

/* ----------------------------- utilidades ------------------------------ */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function firstName(full = '') {
  return String(full).trim().split(/\s+/)[0] || '';
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

function replacePlaceholders(template, lead) {
  if (!template) return '';
  const telFromJid = String(lead.jid || '').split('@')[0].split(':')[0].replace(/\D/g, '');
  const tel = telFromJid || String(lead.telefono || '').replace(/\D/g, '');
  const nameFirst = firstName(lead.nombre || '');
  return String(template).replace(/\{\{(\w+)\}\}/g, (_, key) => {
    if (key === 'telefono') return tel;
    if (key === 'nombre') return nameFirst;
    return lead[key] ?? '';
  });
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

function resolveLeadJidAndPhone(lead) {
  const phoneRaw = lead?.telefono || '';
  let jidCandidate =
    normalizeJid(lead?.jid) ||
    normalizeJid(lead?.id) ||
    null;

  // 🔧 CRÍTICO: Validar que el JID NO sea @lid
  if (jidCandidate && jidCandidate.includes('@lid')) {
    console.warn(`[resolveLeadJidAndPhone] ⚠️ JID inválido (@lid) detectado: ${jidCandidate} - Reconstruyendo desde teléfono`);
    jidCandidate = null; // Forzar reconstrucción desde teléfono
  }

  // 🔧 Validar que el JID sea @s.whatsapp.net
  if (jidCandidate && !jidCandidate.includes('@s.whatsapp.net')) {
    console.warn(`[resolveLeadJidAndPhone] ⚠️ JID sin dominio correcto: ${jidCandidate} - Reconstruyendo`);
    jidCandidate = null;
  }

  const normalizedPhone = normalizePhoneForWA(phoneRaw);

  if (jidCandidate) {
    console.log(`[resolveLeadJidAndPhone] ✅ Usando JID existente: ${jidCandidate}`);
    return { jid: jidCandidate, phone: normalizedPhone };
  }

  if (normalizedPhone) {
    const constructedJid = `${normalizedPhone}@s.whatsapp.net`;
    console.log(`[resolveLeadJidAndPhone] 🔧 JID construido desde teléfono: ${constructedJid}`);
    return { jid: constructedJid, phone: normalizedPhone };
  }

  console.error(`[resolveLeadJidAndPhone] ❌ No se pudo resolver JID ni teléfono para lead:`, {
    leadId: lead?.id,
    telefono: phoneRaw,
    jid: lead?.jid
  });

  return { jid: null, phone: null };
}

function hasSameTrigger(secuencias = [], trigger = '') {
  return Array.isArray(secuencias) && secuencias.some(s => (s?.trigger || '').toLowerCase() === String(trigger).toLowerCase());
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
  ptv: 'videonota'
};
function resolveType(raw) {
  const k = normType(raw);
  return TYPE_MAP[k] || k;
}

/* ---------------- persistencia uniforme de salientes ------------------- */
async function persistOutgoing(leadId, { content = '', mediaType = 'text', mediaUrl = null }) {
  const now = new Date();
  await db.collection('leads').doc(leadId).collection('messages').add({
    content,
    mediaType,
    mediaUrl,
    sender: 'business',
    timestamp: now
  });
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
      const msg = String(err?.message || err || '');
      const transient = /timed\s*out|timeout|socket|network|disconnected|aborted/i.test(msg);
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
export async function scheduleSequenceForLead(leadId, trigger, startAt = new Date()) {
  const leadRef = db.collection('leads').doc(leadId);
  const leadSnap = await leadRef.get();
  const leadData = leadSnap.exists ? leadSnap.data() || {} : {};

  const def = await getSequenceDefinition(trigger);
  if (!def || def.active === false || !def.messages || def.messages.length === 0) return 0;

  // Reinicio si ya existe y no se fuerza: no duplicar
  const secAct = Array.isArray(leadData.secuenciasActivas) ? [...leadData.secuenciasActivas] : [];
  if (hasSameTrigger(secAct, trigger)) {
    console.log(`[scheduleSequenceForLead] trigger '${trigger}' ya presente en ${leadId}, no se duplica.`);
    return 0;
  }

  const newSeq = {
    trigger,
    startTime: toDateSafe(startAt)?.toISOString?.() || new Date().toISOString(),
    index: 0,
    completed: false
  };

  secAct.push(newSeq);

  // limpiar pasos enviados previos de este trigger
  const sent = { ...(leadData.sequenceSentSteps || {}) };
  Object.keys(sent).forEach(k => { if (k.startsWith(`${trigger}:`)) delete sent[k]; });

  const nextAt = computeNextRunForLead(secAct);

  const payload = {
    secuenciasActivas: secAct,
    hasActiveSequences: true,
    sequenceSentSteps: sent
  };
  if (nextAt) payload.nextSequenceRunAt = nextAt;
  await leadRef.set(payload, { merge: true });

  // Etiqueta trigger
  await leadRef.set({ etiquetas: FieldValue.arrayUnion(trigger) }, { merge: true }).catch(() => {});

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
  const { jid, phone } = resolveLeadJidAndPhone(lead);
  if (!jid) throw new Error(`Lead sin JID ni teléfono: ${leadId}`);

  const rawType = (payload?.type || 'texto');
  const type = resolveType(rawType); // ⬅️ normalizado
  const contenido = payload?.contenido || '';
  const seconds = Number.isFinite(+payload?.seconds) ? +payload.seconds : null;

  const sock = getWhatsAppSock();
  if (!sock) throw new Error('Socket de WhatsApp no está conectado');

  console.log(`[SEQ] dispatch → ${jid} type=${type} delay? (payload no incluye delay)`);
  switch (type) {
    case 'texto': {
      const text = replacePlaceholders(contenido, lead).trim();
      if (text) {
        await sendWithRetry(sock, jid, { text, linkPreview: false }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: text, mediaType: 'text' });
      }
      break;
    }

    case 'formulario': {
      const text = replacePlaceholders(contenido, lead).trim();
      if (text) {
        await sendWithRetry(sock, jid, { text, linkPreview: false }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: text, mediaType: 'text' });
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
            await sendAudioMessage(jid, audioSource, { ptt, forwarded });
            sent = true;
          } catch (err) {
            lastErr = err;
            const msg = String(err?.message || err || '');
            const transient = /timed\s*out|timeout|socket|network|disconnected|aborted/i.test(msg);
            if (!transient || i === 2) throw err;
            await sleep((i + 1) * 3000);
          }
        }
        await persistOutgoing(leadId, { content: '', mediaType: 'audio', mediaUrl: src });
      }
      break;
    }



    case 'imagen': {
      const url = replacePlaceholders(contenido, lead).trim();
      if (url) {
        await sendWithRetry(sock, jid, { image: { url } }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: '', mediaType: 'image', mediaUrl: url });
      }
      break;
    }

    case 'video': {
      const url = replacePlaceholders(contenido, lead).trim();
      if (url) {
        await sendWithRetry(sock, jid, { video: { url } }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: '', mediaType: 'video', mediaUrl: url });
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
            await sendVideoNote(phone || jid, url, seconds);
            sent = true;
          } catch (err) {
            lastErr = err;
            const msg = String(err?.message || err || '');
            const transient = /timed\s*out|timeout|socket|network|disconnected|aborted/i.test(msg);
            if (!transient || i === 2) throw err;
            await sleep((i + 1) * 3000);
          }
        }
        await persistOutgoing(leadId, { content: '', mediaType: 'video_note', mediaUrl: url });
      }
      break;
    }

    default: {
      // fallback a texto
      const text = replacePlaceholders(contenido, lead).trim();
      if (text) {
        await sendWithRetry(sock, jid, { text, linkPreview: false }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: text, mediaType: 'text' });
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

      // entrega normal
      await deliverPayload(job.leadId, job.payload);

      await job.ref.update({
        status: 'sent',
        processedAt: FieldValue.serverTimestamp()
      });

      await db.collection('leads').doc(job.leadId).set({
        lastMessageAt: FieldValue.serverTimestamp()
      }, { merge: true });

      await sleep(350);
    } catch (err) {
      const msg = String(err?.message || err);
      console.error(`[QUEUE] error job=${job.id}: ${msg}`);

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
      completed: !!s?.completed
    }))
    .filter(s => !!s.trigger);
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

    if (!secuencias.length) {
      await leadRef.set({
        secuenciasActivas: [],
        nextSequenceRunAt: FieldValue.delete(),
        sequenceSentSteps: FieldValue.delete(),
        hasActiveSequences: false
      }, { merge: true });
      return { processed: 0, reason: 'empty' };
    }

    const now = new Date();

    for (const seq of secuencias) {
      if (seq.completed) continue;
      const def = await getSequenceDefinition(seq.trigger);
      if (!def || def.active === false || !def.messages || def.messages.length === 0) {
        seq.completed = true;
        continue;
      }

      const runAt = computeSequenceStepRun(seq.trigger, seq.startTime, seq.index);
      if (!runAt) {
        seq.completed = true;
        continue;
      }
      if (runAt > now) continue; // aún no vence

      const stepKey = `${seq.trigger}:${seq.index}`;
      if (sentSteps[stepKey]) {
        seq.index += 1;
        if (seq.index >= def.messages.length) seq.completed = true;
        continue;
      }

      const msg = def.messages[seq.index] || {};
      await deliverPayload(leadId, msg);
      processed += 1;
      sentSteps[stepKey] = Timestamp.now();
      await persistSystemMessage(leadId, `[sequence:${seq.trigger}] step ${seq.index} enviado`);

      seq.index += 1;
      if (seq.index >= def.messages.length) seq.completed = true;
    }

    // limpiar completados
    secuencias = secuencias.filter(s => !s.completed);
    const nextAt = computeNextRunForLead(secuencias);

    const patch = {
      secuenciasActivas: secuencias,
      hasActiveSequences: secuencias.length > 0,
      sequenceSentSteps: sentSteps
    };
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
      console.error('[processSequenceLeadsBatch] error:', err?.message || err);
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
  resolveLeadJidAndPhone,
  computeSequenceStepRun,
  computeNextRunForLead,
  hasSameTrigger
};
