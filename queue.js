// queue.js
import { db, admin } from './firebaseAdmin.js';
import {
  sendClipMessage,
  getWhatsAppSock,
  sendVideoNote,
  sendAudioMessage,
} from './whatsappService.js';

const { FieldValue } = admin.firestore;

/* ----------------------------- utilidades ------------------------------ */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function firstName(full = '') {
  return String(full).trim().split(/\s+/)[0] || '';
}

function replacePlaceholders(template, lead) {
  if (!template) return '';
  const tel = String(lead.telefono || '').replace(/\D/g, '');
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

// helper: obtener lead
async function _getLead(leadId) {
  const snap = await db.collection('leads').doc(leadId).get();
  return snap.exists ? { id: snap.id, ...(snap.data() || {}) } : null;
}

/* -------------------- programar / cancelar secuencias ------------------- */
export async function scheduleSequenceForLead(leadId, trigger, startAt = new Date()) {
  // 0) limpiar pendientes del mismo trigger para este lead
  const oldSnap = await db.collection('sequenceQueue')
    .where('leadId', '==', leadId)
    .where('trigger', '==', trigger)
    .where('status', '==', 'pending')
    .get();

  if (!oldSnap.empty) {
    const bdel = db.batch();
    oldSnap.forEach(d => bdel.delete(d.ref));
    await bdel.commit();
  }

  // 1) intenta doc por id
  let seqDoc = await db.collection('secuencias').doc(trigger).get();

  // 2) fallback a where trigger==...
  if (!seqDoc.exists) {
    const q = await db.collection('secuencias')
      .where('trigger', '==', trigger)
      .limit(1)
      .get();
    if (!q.empty) seqDoc = q.docs[0];
  }

  if (!seqDoc.exists) {
    console.warn(`[scheduleSequenceForLead] No existe secuencias/${trigger}`);
    return 0;
  }

  const data = seqDoc.data() || {};
  const active = data.active !== false;
  const messages = Array.isArray(data.messages) ? data.messages : [];
  const oneShot = !!data.oneShot || !!data.once; // ← candado "una sola vez"

  if (!active || messages.length === 0) return 0;

  // === One-shot guard por lead+trigger ===
  if (oneShot) {
    const lead = await _getLead(leadId);
    const seqOnce = lead?.seqOnce || {};
    if (seqOnce[trigger]) {
      console.log(`[scheduleSequenceForLead] '${trigger}' oneShot ya marcado para ${leadId}; skip`);
      return 0;
    }
  }

  const batch = db.batch();
  const startMs = new Date(startAt).getTime();

  messages.forEach((m, idx) => {
    const delayMin = Number(m.delay || 0);
    // Jitter de 250ms por posición para mantener orden dentro del mismo minuto
    const dueAt = new Date(startMs + delayMin * 60_000 + idx * 250);
    const ref = db.collection('sequenceQueue').doc();

    // ⬇️ propagamos seconds si viene desde el front (p.ej. videonota)
    const payload = {
      type: m.type || 'texto',
      contenido: m.contenido || ''
    };
    if (m.seconds != null) payload.seconds = Number(m.seconds);
     if (m.forwarded != null) payload.forwarded = !!m.forwarded; // ← NUEVO
   if (m.ptt != null) payload.ptt = !!m.ptt;

    batch.set(ref, {
      leadId,
      trigger,
      idx,
      payload,
      dueAt,
      status: 'pending',
      shard: Math.floor(Math.random() * 10),
      createdAt: FieldValue.serverTimestamp(),
      retry: 0 // para reintentos simples
    });
  });

  await batch.commit();

  // Marcas en el lead:
  const leadPatch = {
    hasActiveSequences: true,
    seqStatus: {
      [trigger]: {
        scheduledAt: FieldValue.serverTimestamp(),
        count: messages.length
      }
    }
  };
  if (oneShot) {
    leadPatch.seqOnce = { [trigger]: true };
  }
  await db.collection('leads').doc(leadId).set(leadPatch, { merge: true });

  return messages.length;
}

export async function cancelSequences(leadId, triggers = []) {
  if (!leadId || !Array.isArray(triggers) || triggers.length === 0) return 0;

  const snap = await db.collection('sequenceQueue')
    .where('leadId', '==', leadId)
    .where('status', '==', 'pending')
    .get();

  if (snap.empty) return 0;

  const batch = db.batch();
  let n = 0;
  for (const d of snap.docs) {
    const t = d.data();
    if (triggers.includes(t.trigger)) {
      batch.delete(d.ref);
      n++;
    }
  }
  if (n) await batch.commit();
  return n;
}

// 🔹 Cancelar TODO lo pendiente de un lead
export async function cancelAllSequences(leadId) {
  if (!leadId) return 0;

  const snap = await db.collection('sequenceQueue')
    .where('leadId', '==', leadId)
    .where('status', '==', 'pending')
    .get();

  if (snap.empty) return 0;

  const batch = db.batch();
  let n = 0;
  for (const d of snap.docs) { batch.delete(d.ref); n++; }
  if (n) await batch.commit();

  await db.collection('leads').doc(leadId).set({
    hasActiveSequences: false
  }, { merge: true });

  return n;
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

  // Construye JID normalizado 521… para Baileys
  const e164 = toE164(lead.telefono);
  const jid  = e164ToJid(e164);

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
        await sock.sendMessage(jid, { text, linkPreview: false }, { timeoutMs: 60_000 });
        await persistOutgoing(leadId, { content: text, mediaType: 'text' });
      }
      break;
    }

    case 'formulario': {
      const text = replacePlaceholders(contenido, lead).trim();
      if (text) {
        await sock.sendMessage(jid, { text, linkPreview: false }, { timeoutMs: 60_000 });
        await persistOutgoing(leadId, { content: text, mediaType: 'text' });
      }
      break;
    }

   case 'audio': {
  const src = replacePlaceholders(contenido, lead).trim();

  // lee flags desde Firestore; fuerza ptt si quieres el look PTT
  const ptt = payload?.ptt === true || String(payload?.ptt).toLowerCase() === 'true' || true;
  const forwarded = payload?.forwarded === true || String(payload?.forwarded).toLowerCase() === 'true';

  if (src) {
    const audioSource = /^https?:/i.test(src) ? { url: src } : src;
    await sendAudioMessage(e164, audioSource, { ptt, forwarded });
    await persistOutgoing(leadId, { content: '', mediaType: 'audio', mediaUrl: src });
  }
  break;
}



    case 'imagen': {
      const url = replacePlaceholders(contenido, lead).trim();
      if (url) {
        await sock.sendMessage(jid, { image: { url } }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: '', mediaType: 'image', mediaUrl: url });
      }
      break;
    }

    case 'video': {
      const url = replacePlaceholders(contenido, lead).trim();
      if (url) {
        await sock.sendMessage(jid, { video: { url } }, { timeoutMs: 120_000 });
        await persistOutgoing(leadId, { content: '', mediaType: 'video', mediaUrl: url });
      }
      break;
    }

    case 'videonota': { // ← incluye 'video_note', 'video-note', 'ptv', etc. por normalización
      const url = replacePlaceholders(contenido, lead).trim();
      console.log(`[SEQ] videonota → ${jid} url=${url || '(vacío)'} seconds=${seconds ?? 'n/a'}`);
      if (url) {
        await sendVideoNote(e164, url, seconds);
        await persistOutgoing(leadId, { content: '', mediaType: 'video_note', mediaUrl: url });
      }
      break;
    }

    default: {
      // fallback a texto
      const text = replacePlaceholders(contenido, lead).trim();
      if (text) {
        await sock.sendMessage(jid, { text, linkPreview: false }, { timeoutMs: 60_000 });
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

// alias opcional usado por scheduler
export const processDueSequenceJobs = processQueue;
