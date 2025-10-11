// queue.js
import { db, admin } from './firebaseAdmin.js';
import {
  sendClipMessage,
  getWhatsAppSock,
  sendVideoNote
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

// persistencia uniforme en Firestore para salientes (business)
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

  if (!active || messages.length === 0) return 0;

  const batch = db.batch();
  const startMs = new Date(startAt).getTime();

  messages.forEach((m, idx) => {
    const delayMin = Number(m.delay || 0);
    // Jitter de 250ms por posición para mantener orden dentro del mismo minuto
    const dueAt = new Date(startMs + delayMin * 60_000 + idx * 250);
    const ref = db.collection('sequenceQueue').doc();
    batch.set(ref, {
      leadId,
      trigger,
      idx,
      payload: {
        type: m.type || 'texto',
        contenido: m.contenido || ''
      },
      dueAt,
      status: 'pending',
      shard: Math.floor(Math.random() * 10),
      createdAt: FieldValue.serverTimestamp()
    });
  });

  await batch.commit();

  await db.collection('leads').doc(leadId).set({
    hasActiveSequences: true
  }, { merge: true });

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

/* -------------------------- entrega de mensajes ------------------------- */

async function deliverPayload(leadId, payload) {
  const leadSnap = await db.collection('leads').doc(leadId).get();
  if (!leadSnap.exists) throw new Error(`Lead no existe: ${leadId}`);

  const lead = { id: leadSnap.id, ...leadSnap.data() };

  // Construye JID normalizado 521… para Baileys
  const e164 = toE164(lead.telefono);
  const jid  = e164ToJid(e164);

  const type = (payload?.type || 'texto').toLowerCase();
  const contenido = payload?.contenido || '';

  const sock = getWhatsAppSock();
  if (!sock) throw new Error('Socket de WhatsApp no está conectado');

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

    case 'audio':
    case 'clip': {
      const url = replacePlaceholders(contenido, lead).trim();
      if (url) {
        // usa helper robusto con fallback URL→buffer y reintentos
        await sendClipMessage(e164, url).catch(err => { throw err; });
        await persistOutgoing(leadId, { content: '', mediaType: 'audio', mediaUrl: url });
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

    case 'videonota':
    case 'video_note':
    case 'video-note': {
      const url = replacePlaceholders(contenido, lead).trim();
      if (url) {
        await sendVideoNote(e164, url);
        await persistOutgoing(leadId, { content: '', mediaType: 'video_note', mediaUrl: url });
      }
      break;
    }

    default: {
      const text = replacePlaceholders(contenido, lead).trim();
      if (text) {
        await sock.sendMessage(jid, { text, linkPreview: false }, { timeoutMs: 60_000 });
        await persistOutgoing(leadId, { content: text, mediaType: 'text' });
      }
    }
  }
}

/* ----------------------------- procesar cola ---------------------------- */
/**
 * Procesa jobs pendientes cuya dueAt <= ahora.
 * Orden total: dueAt ASC, idx ASC, createdAt ASC.
 * ENVÍO SECUENCIAL para preservar orden exacto.
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
      await job.ref.update({
        status: 'error',
        processedAt: FieldValue.serverTimestamp(),
        error: String(err?.message || err)
      });
    }
  }

  return jobs.length;
}
