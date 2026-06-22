// whatsappCrm.js
//
// Pipeline multi-tenant de WhatsApp: convierte los mensajes entrantes/salientes
// en conversaciones (leads) y mensajes, SCOPEADOS por negocio en Firestore:
//
//   Negocios/{negocioId}/leads/{contactId}
//   Negocios/{negocioId}/leads/{contactId}/mensajes/{messageId}
//
// Se conecta al transporte vía setInboundHandler(handleInbound) del
// whatsappSessionManager. Idempotente: usa messageId como id de doc para no
// duplicar (Baileys puede reemitir).

import admin from 'firebase-admin';
import { db } from './firebaseAdmin.js';
import { sendText as waSendText } from './whatsappSessionManager.js';
import { getWhatsAppConfig, isWithinBusinessHours, findKeywordReply } from './whatsappConfig.js';

const { FieldValue } = admin.firestore;

const PREVIEW_MAX = 200;
const AWAY_COOLDOWN_MS = 6 * 60 * 60 * 1000; // máx. 1 mensaje de ausencia cada 6h por contacto

// Dedupe en memoria para no auto-responder dos veces el mismo mensaje.
const processedAutoReply = new Set();
function alreadyAutoReplied(messageId) {
  if (processedAutoReply.has(messageId)) return true;
  processedAutoReply.add(messageId);
  if (processedAutoReply.size > 5000) {
    // poda simple
    processedAutoReply.clear();
  }
  return false;
}

// Caché ligero de config por negocio (TTL 60s) para no leer Firestore por mensaje.
const configCache = new Map();
async function getCachedConfig(negocioId) {
  const hit = configCache.get(negocioId);
  if (hit && Date.now() - hit.at < 60000) return hit.config;
  const config = await getWhatsAppConfig(negocioId);
  configCache.set(negocioId, { config, at: Date.now() });
  return config;
}
export function invalidateConfigCache(negocioId) {
  configCache.delete(negocioId);
}

function phoneFromJid(jid) {
  return String(jid || '').split('@')[0].split(':')[0].replace(/\D/g, '');
}

function isProcessableJid(jid) {
  const j = String(jid || '');
  if (!j) return false;
  // Solo chats 1:1. Ignoramos grupos, status y newsletters.
  if (j.endsWith('@g.us')) return false;
  if (j.includes('@broadcast')) return false;
  if (j.endsWith('@newsletter')) return false;
  return true;
}

// Devuelve { text, type } a partir del contenido Baileys.
function extractContent(message) {
  const inner = message || {};
  if (inner.conversation) return { text: String(inner.conversation).trim(), type: 'text' };
  if (inner.extendedTextMessage?.text) return { text: String(inner.extendedTextMessage.text).trim(), type: 'text' };
  if (inner.imageMessage) return { text: String(inner.imageMessage.caption || '').trim(), type: 'image' };
  if (inner.videoMessage) return { text: String(inner.videoMessage.caption || '').trim(), type: 'video' };
  if (inner.documentMessage) return { text: String(inner.documentMessage.fileName || 'Documento').trim(), type: 'document' };
  if (inner.audioMessage) return { text: '', type: inner.audioMessage.ptt ? 'voice' : 'audio' };
  if (inner.stickerMessage) return { text: '', type: 'sticker' };
  if (inner.locationMessage) return { text: '', type: 'location' };
  if (inner.contactMessage || inner.contactsArrayMessage) return { text: '', type: 'contact' };
  if (inner.reactionMessage) return { text: String(inner.reactionMessage.text || '').trim(), type: 'reaction' };
  return { text: '', type: 'other' };
}

function previewFor(type, text) {
  if (text) return text.slice(0, PREVIEW_MAX);
  const labels = {
    image: '📷 Imagen', video: '🎥 Video', audio: '🎵 Audio', voice: '🎤 Nota de voz',
    document: '📄 Documento', sticker: 'Sticker', location: '📍 Ubicación', contact: '👤 Contacto',
  };
  return labels[type] || 'Mensaje';
}

function tsToMillis(messageTimestamp) {
  const n = Number(messageTimestamp || 0);
  if (!n) return Date.now();
  return n > 1e12 ? n : n * 1000; // soporta segundos o milisegundos
}

function leadRef(negocioId, contactId) {
  return db.collection('Negocios').doc(negocioId).collection('leads').doc(contactId);
}

/**
 * Persiste un mensaje (entrante o saliente) y actualiza el lead.
 */
async function persistMessage(negocioId, {
  jid, contactId, messageId, direction, text, type, name, atMillis, defaultStage = 'nuevo',
}) {
  const preview = previewFor(type, text);
  const at = admin.firestore.Timestamp.fromMillis(atMillis || Date.now());

  const ref = leadRef(negocioId, contactId);

  // Lead (conversación): upsert. unreadCount solo sube en entrantes.
  const leadPatch = {
    contactId,
    jid,
    phone: contactId,
    lastMessage: preview,
    lastMessageAt: at,
    lastDirection: direction,
    channel: 'whatsapp',
    updatedAt: FieldValue.serverTimestamp(),
  };
  if (name) leadPatch.name = name;
  if (direction === 'in') {
    leadPatch.unreadCount = FieldValue.increment(1);
  }

  // Transacción: createdAt solo en la primera vez (no se pisa en cada mensaje).
  let isNew = false;
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const patch = { ...leadPatch };
    if (!snap.exists) {
      isNew = true;
      patch.createdAt = FieldValue.serverTimestamp();
      patch.unreadCount = direction === 'in' ? 1 : 0; // valor inicial concreto
      patch.stage = defaultStage || 'nuevo'; // etapa de entrada del pipeline
    }
    tx.set(ref, patch, { merge: true });
  });

  // Mensaje (idempotente por messageId).
  const msgRef = ref.collection('mensajes').doc(String(messageId));
  await msgRef.set({
    messageId: String(messageId),
    direction,
    text: text || '',
    type,
    at,
    createdAt: FieldValue.serverTimestamp(),
  }, { merge: true });

  return { isNew };
}

// Envía un mensaje automático y lo registra en la conversación (no bloqueante).
async function sendAuto(negocioId, contactId, text) {
  try {
    const result = await waSendText(negocioId, contactId, text);
    await recordOutboundMessage(negocioId, contactId, text, result);
  } catch (e) {
    console.error(`[WA-CRM] auto-reply negocio=${negocioId} contacto=${contactId} error:`, e?.message);
  }
}

// Ejecuta las automatizaciones de configuración sobre un mensaje ENTRANTE.
async function runAutomations(negocioId, { contactId, messageId, text, isNew }) {
  if (alreadyAutoReplied(messageId)) return;
  const config = await getCachedConfig(negocioId);

  // 1) Bienvenida (solo en el primer mensaje del contacto).
  if (isNew && config?.welcome?.enabled && config.welcome.message) {
    await sendAuto(negocioId, contactId, config.welcome.message);
  }

  // 2) Respuesta por palabra clave.
  const keywordReply = findKeywordReply(config, text);
  if (keywordReply) {
    await sendAuto(negocioId, contactId, keywordReply);
    return; // si respondimos por keyword, no mandamos también el de ausencia
  }

  // 3) Mensaje de ausencia (fuera de horario), con cooldown por contacto.
  if (config?.away?.enabled && !isWithinBusinessHours(config)) {
    const ref = leadRef(negocioId, contactId);
    const snap = await ref.get();
    const lastAway = snap.exists ? snap.data()?.awaySentAt : null;
    const lastAwayMs = lastAway?.toMillis ? lastAway.toMillis() : 0;
    if (Date.now() - lastAwayMs > AWAY_COOLDOWN_MS) {
      await sendAuto(negocioId, contactId, config.away.message);
      await ref.set({ awaySentAt: admin.firestore.Timestamp.now() }, { merge: true });
    }
  }
}

/**
 * Handler de mensajes entrantes. Se registra con setInboundHandler.
 */
export async function handleInbound(negocioId, payload) {
  const { messages, type } = payload || {};
  if (!['notify', 'append'].includes(type || '')) return;
  const list = Array.isArray(messages) ? messages : [];

  // Etapa de entrada para leads nuevos = primera etapa del pipeline configurado.
  const config = await getCachedConfig(negocioId);
  const entryStage = config?.pipelineStages?.[0]?.id || 'nuevo';

  for (const msg of list) {
    try {
      const jid = msg?.key?.remoteJid || '';
      if (!isProcessableJid(jid)) continue;

      const fromMe = Boolean(msg?.key?.fromMe);
      const messageId = msg?.key?.id;
      if (!messageId) continue;

      const { text, type: contentType } = extractContent(msg.message);
      const contactId = phoneFromJid(jid);
      if (!contactId) continue;

      const { isNew } = await persistMessage(negocioId, {
        jid,
        contactId,
        messageId,
        direction: fromMe ? 'out' : 'in',
        text,
        type: contentType,
        name: fromMe ? null : (msg.pushName || null),
        atMillis: tsToMillis(msg.messageTimestamp),
        defaultStage: entryStage,
      });

      // Automatizaciones solo sobre mensajes ENTRANTES.
      if (!fromMe) {
        await runAutomations(negocioId, { contactId, messageId, text, isNew });
      }
    } catch (e) {
      console.error(`[WA-CRM] inbound negocio=${negocioId} msg error:`, e?.message);
    }
  }
}

function tsToIso(value) {
  if (!value) return null;
  if (typeof value.toMillis === 'function') return new Date(value.toMillis()).toISOString();
  if (value instanceof Date) return value.toISOString();
  return null;
}

/** Lista las conversaciones (leads) de un negocio, más recientes primero. */
export async function listConversations(negocioId, { limit = 50 } = {}) {
  const snap = await db
    .collection('Negocios').doc(negocioId).collection('leads')
    .orderBy('lastMessageAt', 'desc')
    .limit(Math.min(Math.max(Number(limit) || 50, 1), 200))
    .get();

  return snap.docs.map((doc) => {
    const d = doc.data() || {};
    return {
      contactId: d.contactId || doc.id,
      jid: d.jid || '',
      phone: d.phone || doc.id,
      name: d.name || '',
      lastMessage: d.lastMessage || '',
      lastDirection: d.lastDirection || '',
      lastMessageAt: tsToIso(d.lastMessageAt),
      unreadCount: Number(d.unreadCount || 0),
      stage: d.stage || 'nuevo',
    };
  });
}

/** Cambia la etapa (pipeline) de un lead. */
export async function updateLeadStage(negocioId, contactId, stage) {
  const id = String(contactId || '').replace(/\D/g, '');
  if (!id || !stage) return;
  await db
    .collection('Negocios').doc(negocioId).collection('leads').doc(id)
    .set({ stage: String(stage), updatedAt: FieldValue.serverTimestamp() }, { merge: true });
}

/** Métricas para el dashboard (agrega sobre los leads del negocio). */
export async function getDashboardMetrics(negocioId) {
  const snap = await db
    .collection('Negocios').doc(negocioId).collection('leads')
    .orderBy('lastMessageAt', 'desc')
    .limit(1000)
    .get();

  const byStage = {};
  let total = 0;
  let unread = 0;
  let activeToday = 0;
  const startOfDay = new Date();
  startOfDay.setHours(0, 0, 0, 0);
  const startMs = startOfDay.getTime();

  snap.forEach((doc) => {
    const d = doc.data() || {};
    total += 1;
    const stage = d.stage || 'nuevo';
    byStage[stage] = (byStage[stage] || 0) + 1;
    unread += Number(d.unreadCount || 0);
    const lastMs = d.lastMessageAt?.toMillis ? d.lastMessageAt.toMillis() : 0;
    if (lastMs >= startMs) activeToday += 1;
  });

  return { total, unread, activeToday, byStage };
}

/** Lista los mensajes de una conversación, en orden ascendente (cronológico). */
export async function listMessages(negocioId, contactId, { limit = 100 } = {}) {
  const id = String(contactId || '').replace(/\D/g, '');
  if (!id) return [];
  const snap = await db
    .collection('Negocios').doc(negocioId).collection('leads').doc(id).collection('mensajes')
    .orderBy('at', 'desc')
    .limit(Math.min(Math.max(Number(limit) || 100, 1), 300))
    .get();

  const rows = snap.docs.map((doc) => {
    const d = doc.data() || {};
    return {
      messageId: d.messageId || doc.id,
      direction: d.direction || 'in',
      text: d.text || '',
      type: d.type || 'text',
      at: tsToIso(d.at),
    };
  });
  return rows.reverse(); // ascendente para el hilo de chat
}

/** Marca una conversación como leída (unreadCount = 0). */
export async function markConversationRead(negocioId, contactId) {
  const id = String(contactId || '').replace(/\D/g, '');
  if (!id) return;
  await db
    .collection('Negocios').doc(negocioId).collection('leads').doc(id)
    .set({ unreadCount: 0, updatedAt: FieldValue.serverTimestamp() }, { merge: true });
}

/**
 * Registra un mensaje saliente enviado por la API (sendText), para que la
 * conversación quede completa aunque Baileys no reemita el echo.
 */
export async function recordOutboundMessage(negocioId, toPhone, text, sentResult) {
  try {
    const contactId = phoneFromJid(toPhone) || String(toPhone || '').replace(/\D/g, '');
    if (!contactId) return;
    const jid = `${contactId}@s.whatsapp.net`;
    const messageId = sentResult?.key?.id || `out_${Date.now()}`;
    const config = await getCachedConfig(negocioId);
    const entryStage = config?.pipelineStages?.[0]?.id || 'nuevo';
    await persistMessage(negocioId, {
      jid,
      contactId,
      messageId,
      direction: 'out',
      text: String(text || ''),
      type: 'text',
      name: null,
      atMillis: Date.now(),
      defaultStage: entryStage,
    });
  } catch (e) {
    console.error(`[WA-CRM] recordOutbound negocio=${negocioId} error:`, e?.message);
  }
}
