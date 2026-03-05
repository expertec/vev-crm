// whatsappService.js - VERSIÓN CORREGIDA
// 🔧 FIX APLICADO: Listener procesa 'notify' y 'append' recientes con deduplicación por messageId
// Según documentación oficial de Baileys: https://baileys.wiki/docs/socket/receiving-updates/

import {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  downloadMediaMessage,
} from 'baileys';
import QRCode from 'qrcode-terminal';
import Pino from 'pino';
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import admin from 'firebase-admin';
import { db } from './firebaseAdmin.js';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg';
ffmpeg.setFfmpegPath(ffmpegInstaller.path);

// Cola de secuencias
import {
  scheduleSequenceForLead,
  cancelSequences,
  cancelAllSequences,
  normalizeJid,
  hasSameTrigger,
  phoneFromJid
} from './queue.js';
import { detectMetaAdSignal } from './utils/metaAdDetector.js';


let latestQR = null;
let connectionStatus = 'Desconectado';
let whatsappSock = null;
let sessionPhone = null;

const localAuthFolder = '/var/data';
const { FieldValue } = admin.firestore;
const bucket = admin.storage().bucket();

/* ------------------------------ helpers ------------------------------ */
// alias → trigger (en minúsculas)
const STATIC_HASHTAG_MAP = {
  '#WebPro990':    'LeadWeb',
  '#webPro990':    'LeadWeb',
  '#leadweb':      'LeadWeb',
  '#nuevolead':    'NuevoLeadWeb',
  '#planredes990': 'PlanRedes',
  '#info':         'LeadWeb',
  '#infoweb':      'NuevoLead',

};

// Si el trigger es LeadWeb, cancela estas (evita duplicidad)
const STATIC_CANCEL_BY_TRIGGER = {
  LeadWeb: ['NuevoLeadWeb', 'NuevoLead'],
};

const WEBPROMO_TRIGGER = 'LeadWhatsapp';
const META_ADS_CAMPAIGN = 'whatsapp_click_to_chat';
const FORM_COMPLETED_BLOCKED_TRIGGERS = new Set([
  'leadweb',
  'nuevolead',
  'nuevoleadweb',
  'leadwhatsapp',
  'webpromo',
]);
const APPEND_MAX_AGE_MS_ENV = Number(process.env.WA_APPEND_MAX_AGE_MS);
const APPEND_MAX_AGE_MS = Number.isFinite(APPEND_MAX_AGE_MS_ENV) && APPEND_MAX_AGE_MS_ENV >= 0
  ? APPEND_MAX_AGE_MS_ENV
  : 6 * 60 * 60 * 1000;
const PROCESSED_MESSAGE_ID_TTL_MS = Number(process.env.WA_MSG_ID_TTL_MS) > 0
  ? Number(process.env.WA_MSG_ID_TTL_MS)
  : 6 * 60 * 60 * 1000;
const META_AUTO_REARM_MS = Number(process.env.WA_META_AUTO_REARM_MS) > 0
  ? Number(process.env.WA_META_AUTO_REARM_MS)
  : 2 * 60 * 60 * 1000;
const processedInboundMessageIds = new Map();
const ENABLE_LID_APPEND_META_FALLBACK = process.env.WA_LID_APPEND_META_FALLBACK !== '0';

function firstName(n = '') {
  return String(n).trim().split(/\s+/)[0] || '';
}

function tpl(str, lead) {
  return String(str || '').replace(/\{\{(\w+)\}\}/g, (_, f) => {
    if (f === 'nombre') return firstName(lead?.nombre || '');
    if (f === 'telefono') return String(lead?.telefono || '').replace(/\D/g, '');
    return lead?.[f] ?? '';
  });
}
function now() { return new Date(); }

function messageDocIdFromWaId(waMessageId) {
  const clean = String(waMessageId || '')
    .trim()
    .replace(/[^\w.-]/g, '_');
  if (!clean) return null;
  return `wa_${clean}`;
}

async function persistLeadMessage(leadRef, msgData, waMessageId = null) {
  const payload = {
    ...msgData,
    ...(waMessageId ? { waMessageId: String(waMessageId) } : {}),
  };

  const docId = messageDocIdFromWaId(waMessageId);
  if (docId) {
    await leadRef.collection('messages').doc(docId).set(payload, { merge: true });
    return docId;
  }

  const snap = await leadRef.collection('messages').add(payload);
  return snap.id;
}

function buildMessagePreview(msgData = {}) {
  const content = String(msgData?.content || '').trim();
  if (content) return content.slice(0, 160);

  const mediaType = String(msgData?.mediaType || '').toLowerCase();
  if (mediaType === 'image') return 'Imagen';
  if (mediaType === 'audio' || mediaType === 'audio_ptt' || mediaType === 'ptt') return 'Audio';
  if (mediaType === 'video' || mediaType === 'video_note' || mediaType === 'ptv') return 'Video';
  if (mediaType === 'document' || mediaType === 'pdf') return 'Documento';
  return 'Mensaje';
}

function buildLeadLastMessagePatch(msgData = {}, { incrementUnread = false } = {}) {
  const preview = buildMessagePreview(msgData);
  const rawType = String(msgData?.mediaType || '').toLowerCase();
  const mediaType = rawType || (String(msgData?.content || '').trim() ? 'text' : 'unknown');
  const patch = {
    lastMessageAt: msgData?.timestamp || now(),
    lastMessage: preview,
    lastMessageText: preview,
    lastMessagePreview: preview,
    ultimoMensaje: preview,
    lastMessageMediaType: mediaType,
  };
  if (incrementUnread) patch.unreadCount = FieldValue.increment(1);
  return patch;
}

function buildQuotedPreviewFromMessageObject(message = {}) {
  const inner = message || {};
  if (inner.conversation) return String(inner.conversation || '').trim().slice(0, 160);
  if (inner.extendedTextMessage?.text) return String(inner.extendedTextMessage.text || '').trim().slice(0, 160);
  if (inner.imageMessage) return String(inner.imageMessage?.caption || '').trim() || 'Imagen';
  if (inner.videoMessage) return String(inner.videoMessage?.caption || '').trim() || 'Video';
  if (inner.audioMessage) return 'Audio';
  if (inner.documentMessage) return String(inner.documentMessage?.fileName || '').trim() || 'Documento';
  return 'Mensaje';
}

function extractReplyContextFromIncomingMessage(inner = {}) {
  const contextInfo =
    inner?.extendedTextMessage?.contextInfo ||
    inner?.imageMessage?.contextInfo ||
    inner?.videoMessage?.contextInfo ||
    inner?.audioMessage?.contextInfo ||
    inner?.documentMessage?.contextInfo ||
    null;

  const replyToWaMessageId = String(contextInfo?.stanzaId || '').trim();
  if (!replyToWaMessageId) return {};

  const replyToPreview = buildQuotedPreviewFromMessageObject(contextInfo?.quotedMessage || {});
  const participant = normalizeJid(contextInfo?.participant || '');

  return {
    replyToWaMessageId,
    replyToPreview: String(replyToPreview || 'Mensaje').slice(0, 160),
    ...(participant ? { replyToParticipant: participant } : {}),
  };
}

async function resolveQuotedMessageForSend({
  leadRef,
  targetJid,
  waMessageId,
  fallbackPreview = '',
  fallbackFromMe = false,
}) {
  const quotedWaId = String(waMessageId || '').trim();
  if (!quotedWaId) return null;

  let data = null;
  if (leadRef) {
    try {
      const byDocId = messageDocIdFromWaId(quotedWaId);
      if (byDocId) {
        const byDoc = await leadRef.collection('messages').doc(byDocId).get();
        if (byDoc.exists) data = byDoc.data() || {};
      }

      if (!data) {
        const q = await leadRef
          .collection('messages')
          .where('waMessageId', '==', quotedWaId)
          .limit(1)
          .get();
        if (!q.empty) data = q.docs[0].data() || {};
      }
    } catch (error) {
      console.warn('[WA] No se pudo resolver quoted message en Firestore:', error?.message || error);
    }
  }

  const preview = buildMessagePreview({
    content: String(data?.content || fallbackPreview || '').trim(),
    mediaType: String(data?.mediaType || '').trim(),
    mediaUrl: String(data?.mediaUrl || '').trim(),
  }) || 'Mensaje';

  const fromMe = data
    ? String(data?.sender || '').toLowerCase() === 'business'
    : Boolean(fallbackFromMe);

  return {
    key: {
      id: quotedWaId,
      remoteJid: normalizeJid(targetJid) || String(targetJid || ''),
      fromMe,
    },
    message: {
      conversation: String(preview || 'Mensaje').slice(0, 160),
    },
  };
}

function isUserJid(jid) {
  return String(normalizeJid(jid) || '').endsWith('@s.whatsapp.net');
}

function isLidJid(jid) {
  return /@lid$/i.test(String(normalizeJid(jid) || '').trim());
}

function jidUserDigits(jid) {
  const normalized = String(normalizeJid(jid) || '');
  const [user] = normalized.split('@');
  return String(user || '').split(':')[0].replace(/\D/g, '');
}

function isSuspiciousPseudoPhoneJid(jid) {
  const normalized = String(normalizeJid(jid) || '');
  if (!normalized.endsWith('@s.whatsapp.net')) return false;
  const digits = jidUserDigits(normalized);
  return digits.length > 13;
}

function isSafePhoneForJidFallback(phone = '') {
  const digits = String(phone || '').replace(/\D/g, '');
  return /^\d{10}$/.test(digits) || /^52\d{10}$/.test(digits) || /^521\d{10}$/.test(digits);
}

function getMessageTimestampMs(msg) {
  const raw = msg?.messageTimestamp;
  if (!raw) return null;

  let seconds = null;
  if (typeof raw === 'number') seconds = raw;
  else if (typeof raw === 'string') seconds = Number(raw);
  else if (typeof raw?.toNumber === 'function') seconds = raw.toNumber();
  else if (typeof raw === 'object' && typeof raw.low === 'number') seconds = raw.low;

  if (!Number.isFinite(seconds) || seconds <= 0) return null;
  return seconds > 1e12 ? seconds : seconds * 1000;
}

function getMessageDedupKey(msg) {
  const id = String(msg?.key?.id || '').trim();
  if (!id) return null;
  const remote = String(msg?.key?.remoteJid || msg?.key?.remoteJidAlt || msg?.key?.participant || '').trim();
  return `${remote}:${id}`;
}

function cleanupProcessedMessageIds(refNow = Date.now()) {
  for (const [key, seenAt] of processedInboundMessageIds.entries()) {
    if ((refNow - seenAt) > PROCESSED_MESSAGE_ID_TTL_MS) {
      processedInboundMessageIds.delete(key);
    }
  }
}

function markMessageAsProcessed(msg, refNow = Date.now()) {
  const key = getMessageDedupKey(msg);
  if (!key) return true;
  cleanupProcessedMessageIds(refNow);
  if (processedInboundMessageIds.has(key)) return false;
  processedInboundMessageIds.set(key, refNow);
  return true;
}

function shouldProcessAppendMessage(msg, refNow = Date.now()) {
  if (APPEND_MAX_AGE_MS === 0) return true; // 0 desactiva filtro por antigüedad
  const tsMs = getMessageTimestampMs(msg);
  if (!tsMs) return true; // si no viene timestamp, se procesa para no perder mensajes reales
  return (refNow - tsMs) <= APPEND_MAX_AGE_MS;
}

function extractHashtags(text = '') {
  const found = String(text).toLowerCase().match(/#[\p{L}\p{N}_-]+/gu);
  return found ? Array.from(new Set(found)) : [];
}

// Normaliza a formato que WhatsApp acepta para MX:
function normalizePhoneForWA(phone) {
  let num = String(phone || '').replace(/\D/g, '');
  if (num.length === 10) return '521' + num;
  if (num.length === 12 && num.startsWith('52') && !num.startsWith('521')) {
    return '521' + num.slice(2);
  }
  return num;
}

// Reglas dinámicas opcionales en Firestore
async function resolveHashtagInDB(code) {
  const snap = await db
    .collection('hashtagTriggers')
    .where('code', '==', code.replace(/^#/, '').toLowerCase())
    .limit(1)
    .get();
  if (snap.empty) return null;
  const row = snap.docs[0].data() || {};
  return { trigger: row.trigger, cancel: row.cancel || [] };
}

async function resolveTriggerFromMessage(text, defaultTrigger = 'NuevoLeadWeb') {
  const tags = extractHashtags(text);
  if (tags.length === 0) return { trigger: defaultTrigger, cancel: [], source: 'default' };

  // 1) Firestore (dinámico)
  for (const tag of tags) {
    const dbRule = await resolveHashtagInDB(tag);
    if (dbRule?.trigger) return { ...dbRule, source: 'db' };
  }

  // 2) Estático
  for (const tag of tags) {
    const trg = STATIC_HASHTAG_MAP[tag];
    if (trg) {
      const cancel = STATIC_CANCEL_BY_TRIGGER[trg] || [];
      return { trigger: trg, cancel, source: 'hashtag' };
    }
  }

  // 3) Default
  return { trigger: defaultTrigger, cancel: [], source: 'default' };
}

function shouldBlockSequences(leadData, nextTrigger) {
  const etiquetas = Array.isArray(leadData?.etiquetas)
    ? leadData.etiquetas.map((t) => String(t || '').toLowerCase())
    : [];
  const etapa = String(leadData?.etapa || '').toLowerCase();
  const estado = (leadData?.estado || '').toLowerCase();
  const trigger = String(nextTrigger || '').toLowerCase();

  if (leadData?.seqPaused) return true;
  if (leadData?.stopSequences) return true;
  if (estado === 'compro' || etiquetas.includes('compro')) return true;

  const hasCompletedForm = etapa === 'form_submitted'
    || etiquetas.includes('formok')
    || etiquetas.includes('formulariocompletado');
  if (hasCompletedForm) {
    if (FORM_COMPLETED_BLOCKED_TRIGGERS.has(trigger)) return true;
  }
  return false;
}

function toMillisSafe(value) {
  if (!value) return 0;
  if (typeof value?.toMillis === 'function') {
    const ms = value.toMillis();
    return Number.isFinite(ms) ? ms : 0;
  }
  if (value instanceof Date) {
    const ms = value.getTime();
    return Number.isFinite(ms) ? ms : 0;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : 0;
  }
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function hasRecentMetaAutoSchedule(leadData, refDate = now()) {
  const refMs = toMillisSafe(refDate);
  if (!refMs) return false;
  const lastMs = Math.max(
    toMillisSafe(leadData?.lastMetaAutoScheduledAt),
    toMillisSafe(leadData?.lastMetaSequenceAt)
  );
  if (!lastMs) return false;
  return (refMs - lastMs) < META_AUTO_REARM_MS;
}

function isLeadFromMetaAds(leadData) {
  if (!leadData || typeof leadData !== 'object') return false;
  const source = String(leadData.source || '').toLowerCase();
  const campaign = String(leadData.campaign || '').toLowerCase();
  const etiquetas = Array.isArray(leadData.etiquetas) ? leadData.etiquetas.map((e) => String(e).toLowerCase()) : [];
  return source === 'meta_ads' || campaign === META_ADS_CAMPAIGN.toLowerCase() || etiquetas.includes('metaads');
}

function resolveMetaAdInbound({ baseDetected, leadData, isLidRemote, upsertType }) {
  if (baseDetected) return { isMetaAd: true, reason: 'signal' };
  if (isLeadFromMetaAds(leadData)) return { isMetaAd: true, reason: 'lead_context' };
  if (ENABLE_LID_APPEND_META_FALLBACK && isLidRemote && (upsertType === 'append' || upsertType === 'notify')) {
    return { isMetaAd: true, reason: `lid_${upsertType}_fallback` };
  }
  return { isMetaAd: false, reason: 'none' };
}

function resolveSenderFromLid(msg) {
  const remoteJidAlt = normalizeJid(msg?.key?.remoteJidAlt);
  if (remoteJidAlt && remoteJidAlt.includes('@s.whatsapp.net')) {
    console.log(`[resolveSenderFromLid] ✅ remoteJidAlt detectado: ${remoteJidAlt}`);
    return remoteJidAlt;
  }

  // Prioridad 1: key.participant (más confiable para mensajes de Business API)
  if (msg?.key?.participant && msg.key.participant.includes('@s.whatsapp.net')) {
    console.log(`[resolveSenderFromLid] ✅ Usando key.participant: ${msg.key.participant}`);
    return msg.key.participant;
  }

  // Prioridad 2: key.remoteJid si ya es @s.whatsapp.net (raro pero posible)
  if (msg?.key?.remoteJid && msg.key.remoteJid.includes('@s.whatsapp.net')) {
    console.log(`[resolveSenderFromLid] ✅ remoteJid ya es válido: ${msg.key.remoteJid}`);
    return msg.key.remoteJid;
  }

  // Prioridad 3: Extraer de remoteJid antes del @lid
  const remoteJid = String(msg?.key?.remoteJid || '');
  if (remoteJid.endsWith('@lid')) {
    const normalizedLid = normalizeJid(remoteJid);
    if (normalizedLid) {
      console.warn(
        `[resolveSenderFromLid] ⚠️ remoteJid @lid sin remoteJidAlt; se conserva provisional: ${normalizedLid}`
      );
      return normalizedLid;
    }
  }

  // Prioridad 4: Buscar en otros campos
  const candidates = [
    msg?.key?.senderPn,
    msg?.participant,
    msg?.message?.extendedTextMessage?.contextInfo?.participant,
  ].filter(Boolean);

  for (const cand of candidates) {
    if (String(cand).includes('@s.whatsapp.net')) {
      console.log(`[resolveSenderFromLid] ✅ Encontrado en candidates: ${cand}`);
      return cand;
    }

    const digits = String(cand).replace(/\D/g, '');
    if (digits.length >= 10) {
      const normalized = normalizePhoneForWA(digits);
      const jid = `${normalized}@s.whatsapp.net`;
      console.log(`[resolveSenderFromLid] ✅ Normalizado de candidate: ${cand} → ${jid}`);
      return jid;
    }
  }

  console.warn(`[resolveSenderFromLid] ❌ No se pudo resolver sender desde:`, {
    remoteJid: msg?.key?.remoteJid,
    participant: msg?.key?.participant,
    senderPn: msg?.key?.senderPn
  });

  return null;
}

function buildStableLeadId({ normalizedPhone, resolvedJid, fallbackJid }) {
  const resolved = normalizeJid(resolvedJid);
  if (resolved && (isUserJid(resolved) || isLidJid(resolved))) return resolved;
  const fallback = normalizeJid(fallbackJid);
  if (fallback && (isUserJid(fallback) || isLidJid(fallback))) return fallback;
  const phone = String(normalizedPhone || '').replace(/\D/g, '');
  if (phone.length >= 10 && phone.length <= 13) return `${phone}@s.whatsapp.net`;
  if (resolved) return resolved;
  if (fallback) return fallback;
  return '';
}

async function resolveExistingLeadReference({
  provisionalLeadId,
  normalizedPhone,
  resolvedJid,
  jid,
  lidJid,
}) {
  const leadsCol = db.collection('leads');

  const candidateMap = new Map();
  const pushCandidate = (snap, source) => {
    if (!snap?.exists) return;
    const id = snap.id;
    if (!candidateMap.has(id)) {
      candidateMap.set(id, { snap, sources: [source] });
      return;
    }
    const current = candidateMap.get(id);
    current.sources.push(source);
    candidateMap.set(id, current);
  };

  const unique = (items) => Array.from(new Set(items.filter(Boolean)));
  const provisional = String(provisionalLeadId || '').trim();

  const jidCandidates = unique([
    normalizeJid(resolvedJid),
    normalizeJid(jid),
    normalizeJid(lidJid),
    provisional.includes('@') ? normalizeJid(provisional) : null,
  ]);

  const rawPhone = String(normalizedPhone || '').replace(/\D/g, '');
  const fromJids = jidCandidates
    .map((value) => phoneFromJid(value))
    .filter(Boolean);

  const phoneCandidates = unique([
    rawPhone,
    ...fromJids,
  ]);

  const expandedPhones = new Set();
  phoneCandidates.forEach((value) => {
    const digits = String(value || '').replace(/\D/g, '');
    if (!digits) return;
    expandedPhones.add(digits);
    expandedPhones.add(`+${digits}`);

    if (/^\d{10}$/.test(digits)) {
      expandedPhones.add(`52${digits}`);
      expandedPhones.add(`+52${digits}`);
      expandedPhones.add(`521${digits}`);
      expandedPhones.add(`+521${digits}`);
    } else if (/^52\d{10}$/.test(digits) && !digits.startsWith('521')) {
      const tail = digits.slice(2);
      expandedPhones.add(tail);
      expandedPhones.add(`521${tail}`);
      expandedPhones.add(`+521${tail}`);
    } else if (/^521\d{10}$/.test(digits)) {
      const tail = digits.slice(3);
      expandedPhones.add(tail);
      expandedPhones.add(`52${tail}`);
      expandedPhones.add(`+52${tail}`);
    }
  });

  const idCandidates = unique([
    provisional,
    ...jidCandidates,
  ]);

  for (const candidate of idCandidates) {
    const snap = await leadsCol.doc(candidate).get();
    pushCandidate(snap, `doc:${candidate}`);
  }

  for (const jidCandidate of jidCandidates) {
    for (const field of ['resolvedJid', 'jid', 'lidJid']) {
      const byField = await leadsCol.where(field, '==', jidCandidate).limit(10).get();
      byField.docs.forEach((docSnap) => pushCandidate(docSnap, `${field}:${jidCandidate}`));
    }
  }

  for (const phone of expandedPhones) {
    const byPhone = await leadsCol.where('telefono', '==', phone).limit(20).get();
    byPhone.docs.forEach((docSnap) => pushCandidate(docSnap, `telefono:${phone}`));
  }

  if (!candidateMap.size) return null;

  const resolveCanonicalLead = async (snap) => {
    let current = snap;
    const visited = new Set([snap.id]);
    for (let depth = 0; depth < 4; depth += 1) {
      const data = current.data() || {};
      const mergedInto = String(data.mergedInto || '').trim();
      if (!mergedInto || mergedInto === current.id || visited.has(mergedInto)) break;
      const nextSnap = await leadsCol.doc(mergedInto).get();
      if (!nextSnap.exists) break;
      visited.add(mergedInto);
      current = nextSnap;
    }
    return current;
  };

  const canonicalMap = new Map();
  for (const entry of candidateMap.values()) {
    const originalSnap = entry.snap;
    const originalData = originalSnap.data() || {};
    let canonicalSnap = originalSnap;

    if (String(originalData.mergedInto || '').trim()) {
      canonicalSnap = await resolveCanonicalLead(originalSnap);
    }

    const canonicalId = canonicalSnap.id;
    const prev = canonicalMap.get(canonicalId);
    if (!prev) {
      canonicalMap.set(canonicalId, {
        snap: canonicalSnap,
        sources: [
          ...entry.sources,
          ...(canonicalId !== originalSnap.id ? [`mergedFrom:${originalSnap.id}`] : []),
        ],
      });
      continue;
    }

    canonicalMap.set(canonicalId, {
      snap: canonicalSnap,
      sources: [
        ...prev.sources,
        ...entry.sources,
        ...(canonicalId !== originalSnap.id ? [`mergedFrom:${originalSnap.id}`] : []),
      ],
    });
  }

  if (!canonicalMap.size) return null;

  const scoreCandidate = ({ snap, sources }) => {
    const data = snap.data() || {};
    const sourceSet = new Set(sources);
    let score = 0;

    // Prioridad: no perder ownership del chat
    if (data.assignedTo) score += 1000;
    if (snap.id === provisional) score += 350;
    if (sourceSet.has(`doc:${provisional}`)) score += 250;

    // Coincidencias de identidad
    score += Array.from(sourceSet).filter((s) => s.startsWith('resolvedJid:')).length * 120;
    score += Array.from(sourceSet).filter((s) => s.startsWith('jid:')).length * 100;
    score += Array.from(sourceSet).filter((s) => s.startsWith('lidJid:')).length * 90;
    score += Array.from(sourceSet).filter((s) => s.startsWith('telefono:')).length * 70;

    // Conversaciones vivas tienen preferencia
    if (data.lastMessageAt) score += 30;
    if (Number(data.unreadCount || 0) > 0) score += 20;
    if (Array.isArray(data.secuenciasActivas) && data.secuenciasActivas.length > 0) score += 10;

    const lastMsgMs = (() => {
      const value = data.lastMessageAt;
      if (!value) return 0;
      if (typeof value?.toMillis === 'function') return value.toMillis();
      const parsed = new Date(value).getTime();
      return Number.isFinite(parsed) ? parsed : 0;
    })();

    return { score, lastMsgMs };
  };

  let best = null;
  for (const entry of canonicalMap.values()) {
    const metrics = scoreCandidate(entry);
    if (!best) {
      best = { ...entry, ...metrics };
      continue;
    }

    const betterScore = metrics.score > best.score;
    const sameScoreButNewer = metrics.score === best.score && metrics.lastMsgMs > best.lastMsgMs;
    if (betterScore || sameScoreButNewer) {
      best = { ...entry, ...metrics };
    }
  }

  if (!best) return null;

  return {
    leadId: best.snap.id,
    leadRef: best.snap.ref,
    leadSnap: best.snap,
    source: Array.from(new Set(best.sources)).join('|'),
  };
}

/* ---------------------------- conexión WA ---------------------------- */
export async function connectToWhatsApp() {
  try {
    if (!fs.existsSync(localAuthFolder)) {
      fs.mkdirSync(localAuthFolder, { recursive: true });
    }

    const { state, saveCreds } = await useMultiFileAuthState(localAuthFolder);
    if (state.creds.me?.id) sessionPhone = state.creds.me.id.split('@')[0];

    const { version } = await fetchLatestBaileysVersion();
    const sock = makeWASocket({
      auth: state,
      logger: Pino({ level: 'info' }),
      printQRInTerminal: true,
      version,
    });
    whatsappSock = sock;

    // ── eventos de conexión
    sock.ev.on('connection.update', ({ connection, lastDisconnect, qr }) => {
      if (qr) {
        latestQR = qr;
        connectionStatus = 'QR disponible. Escanéalo.';
        QRCode.generate(qr, { small: true });
      }
      if (connection === 'open') {
        connectionStatus = 'Conectado';
        latestQR = null;
        if (sock.user?.id) sessionPhone = sock.user.id.split('@')[0];
      }
      if (connection === 'close') {
        const reason = lastDisconnect?.error?.output?.statusCode;
        connectionStatus = 'Desconectado';
        if (reason === DisconnectReason.loggedOut) {
          for (const f of fs.readdirSync(localAuthFolder)) {
            fs.rmSync(path.join(localAuthFolder, f), { force: true, recursive: true });
          }
          sessionPhone = null;
        }
        // Backoff más largo para Render
        const delay = Math.floor(Math.random() * 8000) + 5000;
        setTimeout(() => connectToWhatsApp().catch(() => {}), delay);
      }
    });

    sock.ev.on('creds.update', saveCreds);

    /* -------------------- 🔧 FIX: recepción de mensajes -------------------- */
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
      // ✅ CORRECCIÓN: Procesar notify + append recientes (con dedupe) según documentación oficial de Baileys
      // https://baileys.wiki/docs/socket/receiving-updates/
      // type === 'notify': mensajes NUEVOS entrantes (lo que necesitamos)
      // type === 'append': puede incluir mensajes recientes tras reconexión
      // type === 'prepend': historial antiguo (ignorar)
      if (!['notify', 'append'].includes(type || '')) {
        console.log(`[WA] ⏭️ Ignorando mensajes tipo '${type || 'undefined'}' (solo notify/append)`);
        return;
      }

      const incomingMessages = Array.isArray(messages) ? messages : [];
      let messagesToProcess = incomingMessages;
      if (messagesToProcess.length === 0) return;

      if (type === 'append') {
        const refNow = Date.now();
        messagesToProcess = messagesToProcess.filter((m) => shouldProcessAppendMessage(m, refNow));
        const skippedByAge = incomingMessages.length - messagesToProcess.length;
        if (skippedByAge > 0) {
          console.log(`[WA] ⏭️ append: ${skippedByAge} mensaje(s) descartados por antigüedad (> ${Math.round(APPEND_MAX_AGE_MS / 60000)} min)`);
        }
        if (messagesToProcess.length === 0) {
          console.log('[WA] ⏭️ append sin mensajes recientes para procesar');
          return;
        }
      }

      // ✅ Log de debugging mejorado
      console.log(`[WA] 📩 Procesando ${messagesToProcess.length} mensaje(s) | tipo: ${type} | ${new Date().toISOString()}`);

      for (const msg of messagesToProcess) {
        if (!markMessageAsProcessed(msg)) {
          console.log(`[WA] ⏭️ Mensaje duplicado omitido: id=${msg?.key?.id || 'N/A'} tipo=${type}`);
          continue;
        }
        try {
          if (type === 'append') {
            const tsMs = getMessageTimestampMs(msg);
            console.log(`[WA] ℹ️ append aceptado: id=${msg?.key?.id || 'N/A'} ts=${tsMs ? new Date(tsMs).toISOString() : 'sin-timestamp'}`);
          }

          // Validación de JID
          let rawJid = (msg?.key?.remoteJid || '').trim();
          const remoteJidAltRaw = msg?.key?.remoteJidAlt;
          const remoteJidAlt = normalizeJid(remoteJidAltRaw);
          const addressingMode = msg?.key?.addressingMode || 'pn';

          if (!rawJid && !remoteJidAlt) {
            const recoveredJid = normalizeJid(resolveSenderFromLid(msg));
            if (recoveredJid) {
              rawJid = recoveredJid;
              console.log(`[WA] ♻️ JID recuperado sin remoteJid/remoteJidAlt: ${recoveredJid}`);
            } else {
              console.warn('[WA] mensaje sin remoteJid/remoteJidAlt ni fallback resoluble, se ignora');
              continue;
            }
          }

          // Ignorar grupos/estados/newsletters
          if ((rawJid || '').endsWith('@g.us') || rawJid === 'status@broadcast' || (rawJid || '').endsWith('@newsletter')) {
            console.log(`[WA] ⏭️ Ignorando mensaje de: ${rawJid} (grupo/canal/newsletter)`);
            continue;
          }

          const isLidRemote = (rawJid || '').endsWith('@lid') || addressingMode === 'lid';
          let jidToUse = normalizeJid(remoteJidAlt || rawJid);

          if (remoteJidAlt && remoteJidAlt.includes('@s.whatsapp.net')) {
            console.log(`[WA] ✅ Usando remoteJidAlt (número real): ${remoteJidAlt}`);
            jidToUse = remoteJidAlt;
          }

          // Manejar mensajes de Business API (@lid) que vienen de FB Ads
          // Estos mensajes tienen el remitente real en senderPn o participant
          if (isLidRemote) {
            console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
            console.log(`[WA] 📱 MENSAJE DE FACEBOOK ADS DETECTADO (@lid)`);
            console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
            console.log(`   🆔 Message ID: ${msg.key.id}`);
            console.log(`   📍 Remote JID original: ${rawJid || 'N/A'}`);
            console.log(`   👤 Push Name: ${msg.pushName || 'N/A'}`);
            console.log(`   🔍 Key.participant: ${msg.key.participant || 'N/A'}`);
            console.log(`   🔍 Key.senderPn: ${msg.key.senderPn || 'N/A'}`);
            console.log(`   🔍 addressingMode: ${addressingMode}`);
            console.log(`   🔍 remoteJidAlt: ${remoteJidAlt || 'N/A'}`);

            // 🔧 CORRECCIÓN CRÍTICA: Resolver el JID real del usuario
            if (!remoteJidAlt) {
              const realSender = resolveSenderFromLid(msg);

              if (realSender) {
                const normalizedRealSender = normalizeJid(realSender);
                jidToUse = normalizedRealSender;
                if (isUserJid(normalizedRealSender)) {
                  console.log(`   ✅ JID real extraído correctamente: ${normalizedRealSender}`);
                } else if (isLidJid(normalizedRealSender)) {
                  console.warn(
                    `   ⚠️ Sin número real; usando JID @lid provisional: ${normalizedRealSender}`
                  );
                } else {
                  console.warn(
                    `   ⚠️ JID no estándar recuperado; usando provisional: ${normalizedRealSender}`
                  );
                }
              } else {
                const fallbackJid = normalizeJid(rawJid) || normalizeJid(msg?.key?.participant);
                if (fallbackJid) {
                  jidToUse = fallbackJid;
                  console.warn(`   ⚠️ JID real no resuelto; guardando lead provisional con ${fallbackJid}`);
                } else {
                  console.error(`   ❌ NO SE PUDO RESOLVER JID REAL - Mensaje será ignorado`);
                  console.log(`   🔍 Estructura completa del mensaje:`);
                  console.log(JSON.stringify({
                    key: msg.key,
                    pushName: msg.pushName,
                    hasMessage: !!msg.message
                  }, null, 2));
                  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);
                  continue; // ❌ Saltar este mensaje si no se puede resolver ningún JID
                }
              }
            } else {
              jidToUse = remoteJidAlt;
            }
            console.log(`   ✅ JID final a usar: ${jidToUse}`);
            console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);
          }

          const finalJid = normalizeJid(jidToUse);
          const lidJid = isLidRemote ? normalizeJid(rawJid) : null;
          const jidResolved = normalizeJid(remoteJidAlt || finalJid);

          if (!finalJid) {
            console.warn('[WA] mensaje sin JID final válido, se ignora');
            continue;
          }

          const [jidUser, jidDomain] = finalJid.split('@');
          const cleanUser = (jidUser || '').split(':')[0].replace(/\s+/g, '');
          if (!['s.whatsapp.net', 'lid'].includes(jidDomain)) continue;

          const phoneFromResolved = phoneFromJid(jidResolved);
          const phoneFromFinalJid = phoneFromJid(finalJid);
          const derivedPhoneFromUser = jidDomain === 's.whatsapp.net'
            ? normalizePhoneForWA(cleanUser)
            : '';
          const normNum  = phoneFromResolved || phoneFromFinalJid || derivedPhoneFromUser;
          const hasReachableTarget = Boolean(
            isUserJid(jidResolved)
            || isUserJid(finalJid)
            || isLidJid(jidResolved)
            || isLidJid(finalJid)
            || phoneFromResolved
            || phoneFromFinalJid
            || (typeof derivedPhoneFromUser === 'string' && derivedPhoneFromUser.length >= 10)
          );
          const shouldTrustUnsafeTarget = sender === 'lead' && (
            isSuspiciousPseudoPhoneJid(jidResolved)
            || isSuspiciousPseudoPhoneJid(finalJid)
          );
          const stableLeadId = buildStableLeadId({
            normalizedPhone: normNum,
            resolvedJid: jidResolved,
            fallbackJid: finalJid
          });
          if (!stableLeadId) {
            console.warn('[WA] no se pudo construir leadId estable, se ignora mensaje');
            continue;
          }

          let leadId = stableLeadId;
          const sender   = msg.key.fromMe ? 'business' : 'lead';
          const jid      = finalJid;
          const leadResolution = await resolveExistingLeadReference({
            provisionalLeadId: stableLeadId,
            normalizedPhone: normNum,
            resolvedJid: jidResolved,
            jid,
            lidJid,
          });

          if (leadResolution?.leadId && leadResolution.leadId !== stableLeadId) {
            console.log(
              `[WA] ♻️ Lead existente reutilizado: ${stableLeadId} -> ${leadResolution.leadId} (${leadResolution.source})`
            );
            leadId = leadResolution.leadId;
          }

          const leadRef = leadResolution?.leadRef || db.collection('leads').doc(leadId);
          const leadSnap = leadResolution?.leadSnap || await leadRef.get();
          const existingLeadData = leadSnap.exists ? (leadSnap.data() || {}) : null;

          if (!leadSnap.exists) {
            console.log(
              `[WA] 🆕 Creando lead nuevo para mensaje entrante: ${leadId} (stable=${stableLeadId})`
            );
          }

          const adSignal = sender === 'lead' ? detectMetaAdSignal(msg) : { isFromMetaAd: false, indicator: null, path: null };
          const inboundFromMetaAd = sender === 'lead' && adSignal.isFromMetaAd;
          if (inboundFromMetaAd) {
            console.log(`[WA] 🎯 Indicador Meta Ads detectado para ${leadId}: ${adSignal.indicator} (${adSignal.path})`);
          } else if (sender === 'lead' && isLidRemote) {
            const msgKeys = Object.keys(msg?.message || {});
            console.log(`[WA] ℹ️ @lid sin indicador Meta Ads para ${leadId}. keys=${msgKeys.join(',') || 'sin-keys'}`);
          }

          // Verificar que el mensaje tenga contenido desencriptado
          if (!msg.message || Object.keys(msg.message).length === 0) {
            console.warn(`[WA] ⚠️ Mensaje sin contenido desencriptado desde ${finalJid} - ID: ${msg.key.id}`);

            // Para mensajes con remitente válido, intentar crear el lead de todas formas
            if (finalJid) {
              console.log(`[WA] 🔄 Intentando crear/actualizar lead sin contenido de mensaje para ${finalJid}`);

              const metaAdDecision = resolveMetaAdInbound({
                baseDetected: inboundFromMetaAd,
                leadData: existingLeadData,
                isLidRemote,
                upsertType: type,
              });
              const shouldTreatAsMetaAdInbound = metaAdDecision.isMetaAd;
              if (shouldTreatAsMetaAdInbound && !inboundFromMetaAd) {
                console.log(`[WA] ⚠️ Meta Ads fallback aplicado para ${leadId} (motivo: ${metaAdDecision.reason})`);
              }

              // Trigger interno para inbound de anuncio de Meta
              const cfgSnap = await db.collection('config').doc('appConfig').get();
              const cfg = cfgSnap.exists ? cfgSnap.data() : {};
              const detectedTrigger = cfg.defaultTriggerMetaAds || WEBPROMO_TRIGGER;
              const inboundAt = now();

              const baseEtiquetas = [];
              if (shouldTreatAsMetaAdInbound) baseEtiquetas.push('MetaAds', detectedTrigger);
              if (!msg.message) baseEtiquetas.push('MensajeNoDesencriptado');

              // 🔧 CRÍTICO: Guardar ambos JID para futuras resoluciones
              const finalJidToPersist = jidResolved || finalJid;
              console.log(`[WA] 📝 Guardando lead con JID: ${finalJidToPersist}`);

              const leadPayload = {
                telefono: normNum,
                nombre: msg.pushName || '',
                jid: finalJidToPersist,
                resolvedJid: jidResolved,
                lidJid,
                addressingMode,
                needsJidResolution: !hasReachableTarget,
                ...(shouldTrustUnsafeTarget ? { allowUnsafeTarget: true } : {}),
                source: shouldTreatAsMetaAdInbound ? 'meta_ads' : 'WhatsApp Business API',
                ...(shouldTreatAsMetaAdInbound
                  ? {
                      campaign: META_ADS_CAMPAIGN,
                      lastInboundFromAd: true,
                      lastInboundAt: inboundAt,
                    }
                  : {}),
                lastMessageAt: inboundAt,
              };

              if (!leadSnap.exists) {
                // Crear lead nuevo sin mensaje
                await leadRef.set({
                  ...leadPayload,
                  fecha_creacion: inboundAt,
                  estado: 'nuevo',
                  etiquetas: baseEtiquetas,
                  unreadCount: 1,
                });

                if (shouldTreatAsMetaAdInbound) {
                  const blocked = shouldBlockSequences({}, detectedTrigger);
                  if (!blocked && hasReachableTarget) {
                    try {
                      await scheduleSequenceForLead(leadId, detectedTrigger, inboundAt);
                      await leadRef.set(
                        {
                          hasActiveSequences: true,
                          estado: 'nuevo',
                          lastMetaAutoScheduledAt: inboundAt,
                          lastMetaSequenceAt: inboundAt,
                        },
                        { merge: true }
                      );
                      console.log(`[WA] 🎯 Meta Ads inbound → secuencia '${detectedTrigger}' programada para ${leadId}`);
                    } catch (seqErr) {
                      console.error(`[WA] ❌ Error programando secuencia desde Meta Ads: ${seqErr?.message || seqErr}`);
                    }
                  } else if (!hasReachableTarget) {
                    console.log(`[WA] ⏭️ Meta Ads inbound sin ruta de envío (${leadId}); secuencia pendiente de resolución de JID.`);
                  } else {
                    console.log(`[WA] ⏭️ Meta Ads inbound detectado pero bloqueado para ${leadId}`);
                  }
                }
              } else {
                // Lead existente: actualizar y verificar si necesita secuencia
                const current = { id: leadSnap.id, ...(existingLeadData || {}) };
                const updatePayload = {
                  ...leadPayload,
                  unreadCount: FieldValue.increment(1),
                };
                if (baseEtiquetas.length > 0) {
                  updatePayload.etiquetas = FieldValue.arrayUnion(...baseEtiquetas);
                }
                await leadRef.set(updatePayload, { merge: true });

                if (shouldTreatAsMetaAdInbound) {
                  const alreadyHas = hasSameTrigger(current.secuenciasActivas, detectedTrigger);
                  const blocked = shouldBlockSequences(current, detectedTrigger);
                  const metaCooldownActive = hasRecentMetaAutoSchedule(current, inboundAt);

                  if (!blocked && !alreadyHas && !metaCooldownActive && hasReachableTarget) {
                    try {
                      const programmed = await scheduleSequenceForLead(leadId, detectedTrigger, inboundAt);
                      if (programmed > 0) {
                        await leadRef.set(
                          {
                            hasActiveSequences: true,
                            estado: 'nuevo',
                            lastMetaAutoScheduledAt: inboundAt,
                            lastMetaSequenceAt: inboundAt,
                          },
                          { merge: true }
                        );
                        console.log(`[WA] 🎯 Meta Ads inbound → secuencia '${detectedTrigger}' programada para ${leadId}`);
                      } else {
                        console.log(`[WA] ⏭️ Meta Ads inbound sin reprogramar para ${leadId}: schedule-omit`);
                      }
                    } catch (seqErr) {
                      console.error(`[WA] ❌ Error programando secuencia desde Meta Ads: ${seqErr?.message || seqErr}`);
                    }
                  } else if (!hasReachableTarget) {
                    console.log(`[WA] ⏭️ Meta Ads inbound sin ruta de envío (${leadId}); secuencia pendiente de resolución de JID.`);
                  } else {
                    console.log(
                      `[WA] ⏭️ Meta Ads inbound sin reprogramar para ${leadId}: blocked=${blocked}, alreadyHas=${alreadyHas}, cooldown=${metaCooldownActive}`
                    );
                  }
                } else {
                  console.log(`[WA] ⏭️ Mensaje no desencriptado sin indicador Meta Ads para ${leadId}; no se activa WebPromo.`);
                }

                console.log(`[WA] ✅ Lead actualizado desde mensaje no desencriptado: ${leadId}`);
              }
            }
            continue;
          }

          // Parseo de contenido
          let content = '';
          let mediaType = null;
          let mediaUrl = null;

          const baseMessage = msg.message || {};
          const inner =
            baseMessage?.ephemeralMessage?.message ||
            baseMessage?.viewOnceMessage?.message ||
            baseMessage?.deviceSentMessage?.message ||
            baseMessage;

          if (inner.videoMessage) {
            mediaType = 'video';
            const buffer = await downloadMediaMessage(msg, 'buffer', {}, { logger: Pino() });
            const fileRef = bucket.file(`videos/${normNum}-${Date.now()}.mp4`);
            await fileRef.save(buffer, { contentType: 'video/mp4' });
            const [url] = await fileRef.getSignedUrl({ action: 'read', expires: '03-01-2500' });
            mediaUrl = url;
          } else if (inner.imageMessage) {
            mediaType = 'image';
            const buffer = await downloadMediaMessage(msg, 'buffer', {}, { logger: Pino() });
            const fileRef = bucket.file(`images/${normNum}-${Date.now()}.jpg`);
            await fileRef.save(buffer, { contentType: 'image/jpeg' });
            const [url] = await fileRef.getSignedUrl({ action: 'read', expires: '03-01-2500' });
            mediaUrl = url;
          } else if (inner.audioMessage) {
            mediaType = 'audio';
            const buffer = await downloadMediaMessage(msg, 'buffer', {}, { logger: Pino() });
            const fileRef = bucket.file(`audios/${normNum}-${Date.now()}.ogg`);
            await fileRef.save(buffer, { contentType: 'audio/ogg' });
            const [url] = await fileRef.getSignedUrl({ action: 'read', expires: '03-01-2500' });
            mediaUrl = url;
          } else if (inner.documentMessage) {
            mediaType = 'document';
            const { mimetype, fileName: origName } = inner.documentMessage;
            const buffer = await downloadMediaMessage(msg, 'buffer', {}, { logger: Pino() });
            const ext = path.extname(origName || '') || '';
            const fileRef = bucket.file(`docs/${normNum}-${Date.now()}${ext}`);
            await fileRef.save(buffer, { contentType: mimetype || 'application/octet-stream' });
            const [url] = await fileRef.getSignedUrl({ action: 'read', expires: '03-01-2500' });
            mediaUrl = url;
          } else if (inner.conversation) {
            mediaType = 'text';
            content = inner.conversation.trim();
          } else if (inner.extendedTextMessage?.text) {
            mediaType = 'text';
            content = inner.extendedTextMessage.text.trim();
          } else {
            mediaType = 'unknown';
            content = '';
          }

          const replyContext = extractReplyContextFromIncomingMessage(inner);

          // Mensajes propios (fromMe)
          if (sender === 'business') {
            const msgData = {
              content,
              mediaType,
              mediaUrl,
              sender,
              timestamp: now(),
              ...replyContext,
            };

            await leadRef.set({
              telefono: normNum,
              jid,
              resolvedJid: jidResolved,
              lidJid,
              addressingMode,
              source: 'WhatsApp',
              ...buildLeadLastMessagePatch(msgData),
            }, { merge: true });

            await persistLeadMessage(leadRef, msgData, msg?.key?.id || null);

            // Comandos administrativos
            const textLower = String(content || '').toLowerCase();

            // #ok = detener secuencias
            if (/\B#ok\b/.test(textLower)) {
              try {
                await cancelAllSequences(leadId);
                await leadRef.set({
                  stopSequences: true,
                  hasActiveSequences: false,
                  etiquetas: FieldValue.arrayUnion('DetenerSecuencia'),
                }, { merge: true });

                console.log(`[WA] #ok → secuencias canceladas para ${leadId}`);
              } catch (e) {
                console.warn('[WA] error aplicando #ok:', e?.message || e);
              }
            }

            // #info = forzar secuencia
            if (/\B#info\b/.test(textLower)) {
              try {
                const cfgSnap = await db.collection('config').doc('appConfig').get();
                const cfg = cfgSnap.exists ? cfgSnap.data() : {};
                const defaultTrigger = cfg.defaultTrigger || 'NuevoLeadWeb';

                const rule = await resolveTriggerFromMessage(content, defaultTrigger);
                const trigger = rule.trigger || defaultTrigger;
                const toCancel = rule.cancel || [];

                const leadSnap = await leadRef.get();
                const leadData = leadSnap.exists ? leadSnap.data() : {};

                if (toCancel.length) {
                  await cancelSequences(leadId, toCancel);
                }

                if (!shouldBlockSequences(leadData, trigger)) {
                  await leadRef.set({
                    etiquetas: FieldValue.arrayUnion(trigger),
                    hasActiveSequences: true,
                  }, { merge: true });

                  await scheduleSequenceForLead(leadId, trigger, now());
                  console.log(`[WA] #info → secuencia ${trigger} programada para ${leadId}`);
                } else {
                  console.log(`[WA] #info → bloqueado para ${leadId}`);
                }
              } catch (e) {
                console.warn('[WA] error aplicando #info:', e?.message || e);
              }
            }

            // #WebPromo = reactivar secuencia manualmente (funciona con @lid)
            try {
              const tags = extractHashtags(content || '');
              const hasWebPromo = tags.some(t => t === '#webpromo' || t === 'webpromo');
              if (hasWebPromo && leadId) {
                const trigger = WEBPROMO_TRIGGER;
                console.log(`[WA] #WebPromo detectado. Activando secuencia '${trigger}' para ${leadId}`);

                await leadRef.set({
                  estado: 'nuevo',
                  etiquetas: FieldValue.arrayUnion(trigger, WEBPROMO_TRIGGER),
                  hasActiveSequences: true
                }, { merge: true });

                const scheduled = await scheduleSequenceForLead(leadId, trigger, now());
                if (scheduled > 0) {
                  console.log(`[WA] ✅ #WebPromo → Secuencia programada (${scheduled} pasos) para ${leadId}`);
                } else {
                  console.error(`[WA] ❌ #WebPromo → No se pudo programar secuencia para ${leadId}`);
                }
              }
            } catch (webPromoErr) {
              console.error('[WA] Error procesando #WebPromo:', webPromoErr?.message || webPromoErr);
            }

            console.log('[WA] (fromMe) Mensaje propio guardado →', leadId);
            continue;
          }

          // Mensajes de leads
          const cfgSnap = await db.collection('config').doc('appConfig').get();
          const cfg = cfgSnap.exists ? cfgSnap.data() : {};
          const defaultTrigger = cfg.defaultTrigger || 'NuevoLeadWeb';
          const metaAdTrigger = cfg.defaultTriggerMetaAds || WEBPROMO_TRIGGER;
          const rule = await resolveTriggerFromMessage(content, defaultTrigger);
          let trigger = rule.trigger;
          let triggerSource = rule.source;
          let toCancel = rule.cancel || [];
          const inboundAt = now();
          const metaAdDecision = resolveMetaAdInbound({
            baseDetected: inboundFromMetaAd,
            leadData: existingLeadData,
            isLidRemote,
            upsertType: type,
          });
          const shouldTreatAsMetaAdInbound = metaAdDecision.isMetaAd;
          if (shouldTreatAsMetaAdInbound) {
            trigger = metaAdTrigger;
            triggerSource = 'meta_ad';
            toCancel = [];
            const metaReason = inboundFromMetaAd ? 'signal' : metaAdDecision.reason;
            console.log(`[WA] 🎯 Inbound tratado como Meta Ads (${metaReason}): trigger interno '${trigger}' para ${leadId}`);
          }

          const etiquetaUnion = shouldTreatAsMetaAdInbound ? [trigger, 'MetaAds'] : [trigger];

          const baseLead = {
            telefono: normNum,
            nombre: msg.pushName || '',
            jid,
            resolvedJid: jidResolved,
            lidJid,
            addressingMode,
            needsJidResolution: !hasReachableTarget,
            ...(shouldTrustUnsafeTarget ? { allowUnsafeTarget: true } : {}),
            source: shouldTreatAsMetaAdInbound ? 'meta_ads' : 'WhatsApp',
            ...(shouldTreatAsMetaAdInbound
              ? {
                  campaign: META_ADS_CAMPAIGN,
                  lastInboundFromAd: true,
                  lastInboundAt: inboundAt,
                }
              : {}),
          };

          // Lead nuevo
          if (!leadSnap.exists) {
            await leadRef.set({
              ...baseLead,
              fecha_creacion: inboundAt,
              estado: 'nuevo',
              etiquetas: etiquetaUnion,
              unreadCount: 0,
              lastMessageAt: inboundAt,
            });

            if (toCancel.length) await cancelSequences(leadId, toCancel).catch(() => {});

            const canSchedule = hasReachableTarget && !shouldBlockSequences({}, trigger);
            if (canSchedule) {
              await scheduleSequenceForLead(leadId, trigger, inboundAt);
              if (triggerSource === 'meta_ad') {
                await leadRef.set(
                  {
                    hasActiveSequences: true,
                    estado: 'nuevo',
                    lastMetaAutoScheduledAt: inboundAt,
                    lastMetaSequenceAt: inboundAt,
                  },
                  { merge: true }
                );
              }
              console.log('[WA] ✅ Lead CREADO + secuencia programada:', { leadId, phone: normNum, trigger, source: triggerSource });
            } else if (!hasReachableTarget) {
              console.log('[WA] ⏭️ Lead CREADO sin ruta de envío; secuencia pendiente de resolución:', { leadId, trigger, source: triggerSource });
            } else {
              console.log('[WA] Lead CREADO (bloqueado); no se programa:', { leadId, trigger });
            }
          } else {
            // Lead existente
            const current = { id: leadSnap.id, ...(existingLeadData || {}) };
            const updatePayload = {
              lastMessageAt: inboundAt,
              jid,
              resolvedJid: jidResolved,
              lidJid,
              telefono: normNum,
              addressingMode,
              needsJidResolution: !hasReachableTarget,
              ...(shouldTrustUnsafeTarget ? { allowUnsafeTarget: true } : {})
            };
            if (shouldTreatAsMetaAdInbound) {
              updatePayload.source = 'meta_ads';
              updatePayload.campaign = META_ADS_CAMPAIGN;
              updatePayload.lastInboundFromAd = true;
              updatePayload.lastInboundAt = inboundAt;
            }
            await leadRef.update(updatePayload);

            if (!current.nombre && msg.pushName) {
              await leadRef.set({ nombre: msg.pushName }, { merge: true });
            }

            await leadRef.set({ etiquetas: FieldValue.arrayUnion(...etiquetaUnion) }, { merge: true });

            if (toCancel.length) await cancelSequences(leadId, toCancel).catch(() => {});

            const blocked = shouldBlockSequences(current, trigger);
            const alreadyHas = hasSameTrigger(current.secuenciasActivas, trigger);
            const isMetaAutoTrigger = triggerSource === 'meta_ad';
            const isSchedulableSource = triggerSource === 'hashtag' || triggerSource === 'db' || isMetaAutoTrigger;
            const metaCooldownActive = isMetaAutoTrigger && hasRecentMetaAutoSchedule(current, inboundAt);

            if (!blocked && !alreadyHas && !metaCooldownActive && hasReachableTarget && isSchedulableSource) {
              const programmed = await scheduleSequenceForLead(leadId, trigger, inboundAt);
              if (programmed > 0) {
                if (triggerSource === 'meta_ad') {
                  await leadRef.set(
                    {
                      estado: 'nuevo',
                      hasActiveSequences: true,
                      lastMetaAutoScheduledAt: inboundAt,
                      lastMetaSequenceAt: inboundAt,
                    },
                    { merge: true }
                  );
                }
                console.log('[WA] ✅ Lead ACTUALIZADO (reprogramado):', { leadId, trigger, source: triggerSource });
              } else {
                console.log('[WA] Lead ACTUALIZADO (sin reprogramar):', {
                  leadId,
                  trigger,
                  source: triggerSource,
                  blocked,
                  reason: 'schedule-omit'
                });
              }
            } else {
              console.log('[WA] Lead ACTUALIZADO (sin reprogramar):', {
                leadId,
                trigger,
                source: triggerSource,
                blocked,
                reason: !hasReachableTarget
                  ? 'sin-ruta-envio'
                  : (blocked
                    ? 'bloqueado'
                    : (alreadyHas
                      ? 'ya-activo'
                      : (metaCooldownActive ? 'meta-cooldown' : 'trigger=default')))
              });
            }
          }

          // Guardar mensaje
          const msgData = {
            content,
            mediaType,
            mediaUrl,
            sender,
            timestamp: now(),
            ...replyContext,
          };
          await persistLeadMessage(leadRef, msgData, msg?.key?.id || null);

          const upd = buildLeadLastMessagePatch(msgData, { incrementUnread: sender === 'lead' });
          await leadRef.update(upd);

          console.log('[WA] Guardado mensaje →', leadId, { mediaType, hasText: !!content, hasMedia: !!mediaUrl });
        } catch (err) {
          console.error('[WA] ❌ Error procesando mensaje:', err);
        }
      }
    });

    return sock;
  } catch (error) {
    console.error('Error al conectar con WhatsApp:', error);
    throw error;
  }
}


/* ----------------------------- helpers envío ---------------------------- */
export function getLatestQR() { return latestQR; }
export function getConnectionStatus() { return connectionStatus; }
export function getWhatsAppSock() { return whatsappSock; }
export function getSessionPhone() { return sessionPhone; }

async function resolveLeadAndTarget(phoneOrJid) {
  const raw = String(phoneOrJid || '');
  const isJidInput = raw.includes('@');
  const normalizedInputJid = isJidInput ? normalizeJid(raw) : null;
  let num = isJidInput ? phoneFromJid(normalizedInputJid) : normalizePhoneForWA(raw);

  const provisionalLeadId = normalizedInputJid || null;
  const initialNumericTarget = isSafePhoneForJidFallback(num) ? `${num}@s.whatsapp.net` : null;
  let leadId = normalizedInputJid || null;
  let targetJid = normalizedInputJid || initialNumericTarget;
  let leadData = null;
  let leadRef = null;

  if (normalizedInputJid) {
    const snap = await db.collection('leads').doc(normalizedInputJid).get();
    if (snap.exists) {
      leadId = snap.id;
      leadData = snap.data() || {};
      leadRef = snap.ref;
    }
  }

  if (!num && leadData) {
    num = normalizePhoneForWA(leadData.telefono || '') || phoneFromJid(leadData.resolvedJid || leadData.jid || leadId);
  }

  if (!leadData && num) {
    const q = await db.collection('leads').where('telefono', '==', num).limit(1).get();
    if (!q.empty) {
      leadId = q.docs[0].id;
      leadData = q.docs[0].data() || {};
      leadRef = q.docs[0].ref;
    }
  }

  const bestRef = await resolveExistingLeadReference({
    provisionalLeadId: leadId || provisionalLeadId || '',
    normalizedPhone: num,
    resolvedJid: leadData?.resolvedJid || normalizedInputJid,
    jid: leadData?.jid || normalizedInputJid,
    lidJid: leadData?.lidJid || (normalizedInputJid?.endsWith('@lid') ? normalizedInputJid : null),
  });
  if (bestRef?.leadId) {
    leadId = bestRef.leadId;
    leadRef = bestRef.leadRef;
    leadData = bestRef.leadSnap?.data?.() || bestRef.leadSnap?.data() || leadData;
  }

  if (!num && leadData) {
    num = normalizePhoneForWA(leadData.telefono || '') || phoneFromJid(leadData.resolvedJid || leadData.jid || leadId);
  }

  if (leadData) {
    const candidateJid = normalizeJid(leadData.resolvedJid || leadData.jid || leadId);
    const lidCandidate = normalizeJid(leadData.lidJid || '');
    const allowUnsafeTarget = leadData?.allowUnsafeTarget === true;
    const hasLidContext = isLidJid(lidCandidate) || isLidJid(normalizedInputJid);
    const candidateIsSuspicious = isSuspiciousPseudoPhoneJid(candidateJid);
    const canUseNumericFallback = Boolean(
      num
      && (
        /^521\d{10}$/.test(String(num))
        || (!hasLidContext && /^52\d{10}$/.test(String(num)))
        || (!hasLidContext && /^\d{10}$/.test(String(num)))
      )
    );
    const candidateLooksWrongForLid = Boolean(
      hasLidContext && isUserJid(candidateJid) && candidateIsSuspicious
    );

    if (candidateLooksWrongForLid && isLidJid(lidCandidate)) {
      targetJid = lidCandidate;
    } else if (isUserJid(candidateJid) && (!candidateIsSuspicious || allowUnsafeTarget)) {
      targetJid = candidateJid;
    } else if (isLidJid(candidateJid)) {
      targetJid = candidateJid;
    } else if (isLidJid(lidCandidate)) {
      targetJid = lidCandidate;
    } else if (canUseNumericFallback) {
      targetJid = `${num}@s.whatsapp.net`;
    } else if (candidateJid && (!candidateIsSuspicious || allowUnsafeTarget)) {
      targetJid = candidateJid;
    } else {
      targetJid = null;
      if (candidateIsSuspicious) {
        console.warn(
          `[WA] Destino bloqueado por JID sospechoso (${candidateJid}) para lead ${leadId}.`
        );
      }
    }
  } else if (num) {
    targetJid = isSafePhoneForJidFallback(num) ? `${num}@s.whatsapp.net` : null;
  }

  if (!leadId && num && isSafePhoneForJidFallback(num)) {
    leadId = `${num}@s.whatsapp.net`;
  }

  return { leadId, targetJid, num, leadData, leadRef, provisionalLeadId };
}

async function ensureLeadDocForOutbound({
  leadRef,
  leadId,
  targetJid,
  num,
}) {
  const targetRef = leadRef || (leadId ? db.collection('leads').doc(String(leadId)) : null);
  if (!targetRef) return null;

  const snap = await targetRef.get();
  if (snap.exists) return targetRef;

  const normalizedTarget = normalizeJid(targetJid || leadId || '');
  const phone = String(num || phoneFromJid(normalizedTarget) || '').replace(/\D/g, '');

  const patch = {
    fecha_creacion: now(),
    estado: 'nuevo',
    source: 'manual',
    unreadCount: 0,
    ...(phone ? { telefono: phone } : {}),
    ...(normalizedTarget ? { jid: normalizedTarget } : {}),
  };

  if (isUserJid(normalizedTarget) && !isSuspiciousPseudoPhoneJid(normalizedTarget)) {
    patch.resolvedJid = normalizedTarget;
    patch.needsJidResolution = false;
  } else if (isLidJid(normalizedTarget)) {
    patch.lidJid = normalizedTarget;
    patch.needsJidResolution = true;
  }

  await targetRef.set(patch, { merge: true });
  return targetRef;
}

async function syncAliasLeadToCanonical({
  provisionalLeadId,
  canonicalLeadId,
  targetJid,
  num,
}) {
  if (!provisionalLeadId || !canonicalLeadId || provisionalLeadId === canonicalLeadId) return;

  const aliasRef = db.collection('leads').doc(String(provisionalLeadId));
  const aliasSnap = await aliasRef.get();
  if (!aliasSnap.exists) return;

  const patch = {
    mergedInto: canonicalLeadId,
    mergedAt: now(),
    ...(targetJid ? { resolvedJid: targetJid, jid: targetJid } : {}),
    ...(num ? { telefono: num } : {}),
  };

  if (isUserJid(targetJid)) {
    patch.needsJidResolution = false;
  }

  await aliasRef.set(patch, { merge: true });
}

export async function sendMessageToLead(phoneOrJid, messageContent, options = {}) {
  if (!whatsappSock) throw new Error('No hay conexión activa con WhatsApp');

  const { leadId, targetJid, num, provisionalLeadId, leadRef } = await resolveLeadAndTarget(phoneOrJid);

  if (!targetJid) throw new Error('No se pudo resolver JID de destino');

  const replyToWaMessageId = String(options?.replyToWaMessageId || '').trim();
  const replyPreview = String(options?.replyPreview || '').trim();
  const replySender = String(options?.replySender || '').toLowerCase();
  const quoted = await resolveQuotedMessageForSend({
    leadRef,
    targetJid,
    waMessageId: replyToWaMessageId,
    fallbackPreview: replyPreview,
    fallbackFromMe: replySender === 'business',
  });

  const sendOptions = { timeoutMs: 60_000 };
  if (quoted) sendOptions.quoted = quoted;

  let sent;
  try {
    sent = await whatsappSock.sendMessage(
      targetJid,
      { text: messageContent, linkPreview: false },
      sendOptions
    );
  } catch (error) {
    if (quoted) {
      console.warn(
        `[WA] quoted falló para ${targetJid} (msgId=${replyToWaMessageId}). Reintentando sin quoted:`,
        error?.message || error
      );
      sent = await whatsappSock.sendMessage(
        targetJid,
        { text: messageContent, linkPreview: false },
        { timeoutMs: 60_000 }
      );
    } else {
      throw error;
    }
  }

  if (leadId) {
    const outMsg = {
      content: messageContent,
      sender: 'business',
      timestamp: now(),
      ...(replyToWaMessageId ? { replyToWaMessageId } : {}),
      ...(replyPreview ? { replyToPreview: replyPreview.slice(0, 160) } : {}),
    };
    const canonicalRef = await ensureLeadDocForOutbound({
      leadRef,
      leadId,
      targetJid,
      num,
    });
    await persistLeadMessage(canonicalRef, outMsg, sent?.key?.id || null);
    await canonicalRef.set(
      {
        ...buildLeadLastMessagePatch(outMsg),
        ...(num ? { telefono: num } : {}),
        ...(targetJid ? { jid: targetJid } : {}),
        ...(isUserJid(targetJid) && !isSuspiciousPseudoPhoneJid(targetJid)
          ? { resolvedJid: targetJid, needsJidResolution: false }
          : {}),
        ...(isLidJid(targetJid) ? { lidJid: targetJid, needsJidResolution: true } : {}),
      },
      { merge: true }
    );
    await syncAliasLeadToCanonical({
      provisionalLeadId,
      canonicalLeadId: leadId,
      targetJid,
      num,
    });
  }
  return { success: true };
}

export async function sendImageToLead(phoneOrJid, imageUrl, caption = '') {
  if (!whatsappSock) throw new Error('No hay conexión activa con WhatsApp');

  const { leadId, targetJid, num, provisionalLeadId, leadRef } = await resolveLeadAndTarget(phoneOrJid);
  if (!targetJid) throw new Error('No se pudo resolver JID de destino');

  const sent = await whatsappSock.sendMessage(
    targetJid,
    {
      image: { url: imageUrl },
      caption: caption || '',
    },
    { timeoutMs: 120_000 }
  );

  if (leadId) {
    const outMsg = {
      content: caption || '',
      mediaType: 'image',
      mediaUrl: imageUrl,
      sender: 'business',
      timestamp: now(),
    };
    const canonicalRef = await ensureLeadDocForOutbound({
      leadRef,
      leadId,
      targetJid,
      num,
    });
    await persistLeadMessage(canonicalRef, outMsg, sent?.key?.id || null);
    await canonicalRef.set(
      {
        ...buildLeadLastMessagePatch(outMsg),
        ...(num ? { telefono: num } : {}),
        ...(targetJid ? { jid: targetJid } : {}),
        ...(isUserJid(targetJid) && !isSuspiciousPseudoPhoneJid(targetJid)
          ? { resolvedJid: targetJid, needsJidResolution: false }
          : {}),
        ...(isLidJid(targetJid) ? { lidJid: targetJid, needsJidResolution: true } : {}),
      },
      { merge: true }
    );
    await syncAliasLeadToCanonical({
      provisionalLeadId,
      canonicalLeadId: leadId,
      targetJid,
      num,
    });
  }

  return { success: true };
}

export async function sendFullAudioAsDocument(phone, fileUrl) {
  const sock = getWhatsAppSock();
  if (!sock) throw new Error('No hay conexión activa con WhatsApp');

  const num = normalizePhoneForWA(phone);
  const jid = `${num}@s.whatsapp.net`;

  const res = await axios.get(fileUrl, { responseType: 'arraybuffer' });
  const buffer = Buffer.from(res.data);

  await sock.sendMessage(jid, {
    document: buffer,
    mimetype: 'audio/mpeg',
    fileName: 'cancion_completa.mp3',
    caption: '¡Te comparto tu canción completa!',
  });
  console.log(`✅ Canción completa enviada como adjunto a ${jid}`);
}

export async function sendAudioMessage(phoneOrJid, audioSrc, {
  ptt = true,
  seconds = null,
  forwarded = false,
  quoted = null,
  mimetype = null
} = {}) {
  const sock = getWhatsAppSock();
  if (!sock) throw new Error('Socket de WhatsApp no está conectado');

  const raw = String(phoneOrJid || '').trim();
  let jid = null;

  if (raw && !raw.includes('@')) {
    try {
      const byId = await db.collection('leads').doc(raw).get();
      if (byId.exists) {
        const resolved = await resolveLeadAndTarget(raw);
        jid = normalizeJid(resolved?.targetJid);
      }
    } catch (error) {
      console.warn('[sendAudioMessage] No se pudo resolver leadId, se intentará como teléfono:', error?.message || error);
    }
  }

  if (!jid) {
    const normalizedInputJid = raw.includes('@') ? normalizeJid(raw) : null;
    const digits = raw.replace(/\D/g, '');
    const normalizedPhone = (() => {
      if (/^\d{10}$/.test(digits)) return `521${digits}`;
      if (/^52\d{10}$/.test(digits)) return `521${digits.slice(2)}`;
      return digits;
    })();
    jid = normalizedInputJid || (normalizedPhone ? `${normalizedPhone}@s.whatsapp.net` : null);
  }
  if (!jid) throw new Error('Número o JID de destino inválido');

  const isHttp = (v) => typeof v === 'string' && /^https?:/i.test(v);
  const inferAudioMime = (value) => {
    const clean = String(value || '').split('?')[0].toLowerCase();
    if (clean.endsWith('.ogg') || clean.endsWith('.opus')) return 'audio/ogg; codecs=opus';
    if (clean.endsWith('.mp3')) return 'audio/mpeg';
    if (clean.endsWith('.m4a') || clean.endsWith('.mp4') || clean.endsWith('.aac')) return 'audio/mp4';
    if (clean.endsWith('.wav')) return 'audio/wav';
    if (clean.endsWith('.webm')) return 'audio/webm';
    return null;
  };

  const audioPayload =
    (typeof audioSrc === 'string')
      ? (isHttp(audioSrc) ? { url: audioSrc } : fs.readFileSync(audioSrc))
      : (Buffer.isBuffer(audioSrc) ? audioSrc
         : (audioSrc && typeof audioSrc === 'object' && audioSrc.url ? { url: audioSrc.url } : null));

  if (!audioPayload) throw new Error('Fuente de audio inválida');
  const inferredMime =
    typeof audioSrc === 'string'
      ? inferAudioMime(audioSrc)
      : inferAudioMime(audioSrc?.url);
  const finalMime = mimetype || inferredMime || (ptt ? 'audio/ogg; codecs=opus' : 'audio/mp4');

  const message = {
    audio: audioPayload,
    mimetype: finalMime,
    ptt: !!ptt,
    ...(forwarded ? { contextInfo: { isForwarded: true, forwardingScore: 5 } } : {})
  };
  if (Number.isFinite(seconds)) {
    message.seconds = Math.max(1, Math.round(seconds));
  }

  const options = { timeoutMs: 120_000 };
  if (quoted) options.quoted = quoted;

  return sock.sendMessage(jid, message, options);
}

export async function sendClipMessage(phone, clipUrl) {
  const sock = getWhatsAppSock();
  if (!sock) throw new Error('No hay conexión activa con WhatsApp');

  const num = normalizePhoneForWA(phone);
  const jid = `${num}@s.whatsapp.net`;

  const isOgg = /\.(ogg|opus)(\?|#|$)/i.test(clipUrl);
  const urlPayload = isOgg
    ? { audio: { url: clipUrl }, mimetype: 'audio/ogg; codecs=opus', ptt: true }
    : { audio: { url: clipUrl }, mimetype: 'audio/mp4', ptt: false };

  const opts = { timeoutMs: 120_000, sendSeen: false };

  try {
    await sock.sendMessage(jid, urlPayload, opts);
    console.log(`✅ clip enviado por URL a ${jid}`);
    return;
  } catch (err) {
    console.warn(`⚠️ fallo envío por URL: ${err?.message || err}`);
  }

  try {
    const res = await axios.get(clipUrl, { responseType: 'arraybuffer' });
    const buf = Buffer.from(res.data);

    const mime =
      isOgg ? 'audio/ogg; codecs=opus' :
      (res.headers['content-type']?.toLowerCase().startsWith('audio/')
        ? res.headers['content-type']
        : 'audio/mp4');

    const payload = { audio: buf, mimetype: mime, ptt: isOgg };
    await sock.sendMessage(jid, payload, opts);

    console.log(`✅ clip enviado como buffer a ${jid} (mime=${mime})`);
    return;
  } catch (err2) {
    console.error(`❌ envío de clip falló también con buffer: ${err2?.message || err2}`);
    throw err2;
  }
}

export async function sendVoiceNoteFromUrl(phone, fileUrl, secondsHint = null) {
  const sock = getWhatsAppSock();
  if (!sock) throw new Error('No hay conexión activa con WhatsApp');

  const num = normalizePhoneForWA(phone);
  const jid = `${num}@s.whatsapp.net`;

  const msg = {
    audio: { url: fileUrl },
    mimetype: 'audio/ogg; codecs=opus',
    ptt: true
  };
  if (Number.isFinite(secondsHint)) {
    msg.seconds = Math.max(1, Math.round(secondsHint));
  }

  const sent = await sock.sendMessage(jid, msg, { timeoutMs: 120_000 });

  const q = await db.collection('leads').where('telefono', '==', num).limit(1).get();
  if (!q.empty) {
    const leadId = q.docs[0].id;
    const leadRef = db.collection('leads').doc(leadId);
    const msgData = {
      content: '',
      mediaType: 'audio_ptt',
      mediaUrl: fileUrl,
      sender: 'business',
      timestamp: new Date()
    };
    await persistLeadMessage(leadRef, msgData, sent?.key?.id || null);
    await leadRef.update(buildLeadLastMessagePatch(msgData));
  }
}

export async function sendVideoNote(phone, videoUrlOrPath, secondsHint = null) {
  const sock = getWhatsAppSock();
  if (!sock) throw new Error('No hay conexión activa con WhatsApp');

  const num = normalizePhoneForWA(phone);
  const jid = `${num}@s.whatsapp.net`;

  const opts = { timeoutMs: 120_000, sendSeen: false };

  const buildMsg = (video) => {
    const msg = { video, ptv: true, mimetype: 'video/mp4' };
    if (Number.isFinite(secondsHint)) {
      msg.seconds = Math.max(1, Math.round(secondsHint));
    }
    return msg;
  };

  const isHttp = String(videoUrlOrPath).startsWith('http');
  let sentMsg = null;

  if (isHttp) {
    try {
      sentMsg = await sock.sendMessage(jid, buildMsg({ url: videoUrlOrPath }), opts);
      console.log(`✅ videonota enviada por URL a ${jid}`);
    } catch (e1) {
      console.warn(`[videonota] fallo por URL: ${e1?.message || e1}`);
      try {
        const res = await axios.get(videoUrlOrPath, { responseType: 'arraybuffer' });
        const buf = Buffer.from(res.data);

        const ct = String(res.headers?.['content-type'] || '').toLowerCase();
        const msg = buildMsg(buf);
        if (ct.startsWith('video/')) msg.mimetype = ct;

        sentMsg = await sock.sendMessage(jid, msg, opts);
        console.log(`✅ videonota enviada como buffer a ${jid} (mime=${msg.mimetype})`);
      } catch (e2) {
        console.error(`❌ videonota falló también con buffer: ${e2?.message || e2}`);
        throw e2;
      }
    }
  } else {
    const buf = fs.readFileSync(videoUrlOrPath);
    sentMsg = await sock.sendMessage(jid, buildMsg(buf), opts);
    console.log(`✅ videonota enviada desde archivo local a ${jid}`);
  }

  const q = await db.collection('leads').where('telefono', '==', num).limit(1).get();
  if (!q.empty) {
    const leadId = q.docs[0].id;
    const leadRef = db.collection('leads').doc(leadId);
    const msgData = {
      content: '',
      mediaType: 'video_note',
      mediaUrl: isHttp ? videoUrlOrPath : null,
      sender: 'business',
      timestamp: new Date()
    };
    await persistLeadMessage(leadRef, msgData, sentMsg?.key?.id || null);
    await leadRef.update(buildLeadLastMessagePatch(msgData));
  }
}
