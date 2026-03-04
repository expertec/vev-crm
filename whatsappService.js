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

function hasTriggerScheduleHistory(leadData, trigger = '') {
  const next = String(trigger || '').toLowerCase();
  if (!next) return false;
  const history = Array.isArray(leadData?.sequenceDeliveredTriggers)
    ? leadData.sequenceDeliveredTriggers
    : [];
  return history.some((t) => String(t || '').toLowerCase() === next);
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
    const phoneDigits = remoteJid.replace('@lid', '').replace(/\D/g, '');
    if (phoneDigits.length >= 10) {
      const normalized = normalizePhoneForWA(phoneDigits);
      const jid = `${normalized}@s.whatsapp.net`;
      console.log(`[resolveSenderFromLid] ✅ Extraído de remoteJid: ${remoteJid} → ${jid}`);
      return jid;
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
  const phone = String(normalizedPhone || '').replace(/\D/g, '');
  if (phone.length >= 10) return `${phone}@s.whatsapp.net`;
  const resolved = normalizeJid(resolvedJid);
  if (resolved) return resolved;
  const fallback = normalizeJid(fallbackJid);
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

  const idCandidates = [
    String(provisionalLeadId || '').trim(),
    normalizeJid(resolvedJid),
    normalizeJid(jid),
    normalizeJid(lidJid),
  ].filter(Boolean);

  const uniqueIdCandidates = Array.from(new Set(idCandidates));
  for (const candidate of uniqueIdCandidates) {
    const snap = await leadsCol.doc(candidate).get();
    if (snap.exists) {
      return {
        leadId: snap.id,
        leadRef: snap.ref,
        leadSnap: snap,
        source: `doc:${candidate}`,
      };
    }
  }

  const jidCandidates = [
    normalizeJid(resolvedJid),
    normalizeJid(jid),
    normalizeJid(lidJid),
  ].filter(Boolean);

  const uniqueJidCandidates = Array.from(new Set(jidCandidates));
  for (const jidCandidate of uniqueJidCandidates) {
    for (const field of ['resolvedJid', 'jid', 'lidJid']) {
      const byField = await leadsCol.where(field, '==', jidCandidate).limit(1).get();
      if (!byField.empty) {
        const snap = byField.docs[0];
        return {
          leadId: snap.id,
          leadRef: snap.ref,
          leadSnap: snap,
          source: `${field}:${jidCandidate}`,
        };
      }
    }
  }

  const phone = String(normalizedPhone || '').replace(/\D/g, '');
  if (phone.length >= 10) {
    const byPhone = await leadsCol.where('telefono', '==', phone).limit(5).get();
    if (!byPhone.empty) {
      const preferred =
        byPhone.docs.find((docSnap) => {
          const data = docSnap.data() || {};
          return Boolean(data.assignedTo);
        }) || byPhone.docs[0];

      return {
        leadId: preferred.id,
        leadRef: preferred.ref,
        leadSnap: preferred,
        source: `telefono:${phone}`,
      };
    }
  }

  return null;
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

              if (realSender && realSender.includes('@s.whatsapp.net')) {
                console.log(`   ✅ JID real extraído correctamente: ${realSender}`);
                jidToUse = realSender; // ✅ Usar el número real del lead
              } else {
                // ⚠️ FALLBACK: Si no se puede resolver, intentar extraer del remoteJid
                const phoneDigits = String(rawJid || '').replace('@lid', '').replace(/\D/g, '');
                if (phoneDigits.length >= 10) {
                  const normalized = normalizePhoneForWA(phoneDigits);
                  jidToUse = `${normalized}@s.whatsapp.net`;
                  console.log(`   ⚠️ FALLBACK: Usando dígitos del remoteJid: ${jidToUse}`);
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
          const normNum  = phoneFromResolved || phoneFromFinalJid || normalizePhoneForWA(cleanUser);
          const hasReachableTarget = Boolean(
            phoneFromResolved
            || phoneFromFinalJid
            || (typeof normNum === 'string' && normNum.length >= 10)
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
                      await leadRef.set({ hasActiveSequences: true, estado: 'nuevo' }, { merge: true });
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
                  const alreadyScheduled = hasTriggerScheduleHistory(current, detectedTrigger);
                  const blocked = shouldBlockSequences(current, detectedTrigger);

                  if (!blocked && !alreadyHas && !alreadyScheduled && hasReachableTarget) {
                    try {
                      const programmed = await scheduleSequenceForLead(leadId, detectedTrigger, inboundAt);
                      if (programmed > 0) {
                        await leadRef.set({ hasActiveSequences: true, estado: 'nuevo' }, { merge: true });
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
                    console.log(`[WA] ⏭️ Meta Ads inbound sin reprogramar para ${leadId}: blocked=${blocked}, alreadyHas=${alreadyHas}, alreadyScheduled=${alreadyScheduled}`);
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

          // Mensajes propios (fromMe)
          if (sender === 'business') {
            const msgData = {
              content,
              mediaType,
              mediaUrl,
              sender,
              timestamp: now(),
            };

            await leadRef.set({
              telefono: normNum,
              jid,
              resolvedJid: jidResolved,
              lidJid,
              addressingMode,
              source: 'WhatsApp',
              lastMessageAt: msgData.timestamp,
            }, { merge: true });

            await leadRef.collection('messages').add(msgData);

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
              needsJidResolution: !hasReachableTarget
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
            const alreadyScheduled = hasTriggerScheduleHistory(current, trigger);
            const isMetaAutoTrigger = triggerSource === 'meta_ad';
            const isSchedulableSource = triggerSource === 'hashtag' || triggerSource === 'db' || isMetaAutoTrigger;
            const skipByMetaHistory = isMetaAutoTrigger && alreadyScheduled;

            if (!blocked && !alreadyHas && !skipByMetaHistory && hasReachableTarget && isSchedulableSource) {
              const programmed = await scheduleSequenceForLead(leadId, trigger, inboundAt);
              if (programmed > 0) {
                if (triggerSource === 'meta_ad') {
                  await leadRef.set({ estado: 'nuevo', hasActiveSequences: true }, { merge: true });
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
                      : (skipByMetaHistory ? 'meta-ya-programada' : 'trigger=default')))
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
          };
          await leadRef.collection('messages').add(msgData);

          const upd = { lastMessageAt: msgData.timestamp };
          if (sender === 'lead') upd.unreadCount = FieldValue.increment(1);
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

export async function sendMessageToLead(phoneOrJid, messageContent) {
  if (!whatsappSock) throw new Error('No hay conexión activa con WhatsApp');

  const raw = String(phoneOrJid || '');
  const isJidInput = raw.includes('@');
  const normalizedInputJid = isJidInput ? normalizeJid(raw) : null;
  const num = isJidInput ? phoneFromJid(normalizedInputJid) : normalizePhoneForWA(raw);

  let leadId = normalizedInputJid || null;
  let targetJid = normalizedInputJid || (num ? `${num}@s.whatsapp.net` : null);
  let leadData = null;

  if (normalizedInputJid) {
    const snap = await db.collection('leads').doc(normalizedInputJid).get();
    if (snap.exists) {
      leadId = snap.id;
      leadData = snap.data() || {};
    }
  }

  if (!leadData && num) {
    const q = await db.collection('leads').where('telefono', '==', num).limit(1).get();
    if (!q.empty) {
      leadId = q.docs[0].id;
      leadData = q.docs[0].data() || {};
    }
  }

  if (leadData) {
    const candidateJid = normalizeJid(leadData.resolvedJid || leadData.jid || leadId);
    if (candidateJid) targetJid = candidateJid;
  }

  if (!targetJid) throw new Error('No se pudo resolver JID de destino');

  await whatsappSock.sendMessage(
    targetJid,
    { text: messageContent, linkPreview: false },
    { timeoutMs: 60_000 }
  );

  if (leadId) {
    const outMsg = { content: messageContent, sender: 'business', timestamp: now() };
    await db.collection('leads').doc(leadId).collection('messages').add(outMsg);
    await db.collection('leads').doc(leadId).update({ lastMessageAt: outMsg.timestamp });
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
  forwarded = false,
  quoted = null,
  mimetype = null
} = {}) {
  const sock = getWhatsAppSock();
  if (!sock) throw new Error('Socket de WhatsApp no está conectado');

  const raw = String(phoneOrJid || '').trim();
  const normalizedInputJid = raw.includes('@') ? normalizeJid(raw) : null;
  const digits = raw.replace(/\D/g, '');
  const normalizedPhone = (() => {
    if (/^\d{10}$/.test(digits)) return `521${digits}`;
    if (/^52\d{10}$/.test(digits)) return `521${digits.slice(2)}`;
    return digits;
  })();
  const jid = normalizedInputJid || (normalizedPhone ? `${normalizedPhone}@s.whatsapp.net` : null);
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

  await sock.sendMessage(jid, msg, { timeoutMs: 120_000 });

  const q = await db.collection('leads').where('telefono', '==', num).limit(1).get();
  if (!q.empty) {
    const leadId = q.docs[0].id;
    const msgData = {
      content: '',
      mediaType: 'audio_ptt',
      mediaUrl: fileUrl,
      sender: 'business',
      timestamp: new Date()
    };
    await db.collection('leads').doc(leadId).collection('messages').add(msgData);
    await db.collection('leads').doc(leadId).update({ lastMessageAt: msgData.timestamp });
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

  if (isHttp) {
    try {
      await sock.sendMessage(jid, buildMsg({ url: videoUrlOrPath }), opts);
      console.log(`✅ videonota enviada por URL a ${jid}`);
    } catch (e1) {
      console.warn(`[videonota] fallo por URL: ${e1?.message || e1}`);
      try {
        const res = await axios.get(videoUrlOrPath, { responseType: 'arraybuffer' });
        const buf = Buffer.from(res.data);

        const ct = String(res.headers?.['content-type'] || '').toLowerCase();
        const msg = buildMsg(buf);
        if (ct.startsWith('video/')) msg.mimetype = ct;

        await sock.sendMessage(jid, msg, opts);
        console.log(`✅ videonota enviada como buffer a ${jid} (mime=${msg.mimetype})`);
      } catch (e2) {
        console.error(`❌ videonota falló también con buffer: ${e2?.message || e2}`);
        throw e2;
      }
    }
  } else {
    const buf = fs.readFileSync(videoUrlOrPath);
    await sock.sendMessage(jid, buildMsg(buf), opts);
    console.log(`✅ videonota enviada desde archivo local a ${jid}`);
  }

  const q = await db.collection('leads').where('telefono', '==', num).limit(1).get();
  if (!q.empty) {
    const leadId = q.docs[0].id;
    const msgData = {
      content: '',
      mediaType: 'video_note',
      mediaUrl: isHttp ? videoUrlOrPath : null,
      sender: 'business',
      timestamp: new Date()
    };
    await db.collection('leads').doc(leadId).collection('messages').add(msgData);
    await db.collection('leads').doc(leadId).update({ lastMessageAt: msgData.timestamp });
  }
}
