// whatsappService.js - VERSIÓN CORREGIDA
// 🔧 FIX APLICADO: Listener corregido para procesar solo mensajes 'notify' (nuevos entrantes)
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
import { scheduleSequenceForLead, cancelSequences, cancelAllSequences } from './queue.js';


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
  '#WebPro990':  'LeadWeb',
  '#webPro990':  'LeadWeb',
  '#leadweb':      'LeadWeb',
  '#nuevolead':    'NuevoLeadWeb',
  '#planredes990': 'PlanRedes',
  '#info':         'LeadWeb', 
};

// Si el trigger es LeadWeb, cancela estas (evita duplicidad)
const STATIC_CANCEL_BY_TRIGGER = {
  LeadWeb: ['NuevoLeadWeb', 'NuevoLead'],
};

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
  const etiquetas = Array.isArray(leadData?.etiquetas) ? leadData.etiquetas : [];
  const etapa = leadData?.etapa || '';
  const estado = (leadData?.estado || '').toLowerCase();

  if (leadData?.seqPaused) return true;
  if (estado === 'compro' || etiquetas.includes('Compro')) return true;

  if (etapa === 'form_submitted' || etiquetas.includes('FormOK')) {
    if (['LeadWeb', 'NuevoLead', 'NuevoLeadWeb'].includes(nextTrigger)) return true;
  }
  return false;
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
      // ✅ CORRECCIÓN: Solo procesar mensajes nuevos (notify) según documentación oficial de Baileys
      // https://baileys.wiki/docs/socket/receiving-updates/
      // type === 'notify': mensajes NUEVOS entrantes (lo que necesitamos)
      // type === 'append': historial/sincronización (ignorar)
      // type === 'prepend': historial antiguo (ignorar)
      if (type !== 'notify') {
        console.log(`[WA] ⏭️ Ignorando mensajes tipo '${type || 'undefined'}' (solo se procesan 'notify')`);
        return;
      }

      // ✅ Log de debugging mejorado
      console.log(`[WA] 📩 Procesando ${messages.length} mensaje(s) NUEVOS | tipo: ${type} | ${new Date().toISOString()}`);

      for (const msg of messages) {
        try {
          // Validación de JID
          let rawJid = (msg?.key?.remoteJid || '').trim();
          if (!rawJid) {
            console.warn('[WA] mensaje sin remoteJid, se ignora');
            continue;
          }

          // Ignorar grupos/estados/newsletters
          if (rawJid.endsWith('@g.us') || rawJid === 'status@broadcast' || rawJid.endsWith('@newsletter')) {
            console.log(`[WA] ⏭️ Ignorando mensaje de: ${rawJid} (grupo/canal/newsletter)`);
            continue;
          }

          // Manejar mensajes de Business API (@lid) que vienen de FB Ads
          // Estos mensajes tienen el remitente real en senderPn o participant
          if (rawJid.endsWith('@lid')) {
            console.log(`[WA] 📱 Mensaje de Business API detectado (@lid) - ID: ${msg.key.id}`);

            // Intentar extraer el número real (primero senderPn, luego participant)
            const realSender = msg.key.senderPn || msg.key.participant;

            if (realSender && realSender.includes('@s.whatsapp.net')) {
              console.log(`[WA] ✅ Remitente real extraído de @lid: ${realSender}`);
              rawJid = realSender; // Usar el número real del lead
            } else {
              console.warn(`[WA] ⚠️ No se pudo extraer remitente real de mensaje @lid.`);
              console.log('[WA] 🔍 msg.key:', JSON.stringify(msg.key, null, 2));
              console.log('[WA] 🔍 msg.pushName:', msg.pushName);
            }
          }

          // Verificar que el mensaje tenga contenido desencriptado
          if (!msg.message || Object.keys(msg.message).length === 0) {
            console.warn(`[WA] ⚠️ Mensaje sin contenido desencriptado desde ${rawJid} - ID: ${msg.key.id}`);

            // Para mensajes con remitente válido, intentar crear el lead de todas formas
            if (rawJid.includes('@s.whatsapp.net')) {
              console.log(`[WA] 🔄 Intentando crear/actualizar lead sin contenido de mensaje para ${rawJid}`);
              const [jidUser] = rawJid.split('@');
              const cleanUser = jidUser.split(':')[0].replace(/\s+/g, '');
              const normNum = normalizePhoneForWA(cleanUser);
              const leadId = `${normNum}@s.whatsapp.net`;

              const leadRef = db.collection('leads').doc(leadId);
              const leadSnap = await leadRef.get();

              if (!leadSnap.exists) {
                // Crear lead nuevo sin mensaje
                await leadRef.set({
                  telefono: normNum,
                  nombre: msg.pushName || '',
                  source: 'WhatsApp Business API',
                  fecha_creacion: now(),
                  estado: 'nuevo',
                  etiquetas: ['MensajeNoDesencriptado', 'FacebookAds'],
                  unreadCount: 1,
                  lastMessageAt: now(),
                });
                console.log(`[WA] ✅ Lead creado desde mensaje no desencriptado: ${leadId}`);
              } else {
                await leadRef.update({
                  lastMessageAt: now(),
                  unreadCount: FieldValue.increment(1),
                  etiquetas: FieldValue.arrayUnion('MensajeNoDesencriptado')
                });
                console.log(`[WA] ✅ Lead actualizado desde mensaje no desencriptado: ${leadId}`);
              }
            }
            continue;
          }

          const [jidUser, jidDomain] = rawJid.split('@');
          const cleanUser = jidUser.split(':')[0].replace(/\s+/g, '');
          const jid = `${cleanUser}@${jidDomain}`;
          if (jidDomain !== 's.whatsapp.net') continue;

          const waNumber = cleanUser;
          const normNum  = normalizePhoneForWA(waNumber);
          const leadId   = `${normNum}@s.whatsapp.net`;
          const sender   = msg.key.fromMe ? 'business' : 'lead';

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
            const leadRef = db.collection('leads').doc(leadId);

            const msgData = {
              content,
              mediaType,
              mediaUrl,
              sender,
              timestamp: now(),
            };

            await leadRef.set({
              telefono: normNum,
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
                  await cancelSequences(leadId, ...toCancel);
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

            console.log('[WA] (fromMe) Mensaje propio guardado →', leadId);
            continue;
          }

          // Mensajes de leads
          const cfgSnap = await db.collection('config').doc('appConfig').get();
          const cfg = cfgSnap.exists ? cfgSnap.data() : {};
          const defaultTrigger = cfg.defaultTrigger || 'NuevoLeadWeb';
          const rule = await resolveTriggerFromMessage(content, defaultTrigger);
          const trigger = rule.trigger;
          const toCancel = rule.cancel || [];

          const leadRef = db.collection('leads').doc(leadId);
          const leadSnap = await leadRef.get();

          const baseLead = {
            telefono: normNum,
            nombre: msg.pushName || '',
            source: 'WhatsApp',
          };

          // Lead nuevo
          if (!leadSnap.exists) {
            await leadRef.set({
              ...baseLead,
              fecha_creacion: now(),
              estado: 'nuevo',
              etiquetas: [trigger],
              unreadCount: 0,
              lastMessageAt: now(),
            });

            if (toCancel.length) await cancelSequences(leadId, toCancel).catch(() => {});

            const canSchedule = !shouldBlockSequences({}, trigger);
            if (canSchedule) {
              await scheduleSequenceForLead(leadId, trigger, now());
              console.log('[WA] ✅ Lead CREADO + secuencia programada:', { leadId, phone: normNum, trigger, source: rule.source });
            } else {
              console.log('[WA] Lead CREADO (bloqueado); no se programa:', { leadId, trigger });
            }
          } else {
            // Lead existente
            const current = { id: leadSnap.id, ...(leadSnap.data() || {}) };
            await leadRef.update({ lastMessageAt: now() });

            if (!current.nombre && msg.pushName) {
              await leadRef.set({ nombre: msg.pushName }, { merge: true });
            }

            await leadRef.set({ etiquetas: FieldValue.arrayUnion(trigger) }, { merge: true });

            if (toCancel.length) await cancelSequences(leadId, toCancel).catch(() => {});

            const blocked = shouldBlockSequences(current, trigger);

            if (!blocked && (rule.source === 'hashtag' || rule.source === 'db')) {
              await scheduleSequenceForLead(leadId, trigger, now());
              console.log('[WA] ✅ Lead ACTUALIZADO (reprogramado):', { leadId, trigger, source: rule.source });
            } else {
              console.log('[WA] Lead ACTUALIZADO (sin reprogramar):', {
                leadId,
                trigger,
                source: rule.source,
                blocked,
                reason: blocked ? 'bloqueado' : 'trigger=default'
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

export async function sendMessageToLead(phone, messageContent) {
  if (!whatsappSock) throw new Error('No hay conexión activa con WhatsApp');
  const num = normalizePhoneForWA(phone);
  const jid = `${num}@s.whatsapp.net`;

  await whatsappSock.sendMessage(
    jid,
    { text: messageContent, linkPreview: false },
    { timeoutMs: 60_000 }
  );

  const q = await db.collection('leads').where('telefono', '==', num).limit(1).get();
  if (!q.empty) {
    const leadId = q.docs[0].id;
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
  quoted = null
} = {}) {
  const sock = getWhatsAppSock();
  if (!sock) throw new Error('Socket de WhatsApp no está conectado');

  const raw = String(phoneOrJid || '');
  const digits = raw.replace(/\D/g, '');
  const norm = (() => {
    if (raw.includes('@s.whatsapp.net')) return raw;
    if (/^\d{10}$/.test(digits)) return `521${digits}`;
    if (/^52\d{10}$/.test(digits)) return `521${digits.slice(2)}`;
    return digits;
  })();
  const jid = norm.includes('@s.whatsapp.net') ? norm : `${norm}@s.whatsapp.net`;

  const isHttp = (v) => typeof v === 'string' && /^https?:/i.test(v);
  const audioPayload =
    (typeof audioSrc === 'string')
      ? (isHttp(audioSrc) ? { url: audioSrc } : fs.readFileSync(audioSrc))
      : (Buffer.isBuffer(audioSrc) ? audioSrc
         : (audioSrc && typeof audioSrc === 'object' && audioSrc.url ? { url: audioSrc.url } : null));

  if (!audioPayload) throw new Error('Fuente de audio inválida');

  const message = {
    audio: audioPayload,
    mimetype: 'audio/ogg; codecs=opus',
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