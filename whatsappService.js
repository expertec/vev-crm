// whatsappService.js
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
import os from 'os';
import path from 'path';
import axios from 'axios';
import admin from 'firebase-admin';
import { db } from './firebaseAdmin.js';

import ffmpeg from 'fluent-ffmpeg';
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg';
ffmpeg.setFfmpegPath(ffmpegInstaller.path);

// (opcional) si tienes @ffprobe-installer/ffprobe en package.json, usa su binario;
// si no, fluent-ffmpeg intentará usar el del sistema.
try {
  const ffprobeInstaller = await import('@ffprobe-installer/ffprobe').then(m => m.default || m);
  if (ffprobeInstaller?.path) {
    ffmpeg.setFfprobePath(ffprobeInstaller.path);
  }
} catch (_) { /* ignore - optional */ }

// Cola de secuencias
import { scheduleSequenceForLead, cancelSequences } from './queue.js';

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
  '#promoweb990':  'LeadWeb',
  '#miwebgratis':  'LeadWeb',   // ← reemplazo de #webpro990
  '#leadweb':      'LeadWeb',
  '#nuevolead':    'NuevoLeadWeb',
  '#planredes990': 'PlanRedes',
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

// Normaliza a formato que WhatsApp acepta para MX.
// - 10 dígitos → 521 + 10
// - 52XXXXXXXXXX (12 dígitos) → 521XXXXXXXXXX
// - 521XXXXXXXXXX → se mantiene
function normalizePhoneForWA(phone) {
  let num = String(phone || '').replace(/\D/g, '');
  if (num.length === 10) return '521' + num; // MX móvil clásico
  if (num.length === 12 && num.startsWith('52') && !num.startsWith('521')) {
    return '521' + num.slice(2);
  }
  return num;
}

// Reglas dinámicas opcionales en Firestore
// Collection: hashtagTriggers, doc fields: { code: 'miwebgratis', trigger: 'LeadWeb', cancel: ['NuevoLead'] }
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

/**
 * Devuelve { trigger, cancel, source }
 * source: 'db' | 'hashtag' | 'default'
 */
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

/** Determina si debemos BLOQUEAR programación de secuencias para este lead. */
function shouldBlockSequences(leadData, nextTrigger) {
  const etiquetas = Array.isArray(leadData?.etiquetas) ? leadData.etiquetas : [];
  const etapa = leadData?.etapa || '';
  const estado = (leadData?.estado || '').toLowerCase();

  // Pausa administrativa
  if (leadData?.seqPaused) return true;

  // Venta cerrada
  if (estado === 'compro' || etiquetas.includes('Compro')) return true;

  // Ya llenó formulario: bloquea reactivar captación (LeadWeb / NuevoLead*)
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
          // limpiar sesión local y forzar re-login
          for (const f of fs.readdirSync(localAuthFolder)) {
            fs.rmSync(path.join(localAuthFolder, f), { force: true, recursive: true });
          }
          sessionPhone = null;
        }
        // reintento con pequeño backoff aleatorio
        const delay = Math.floor(Math.random() * 4000) + 1000;
        setTimeout(() => connectToWhatsApp().catch(() => {}), delay);
      }
    });

    sock.ev.on('creds.update', saveCreds);

    /* -------------------- recepción de mensajes -------------------- */
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
      if (type !== 'notify') return;

      for (const msg of messages) {
        try {
          // --- Normalización robusta del JID ---
          let rawJid = (msg?.key?.remoteJid || '').trim();
          if (!rawJid) {
            console.warn('[WA] mensaje sin remoteJid, se ignora');
            continue;
          }

          // Ignorar grupos/estados/newsletters
          if (rawJid.endsWith('@g.us') || rawJid === 'status@broadcast' || rawJid.endsWith('@newsletter')) {
            continue;
          }

          const [jidUser, jidDomain] = rawJid.split('@');
          const cleanUser = jidUser.split(':')[0].replace(/\s+/g, '');
          const jid = `${cleanUser}@${jidDomain}`;
          if (jidDomain !== 's.whatsapp.net') continue;

          // Número como viene de WA y normalizado a 521 si aplica
          const waNumber = cleanUser;               // típico: 521XXXXXXXXXX (a veces 52XXXXXXXXXX)
          const normNum  = normalizePhoneForWA(waNumber);
          const leadId   = `${normNum}@s.whatsapp.net`; // usamos SIEMPRE normalizado como ID
          const sender   = msg.key.fromMe ? 'business' : 'lead';

          // ------- parseo de tipos (para leer hashtags del texto) -------
          let content = '';
          let mediaType = null;
          let mediaUrl = null;

          if (msg.message?.videoMessage) {
            mediaType = 'video';
            const buffer = await downloadMediaMessage(msg, 'buffer', {}, { logger: Pino() });
            const fileRef = bucket.file(`videos/${normNum}-${Date.now()}.mp4`);
            await fileRef.save(buffer, { contentType: 'video/mp4' });
            const [url] = await fileRef.getSignedUrl({ action: 'read', expires: '03-01-2500' });
            mediaUrl = url;
          } else if (msg.message?.imageMessage) {
            mediaType = 'image';
            const buffer = await downloadMediaMessage(msg, 'buffer', {}, { logger: Pino() });
            const fileRef = bucket.file(`images/${normNum}-${Date.now()}.jpg`);
            await fileRef.save(buffer, { contentType: 'image/jpeg' });
            const [url] = await fileRef.getSignedUrl({ action: 'read', expires: '03-01-2500' });
            mediaUrl = url;
          } else if (msg.message?.audioMessage) {
            mediaType = 'audio';
            const buffer = await downloadMediaMessage(msg, 'buffer', {}, { logger: Pino() });
            const fileRef = bucket.file(`audios/${normNum}-${Date.now()}.ogg`);
            await fileRef.save(buffer, { contentType: 'audio/ogg' });
            const [url] = await fileRef.getSignedUrl({ action: 'read', expires: '03-01-2500' });
            mediaUrl = url;
          } else if (msg.message?.documentMessage) {
            mediaType = 'document';
            const { mimetype, fileName: origName } = msg.message.documentMessage;
            const buffer = await downloadMediaMessage(msg, 'buffer', {}, { logger: Pino() });
            const ext = path.extname(origName || '') || '';
            const fileRef = bucket.file(`docs/${normNum}-${Date.now()}${ext}`);
            await fileRef.save(buffer, { contentType: mimetype || 'application/octet-stream' });
            const [url] = await fileRef.getSignedUrl({ action: 'read', expires: '03-01-2500' });
            mediaUrl = url;
          } else if (msg.message?.conversation) {
            mediaType = 'text';
            content = msg.message.conversation.trim();
          } else if (msg.message?.extendedTextMessage?.text) {
            mediaType = 'text';
            content = msg.message.extendedTextMessage.text.trim();
          } else {
            mediaType = 'unknown';
            content = '';
          }

          // ---------- Branch fromMe: guarda mensaje propio sin tocar nombre ----------
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
            console.log('[WA] (fromMe) Mensaje propio guardado →', leadId);
            continue;
          }

          // ------- config global + resolver trigger (hashtags > DB > estático > default) -------
          const cfgSnap = await db.collection('config').doc('appConfig').get();
          const cfg = cfgSnap.exists ? cfgSnap.data() : {};
          const defaultTrigger = cfg.defaultTrigger || 'NuevoLeadWeb';
          const rule = await resolveTriggerFromMessage(content, defaultTrigger);
          const trigger = rule.trigger;
          const toCancel = rule.cancel || [];

          // ------- crear/actualizar LEAD (siempre con número normalizado) -------
          const leadRef = db.collection('leads').doc(leadId);
          const leadSnap = await leadRef.get();

          const baseLead = {
            telefono: normNum,        // SIEMPRE normalizado
            nombre: msg.pushName || '',
            source: 'WhatsApp',
          };

          // ------------- Lead nuevo -------------
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
              console.log('[WA] Lead CREADO + secuencia programada:', { leadId, phone: normNum, trigger, source: rule.source });
            } else {
              console.log('[WA] Lead CREADO (bloqueado por estado); no se programa:', { leadId, trigger });
            }
          } else {
            // ------------- Lead existente -------------
            const current = { id: leadSnap.id, ...(leadSnap.data() || {}) };
            await leadRef.update({ lastMessageAt: now() });

            // Sólo si viene del lead y nombre está vacío
            if (!current.nombre && msg.pushName) {
              await leadRef.set({ nombre: msg.pushName }, { merge: true });
            }

            await leadRef.set({ etiquetas: FieldValue.arrayUnion(trigger) }, { merge: true });

            if (toCancel.length) await cancelSequences(leadId, toCancel).catch(() => {});
            const blocked = shouldBlockSequences(current, trigger);

            // Reprogramar solo si el trigger viene de hashtag/DB y no está bloqueado
            if (!blocked && (rule.source === 'hashtag' || rule.source === 'db')) {
              await scheduleSequenceForLead(leadId, trigger, now());
              console.log('[WA] Lead ACTUALIZADO (reprogramado por hashtag/db):', { leadId, trigger, source: rule.source });
            } else {
              console.log('[WA] Lead ACTUALIZADO (sin reprogramar):', {
                leadId,
                trigger,
                source: rule.source,
                blocked,
                reason: blocked ? 'seqPaused/Compro/FormOK' : 'trigger=default'
              });
            }
          }

          // ------- guardar mensaje -------
          const msgData = {
            content,
            mediaType,
            mediaUrl,
            sender,
            timestamp: now(),
          };
          await leadRef.collection('messages').add(msgData);

          // actualizar counters
          const upd = { lastMessageAt: msgData.timestamp };
          if (sender === 'lead') upd.unreadCount = FieldValue.increment(1);
          await leadRef.update(upd);

          console.log('[WA] Guardado mensaje →', leadId, { mediaType, hasText: !!content, hasMedia: !!mediaUrl });
        } catch (err) {
          console.error('messages.upsert error:', err);
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

  // persistir en Firestore si existe el lead
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

// Enviar audio con soporte de "Reenviado", PTT y quoted.
export async function sendAudioMessage(phoneOrJid, audioSrc, {
  ptt = true,
  forwarded = false,
  quoted = null
} = {}) {
  const sock = getWhatsAppSock();
  if (!sock) throw new Error('Socket de WhatsApp no está conectado');

  // Resolver JID (acepta JID directo o teléfono)
  const raw = String(phoneOrJid || '');
  const digits = raw.replace(/\D/g, '');
  const norm = (() => {
    if (raw.includes('@s.whatsapp.net')) return raw;
    if (/^\d{10}$/.test(digits)) return `521${digits}`;
    if (/^52\d{10}$/.test(digits)) return `521${digits.slice(2)}`;
    return digits;
  })();
  const jid = norm.includes('@s.whatsapp.net') ? norm : `${norm}@s.whatsapp.net`;

  // Preparar fuente (URL, Buffer o path). Para PTT: OGG/OPUS recomendado
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

/**
 * Envía audio por URL. Si es .ogg/.opus lo envía como **nota de voz** (PTT).
 */
export async function sendClipMessage(phone, clipUrl) {
  const sock = getWhatsAppSock();
  if (!sock) throw new Error('No hay conexión activa con WhatsApp');

  const num = normalizePhoneForWA(phone);
  const jid = `${num}@s.whatsapp.net`;

  // Detecta .ogg/.opus para PTT
  const isOgg = /\.(ogg|opus)(\?|#|$)/i.test(clipUrl);
  const urlPayload = isOgg
    ? { audio: { url: clipUrl }, mimetype: 'audio/ogg; codecs=opus', ptt: true }
    : { audio: { url: clipUrl }, mimetype: 'audio/mp4', ptt: false };

  const opts = { timeoutMs: 120_000, sendSeen: false };

  // 1) Primer intento: por URL directa
  try {
    await sock.sendMessage(jid, urlPayload, opts);
    console.log(`✅ clip enviado por URL a ${jid}`);
    return;
  } catch (err) {
    console.warn(`⚠️ fallo envío por URL: ${err?.message || err}`);
  }

  // 2) Fallback: descargar y enviar como buffer
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

/**
 * Envía **nota de voz** (PTT) desde una URL (ogg/opus) o cualquiera compatible con WhatsApp.
 */
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

/* -------------------- VIDEO: auto-rotar y normalizar antes de enviar -------------------- */
/**
 * Descarga (si es URL) y re-encodea el video “horneando” la rotación para evitar
 * que se vea acostado. También normaliza SAR=1 y formato yuv420p (compatibilidad WA).
 *
 * @param {string} phoneOrJid  Teléfono (10 dígitos MX, 52/521…) o JID completo.
 * @param {string} srcUrlOrPath  URL http(s) o ruta local.
 * @param {object} opts
 *   - seconds: number|null       // pista de duración para WA (opcional)
 *   - fileName: string           // nombre visible del archivo (opcional)
 *   - width: number              // ancho objetivo (default 720). Mantiene aspect ratio.
 */
export async function sendVideoWithAutoRotate(phoneOrJid, srcUrlOrPath, { seconds = null, fileName = 'video.mp4', width = 720 } = {}) {
  const sock = getWhatsAppSock();
  if (!sock) throw new Error('No hay conexión activa con WhatsApp');

  // --- Resolver JID ---
  const raw = String(phoneOrJid || '');
  const digits = raw.replace(/\D/g, '');
  const norm = (() => {
    if (raw.includes('@s.whatsapp.net')) return raw;
    if (/^\d{10}$/.test(digits)) return `521${digits}`;
    if (/^52\d{10}$/.test(digits)) return `521${digits.slice(2)}`;
    return digits;
  })();
  const jid = norm.includes('@s.whatsapp.net') ? norm : `${norm}@s.whatsapp.net`;

  // --- Preparar archivos temporales ---
  const tmpIn  = path.join(os.tmpdir(), `wa-in-${Date.now()}-${Math.random().toString(36).slice(2)}.mp4`);
  const tmpOut = path.join(os.tmpdir(), `wa-out-${Date.now()}-${Math.random().toString(36).slice(2)}.mp4`);

  // 1) Obtener fuente
  if (/^https?:/i.test(srcUrlOrPath)) {
    const res = await axios.get(srcUrlOrPath, { responseType: 'arraybuffer' });
    fs.writeFileSync(tmpIn, Buffer.from(res.data));
  } else {
    fs.copyFileSync(srcUrlOrPath, tmpIn);
  }

  // 2) Inspeccionar rotación vía ffprobe (si disponible)
  let rotate = 0;
  try {
    const meta = await new Promise((resolve) => {
      ffmpeg.ffprobe(tmpIn, (err, data) => resolve(err ? {} : data));
    });
    const v = (meta.streams || []).find(s => s.codec_type === 'video') || {};
    rotate = Number((v.tags && v.tags.rotate) || 0) || 0;
  } catch (_) {
    // si falla ffprobe, seguimos sin rotación detectada (forzamos normalización igual)
  }

  // 3) Filtros de video
  // - setsar=1 para quitar pixel aspect raro (iPhone)
  // - scale mantiene AR con height=-2
  // - format=yuv420p para compatibilidad
  const chain = [`scale=${Number(width) || 720}:-2`, 'setsar=1', 'format=yuv420p'];
  if (rotate === 90)  chain.unshift('transpose=1');
  if (rotate === 270 || rotate === -90) chain.unshift('transpose=2');
  if (rotate === 180) chain.unshift('transpose=1,transpose=1');

  // 4) Re-encodear “horneando” la orientación
  await new Promise((resolve, reject) => {
    ffmpeg(tmpIn)
      .videoFilters(chain.join(','))
      .outputOptions([
        '-movflags +faststart',
        '-metadata:s:v:0 rotate=0'
      ])
      .videoCodec('libx264')
      .audioCodec('aac')
      .output(tmpOut)
      .on('end', resolve)
      .on('error', reject)
      .run();
  });

  // 5) Enviar a WA como buffer
  const buf = fs.readFileSync(tmpOut);
  const msg = { video: buf, mimetype: 'video/mp4' };
  if (fileName) msg.fileName = fileName;
  if (Number.isFinite(seconds)) msg.seconds = Math.max(1, Math.round(seconds));

  await sock.sendMessage(jid, msg, { timeoutMs: 120_000 });

  // 6) Limpiar temporales
  try { fs.unlinkSync(tmpIn); } catch {}
  try { fs.unlinkSync(tmpOut); } catch {}
}

/**
 * (opcional) Enviar videonota (PTV = video redondo).
 * Si mandas material vertical aquí, considera pre-procesarlo con crop/scale para centrar rostro.
 */
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

  // Persistencia en Firestore (si el lead existe)
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
