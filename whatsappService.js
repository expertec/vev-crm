// whatsappService.js
import {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  downloadMediaMessage
} from '@whiskeysockets/baileys';
import QRCode from 'qrcode-terminal';
import Pino from 'pino';
import fs from 'fs';
import path from 'path';
import admin from 'firebase-admin';
import { db } from './firebaseAdmin.js';

let latestQR = null;
let connectionStatus = "Desconectado";
let whatsappSock = null;
let sessionPhone = null; // almacenará el número de la sesión activa

const localAuthFolder = '/var/data';
const { FieldValue } = admin.firestore;
const bucket = admin.storage().bucket();

export async function connectToWhatsApp() {
  try {
    // Asegurar carpeta de auth
    if (!fs.existsSync(localAuthFolder)) {
      fs.mkdirSync(localAuthFolder, { recursive: true });
    }

    const { state, saveCreds } = await useMultiFileAuthState(localAuthFolder);

    // Extraer número de sesión
    if (state.creds.me?.id) {
      sessionPhone = state.creds.me.id.split('@')[0];
    }

    const { version } = await fetchLatestBaileysVersion();
    const sock = makeWASocket({
      auth: state,
      logger: Pino({ level: 'info' }),
      printQRInTerminal: true,
      version,
    });
    whatsappSock = sock;

    // Manejo de conexión
    sock.ev.on('connection.update', ({ connection, lastDisconnect, qr }) => {
      if (qr) {
        latestQR = qr;
        connectionStatus = "QR disponible. Escanéalo.";
        QRCode.generate(qr, { small: true });
      }
      if (connection === 'open') {
        connectionStatus = "Conectado";
        latestQR = null;
        if (sock.user?.id) {
          sessionPhone = sock.user.id.split('@')[0];
        }
      }
      if (connection === 'close') {
        const reason = lastDisconnect?.error?.output?.statusCode;
        connectionStatus = "Desconectado";
        if (reason === DisconnectReason.loggedOut) {
          fs.readdirSync(localAuthFolder).forEach(f =>
            fs.rmSync(path.join(localAuthFolder, f), { force: true, recursive: true })
          );
          sessionPhone = null;
        }
        connectToWhatsApp();
      }
    });

    sock.ev.on('creds.update', saveCreds);

    // Dentro de whatsappService.js, ubica tu sección donde tienes:
// sock.ev.on('messages.upsert', async ({ messages, type }) => { … });

sock.ev.on('messages.upsert', async ({ messages, type }) => {
  if (type !== 'notify') return;

  for (const msg of messages) {
    if (!msg.key) continue;
    const jid = msg.key.remoteJid;
    if (!jid || jid.endsWith('@g.us')) continue; // ignorar grupos

    // 1) Determinar número de teléfono y quién envía
    const phone = jid.split('@')[0];
    const sender = msg.key.fromMe ? 'business' : 'lead';

    // 2) Inicializar variables para contenido y tipo de media
    let content = '';
    let mediaType = null;
    let mediaUrl = null;

    // 3) Procesar distintos tipos de mensaje
    try {
      // 3.1) Video
      if (msg.message.videoMessage) {
        mediaType = 'video';
        const buffer = await downloadMediaMessage(
          msg,
          'buffer',
          {},
          { logger: Pino() }
        );
        const fileName = `videos/${phone}-${Date.now()}.mp4`;
        const fileRef = admin.storage().bucket().file(fileName);
        await fileRef.save(buffer, { contentType: 'video/mp4' });
        const [url] = await fileRef.getSignedUrl({
          action: 'read',
          expires: '03-01-2500'
        });
        mediaUrl = url;
      }
      // 3.2) Imagen
      else if (msg.message.imageMessage) {
        mediaType = 'image';
        const buffer = await downloadMediaMessage(
          msg,
          'buffer',
          {},
          { logger: Pino() }
        );
        const fileName = `images/${phone}-${Date.now()}.jpg`;
        const fileRef = admin.storage().bucket().file(fileName);
        await fileRef.save(buffer, { contentType: 'image/jpeg' });
        const [url] = await fileRef.getSignedUrl({
          action: 'read',
          expires: '03-01-2500'
        });
        mediaUrl = url;
      }
      // 3.3) Audio
      else if (msg.message.audioMessage) {
        mediaType = 'audio';
        const buffer = await downloadMediaMessage(
          msg,
          'buffer',
          {},
          { logger: Pino() }
        );
        const fileName = `audios/${phone}-${Date.now()}.ogg`;
        const fileRef = admin.storage().bucket().file(fileName);
        await fileRef.save(buffer, { contentType: 'audio/ogg' });
        const [url] = await fileRef.getSignedUrl({
          action: 'read',
          expires: '03-01-2500'
        });
        mediaUrl = url;
      }
      // 3.4) Documento
      else if (msg.message.documentMessage) {
        mediaType = 'document';
        const { mimetype, fileName: origName } = msg.message.documentMessage;
        const buffer = await downloadMediaMessage(
          msg,
          'buffer',
          {},
          { logger: Pino() }
        );
        const ext = path.extname(origName) || '';
        const fileName = `docs/${phone}-${Date.now()}${ext}`;
        const fileRef = admin.storage().bucket().file(fileName);
        await fileRef.save(buffer, { contentType: mimetype });
        const [url] = await fileRef.getSignedUrl({
          action: 'read',
          expires: '03-01-2500'
        });
        mediaUrl = url;
      }
      // 3.5) Texto o extendedTextMessage
      else if (msg.message.conversation) {
        content = msg.message.conversation.trim();
        mediaType = 'text';
      } else if (msg.message.extendedTextMessage?.text) {
        content = msg.message.extendedTextMessage.text.trim();
        mediaType = 'text';
      } else {
        // Cualquier otro tipo, lo ignoramos por ahora
        continue;
      }
    } catch (err) {
      console.error('Error descargando/guardando media:', err);
      continue; // saltar este mensaje si falla descarga
    }

    // 4) BUSCAR O CREAR EL LEAD en Firestore
    let leadId = null;
    const q = await db
      .collection('leads')
      .where('telefono', '==', phone)
      .limit(1)
      .get();

    if (q.empty) {
      // 4.1) Si NO existe, leemos configuración para defaultTrigger
      const cfgSnap = await db.collection('config').doc('appConfig').get();
      const cfg = cfgSnap.exists ? cfgSnap.data() : {};

      // 4.2) Detectamos si el mensaje incluye "#webPro1490"
      let trigger;
      if (content.includes('#webPro1490')) {
        trigger = 'LeadWeb1490';
      } else {
        trigger = cfg.defaultTrigger || 'NuevoLead';
      }
      const nowIso = new Date().toISOString();

      // 4.3) Creamos el lead nuevo con la etiqueta según trigger
      const newLeadRef = await db.collection('leads').add({
        telefono: phone,
        nombre: msg.pushName || '',
        source: 'WhatsApp',
        fecha_creacion: new Date(),
        estado: 'nuevo',
        etiquetas: [trigger],
        secuenciasActivas: [
          {
            trigger,
            startTime: nowIso,
            index: 0
          }
        ],
        unreadCount: 0,
        lastMessageAt: new Date()
      });
      leadId = newLeadRef.id;
    } else {
      // 4.4) Si YA existe, obtenemos su ID y actualizamos etiquetas
      leadId = q.docs[0].id;

      // Leer de nuevo la configuración por si cambió en Firestore
      const cfgSnap2 = await db.collection('config').doc('appConfig').get();
      const cfg2 = cfgSnap2.exists ? cfgSnap2.data() : {};

      // 4.5) Volvemos a calcular el trigger en base a "#webPro1490"
      let trigger;
      if (content.includes('#webPro1490')) {
        trigger = 'LeadWeb1490';
      } else {
        trigger = cfg2.defaultTrigger || 'NuevoLead';
      }

      // 4.6) Actualizamos array de etiquetas sin duplicados
      await db
        .collection('leads')
        .doc(leadId)
        .update({
          etiquetas: admin.firestore.FieldValue.arrayUnion(trigger),
          lastMessageAt: new Date()
        });
    }

    // 5) GUARDAR el mensaje dentro de /leads/{leadId}/messages
    const msgData = {
      content,
      mediaType,
      mediaUrl,
      sender,
      timestamp: new Date()
    };
    await db
      .collection('leads')
      .doc(leadId)
      .collection('messages')
      .add(msgData);

    // 6) ACTUALIZAR el lead: incrementar unreadCount si envió el lead
    const updateData = { lastMessageAt: msgData.timestamp };
    if (sender === 'lead') {
      updateData.unreadCount = admin.firestore.FieldValue.increment(1);
    }
    await db.collection('leads').doc(leadId).update(updateData);
  }
});

    

    return sock;
  } catch (error) {
    console.error("Error al conectar con WhatsApp:", error);
    throw error;
  }
}

export async function sendMessageToLead(phone, messageContent) {
  try {
    if (!whatsappSock) throw new Error('No hay conexión activa con WhatsApp');
    // Normalizar E.164 sin '+'
    let num = String(phone).replace(/\D/g, '');
    if (num.length === 10) num = '52' + num;
    const jid = `${num}@s.whatsapp.net`;

    // Enviar mensaje
    await whatsappSock.sendMessage(jid, { text: messageContent });

    // Guardar en Firestore bajo sender 'business'
    const q = await db.collection('leads')
                     .where('telefono', '==', num)
                     .limit(1)
                     .get();
    if (!q.empty) {
      const leadId = q.docs[0].id;
      const outMsg = {
        content: messageContent,
        sender: 'business',
        timestamp: new Date()
      };
      await db.collection('leads')
              .doc(leadId)
              .collection('messages')
              .add(outMsg);
      await db.collection('leads')
              .doc(leadId)
              .update({ lastMessageAt: outMsg.timestamp });
    }

    return { success: true };
  } catch (error) {
    console.error("Error enviando mensaje de WhatsApp:", error);
    throw error;
  }
}

export function getLatestQR() {
  return latestQR;
}

export function getConnectionStatus() {
  return connectionStatus;
}

export function getWhatsAppSock() {
  return whatsappSock;
}

export function getSessionPhone() {
  return sessionPhone;
}

/**
 * Envía una nota de voz en M4A, la sube a Firebase Storage y la guarda en Firestore.
 * @param {string} phone    — número limpio (solo dígitos, con código de país).
 * @param {string} filePath — ruta al archivo .m4a en el servidor.
 */
export async function sendAudioMessage(phone, filePath) {
  const sock = getWhatsAppSock();
  if (!sock) throw new Error('Socket de WhatsApp no está conectado');

  const num = String(phone).replace(/\D/g, '');
  const jid = `${num}@s.whatsapp.net`;

  // 1) Leer y enviar por Baileys como audio/mp4
  const audioBuffer = fs.readFileSync(filePath);
  await sock.sendMessage(jid, {
    audio: audioBuffer,
    mimetype: 'audio/mp4',
    ptt: true,    // ← activa el modo nota de voz
  });

  // 2) Subir a Firebase Storage
  const bucket = admin.storage().bucket();
  const dest   = `audios/${num}-${Date.now()}.m4a`;
  const file   = bucket.file(dest);
  await file.save(audioBuffer, { contentType: 'audio/mp4' });
  const [mediaUrl] = await file.getSignedUrl({
    action: 'read',
    expires: '03-01-2500'
  });

  // 3) Guardar en Firestore
  const q = await db.collection('leads')
                    .where('telefono', '==', num)
                    .limit(1)
                    .get();
  if (!q.empty) {
    const leadId = q.docs[0].id;
    const msgData = {
      content: '',
      mediaType: 'audio',
      mediaUrl,
      sender: 'business',
      timestamp: new Date()
    };
    await db.collection('leads')
            .doc(leadId)
            .collection('messages')
            .add(msgData);
    await db.collection('leads')
            .doc(leadId)
            .update({ lastMessageAt: msgData.timestamp });
  }
}