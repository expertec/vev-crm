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
const localAuthFolder = '/var/data';

// Asegúrate de que en firebaseAdmin.js ya hayas hecho admin.initializeApp(...)
const bucket = admin.storage().bucket();

export async function connectToWhatsApp() {
  try {
    console.log("Verificando carpeta de autenticación en:", localAuthFolder);
    if (!fs.existsSync(localAuthFolder)) {
      fs.mkdirSync(localAuthFolder, { recursive: true });
      console.log("Carpeta creada:", localAuthFolder);
    } else {
      console.log("Carpeta de autenticación existente:", localAuthFolder);
    }

    const { state, saveCreds } = await useMultiFileAuthState(localAuthFolder);
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
      auth: state,
      logger: Pino({ level: 'info' }),
      printQRInTerminal: true,
      version,
    });

    whatsappSock = sock;

    sock.ev.on('connection.update', (update) => {
      const { connection, lastDisconnect, qr } = update;
      if (qr) {
        latestQR = qr;
        connectionStatus = "QR disponible. Escanéalo.";
        QRCode.generate(qr, { small: true });
        console.log("QR generado, escanéalo.");
      }
      if (connection === 'open') {
        connectionStatus = "Conectado";
        latestQR = null;
        console.log("Conexión exitosa con WhatsApp!");
      }
      if (connection === 'close') {
        const reason = lastDisconnect?.error?.output?.statusCode;
        connectionStatus = "Desconectado";
        console.log("Conexión cerrada. Razón:", reason);
        if (reason === DisconnectReason.loggedOut) {
          // Limpiar credenciales
          if (fs.existsSync(localAuthFolder)) {
            fs.readdirSync(localAuthFolder).forEach(file =>
              fs.rmSync(path.join(localAuthFolder, file), { recursive: true, force: true })
            );
            console.log("Estado de autenticación limpiado.");
          }
          connectToWhatsApp();
        } else {
          connectToWhatsApp();
        }
      }
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('messages.upsert', async (m) => {
      console.log("Nuevo mensaje upsert:", JSON.stringify(m, null, 2));
      for (const msg of m.messages) {
        if (!msg.key) continue;
        const jid = msg.key.remoteJid;
        if (!jid || jid.endsWith('@g.us')) continue; // ignorar grupos

        try {
          const leadRef = db.collection('leads').doc(jid);
          const docSnap = await leadRef.get();

          // Obtener configuración global
          const configSnap = await db.collection('config').doc('appConfig').get();
          const cfg = configSnap.exists
            ? configSnap.data()
            : { autoSaveLeads: true, defaultTrigger: 'NuevoLead' };

          // Crear lead si no existe y es mensaje entrante
          if (!docSnap.exists && !msg.key.fromMe) {
            const telefono = jid.split('@')[0];
            const nombre = msg.pushName || "Sin nombre";

            if (cfg.autoSaveLeads) {
              const secuenciasActivas = [{
                trigger: cfg.defaultTrigger || 'NuevoLead',
                startTime: new Date().toISOString(),
                index: 0
              }];
              await leadRef.set({
                nombre,
                telefono,
                fecha_creacion: new Date(),
                estado: 'nuevo',
                source: 'WhatsApp',
                etiquetas: [cfg.defaultTrigger || 'NuevoLead'],
                secuenciasActivas
              });
              console.log("Nuevo lead guardado:", telefono);
            } else {
              console.log("AutoSaveLeads deshabilitado, no se guarda el lead:", telefono);
            }
          }

          let mediaType = null;
          let mediaUrl = null;
          let content = '';

          if (msg.message.imageMessage) {
            mediaType = 'image';
            const buffer = await downloadMediaMessage(msg, 'buffer', {}, { logger: Pino() });
            const fileName = `images/${jid}-${Date.now()}.jpg`;
            const file = bucket.file(fileName);
            await file.save(buffer, { contentType: 'image/jpeg' });
            const [url] = await file.getSignedUrl({ action: 'read', expires: '03-01-2500' });
            mediaUrl = url;
            console.log("Imagen guardada:", fileName);
          } else if (msg.message.audioMessage) {
            mediaType = 'audio';
            const buffer = await downloadMediaMessage(msg, 'buffer', {}, { logger: Pino() });
            const fileName = `audios/${jid}-${Date.now()}.ogg`;
            const file = bucket.file(fileName);
            await file.save(buffer, { contentType: 'audio/ogg' });
            const [url] = await file.getSignedUrl({ action: 'read', expires: '03-01-2500' });
            mediaUrl = url;
            console.log("Audio guardado:", fileName);
          } else {
            content = msg.message.conversation || msg.message.extendedTextMessage?.text || '';
          }

          const newMessage = {
            content,
            mediaType,
            mediaUrl,
            sender: msg.key.fromMe ? 'business' : 'lead',
            timestamp: new Date(),
          };

          // Guardar en subcolección y actualizar lastMessageAt
          await leadRef.collection('messages').add(newMessage);
          await leadRef.update({ lastMessageAt: newMessage.timestamp });

          console.log("Mensaje guardado en Firebase:", newMessage);
        } catch (err) {
          console.error("Error procesando mensaje:", err);
        }
      }
    });

    console.log("WhatsApp socket inicializado correctamente.");
    return sock;
  } catch (error) {
    console.error("Error al conectar con WhatsApp:", error);
    throw error;
  }
}

export async function sendMessageToLead(phone, messageContent) {
  try {
    const sock = whatsappSock;
    if (!sock) throw new Error('No hay conexión activa con WhatsApp');

    let number = phone;
    if (!number.startsWith('521')) number = `521${number}`;
    const jid = `${number}@s.whatsapp.net`;

    await sock.sendMessage(jid, { text: messageContent });
    console.log(`Mensaje enviado a ${jid}: ${messageContent}`);
    return { success: true, message: 'Mensaje enviado a WhatsApp' };
  } catch (error) {
    console.error("Error enviando mensaje de WhatsApp:", error);
    throw new Error(`Error enviando mensaje: ${error.message}`);
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
