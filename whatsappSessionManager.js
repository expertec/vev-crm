// whatsappSessionManager.js
//
// Gestor MULTI-TENANT de sesiones de WhatsApp (Baileys): una sesión por negocio.
// Aislado del whatsappService.js legacy (single-tenant) para no romper el flujo
// actual durante la transición.
//
// Modelo:
//   - Cada negocio (negocioId) tiene su propia carpeta de auth en disco:
//       {WA_SESSIONS_ROOT}/{negocioId}/   (por defecto /var/data/wa-sessions/{negocioId})
//   - Un registro en memoria: Map<negocioId, session>.
//   - El handler de mensajes entrantes es un closure que CONOCE su negocioId,
//     así el inbound se rutea al negocio correcto (se conecta vía setInboundHandler).
//
// Escala: ~decenas-100 sesiones por proceso (cada socket consume RAM + 1 WebSocket).
// Para más, hay que repartir negocios entre varios workers/instancias (sharding):
// este módulo ya está keyeado por negocioId para facilitarlo.

import {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
} from 'baileys';
import Pino from 'pino';
import fs from 'fs';
import path from 'path';

const SESSIONS_ROOT = process.env.WA_SESSIONS_ROOT || '/var/data/wa-sessions';

// Estados expuestos a la API/panel.
export const WA_SESSION_STATUS = Object.freeze({
  DISCONNECTED: 'disconnected',
  CONNECTING: 'connecting',
  QR: 'qr',            // QR disponible, esperando escaneo
  CONNECTED: 'connected',
  LOGGED_OUT: 'logged_out', // el dispositivo cerró sesión; requiere nuevo QR
});

// Registro en memoria: negocioId -> session.
const sessions = new Map();

// Hook de inbound. Fase 3 lo conecta al pipeline (leads por negocio).
// firma: async (negocioId, { messages, type, sock }) => void
let inboundHandler = null;
export function setInboundHandler(fn) {
  inboundHandler = typeof fn === 'function' ? fn : null;
}

const logger = Pino({ level: process.env.WA_LOG_LEVEL || 'warn' });

function sanitizeNegocioId(negocioId) {
  // Evita path traversal: solo permitimos ids "seguros" para nombre de carpeta.
  const id = String(negocioId || '').trim();
  if (!id || !/^[A-Za-z0-9_-]{1,128}$/.test(id)) {
    throw new Error(`negocioId inválido para sesión de WhatsApp: "${negocioId}"`);
  }
  return id;
}

function getAuthDir(negocioId) {
  return path.join(SESSIONS_ROOT, sanitizeNegocioId(negocioId));
}

function ensureRoot() {
  if (!fs.existsSync(SESSIONS_ROOT)) {
    fs.mkdirSync(SESSIONS_ROOT, { recursive: true });
  }
}

function getOrInitSession(negocioId) {
  const id = sanitizeNegocioId(negocioId);
  let session = sessions.get(id);
  if (!session) {
    session = {
      negocioId: id,
      sock: null,
      qr: null,
      qrAt: 0,
      status: WA_SESSION_STATUS.DISCONNECTED,
      phone: null,
      lastError: '',
      starting: false,
      reconnectTimer: null,
      updatedAt: Date.now(),
    };
    sessions.set(id, session);
  }
  return session;
}

function patchSession(session, patch) {
  Object.assign(session, patch, { updatedAt: Date.now() });
  return session;
}

/**
 * Conecta (o restaura) la sesión de WhatsApp de un negocio.
 * Idempotente: si ya está conectando/conectada, no abre otra.
 */
export async function connectSession(negocioId) {
  const id = sanitizeNegocioId(negocioId);
  const session = getOrInitSession(id);

  if (session.starting) return session;
  if (session.sock && session.status === WA_SESSION_STATUS.CONNECTED) return session;

  session.starting = true;
  patchSession(session, { status: WA_SESSION_STATUS.CONNECTING, lastError: '' });

  try {
    ensureRoot();
    const authDir = getAuthDir(id);
    if (!fs.existsSync(authDir)) fs.mkdirSync(authDir, { recursive: true });

    const { state, saveCreds } = await useMultiFileAuthState(authDir);
    if (state.creds?.me?.id) {
      patchSession(session, { phone: state.creds.me.id.split('@')[0] });
    }

    const { version } = await fetchLatestBaileysVersion();
    const sock = makeWASocket({
      auth: state,
      logger,
      printQRInTerminal: false, // el QR se expone por API; el panel lo renderiza
      version,
      browser: ['NegociosWeb CRM', 'Chrome', '1.0.0'],
      markOnlineOnConnect: false,
    });

    session.sock = sock;

    sock.ev.on('connection.update', (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        patchSession(session, { qr, qrAt: Date.now(), status: WA_SESSION_STATUS.QR });
      }

      if (connection === 'open') {
        patchSession(session, {
          status: WA_SESSION_STATUS.CONNECTED,
          qr: null,
          lastError: '',
          phone: sock.user?.id ? sock.user.id.split('@')[0] : session.phone,
        });
        console.log(`[WA-MT] ✅ Conectado negocio=${id} phone=${session.phone || '?'}`);
      }

      if (connection === 'close') {
        const reason = lastDisconnect?.error?.output?.statusCode;
        const loggedOut = reason === DisconnectReason.loggedOut;

        if (loggedOut) {
          // El dispositivo cerró sesión: limpiamos credenciales, requiere nuevo QR.
          patchSession(session, {
            status: WA_SESSION_STATUS.LOGGED_OUT,
            qr: null,
            phone: null,
            lastError: 'El dispositivo cerró sesión. Vuelve a escanear el QR.',
          });
          clearAuthDir(id);
          session.sock = null;
          console.log(`[WA-MT] 🚪 logged_out negocio=${id}`);
          return;
        }

        // Reconexión con backoff (evita timers duplicados).
        patchSession(session, { status: WA_SESSION_STATUS.DISCONNECTED });
        session.sock = null;
        if (!session.reconnectTimer) {
          const delay = Math.floor(Math.random() * 8000) + 5000;
          session.reconnectTimer = setTimeout(() => {
            session.reconnectTimer = null;
            connectSession(id).catch((e) =>
              console.error(`[WA-MT] reconexión negocio=${id} falló:`, e?.message)
            );
          }, delay);
          console.log(`[WA-MT] 🔁 negocio=${id} reconecta en ${delay}ms (reason=${reason})`);
        }
      }
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('messages.upsert', async (payload) => {
      if (!inboundHandler) return; // Fase 3 conecta el pipeline de leads por negocio
      try {
        await inboundHandler(id, { ...payload, sock });
      } catch (e) {
        console.error(`[WA-MT] inboundHandler negocio=${id} error:`, e?.message);
      }
    });

    return session;
  } catch (error) {
    patchSession(session, {
      status: WA_SESSION_STATUS.DISCONNECTED,
      lastError: error?.message || 'No se pudo iniciar la sesión de WhatsApp.',
    });
    console.error(`[WA-MT] connectSession negocio=${id} error:`, error?.message);
    throw error;
  } finally {
    session.starting = false;
  }
}

function clearAuthDir(negocioId) {
  const authDir = getAuthDir(negocioId);
  if (fs.existsSync(authDir)) {
    for (const f of fs.readdirSync(authDir)) {
      fs.rmSync(path.join(authDir, f), { force: true, recursive: true });
    }
  }
}

/** Estado público de la sesión (sin objetos internos). Seguro para el panel. */
export function getSessionState(negocioId) {
  const id = sanitizeNegocioId(negocioId);
  const s = sessions.get(id);
  if (!s) {
    return { negocioId: id, status: WA_SESSION_STATUS.DISCONNECTED, qr: null, phone: null, connected: false };
  }
  return {
    negocioId: id,
    status: s.status,
    qr: s.status === WA_SESSION_STATUS.QR ? s.qr : null,
    phone: s.phone,
    connected: s.status === WA_SESSION_STATUS.CONNECTED,
    lastError: s.lastError || '',
    updatedAt: s.updatedAt,
  };
}

export function getSock(negocioId) {
  const s = sessions.get(sanitizeNegocioId(negocioId));
  return s?.sock || null;
}

export function listSessions() {
  return Array.from(sessions.values()).map((s) => getSessionState(s.negocioId));
}

function toJid(phone) {
  const raw = String(phone || '').trim();
  if (raw.includes('@')) return raw;
  const digits = raw.replace(/\D/g, '');
  return `${digits}@s.whatsapp.net`;
}

/** Envía un mensaje de texto desde la sesión del negocio. */
export async function sendText(negocioId, phone, text) {
  const id = sanitizeNegocioId(negocioId);
  const sock = getSock(id);
  if (!sock) {
    const err = new Error('La sesión de WhatsApp de este negocio no está conectada.');
    err.code = 'WA_NOT_CONNECTED';
    throw err;
  }
  return sock.sendMessage(toJid(phone), { text: String(text ?? '') });
}

/**
 * Envía un contenido Baileys arbitrario (imagen, documento, texto…) desde la
 * sesión del negocio. `content` es el objeto que espera sock.sendMessage:
 *   texto     -> { text }
 *   imagen    -> { image: <Buffer>, caption }
 *   documento -> { document: <Buffer>, mimetype, fileName }
 */
export async function sendMedia(negocioId, phone, content) {
  const id = sanitizeNegocioId(negocioId);
  const sock = getSock(id);
  if (!sock) {
    const err = new Error('La sesión de WhatsApp de este negocio no está conectada.');
    err.code = 'WA_NOT_CONNECTED';
    throw err;
  }
  return sock.sendMessage(toJid(phone), content);
}

/** Cierra la sesión y borra credenciales (requiere nuevo QR para reconectar). */
export async function logoutSession(negocioId) {
  const id = sanitizeNegocioId(negocioId);
  const session = sessions.get(id);
  if (session?.reconnectTimer) {
    clearTimeout(session.reconnectTimer);
    session.reconnectTimer = null;
  }
  try {
    if (session?.sock) await session.sock.logout();
  } catch (_e) {
    // ignoramos errores de logout (puede estar ya caído)
  }
  clearAuthDir(id);
  if (session) {
    patchSession(session, {
      sock: null,
      qr: null,
      phone: null,
      status: WA_SESSION_STATUS.LOGGED_OUT,
    });
  }
  return getSessionState(id);
}

/**
 * Restaura todas las sesiones persistidas en disco al arrancar el server.
 * Cada subcarpeta con creds.json se reconecta automáticamente.
 */
export async function restoreAllSessions() {
  ensureRoot();
  let restored = 0;
  for (const entry of fs.readdirSync(SESSIONS_ROOT, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const negocioId = entry.name;
    const credsPath = path.join(SESSIONS_ROOT, negocioId, 'creds.json');
    if (!fs.existsSync(credsPath)) continue;
    try {
      sanitizeNegocioId(negocioId);
    } catch {
      console.warn(`[WA-MT] carpeta de sesión ignorada (id inválido): ${negocioId}`);
      continue;
    }
    restored++;
    // No bloqueamos el arranque: reconectamos en paralelo, con un pequeño escalonado
    // para no saturar al levantar muchas sesiones a la vez.
    setTimeout(() => {
      connectSession(negocioId).catch((e) =>
        console.error(`[WA-MT] restore negocio=${negocioId} falló:`, e?.message)
      );
    }, restored * 400);
  }
  console.log(`[WA-MT] restaurando ${restored} sesión(es) de WhatsApp desde ${SESSIONS_ROOT}`);
  return restored;
}
