// server.js
import express from 'express';
import cors from 'cors';
import bodyParser from 'body-parser';
import dotenv from 'dotenv';
import cron from 'node-cron';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg';
import dayjs from 'dayjs';
import axios from 'axios';

dotenv.config();

// ================ FFmpeg ================
ffmpeg.setFfmpegPath(ffmpegInstaller.path);

// ================ Firebase / WhatsApp ================
import { admin, db } from './firebaseAdmin.js';
import {
  connectToWhatsApp,
  getLatestQR,
  getConnectionStatus,
  sendMessageToLead,
  getSessionPhone,
  sendAudioMessage,
} from './whatsappService.js';

// ================ Secuencias / Scheduler (web) ================
import {
  processSequences,
  generateSiteSchemas,
  archivarNegociosAntiguos,
  enviarSitiosPendientes,
} from './scheduler.js';

// (opcional) si tu queue exporta estas funciones, las usamos para cancelar/encolar
let cancelSequences = null;
let scheduleSequenceForLead = null;
try {
  const q = await import('./queue.js');
  cancelSequences = q.cancelSequences || null;
  scheduleSequenceForLead = q.scheduleSequenceForLead || null;
} catch { /* continúa sin romper */ }

// ================ OpenAI compat (para mensaje empático) ================
import OpenAIImport from 'openai';
const OpenAICtor = OpenAIImport?.OpenAI || OpenAIImport;
function assertOpenAIKey() {
  if (!process.env.OPENAI_API_KEY) {
    throw new Error('Falta la variable de entorno OPENAI_API_KEY');
  }
}
async function getOpenAI() {
  assertOpenAIKey();
  try {
    const client = new OpenAICtor({ apiKey: process.env.OPENAI_API_KEY });
    const hasChatCompletions = !!client?.chat?.completions?.create;
    const hasResponses = !!client?.responses?.create;
    if (hasChatCompletions) return { client, mode: 'v4-chat' };
    if (hasResponses) return { client, mode: 'v4-resp' };
  } catch {}
  const { Configuration, OpenAIApi } = await import('openai');
  const configuration = new Configuration({ apiKey: process.env.OPENAI_API_KEY });
  const client = new OpenAIApi(configuration);
  return { client, mode: 'v3' };
}
function extractText(resp, mode) {
  try {
    if (mode === 'v4-chat') return resp?.choices?.[0]?.message?.content?.trim() || '';
    if (mode === 'v4-resp') {
      if (resp?.output_text) return String(resp.output_text).trim();
      const parts = [];
      const content = resp?.output || resp?.content || [];
      for (const item of content) {
        if (typeof item === 'string') parts.push(item);
        else if (Array.isArray(item?.content)) {
          for (const c of item.content) if (c?.text?.value) parts.push(c.text.value);
        } else if (item?.text?.value) parts.push(item.text.value);
      }
      return parts.join(' ').trim();
    }
    return resp?.data?.choices?.[0]?.message?.content?.trim() || '';
  } catch { return ''; }
}
async function chatCompletionCompat({ model, messages, max_tokens = 120, temperature = 0.35 }) {
  const { client, mode } = await getOpenAI();
  if (mode === 'v4-chat') {
    const resp = await client.chat.completions.create({ model, messages, max_tokens, temperature });
    return extractText(resp, mode);
  }
  if (mode === 'v4-resp') {
    const input = messages.map(m => `${m.role.toUpperCase()}: ${m.content}`).join('\n\n');
    const resp = await client.responses.create({ model, input });
    return extractText(resp, mode);
  }
  const resp = await client.createChatCompletion({ model, messages, max_tokens, temperature });
  return extractText(resp, 'v3');
}

// ================ Teléfonos (E.164 → JID) ================
import { parsePhoneNumberFromString } from 'libphonenumber-js';
function toE164(num, defaultCountry = 'MX') {
  const raw = String(num || '').replace(/\D/g, '');
  const p = parsePhoneNumberFromString(raw, defaultCountry);
  if (p && p.isValid()) return p.number; // +521...
  if (/^\d{10}$/.test(raw)) return `+52${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('521')) return `+${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('52'))  return `+${raw}`;
  return `+${raw}`;
}

// ================ App base ================
const app = express();
const port = process.env.PORT || 3001;
const upload = multer({ dest: path.resolve('./uploads') });

app.use(cors());
app.use(bodyParser.json());

// ---------------- WhatsApp status / número ----------------
app.get('/api/whatsapp/status', (_req, res) => {
  res.json({ status: getConnectionStatus(), qr: getLatestQR() });
});
app.get('/api/whatsapp/number', (_req, res) => {
  const phone = getSessionPhone();
  if (phone) return res.json({ phone });
  return res.status(503).json({ error: 'WhatsApp no conectado' });
});

// ---------------- Enviar mensaje manual ----------------
app.post('/api/whatsapp/send-message', async (req, res) => {
  const { leadId, message } = req.body;
  if (!leadId || !message) return res.status(400).json({ error: 'Faltan leadId o message' });

  try {
    const leadDoc = await db.collection('leads').doc(leadId).get();
    if (!leadDoc.exists) return res.status(404).json({ error: 'Lead no encontrado' });
    const { telefono } = leadDoc.data() || {};
    if (!telefono) return res.status(400).json({ error: 'Lead sin teléfono' });
    const result = await sendMessageToLead(telefono, message);
    return res.json(result);
  } catch (error) {
    console.error('Error enviando WhatsApp:', error);
    return res.status(500).json({ error: error.message });
  }
});

// ---------------- Enviar audio (convierte a M4A) ----------------
app.post('/api/whatsapp/send-audio', upload.single('audio'), async (req, res) => {
  const { phone } = req.body;
  const uploadPath = req.file.path;
  const m4aPath = `${uploadPath}.m4a`;

  try {
    await new Promise((resolve, reject) => {
      ffmpeg(uploadPath)
        .outputOptions(['-c:a aac', '-vn'])
        .toFormat('mp4')
        .save(m4aPath)
        .on('end', resolve)
        .on('error', reject);
    });

    await sendAudioMessage(phone, m4aPath);
    fs.unlinkSync(uploadPath);
    fs.unlinkSync(m4aPath);
    return res.json({ success: true });
  } catch (error) {
    console.error('Error enviando audio:', error);
    try { fs.unlinkSync(uploadPath); } catch {}
    try { fs.unlinkSync(m4aPath); } catch {}
    return res.status(500).json({ success: false, error: error.message });
  }
});

// ---------------- Crear usuario + bienvenida WA ----------------
app.post('/api/crear-usuario', async (req, res) => {
  const { email, negocioId } = req.body;
  if (!email || !negocioId) return res.status(400).json({ error: 'Faltan email o negocioId' });

  try {
    const tempPassword = Math.random().toString(36).slice(-8);
    let userRecord, isNewUser = false;
    try {
      userRecord = await admin.auth().getUserByEmail(email);
    } catch {
      userRecord = await admin.auth().createUser({ email, password: tempPassword });
      isNewUser = true;
    }

    await db.collection('Negocios').doc(negocioId).update({
      ownerUID: userRecord.uid,
      ownerEmail: email,
    });

    const negocioDoc = await db.collection('Negocios').doc(negocioId).get();
    const negocio = negocioDoc.data() || {};
    let telefono = toE164(negocio?.leadPhone); // robusto
    const urlAcceso = 'https://negociosweb.mx/login';

    let mensaje = `¡Bienvenido a tu panel de administración de tu página web! 👋

🔗 Accede aquí: ${urlAcceso}
📧 Usuario: ${email}
`;
    if (isNewUser) mensaje += `🔑 Contraseña temporal: ${tempPassword}\n`;
    else mensaje += `🔄 Si no recuerdas tu contraseña, usa "¿Olvidaste tu contraseña?"\n`;
    let fechaCorte = '-';
    const d = negocio.planRenewalDate;
    if (d?.toDate) fechaCorte = dayjs(d.toDate()).format('DD/MM/YYYY');
    else if (d instanceof Date) fechaCorte = dayjs(d).format('DD/MM/YYYY');
    else if (typeof d === 'string' || typeof d === 'number') fechaCorte = dayjs(d).format('DD/MM/YYYY');
    mensaje += `\n🗓️ Tu plan termina el día: ${fechaCorte}\n\nPor seguridad, cambia tu contraseña después de ingresar.\n`;

    if (telefono && telefono.length >= 12) {
      try { await sendMessageToLead(telefono, mensaje); }
      catch (waError) { console.error('[CREAR USUARIO] Error WA:', waError); }
    }

    if (!isNewUser) await admin.auth().generatePasswordResetLink(email);
    return res.json({ success: true, uid: userRecord.uid, email });
  } catch (err) {
    console.error('Error creando usuario:', err);
    return res.status(500).json({ error: err.message });
  }
});

// ---------------- Marcar como leídos ----------------
app.post('/api/whatsapp/mark-read', async (req, res) => {
  const { leadId } = req.body;
  if (!leadId) return res.status(400).json({ error: 'Falta leadId' });
  try {
    await db.collection('leads').doc(leadId).update({ unreadCount: 0 });
    return res.json({ success: true });
  } catch (err) {
    console.error('Error mark-read:', err);
    return res.status(500).json({ error: err.message });
  }
});

// ============== NUEVO: after-form (web) ==============
app.post('/api/web/after-form', async (req, res) => {
  try {
    const { leadId, summary } = req.body; // summary: { companyName, email, businessType, primaryColor }
    if (!leadId || !summary) return res.status(400).json({ error: 'leadId y summary son requeridos' });

    // 1) Lead y teléfono
    const leadSnap = await db.collection('leads').doc(leadId).get();
    if (!leadSnap.exists) return res.status(404).json({ error: 'Lead no encontrado' });

    const lead = leadSnap.data() || {};
    const e164 = toE164(lead.telefono);
    if (!e164) return res.status(400).json({ error: 'Lead sin teléfono' });

    // 2) Cancelar secuencia inicial (si tu motor lo soporta)
    try {
      if (cancelSequences) {
        await cancelSequences(leadId, ['NuevoLeadWeb']);
        await db.collection('leads').doc(leadId).set({ nuevoLeadWebCancelled: true }, { merge: true });
      }
    } catch (e) {
      console.warn('cancelSequences no disponible o falló:', e?.message);
    }

    // 3) Crear/actualizar doc en Negocios para pipeline web
    const negocioRef = await db.collection('Negocios').add({
      leadId,
      leadPhone: e164,
      companyInfo: summary.companyName || '',
      contactEmail: summary.email || '',
      businessSector: summary.businessType || '',
      palette: { primary: summary.primaryColor || '#1890ff' },
      status: 'Sin procesar',
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    // 4) Mensaje empático diferido (60–120s)
    const first = String(lead.nombre || '').split(/\s+/)[0] || '';
    const reglas = `
Escribe un solo mensaje breve de WhatsApp, cálido y profesional.
Contexto: vamos a crear una demo de sitio web exprés.
Requisitos:
- Saluda por el primer nombre si existe.
- 1 sola frase natural (sin ":").
- Menciona que trabajarás con lo que compartió (empresa/tipo/color).
- Sin comillas, sin emojis, 18–35 palabras.
No incluyas cierre fijo; lo agregaré yo.
`.trim();
    const contexto = `
Datos:
- Nombre: ${first || '(no)'}
- Empresa: ${summary.companyName || '(no)'}
- Tipo: ${summary.businessType || '(no)'}
- Color: ${summary.primaryColor || '(no)'}
`.trim();

    let principal = '';
    try {
      const text = await chatCompletionCompat({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: 'Eres conciso, cálido y profesional.' },
          { role: 'user', content: `${reglas}\n\n${contexto}\n\nRedacta SOLO la frase principal.` },
        ],
        max_tokens: 120,
        temperature: 0.35,
      });
      principal = String(text || '').replace(/[“”"']/g, '').replace(/\s+/g, ' ').trim();
      principal = principal.split(/(?<=[.!?])\s+/)[0] || principal;
    } catch {
      principal = 'Gracias por la información; me gusta el enfoque que buscas para tu sitio.';
    }
    const CIERRE = 'Voy a poner todo de mí para construir tu demo; enseguida te la comparto.';
    const mensaje = `${first ? first + ', ' : ''}${principal} ${CIERRE}`;

    const delayMs = 60_000 + Math.floor(Math.random() * 60_000);
    setTimeout(() => {
      sendMessageToLead(e164, mensaje).catch(err => console.error('Empatía web diferida error:', err));
    }, delayMs);

    return res.json({ ok: true, negocioId: negocioRef.id });
  } catch (e) {
    console.error('/api/web/after-form error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});

// ============== Arranque servidor + WA ==============
app.listen(port, () => {
  console.log(`Servidor corriendo en el puerto ${port}`);
  connectToWhatsApp().catch(err => console.error('Error al conectar WhatsApp en startup:', err));
});

// ============== CRON JOBS ==============
// Secuencias (cada 30s)
cron.schedule('*/30 * * * * *', () => {
  console.log('⏱️ processSequences:', new Date().toISOString());
  processSequences().catch(err => console.error('Error en processSequences:', err));
});

// Generar schemas (cada 1 min)
cron.schedule('* * * * *', () => {
  console.log('⏱️ generateSiteSchemas:', new Date().toISOString());
  generateSiteSchemas().catch(err => console.error('Error en generateSiteSchemas:', err));
});

// Enviar sitios procesados (cada 5 min)
cron.schedule('*/5 * * * *', () => {
  console.log('⏱️ enviarSitiosPendientes:', new Date().toISOString());
  enviarSitiosPendientes().catch(err => console.error('Error en enviarSitiosPendientes:', err));
});

// Archivar >24h sin plan (cada hora)
cron.schedule('0 * * * *', () => {
  console.log('⏱️ archivarNegociosAntiguos:', new Date().toISOString());
  archivarNegociosAntiguos().catch(err => console.error('Error en archivarNegociosAntiguos:', err));
});
