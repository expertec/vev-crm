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
import slugify from 'slugify';

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

// (opcional) queue helpers
let cancelSequences = null;
let scheduleSequenceForLead = null;
try {
  const q = await import('./queue.js');
  cancelSequences = q.cancelSequences || null;
  scheduleSequenceForLead = q.scheduleSequenceForLead || null;
} catch { /* continúa sin romper */ }

// ================ OpenAI compat (para mensajes GPT) ================
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
    const hasResponses        = !!client?.responses?.create;
    if (hasChatCompletions) return { client, mode: 'v4-chat' };
    if (hasResponses)       return { client, mode: 'v4-resp'  };
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
async function chatCompletionCompat({ model, messages, max_tokens = 300, temperature = 0.55 }) {
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

// ================ Teléfonos helpers ================
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
function e164ToLeadId(e164) {
  const digits = String(e164 || '').replace(/\D/g, '');
  return `${digits}@s.whatsapp.net`;
}
function firstName(n = '') {
  return String(n).trim().split(/\s+/)[0] || '';
}

// ================== Personalización por giro (helpers ligeros) ==================
const GIRO_ALIAS = {
  restaurantes: ['restaurante', 'cafetería', 'bar'],
  tiendaretail: ['tienda física', 'retail'],
  ecommerce: ['ecommerce', 'tienda online'],
  saludbienestar: ['salud y bienestar', 'wellness'],
  belleza: ['belleza', 'estética', 'cuidado personal'],
  serviciosprofesionales: ['servicios profesionales', 'consultoría'],
  educacioncapacitacion: ['educación', 'capacitaciones', 'cursos'],
  artecultura: ['arte', 'cultura', 'entretenimiento'],
  hosteleria: ['hotelería', 'turismo', 'hospedaje'],
  salonpeluqueria: ['salón de belleza', 'barbería'],
  fitnessdeporte: ['fitness', 'gimnasio', 'yoga', 'deportes'],
  hogarjardin: ['hogar', 'jardinería'],
  mascotas: ['mascotas', 'veterinaria'],
  construccion: ['construcción', 'remodelación'],
  medicina: ['medicina', 'clínica'],
  finanzas: ['finanzas', 'banca'],
  marketing: ['marketing', 'diseño', 'publicidad'],
  tecnologia: ['tecnología', 'software', 'SaaS'],
  transporte: ['transporte', 'logística'],
  automotriz: ['automotriz', 'taller'],
  legal: ['servicios legales', 'despacho'],
  agricultura: ['agricultura', 'ganadería'],
  inmobiliario: ['bienes raíces', 'inmobiliario'],
  eventos: ['eventos', 'banquetes'],
  comunicaciones: ['comunicaciones', 'medios'],
  industria: ['industria', 'manufactura'],
  otros: ['negocio']
};
function humanizeGiro(code = '') {
  const c = String(code || '').toLowerCase();
  if (GIRO_ALIAS[c]) return GIRO_ALIAS[c][0];
  return c.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/[_-]+/g, ' ').trim() || 'negocio';
}
function pickOpportunityTriplet(giroHumano = '') {
  const base = giroHumano.toLowerCase();
  const common = [
    'CTA claro arriba del pliegue',
    'Prueba social visible (reseñas/testimonios)',
    'WhatsApp/botón sticky con texto pensado para convertir',
  ];
  if (/(restaurante|cafeter|bar)/.test(base)) {
    return [
      'Menú actualizado con fotos y precios',
      'Reserva/ordenar en 1 clic por WhatsApp',
      'Google Perfil optimizado con horarios y ubicación',
    ];
  }
  if (/(tienda|retail|ecommerce)/.test(base)) {
    return [
      'Catálogo con categorías y filtros claros',
      'Checkout o contacto rápido por WhatsApp',
      'Sellos de confianza y políticas visibles',
    ];
  }
  if (/(servicio|consultor|profesional|legal|médic|clínic)/.test(base)) {
    return [
      'Agenda/consultas en 1 clic por WhatsApp',
      'Casos de éxito y testimonios en home',
      'Sección de servicios con beneficios y precios guía',
    ];
  }
  if (/(belleza|salón|barber|estética)/.test(base)) {
    return [
      'Portafolio antes/después en galería',
      'Reservaciones rápidas por WhatsApp',
      'Mapa y horarios visibles en la home',
    ];
  }
  return common;
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
    let telefono = toE164(negocio?.leadPhone);
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

// ============== after-form (web) ==============
// Acepta { leadId, leadPhone, summary, negocioId? }
app.post('/api/web/after-form', async (req, res) => {
  try {
    const { leadId, leadPhone, summary, negocioId } = req.body || {};
    if (!leadId && !leadPhone) return res.status(400).json({ error: 'Faltan leadId o leadPhone' });
    if (!summary)              return res.status(400).json({ error: 'Falta summary' });

    // 1) Resolver e164 y leadId
    const e164 = toE164(leadPhone || (leadId || '').split('@')[0]);
    const finalLeadId = leadId || e164ToLeadId(e164);
    const leadPhoneDigits = e164.replace('+', '');

    // 2) Verificar/crear lead
    const leadRef  = db.collection('leads').doc(finalLeadId);
    const leadSnap = await leadRef.get();
    if (!leadSnap.exists) {
      await leadRef.set({
        telefono: leadPhoneDigits,
        nombre: '',
        source: 'Web',
        fecha_creacion: new Date(),
        estado: 'nuevo',
        etiquetas: ['FormularioCompletado'],
        unreadCount: 0,
        lastMessageAt: new Date(),
      }, { merge: true });
    }
    const leadData = (await leadRef.get()).data() || {};

    // 3) Guardar brief en el lead
    await leadRef.set({
      briefWeb: summary || {},
      etiquetas: admin.firestore.FieldValue.arrayUnion('FormularioCompletado'),
      lastMessageAt: new Date(),
    }, { merge: true });

    // 4) Crear/actualizar Negocios — BLOQUEA duplicado por WhatsApp
    let negocioDocId = negocioId;
    if (!negocioDocId) {
      const existSnap = await db.collection('Negocios')
        .where('leadPhone', '==', leadPhoneDigits)
        .limit(1)
        .get();

      if (!existSnap.empty) {
        const exist = existSnap.docs[0];
        const existData = exist.data() || {};
        return res.status(409).json({
          error: 'Ya existe un negocio con ese WhatsApp.',
          negocioId: exist.id,
          slug: existData.slug || existData?.schema?.slug || ''
        });
      }

      const ref = await db.collection('Negocios').add({
        leadId: finalLeadId,
        leadPhone: leadPhoneDigits,
        status: 'Sin procesar',
        companyInfo:     summary.companyName || '',
        businessSector:  summary.businessType || '',
        palette:         summary.palette || (summary.primaryColor ? [summary.primaryColor] : []),
        keyItems:        summary.keyItems || [],
        contactWhatsapp: summary.contactWhatsapp || '',
        contactEmail:    summary.email || '',
        socialFacebook:  summary.socialFacebook || '',
        socialInstagram: summary.socialInstagram || '',
        logoURL:         summary.logoURL || '',
        slug:            summary.slug || '',
        createdAt:       new Date()
      });
      negocioDocId = ref.id;
    }

    // 5) Transición de secuencias: cancelar captación y activar WebEnviada
    try {
      if (cancelSequences) {
        await cancelSequences(finalLeadId, ['NuevoLead', 'NuevoLeadWeb', 'LeadWeb']);
        await leadRef.set({ nuevoLeadWebCancelled: true }, { merge: true });
      }
      // ⚠️ Ya no intentamos FORM_SEQUENCE_ID (FormularioWeb). Activamos WebEnviada directamente.
      if (scheduleSequenceForLead) {
        await scheduleSequenceForLead(finalLeadId, 'WebEnviada', new Date());
      }
    } catch (e) {
      console.warn('[after-form] transición de secuencias falló:', e?.message);
    }

    // 6) Empatía personalizada (un solo mensaje inmediato, iniciando con “tres áreas de oportunidad”)
    const nombreCorto = firstName(leadData?.nombre || summary?.contactName || '');
    const giroHumano  = humanizeGiro(summary?.businessType || summary?.businessSector || '');
    const [op1, op2, op3] = pickOpportunityTriplet(giroHumano);

    const empatiaMsg =
      `${nombreCorto ? nombreCorto + ', ' : ''}ya recibí tu formulario 🙌\n` +
      `Te dejo **tres áreas de oportunidad** para sacarle más provecho a tu web:\n` +
      `1) **${op1}**\n` +
      `2) **${op2}**\n` +
      `3) **${op3}**\n` +
      `Si te late, las implemento en tu demo y te la envío enseguida. ¿Le damos?`;

    try { await sendMessageToLead(leadPhoneDigits, empatiaMsg); } catch (e) {
      console.warn('[after-form] empatía WA error:', e?.message);
    }

    // 7) Marcar estado/etiqueta
    await leadRef.set({
      etapa: 'form_submitted',
      etiquetas: admin.firestore.FieldValue.arrayUnion('FormOK')
    }, { merge: true });

    return res.json({ ok: true, negocioId: negocioDocId });
  } catch (e) {
    console.error('/api/web/after-form error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});

// ============== sample-create (nuevo, para el formulario turbo) ==============
// Acepta { leadPhone, summary: { companyName, businessStory, slug } }
app.post('/api/web/sample-create', async (req, res) => {
  try {
    const { leadPhone, summary } = req.body || {};
    if (!leadPhone) return res.status(400).json({ error: 'Falta leadPhone' });
    if (!summary?.companyName || !summary?.businessStory || !summary?.slug) {
      return res.status(400).json({ error: 'Faltan companyName, businessStory o slug' });
    }

    // Normaliza e164 y leadId
    const e164 = toE164(leadPhone || '');
    const finalLeadId = e164ToLeadId(e164);
    const leadPhoneDigits = e164.replace('+', '');

    // 🔒 Duplicado por WhatsApp
    const existSnap = await db.collection('Negocios')
      .where('leadPhone', '==', leadPhoneDigits)
      .limit(1)
      .get();

    if (!existSnap.empty) {
      const exist = existSnap.docs[0];
      const existData = exist.data() || {};
      return res.status(409).json({
        error: 'Ya existe un negocio con ese WhatsApp.',
        negocioId: exist.id,
        slug: existData.slug || existData?.schema?.slug || ''
      });
    }

    // Asegura slug único en servidor
    const finalSlug = await ensureUniqueSlug(summary.slug || summary.companyName);

    // Crea/asegura el lead
    const leadRef = db.collection('leads').doc(finalLeadId);
    const leadSnap = await leadRef.get();
    if (!leadSnap.exists) {
      await leadRef.set({
        telefono: leadPhoneDigits,
        nombre: '',
        source: 'WebTurbo',
        fecha_creacion: new Date(),
        estado: 'nuevo',
        etiquetas: ['FormularioTurbo'],
        unreadCount: 0,
        lastMessageAt: new Date(),
      }, { merge: true });
    }

    // Crea el Negocio
    const ref = await db.collection('Negocios').add({
      leadId: finalLeadId,
      leadPhone: leadPhoneDigits,
      status: 'Sin procesar',
      companyInfo:     summary.companyName,
      businessSector:  '',
      businessStory:   summary.businessStory,
      palette:         [],
      keyItems:        [],
      contactWhatsapp: '',
      contactEmail:    '',
      socialFacebook:  '',
      socialInstagram: '',
      logoURL:         '',
      slug:            finalSlug,
      createdAt:       new Date()
    });

    // (Opcional) guardar brief
    await leadRef.set({
      briefWeb: {
        companyName: summary.companyName,
        businessStory: summary.businessStory,
        slug: finalSlug,
        turbo: true
      },
      etiquetas: admin.firestore.FieldValue.arrayUnion('FormularioTurbo'),
      lastMessageAt: new Date(),
    }, { merge: true });

    return res.json({ ok: true, negocioId: ref.id, slug: finalSlug });
  } catch (e) {
    console.error('/api/web/sample-create error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});

// ============== Arranque servidor + WA ==============
app.listen(port, () => {
  console.log(`Servidor corriendo en el puerto ${port}`);
  connectToWhatsApp().catch(err => console.error('Error al conectar WhatsApp en startup:', err));
});

// ============== CRON JOBS ==============
cron.schedule('*/30 * * * * *', () => {
  console.log('⏱️ processSequences:', new Date().toISOString());
  processSequences().catch(err => console.error('Error en processSequences:', err));
});

cron.schedule('* * * * *', () => {
  console.log('⏱️ generateSiteSchemas:', new Date().toISOString());
  generateSiteSchemas().catch(err => console.error('Error en generateSiteSchemas:', err));
});

cron.schedule('*/5 * * * *', () => {
  console.log('⏱️ enviarSitiosPendientes:', new Date().toISOString());
  enviarSitiosPendientes().catch(err => console.error('Error en enviarSitiosPendientes:', err));
});

cron.schedule('0 * * * *', () => {
  console.log('⏱️ archivarNegociosAntiguos:', new Date().toISOString());
  archivarNegociosAntiguos().catch(err => console.error('Error en archivarNegociosAntiguos:', err));
});

/* ---------------- Helpers NUEVOS (al final para orden) ---------------- */
async function ensureUniqueSlug(input) {
  const base = slugify(String(input || ''), { lower: true, strict: true }).slice(0, 30) || 'sitio';
  let slug = base;
  let i = 2;
  while (true) {
    const snap = await db.collection('Negocios').where('slug', '==', slug).limit(1).get();
    if (snap.empty) return slug;
    slug = `${base}-${String(i).padStart(2, '0')}`;
    i++;
    if (i > 99) throw new Error('No fue posible generar un slug único');
  }
}
