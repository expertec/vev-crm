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
import axios from 'axios'; // 👈 si ya lo tienes importado, omite esta línea

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
  sendVideoNote,   
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

import { classifyBusiness } from './utils/businessClassifier.js';


function buildUnsplashFeaturedQueries(summary = {}) {
  const objetivoMap = {
    ecommerce: 'tienda online,productos',
    booking:   'reservas,servicios,agenda',
    info:      'negocio local'
  };
  const objetivo = objetivoMap[String(summary.templateId || '').toLowerCase()] || 'negocio local';

  const nombre = (summary.companyName || summary.name || summary.slug || '').toString().trim();

  const descTop = (summary.description || '')
    .toString()
    .replace(/https?:\/\/\S+/g, '')
    .replace(/[^\p{L}\p{N}\s]/gu, '')
    .split(/\s+/).filter(Boolean).slice(0, 4).join(' ');

  const terms = [objetivo, nombre, descTop].filter(Boolean).join(',');
  const q = encodeURIComponent(terms);
  const w = 1200, h = 800;

  return [
    `https://source.unsplash.com/featured/${w}x${h}/?${q}&sig=1`,
    `https://source.unsplash.com/featured/${w}x${h}/?${q}&sig=2`,
    `https://source.unsplash.com/featured/${w}x${h}/?${q}&sig=3`,
  ];
}

async function resolveUnsplashFinalUrl(sourceUrl) {
  try {
    const res = await axios.get(sourceUrl, {
      maxRedirects: 0,
      validateStatus: (s) => s === 302 || (s >= 200 && s < 300),
    });
    return res.headers?.location || sourceUrl;
  } catch {
    try {
      const res2 = await axios.head(sourceUrl, {
        maxRedirects: 0,
        validateStatus: (s) => s === 302 || (s >= 200 && s < 300),
      });
      return res2.headers?.location || sourceUrl;
    } catch {
      return sourceUrl;
    }
  }
}

// Usa Pexels como fuente principal (requiere PEXELS_API_KEY) y
// cae a Unsplash Source (resuelto a URL final) si Pexels falla o no hay resultados.
// Usa el clasificador (LLM + heurístico) para armar keywords,
// busca primero en Pexels y cae a Unsplash (resuelto a URL final) si falla.
async function getStockPhotoUrls(summary, count = 3) {
  // 1) Clasificar sector + keywords
  const { sector, keywords } = await classifyBusiness(summary);

  // 2) Query robusto: sector + keywords + nombre + objetivo
  const objetivoMap = {
    ecommerce: 'tienda online productos',
    booking:   'reservas servicios agenda',
    info:      'negocio local'
  };
  const objetivo = objetivoMap[String(summary?.templateId || '').toLowerCase()] || 'negocio local';
  const nombre   = (summary?.companyName || summary?.name || summary?.slug || '').toString().trim();

  const query = [sector, keywords, objetivo, nombre].filter(Boolean).join(' ').trim();

  // 3) PEXELS primero (si hay API key)
  const apiKey = process.env.PEXELS_API_KEY;
  if (apiKey) {
    try {
      const url = `https://api.pexels.com/v1/search?query=${encodeURIComponent(query)}&per_page=${count}&orientation=landscape&locale=es-ES`;
      const { data } = await axios.get(url, { headers: { Authorization: apiKey } });
      const photos = Array.isArray(data?.photos) ? data.photos : [];
      const pexelsUrls = photos.slice(0, count).map(p =>
        p?.src?.landscape || p?.src?.large2x || p?.src?.large || p?.src?.original
      ).filter(Boolean);
      if (pexelsUrls.length) return pexelsUrls;
    } catch (e) {
      console.error('[getStockPhotoUrls] Pexels error:', e?.message || e);
    }
  }

  // 4) Fallback: Unsplash Source → resolver 302 a URL final
  const termsForUnsplash = [sector, keywords, objetivo, nombre].filter(Boolean).join(',');
  const q = encodeURIComponent(termsForUnsplash);
  const w = 1200, h = 800;
  const sourceList = [
    `https://source.unsplash.com/featured/${w}x${h}/?${q}&sig=1`,
    `https://source.unsplash.com/featured/${w}x${h}/?${q}&sig=2`,
    `https://source.unsplash.com/featured/${w}x${h}/?${q}&sig=3`,
  ];
  const finals = [];
  for (const u of sourceList) finals.push(await resolveUnsplashFinalUrl(u));
  return finals.filter(Boolean);
}




// === Helper: subir imagen base64 a Firebase Storage y devolver URL pública
async function uploadBase64Image({ base64, folder = 'web-assets', filenamePrefix = 'img', contentType = 'image/png' }) {
  if (!base64) return null;
  try {
    // base64 puede venir como "data:image/png;base64,AAAA..." o puro base64
    const matches = String(base64).match(/^data:(image\/[a-zA-Z0-9.+-]+);base64,(.*)$/);
    const mime = matches ? matches[1] : (contentType || 'image/png');
    const b64  = matches ? matches[2] : base64;

    const buffer = Buffer.from(b64, 'base64');
    const ts = Date.now();
    const fileName = `${folder}/${filenamePrefix}_${ts}.png`; // si quieres respeta extensión desde mime
    const file = admin.storage().bucket().file(fileName);

    await file.save(buffer, {
      contentType: mime,
      metadata: { cacheControl: 'public,max-age=31536000' },
      resumable: false,
      public: true,
      validation: false,
    });

    // Asegura que sea público; en buckets con uniform access suele bastar el ACL default
    try { await file.makePublic(); } catch { /* noop si ya es público */ }

    return `https://storage.googleapis.com/${admin.storage().bucket().name}/${fileName}`;
  } catch (err) {
    console.error('[uploadBase64Image] error:', err);
    return null;
  }
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
    'Que el botón principal invite a escribir por WhatsApp',
    'Contar historias de clientes reales con resultados',
    'Pocos pasos para contactar, nada complicado',
  ];
  if (/(restaurante|cafeter|bar)/.test(base)) {
    return [
      'Muestra menú sencillo con fotos y precios claros',
      'Facilita reservar o pedir por WhatsApp en un paso',
      'En Google, mantén horarios y ubicación bien visibles',
    ];
  }
  if (/(tienda|retail|ecommerce)/.test(base)) {
    return [
      'Ordena productos por categorías fáciles de entender',
      'Permite comprar o preguntar por WhatsApp rápidamente',
      'Aclara cambios, envíos y formas de pago desde el inicio',
    ];
  }
  if (/(servicio|consultor|profesional|legal|médic|clínic)/.test(base)) {
    return [
      'Agendar cita o consulta en un paso por WhatsApp',
      'Muestra casos de éxito con fotos o datos simples',
      'Explica cada servicio con beneficios y precio de referencia',
    ];
  }
  if (/(belleza|salón|barber|estética)/.test(base)) {
    return [
      'Galería antes y después para generar confianza',
      'Reservación rápida por WhatsApp sin registro',
      'Ubicación y horarios visibles en la página principal',
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
// ---------------- Enviar audio (convierte a M4A pero manda en Opus/PTT si aplica) ----------------
app.post('/api/whatsapp/send-audio', upload.single('audio'), async (req, res) => {
  const { phone, forwarded, ptt } = req.body; // ← NUEVO: banderas desde el front
  if (!phone || !req.file) {
    return res.status(400).json({ success: false, error: 'Faltan phone o archivo' });
  }

  const uploadPath = req.file.path;
  const m4aPath = `${uploadPath}.m4a`;

  try {
    // 1) Convertir el archivo subido (cualquier formato) a M4A (AAC)
    await new Promise((resolve, reject) => {
      ffmpeg(uploadPath)
        .outputOptions(['-c:a aac', '-vn'])
        .toFormat('mp4')
        .save(m4aPath)
        .on('end', resolve)
        .on('error', reject);
    });

    // 2) Enviar el audio (whatsappService luego lo recodifica a OGG/Opus PTT si es necesario)
    //    y aplica el banner "Reenviado" cuando forwarded === true
    await sendAudioMessage(phone, m4aPath, {
      ptt: String(ptt).toLowerCase() === 'true' || ptt === true,
      forwarded: String(forwarded).toLowerCase() === 'true' || forwarded === true,
    });

    // 3) Limpieza local
    try { fs.unlinkSync(uploadPath); } catch {}
    try { fs.unlinkSync(m4aPath); } catch {}

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
// ============== after-form (web) ==============
// Acepta { leadId, leadPhone, summary, negocioId? }
// ============== after-form (web) ==============
// Acepta { leadId, leadPhone, summary, negocioId? }
// ============== after-form (web) ==============
// ============== after-form (web) ==============
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

    // 3.5) Subir assets (logo e imágenes) a Storage y obtener URLs
    let uploadedLogoURL = null;
    let uploadedPhotos = [];
    try {
      const assets = summary?.assets || {};
      const { logo, images = [] } = assets;

      if (logo) {
        uploadedLogoURL = await uploadBase64Image({
          base64: logo,
          folder: `web-assets/${(summary.slug || 'site').toLowerCase()}`,
          filenamePrefix: 'logo'
        });
      }

      if (Array.isArray(images)) {
        for (let i = 0; i < Math.min(images.length, 3); i++) {
          const b64 = images[i];
          if (!b64) continue;
          const url = await uploadBase64Image({
            base64: b64,
            folder: `web-assets/${(summary.slug || 'site').toLowerCase()}`,
            filenamePrefix: `photo_${i + 1}`
          });
          if (url) uploadedPhotos.push(url);
        }
      }
    } catch (e) {
      console.error('[after-form] error subiendo assets:', e);
    }

    // 3.6) Fallback de imágenes cuando NO se suben fotos → buscar y guardar URL FINAL
    if (!uploadedPhotos || uploadedPhotos.length === 0) {
      try {
        uploadedPhotos = await getStockPhotoUrls(summary); // 👈 usa objetivo+nombre+desc
      } catch (e) {
        console.error('[after-form] stock photos error:', e);
        // Último fallback (sin resolver redirects)
        uploadedPhotos = buildUnsplashFeaturedQueries(summary);
      }
    }

    // 4) Crear/actualizar Negocios — BLOQUEA duplicado por WhatsApp
    let negocioDocId = negocioId;
    let finalSlug = summary.slug || '';
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

      // crear documento en Negocios
      const ref = await db.collection('Negocios').add({
        leadId: finalLeadId,
        leadPhone: leadPhoneDigits,
        status: 'Sin procesar',

        companyInfo:     summary.companyName || summary.name || '',
        businessSector:  '', // en esta versión no hay campo businessType en el form
        businessStory:   summary.description || '',

        templateId:      String(summary.templateId || 'info').toLowerCase(),
        primaryColor:    summary.primaryColor || null,
        palette:         summary.palette || (summary.primaryColor ? [summary.primaryColor] : []),
        keyItems:        summary.keyItems || [],

        contactWhatsapp: summary.contactWhatsapp || '',
        contactEmail:    summary.email || '',
        socialFacebook:  summary.socialFacebook || '',
        socialInstagram: summary.socialInstagram || '',

        // Assets subidos + Fallback
        logoURL:   uploadedLogoURL || summary.logoURL || '',
        photoURLs: uploadedPhotos && uploadedPhotos.length ? uploadedPhotos : (summary.photoURLs || []),

        slug:      summary.slug || '',
        createdAt: new Date()
      });
      negocioDocId = ref.id;
      finalSlug = summary.slug || '';
    }

    // Mensajes al lead (derivados del templateId)
    const first = (v = '') => String(v).trim().split(/\s+/)[0] || '';
    const nombreCorto = first(leadData?.nombre || summary?.contactName || '');
    const giroBase = (() => {
      const t = String(summary?.templateId || '').toLowerCase();
      if (t === 'ecommerce') return 'tienda online';
      if (t === 'booking')   return 'servicio con reservas';
      return 'negocio';
    })();

    const giroHumano  = humanizeGiro ? humanizeGiro(giroBase) : giroBase;
    const [op1, op2, op3] = pickOpportunityTriplet
      ? pickOpportunityTriplet(giroHumano)
      : ['clarificar propuesta de valor', 'CTA visible a WhatsApp', 'pruebas sociales (reseñas)'];

    const msg1 = `${nombreCorto ? nombreCorto + ', ' : ''}ya recibí tu formulario. Mi equipo y yo ya estamos trabajando en tu muestra para que quede clara y útil.`;
    const msg2 = `Platicando con mi equipo, identificamos tres áreas para que tu ${giroHumano} aproveche mejor su web:\n1) ${op1}\n2) ${op2}\n3) ${op3}\nSi te late, las integramos en tu demo y te la comparto.`;

    const d1 = 60_000 + Math.floor(Math.random() * 30_000);
    const d2 = 115_000 + Math.floor(Math.random() * 65_000);

    setTimeout(() => sendMessageToLead(leadPhoneDigits, msg1).catch(console.error), d1);
    setTimeout(() => sendMessageToLead(leadPhoneDigits, msg2).catch(console.error), d2);

    await leadRef.set({
      etapa: 'form_submitted',
      etiquetas: admin.firestore.FieldValue.arrayUnion('FormOK')
    }, { merge: true });

    return res.json({ ok: true, negocioId: negocioDocId, slug: finalSlug });
  } catch (e) {
    console.error('/api/web/after-form error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});






// ============== Activar WebEnviada tras mandar link ==============
// Acepta { leadId?, leadPhone?, link? } — activa 'WebEnviada' 15 min después
app.post('/api/web/sample-sent', async (req, res) => {
  try {
    const { leadId, leadPhone } = req.body || {};
    if (!leadId && !leadPhone) return res.status(400).json({ error: 'Faltan leadId o leadPhone' });

    const e164 = toE164(leadPhone || (leadId || '').split('@')[0]);
    const finalLeadId = leadId || e164ToLeadId(e164);

    if (!scheduleSequenceForLead) {
      return res.status(500).json({ error: 'scheduleSequenceForLead no disponible' });
    }

    const startAt = new Date(Date.now() + 15 * 60 * 1000); // +15 min
    await scheduleSequenceForLead(finalLeadId, 'WebEnviada', startAt);

    await db.collection('leads').doc(finalLeadId).set({
      webLinkSentAt: new Date(),
      etiquetas: admin.firestore.FieldValue.arrayUnion('WebLinkSent')
    }, { merge: true });

    return res.json({ ok: true, scheduledAt: startAt.toISOString() });
  } catch (e) {
    console.error('/api/web/sample-sent error:', e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});

// ============== tracking: link abierto ==============
// Acepta { leadId? , leadPhone? , slug? } — con cualquiera basta.
// Si viene slug, lo resolvemos a phone → leadId.
app.post('/api/track/link-open', async (req, res) => {
  try {
    let { leadId, leadPhone, slug } = req.body || {};

    // a) si vino slug, busca el negocio y toma su leadPhone
    if (slug && !leadPhone && !leadId) {
      const snap = await db.collection('Negocios').where('slug', '==', String(slug)).limit(1).get();
      if (!snap.empty) {
        const d = snap.docs[0].data() || {};
        leadPhone = d.leadPhone || leadPhone;
      }
    }

    // b) normaliza a leadId
    if (!leadId && leadPhone) {
      const e164 = toE164(leadPhone);
      leadId = e164ToLeadId(e164);
    }
    if (!leadId) return res.status(400).json({ error: 'Falta leadId/leadPhone/slug' });

    const leadRef = db.collection('leads').doc(leadId);
    const leadSnap = await leadRef.get();
    if (!leadSnap.exists) return res.status(404).json({ error: 'Lead no encontrado' });

    // Evita re-disparar si ya se marcó
    const leadData = leadSnap.data() || {};
    if (leadData.linkOpenedAt) {
      return res.json({ ok: true, already: true });
    }

    // 1) Marca evento
    await leadRef.set({
      linkOpenedAt: new Date(),
      etiquetas: admin.firestore.FieldValue.arrayUnion('LinkAbierto')
    }, { merge: true });

    // 2) Cambia secuencia: WebEnviada → LinkAbierto
    try {
      if (cancelSequences) {
        await cancelSequences(leadId, ['WebEnviada']);
      }
      if (scheduleSequenceForLead) {
        await scheduleSequenceForLead(leadId, 'LinkAbierto', new Date());
      }
    } catch (seqErr) {
      console.warn('[track/link-open] secuencias:', seqErr?.message);
    }

    return res.json({ ok: true });
  } catch (err) {
    console.error('/api/track/link-open error:', err);
    return res.status(500).json({ error: err.message });
  }
});


// ---------------- Enviar video note (PTV) ----------------
app.post('/api/whatsapp/send-video-note', async (req, res) => {
  try {
    const { phone, url, seconds } = req.body || {};
    if (!phone || !url) {
      return res.status(400).json({ ok: false, error: 'Faltan phone y url' });
    }

    console.log(`[API] send-video-note → ${phone} ${url} s=${seconds ?? 'n/a'}`);
    await sendVideoNote(
      phone,
      url,
      Number.isFinite(+seconds) ? +seconds : null
    );

    return res.json({ ok: true });
  } catch (e) {
    console.error('/api/whatsapp/send-video-note error:', e);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});



// ============== sample-create (nuevo, para el formulario turbo) ==============
// Acepta { leadPhone, summary: { companyName, businessStory, slug } }
// ============== sample-create (turbo, ahora con plantillas y assets) ==============
// Acepta { leadPhone, summary: { companyName, businessStory, slug, templateId?, primaryColor?, assets? } }
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
    const leadRef  = db.collection('leads').doc(finalLeadId);
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

    // ⬆️ Subir assets (logo e imágenes) a Storage y obtener URLs
    let uploadedLogoURL = null;
    let uploadedPhotos = [];
    try {
      const assets = summary?.assets || {};
      const { logo, images = [] } = assets;

      if (logo) {
        uploadedLogoURL = await uploadBase64Image({
          base64: logo,
          folder: `web-assets/${(finalSlug || 'site').toLowerCase()}`,
          filenamePrefix: 'logo'
        });
      }

      if (Array.isArray(images)) {
        for (let i = 0; i < Math.min(images.length, 3); i++) {
          const b64 = images[i];
          if (!b64) continue;
          const url = await uploadBase64Image({
            base64: b64,
            folder: `web-assets/${(finalSlug || 'site').toLowerCase()}`,
            filenamePrefix: `photo_${i + 1}`
          });
          if (url) uploadedPhotos.push(url);
        }
      }
    } catch (e) {
      console.error('[sample-create] error subiendo assets:', e);
    }

    // Crea el Negocio con plantilla/color/urls
    const ref = await db.collection('Negocios').add({
      leadId: finalLeadId,
      leadPhone: leadPhoneDigits,
      status: 'Sin procesar',

      companyInfo:     summary.companyName,
      businessSector:  '',
      businessStory:   summary.businessStory,

      // Plantilla y color
      templateId:      summary.templateId || 'info',           // 'ecommerce' | 'info' | 'booking'
      primaryColor:    summary.primaryColor || null,
      palette:         summary.primaryColor ? [summary.primaryColor] : [],

      keyItems:        [],

      // Contacto y redes
      contactWhatsapp: summary.contactWhatsapp || '',
      contactEmail:    summary.email || '',
      socialFacebook:  summary.socialFacebook || '',
      socialInstagram: summary.socialInstagram || '',

      // Assets subidos
      logoURL:         uploadedLogoURL || summary.logoURL || '',
      photoURLs:       uploadedPhotos && uploadedPhotos.length ? uploadedPhotos : (summary.photoURLs || []),

      slug:            finalSlug,
      createdAt:       new Date()
    });

    // (Opcional) guardar brief
    await leadRef.set({
      briefWeb: {
        companyName: summary.companyName,
        businessStory: summary.businessStory,
        slug: finalSlug,
        templateId: summary.templateId || 'info',
        primaryColor: summary.primaryColor || null,
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
