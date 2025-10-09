// src/server/scheduler.js
import admin from 'firebase-admin';
import { db } from './firebaseAdmin.js';
import { getWhatsAppSock } from './whatsappService.js';

import axios from 'axios';
import { jsonrepair } from 'jsonrepair';
import { Timestamp } from 'firebase-admin/firestore';

import * as Q from './queue.js';
// OpenAI compat (v3/v4)
import OpenAIImport from 'openai';

const { FieldValue } = admin.firestore;

const PEXELS_API_KEY = process.env.PEXELS_API_KEY;
if (!process.env.OPENAI_API_KEY) {
  throw new Error('Falta OPENAI_API_KEY en entorno');
}

// ======================== OpenAI compat wrapper ===========================
function assertOpenAIKey() {
  if (!process.env.OPENAI_API_KEY) throw new Error('Falta OPENAI_API_KEY');
}

const OpenAICtor = OpenAIImport?.OpenAI || OpenAIImport; // v4 ó default
async function getOpenAI() {
  assertOpenAIKey();
  try {
    const client = new OpenAICtor({ apiKey: process.env.OPENAI_API_KEY });
    const hasChatCompletions = !!client?.chat?.completions?.create;
    const hasResponses = !!client?.responses?.create;
    if (hasChatCompletions) return { client, mode: 'v4-chat' };
    if (hasResponses) return { client, mode: 'v4-resp' };
  } catch {
    // cae a v3 dinámicamente
  }
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
    // v3
    return resp?.data?.choices?.[0]?.message?.content?.trim() || '';
  } catch {
    return '';
  }
}

async function chatCompletionCompat({ model, messages, temperature = 0.7, max_tokens = 1200 }) {
  const { client, mode } = await getOpenAI();
  if (mode === 'v4-chat') {
    const resp = await client.chat.completions.create({ model, messages, temperature, max_tokens });
    return extractText(resp, mode);
  }
  if (mode === 'v4-resp') {
    const resp = await client.responses.create({
      model,
      input: messages.map(m => `${m.role.toUpperCase()}: ${m.content}`).join('\n\n'),
    });
    return extractText(resp, mode);
  }
  // v3
  const resp = await client.createChatCompletion({ model, messages, temperature, max_tokens });
  return extractText(resp, 'v3');
}

// ============================ Utils comunes ==============================
import { parsePhoneNumberFromString } from 'libphonenumber-js';

function toE164(num, defaultCountry = 'MX') {
  const raw = String(num || '').replace(/\D/g, '');
  const p = parsePhoneNumberFromString(raw, defaultCountry);
  if (p && p.isValid()) return p.number; // +521... formato E.164
  // fallback MX: si 10 dígitos, anteponer +52
  if (/^\d{10}$/.test(raw)) return `+52${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('521')) return `+${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('52')) return `+${raw}`;
  return `+${raw}`;
}

function e164ToJid(e164) {
  // Baileys espera NNN@s.whatsapp.net, sin '+'
  const digits = String(e164 || '').replace(/\D/g, '');
  return `${digits}@s.whatsapp.net`;
}

function firstName(n = '') {
  return String(n).trim().split(/\s+/)[0] || '';
}

function replacePlaceholders(template, leadData) {
  const str = String(template || '');
  return str.replace(/\{\{(\w+)\}\}/g, (_, field) => {
    const value = leadData?.[field] || '';
    if (field === 'nombre') return firstName(value);
    return value;
  });
}

// ============= Locks simples para evitar dobles ejecuciones ==============
async function withTaskLock(taskKey, ttlSeconds, fn) {
  const ref = db.collection('_locks').doc(taskKey);
  const now = Date.now();
  const snap = await ref.get();
  const expireAt = snap.exists ? (snap.data().expireAt || 0) : 0;

  if (expireAt && expireAt > now) {
    // lock activo
    return { locked: true };
  }
  await ref.set({ expireAt: now + ttlSeconds * 1000 }, { merge: true });
  try {
    const result = await fn();
    return { locked: false, result };
  } finally {
    // suelta lock
    await ref.delete().catch(() => {});
  }
}

// ======================= Generación del site schema ======================
export async function generateSiteSchemas() {
  if (!PEXELS_API_KEY) throw new Error('Falta PEXELS_API_KEY en entorno');

  return withTaskLock('generateSiteSchemas', 45, async () => {
    console.log('▶️ generateSiteSchemas: inicio');
    const snap = await db.collection('Negocios').where('status', '==', 'Sin procesar').get();
    if (snap.empty) {
      console.log('generateSiteSchemas: sin pendientes');
      return 0;
    }

    let processed = 0;

    for (const doc of snap.docs) {
      const data = doc.data();
      try {
        const promptSystem = `
Eres un redactor publicitario SENIOR, experto en copywriting para sitios web.
Devuelve SOLO JSON válido con la estructura pedida.
`.trim();

        const promptUser = `
Negocio de giro: "${data.businessSector}"
Nombre: "${data.companyInfo}"
Historia: "${data.businessStory}"
Colores disponibles: ${JSON.stringify(data.palette || {})}
Servicios/productos: ${JSON.stringify(data.keyItems || [])}
WhatsApp: ${data.contactWhatsapp || ''}
Instagram: ${data.socialInstagram || ''}
Facebook: ${data.socialFacebook || ''}

IMPORTANTE: En "features.items" asigna el icono de Ant Design más representativo:
SafetyOutlined, BulbOutlined, UsergroupAddOutlined, HeartOutlined, RocketOutlined, ExperimentOutlined

Estructura EXACTA a devolver (JSON):
{
  "slug": "<slug>",
  "logoUrl": "<URL>",
  "colors": { "primary": "<hex>", "secondary": "<hex>", "accent": "<hex>", "text": "<hex>" },
  "hero": {
    "title": "<Título>",
    "subtitle": "<Subtítulo>",
    "ctaText": "<CTA>",
    "ctaUrl": "<URL>",
    "backgroundImageUrl": "<URL fondo>"
  },
  "features": {
    "title": "¿Qué nos hace únicos?",
    "items": [
      { "icon":"<Icono1>","title":"<T1>","text":"<D1>" }
    ]
  },
  "products": {
    "title":"<Título productos>",
    "items":[
      { "title":"<nombre>", "text":"<desc>", "imageUrl":"<img>", "buttonText":"<botón>", "buttonUrl":"<url>" }
    ]
  },
  "about": { "title":"<Título>", "text":"<Texto>" },
  "menu":[
    {"id":"services","label":"Servicios"},
    {"id":"about","label":"Nosotros"},
    {"id":"contact","label":"Contáctanos"}
  ],
  "contact": {
    "whatsapp":"<tel>",
    "email":"<email>",
    "facebook":"<url>",
    "instagram":"<url>",
    "youtube":"<url>"
  },
  "testimonials": {
    "title":"<Título testimonios>",
    "items":[ { "text":"<t1>", "author":"<a1>" } ]
  }
}
`.trim();

        // 1) pedir JSON al modelo (compat v4/v3)
        let raw = await chatCompletionCompat({
          model: 'gpt-4o-mini',
          messages: [
            { role: 'system', content: promptSystem },
            { role: 'user', content: promptUser },
          ],
          temperature: 0.6,
          max_tokens: 1800,
        });

        raw = String(raw || '').trim()
          .replace(/^```json\s*/i, '')
          .replace(/```$/i, '')
          .trim();

        let schema;
        try {
          schema = JSON.parse(raw);
        } catch (e) {
          schema = JSON.parse(jsonrepair(raw));
          console.warn('[WARN] JSON reparado para', doc.id);
        }

        // 2) traducir giro a inglés para query Pexels
        const sectorText = Array.isArray(data.businessSector)
          ? data.businessSector.join(', ')
          : (data.businessSector || '');

        const englishQuery = await chatCompletionCompat({
          model: 'gpt-4o-mini',
          messages: [
            { role: 'system', content: 'Convierte el giro a una frase corta de búsqueda en inglés para fotos de stock.' },
            { role: 'user', content: `Giro: "${sectorText}". Dame una frase corta (sin comillas).` },
          ],
          temperature: 0.2,
          max_tokens: 40,
        });

        // 3) Pexels search
        try {
          const px = await axios.get('https://api.pexels.com/v1/search', {
            headers: { Authorization: PEXELS_API_KEY },
            params: { query: englishQuery, per_page: 1 },
          });
          const photo = px.data?.photos?.[0]?.src?.large;
          if (photo) {
            schema.hero = schema.hero || {};
            schema.hero.backgroundImageUrl = photo;
          }
        } catch (e) {
          console.warn('[PEXELS] fallo búsqueda:', e?.message);
        }

        // 4) guardar
        await doc.ref.update({
          schema,
          status: 'Procesado',
          processedAt: FieldValue.serverTimestamp(),
        });

        processed++;
        console.log(`✅ Site schema generado para ${doc.id}`);
      } catch (err) {
        console.error(`❌ generateSiteSchemas(${doc.id})`, err);
      }
    }

    console.log('▶️ generateSiteSchemas: finalizado');
    return processed;
  });
}

// =================== Envío por WhatsApp (texto / links) ==================
export async function enviarMensaje(lead, mensaje) {
  try {
    const sock = getWhatsAppSock();
    if (!sock) return;

    const e164 = toE164(lead.telefono);
    const jid = e164ToJid(e164);

    switch ((mensaje?.type || 'texto').toLowerCase()) {
      case 'texto': {
        const text = replacePlaceholders(mensaje.contenido, lead).trim();
        if (text) await sock.sendMessage(jid, { text });
        break;
      }
      case 'formulario': {
        const raw = String(mensaje.contenido || '');
        const text = raw
          .replace('{{telefono}}', e164.replace(/\D/g, ''))
          .replace('{{nombre}}', encodeURIComponent(lead.nombre || ''))
          .replace(/\r?\n/g, ' ')
          .trim();
        if (text) await sock.sendMessage(jid, { text });
        break;
      }
      case 'audio': {
        const audioUrl = replacePlaceholders(mensaje.contenido, lead).trim();
        if (audioUrl) {
          await sock.sendMessage(jid, { audio: { url: audioUrl }, ptt: true });
        }
        break;
      }
      case 'imagen': {
        const url = replacePlaceholders(mensaje.contenido, lead).trim();
        if (url) await sock.sendMessage(jid, { image: { url } });
        break;
      }
      case 'video': {
        const url = replacePlaceholders(mensaje.contenido, lead).trim();
        if (url) await sock.sendMessage(jid, { video: { url } });
        break;
      }
      default:
        console.warn('Tipo desconocido:', mensaje?.type);
    }
  } catch (err) {
    console.error('Error al enviar mensaje:', err);
  }
}

export async function enviarSitioWebPorWhatsApp(negocio) {
  const slug = negocio?.slug || negocio?.schema?.slug;
  const phoneRaw = negocio?.leadPhone;
  if (!phoneRaw || !slug) {
    console.warn('Faltan datos para enviar el sitio web por WhatsApp', {
      leadPhone: phoneRaw,
      slug,
      schema: negocio?.schema,
    });
    return;
  }

  const e164 = toE164(phoneRaw);
  const sitioUrl = `https://negociosweb.mx/site/${slug}`;

  try {
    console.log(`[ENVIANDO WHATSAPP] A: ${e164} | URL: ${sitioUrl}`);
    await enviarMensaje(
      { telefono: e164, nombre: negocio.companyInfo || '' },
      { type: 'texto', contenido: `¡Tu sitio ya está listo! Puedes verlo aquí: ${sitioUrl}` }
    );
    console.log(`[OK] WhatsApp enviado a ${e164}: ${sitioUrl}`);
  } catch (err) {
    console.error(`[ERROR] enviando WhatsApp a ${e164}:`, err);
  }
}

export async function enviarSitiosPendientes() {
  return withTaskLock('enviarSitiosPendientes', 30, async () => {
    console.log('⏳ Buscando negocios procesados para enviar sitio web...');
    const snap = await db.collection('Negocios').where('status', '==', 'Procesado').get();
    console.log(`[DEBUG] Encontrados: ${snap.size} negocios para enviar`);

    for (const doc of snap.docs) {
      const data = doc.data();
      console.log(`[DEBUG] Procesando negocio: ${doc.id}`, {
        leadPhone: data.leadPhone,
        slug: data.slug,
        schemaSlug: data.schema?.slug,
        status: data.status,
      });

      await enviarSitioWebPorWhatsApp(data);

      // Marca como enviado
      await doc.ref.update({
        status: 'Web enviada',
        siteSentAt: FieldValue.serverTimestamp(),
      });

      // (Opcional) Aquí podrías encolar una secuencia tipo "WebEnviada"
      // usando tu motor de secuencias, en lugar de escribir arrays a mano.
      // Ejemplo (si tienes scheduleSequenceForLead):
      // await scheduleSequenceForLead(leadId, 'WebEnviada');
    }

    return snap.size;
  });
}

export async function archivarNegociosAntiguos() {
  const ahora = Date.now();
  const limite = ahora - 24 * 60 * 60 * 1000;
  const limiteTimestamp = Timestamp.fromMillis(limite);

  const snap = await db.collection('Negocios').where('createdAt', '<', limiteTimestamp).get();
  if (snap.empty) {
    console.log('No hay negocios antiguos para archivar.');
    return 0;
  }

  let n = 0;
  for (const doc of snap.docs) {
    try {
      const data = doc.data();
      if (data.plan !== undefined && data.plan !== null && data.plan !== '') {
        console.log(`Negocio ${doc.id} tiene plan (${data.plan}), no se archiva.`);
        continue;
      }
      await db.collection('ArchivoNegocios').doc(doc.id).set(data);
      await doc.ref.delete();
      console.log(`Negocio ${doc.id} archivado correctamente.`);
      n++;
    } catch (err) {
      console.error(`Error archivando negocio ${doc.id}:`, err);
    }
  }
  return n;
}

// ========================= Proceso de secuencias =========================
export async function processSequences() {
  const fn =
    typeof Q.processDueSequenceJobs === 'function'
      ? Q.processDueSequenceJobs
      : (typeof Q.processQueue === 'function' ? Q.processQueue : null);

  if (!fn) {
    console.warn('No hay función de proceso de cola exportada (processDueSequenceJobs / processQueue).');
    return 0;
  }
  // tu processQueue acepta { batchSize, shard }
  return await fn({ batchSize: 200 });
}

