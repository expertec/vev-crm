// scheduler.js
import admin from 'firebase-admin';
import { db } from './firebaseAdmin.js';
import { getWhatsAppSock } from './whatsappService.js';

import axios from 'axios';
import { jsonrepair } from 'jsonrepair';
import { Timestamp } from 'firebase-admin/firestore';

import * as Q from './queue.js';
// OpenAI compat (v3/v4)
import OpenAIImport from 'openai';




function pickPrimaryColor(data) {
  // Si el cliente eligió un color, úsalo; si no, usa normalizeColors/forzados
  if (data.primaryColor && /^#(?:[0-9a-f]{3}){1,2}$/i.test(data.primaryColor)) {
    return data.primaryColor;
  }
  // fallback a palette[0]
  const fromPalette = Array.isArray(data.palette) && data.palette[0];
  return fromPalette || '#16a34a';
}

function buildSchemaForTemplate(data) {
  const template = (data.templateId || 'info').toLowerCase(); // 'ecommerce' | 'info' | 'booking'
  const brand    = data.companyInfo || data.slug || 'Mi Negocio';
  const waDigits = data.contactWhatsapp || data.leadPhone || '';
  const waUrl    = waDigits ? `https://wa.me/${waDigits}` : '';

  // Imágenes
  const heroImg = Array.isArray(data.photoURLs) && data.photoURLs[0]
    ? data.photoURLs[0]
    : unsplashFallback(brand, 1600, 900);
  const gallery = (Array.isArray(data.photoURLs) && data.photoURLs.length > 0)
    ? data.photoURLs
    : [unsplashFallback(brand, 1200, 800), unsplashFallback(brand, 1200, 800), unsplashFallback(brand, 1200, 800)];

  const primary = pickPrimaryColor(data);
  const colors = normalizeColors({ primary }, { primary, secondary:'#0ea5e9', accent:'#f59e0b', text:'#111827' });

  // Bloques comunes
  const base = {
    slug: data.slug,
    brand: {
      name: brand,
      logo: data.logoURL || null
    },
    contact: {
      whatsapp: waDigits || '',
      email: data.contactEmail || '',
      facebook: data.socialFacebook || '',
      instagram: data.socialInstagram || ''
    },
    colors,
    hero: {
      title: brand,
      subtitle: data.businessSector || data.businessStory || '',
      image: heroImg,
      ctaText: 'Hablar por WhatsApp',
      ctaUrl: waUrl || '#'
    },
    gallery
  };

  if (template === 'ecommerce') {
    // Productos básicos de muestra; luego podrás mapear desde keyItems
    const items = (Array.isArray(data.keyItems) && data.keyItems.length > 0)
      ? data.keyItems.map((k, i) => ({
          id: `p${i+1}`, title: k.title || k, price: k.price || 199, image: gallery[i % gallery.length],
        }))
      : [
          { id:'p1', title:'Producto 1', price:149, image:gallery[0] },
          { id:'p2', title:'Producto 2', price:249, image:gallery[1] },
          { id:'p3', title:'Producto 3', price:199, image:gallery[2] },
        ];

    return {
      templateId: 'ecommerce',
      ...base,
      ecommerce: {
        currency: 'MXN',
        cart: { sendToWhatsApp: true },
        products: items.map(it => ensureWhatsAppButton(it, waUrl))
      }
    };
  }

  if (template === 'booking') {
    // Reservas por WhatsApp: mostramos horarios de ejemplo y CTA que abre wa.me con el slot
    const slots = [
      { id:'s1', label:'Hoy 4:00 PM' },
      { id:'s2', label:'Hoy 6:00 PM' },
      { id:'s3', label:'Mañana 11:00 AM' },
    ];
    return {
      templateId: 'booking',
      ...base,
      booking: {
        slots: slots.map(s => ({
          ...s,
          buttonUrl: waUrl ? `${waUrl}?text=${encodeURIComponent(`Hola, quiero reservar: ${s.label}`)}` : '#',
          buttonText: 'Reservar por WhatsApp'
        }))
      }
    };
  }

  // Presencia (info) por defecto
  return {
    templateId: 'info',
    ...base,
    info: {
      features: [
        { icon: 'BulbOutlined', title: 'Profesional', text: 'Imagen clara y confiable.' },
        { icon: 'RocketOutlined', title: 'Rápido', text: 'Carga optimizada.' },
        { icon: 'HeartOutlined', title: 'Hecho para ti', text: 'A tu medida.' }
      ].map(x => ensureWhatsAppButton(x, waUrl))
    }
  };
}


const { FieldValue } = admin.firestore;

const PEXELS_API_KEY = process.env.PEXELS_API_KEY;
if (!process.env.OPENAI_API_KEY) {
  throw new Error('Falta OPENAI_API_KEY en entorno');
}

/* ======================== OpenAI compat wrapper ======================== */
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
  } catch { /* cae a v3 dinámicamente */ }
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

/* ============================= Utils comunes =========================== */
import { parsePhoneNumberFromString } from 'libphonenumber-js';

function toE164(num, defaultCountry = 'MX') {
  const raw = String(num || '').replace(/\D/g, '');
  const p = parsePhoneNumberFromString(raw, defaultCountry);
  if (p && p.isValid()) return p.number; // +521... formato E.164
  // fallback MX
  if (/^\d{10}$/.test(raw)) return `+52${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('521')) return `+${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('52'))  return `+${raw}`;
  return `+${raw}`;
}

// ⚠️ WhatsApp JID correcto para MX móvil: **521**XXXXXXXXX
function normalizePhoneForWA(phone) {
  let num = String(phone || '').replace(/\D/g, '');
  // si viene como 52XXXXXXXXXX (12), convertir a 521 + 10
  if (num.length === 12 && num.startsWith('52') && !num.startsWith('521')) {
    return '521' + num.slice(2);
  }
  // si 10 dígitos (nacional) => 521 + 10
  if (num.length === 10) return '521' + num;
  // si ya viene con 521… mantener
  return num;
}
function e164ToJid(e164) {
  const digits = String(e164 || '').replace(/\D/g, '');
  return `${normalizePhoneForWA(digits)}@s.whatsapp.net`;
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

/* ======================= Helpers de color / paletas ==================== */
const SECTOR_PALETTES = [
  {
    keys: ['auto', 'automotriz', 'taller', 'mecánico', 'mecanico', 'refacciones', 'servicio automotriz'],
    colors: { primary: '#0F172A', secondary: '#334155', accent: '#2b2b2bff', text: '#FFFFFF' }
  },
  {
    keys: ['restaurante', 'comida', 'cafetería', 'bar', 'panadería', 'cocina', 'food'],
    colors: { primary: '#1F2937', secondary: '#6B7280', accent: '#F59E0B', text: '#FFFFFF' }
  },
  {
    keys: ['salud', 'bienestar', 'spa', 'estética', 'belleza', 'wellness'],
    colors: { primary: '#064E3B', secondary: '#0F766E', accent: '#10B981', text: '#FFFFFF' }
  },
  {
    keys: ['tecnología', 'software', 'ti', 'hosting', 'saas', 'app', 'código', 'code'],
    colors: { primary: '#0B1020', secondary: '#1F2937', accent: '#60A5FA', text: '#FFFFFF' }
  },
  {
    keys: ['educación', 'capacitaci', 'curso', 'academ', 'colegio'],
    colors: { primary: '#1D3557', secondary: '#457B9D', accent: '#001666ff', text: '#FFFFFF' }
  },
  {
    keys: ['construcción', 'obra', 'remodelación', 'albañil', 'arquitect', 'ingenier'],
    colors: { primary: '#111827', secondary: '#374151', accent: '#3c3c3cff', text: '#FFFFFF' }
  },
  {
    keys: ['finanzas', 'banca', 'conta', 'impuestos', 'crédito', 'seguro'],
    colors: { primary: '#0B3D2E', secondary: '#14532D', accent: '#000000ff', text: '#FFFFFF' }
  }
];

function looksTooPink(hex = '') {
  const h = String(hex || '').replace('#', '');
  if (h.length !== 6) return false;
  const r = parseInt(h.slice(0,2),16), g = parseInt(h.slice(2,4),16), b = parseInt(h.slice(4,6),16);
  return (r > 180 && b > 120 && g < 140) || (r > 200 && g < 120);
}

function pickPaletteForBusiness({ sector = '', story = '', name = '' }) {
  const hay = (txt) => String(txt || '').toLowerCase();
  const blob = `${hay(sector)} ${hay(story)} ${hay(name)}`;
  for (const p of SECTOR_PALETTES) {
    if (p.keys.some(k => blob.includes(k))) return p.colors;
  }
  return { primary: '#111827', secondary: '#374151', accent: '#F97316', text: '#FFFFFF' }; // neutral
}

function normalizeColors(schemaColors, forced) {
  const c = schemaColors || {};
  const badPrimary = !c.primary || looksTooPink(c.primary);
  const badAccent  = !c.accent  || looksTooPink(c.accent);
  return {
    primary: badPrimary ? forced.primary : c.primary,
    secondary: !c.secondary ? forced.secondary : c.secondary,
    accent: badAccent ? forced.accent : c.accent,
    text: !c.text ? forced.text : c.text,
  };
}

/* ======================= Helpers de imágenes/avatares =================== */
async function fetchPexelsImage(query, { perPage = 1 } = {}) {
  if (!PEXELS_API_KEY) return null;
  try {
    const resp = await axios.get('https://api.pexels.com/v1/search', {
      headers: { Authorization: PEXELS_API_KEY },
      params: { query, per_page: perPage },
    });
    const p = resp?.data?.photos?.[0];
    return p?.src?.large || p?.src?.medium || null;
  } catch {
    return null;
  }
}

// Sin API key: fuente pública (no-auth) de Unsplash
function unsplashFallback(query, w = 1600, h = 900) {
  const q = encodeURIComponent(query || 'business');
  return `https://source.unsplash.com/${w}x${h}/?${q}`;
}

async function getStockImage(query, { width = 1600, height = 900, perPage = 1 } = {}) {
  const fromPexels = await fetchPexelsImage(query, { perPage });
  if (fromPexels) return fromPexels;
  return unsplashFallback(query, width, height);
}

// Avatares determinísticos (pravatar)
function avatarFor(seed, size = 300) {
  return `https://i.pravatar.cc/${size}?u=${encodeURIComponent(seed)}`;
}

// Detecta URLs vacías o de ejemplo
function isPlaceholderUrl(u = '') {
  const s = String(u || '').trim().toLowerCase();
  if (!s) return true;
  if (s === '#' || s.startsWith('javascript:')) return true;
  try {
    const url = new URL(s);
    if (['example.com', 'placehold.co', 'placeholder.com'].includes(url.hostname)) return true;
  } catch {
    return true; // no es URL válida
  }
  return false;
}

function ensureWhatsAppButton(it = {}, waUrl = '') {
  const out = { ...it };
  if (waUrl) {
    if (isPlaceholderUrl(out.buttonUrl) || !/^https?:\/\//i.test(out.buttonUrl || '')) {
      out.buttonUrl = waUrl;
      out.buttonText = out.buttonText || 'Pedir por WhatsApp';
    }
  }
  return out;
}

/* ============= Locks simples para evitar dobles ejecuciones ============ */
async function withTaskLock(taskKey, ttlSeconds, fn) {
  const ref = db.collection('_locks').doc(taskKey);
  const now = Date.now();
  const snap = await ref.get();
  const expireAt = snap.exists ? (snap.data().expireAt || 0) : 0;

  if (expireAt && expireAt > now) return { locked: true };
  await ref.set({ expireAt: now + ttlSeconds * 1000 }, { merge: true });
  try {
    const result = await fn();
    return { locked: false, result };
  } finally {
    await ref.delete().catch(() => {});
  }
}

/* ====================== Generación del site schema ===================== */
/* ====================== Generación del site schema ===================== */
export async function generateSiteSchemas() {
  if (!PEXELS_API_KEY) {
    console.warn('⚠️ PEXELS_API_KEY no configurada: se usarán imágenes de Unsplash Source como fallback.');
  }

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
      { "title":"<P1>","text":"<desc>","price":199,"imageUrl":"<URL>","buttonText":"Comprar","buttonUrl":"<URL|WA>" }
    ]
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
          max_tokens: 900,
        });

        // 1.0 parse/repair
        let schema;
        try {
          schema = JSON.parse(raw);
        } catch {
          schema = JSON.parse(jsonrepair(raw));
          console.warn('[WARN] JSON reparado para', doc.id);
        }

        // 1.1 Normalizaciones mínimas
        schema = schema && typeof schema === 'object' ? schema : {};
        schema.slug = schema.slug || data.slug || String(doc.id).slice(0, 8);

        // 1.1.1 Paleta por giro/descripcion (evita rosas fuera de contexto)
        const forcedPalette = pickPaletteForBusiness({
          sector: data.businessSector,
          story: data.businessStory,
          name: data.companyInfo
        });
        schema.colors = normalizeColors(schema.colors, forcedPalette);

        // 1.1.1.1 Override explícito si el cliente eligió color primario
        if (data.primaryColor && /^#(?:[0-9a-f]{3}){1,2}$/i.test(data.primaryColor)) {
          schema.colors = schema.colors || {};
          schema.colors.primary = data.primaryColor;
        }

        // 1.1.2 Logo por defecto o si es placeholder (luego preferimos el del cliente)
        if (!schema.logoUrl || isPlaceholderUrl(schema.logoUrl)) {
          const seed = data.companyInfo || schema.slug || 'brand';
          schema.logoUrl = `https://api.dicebear.com/8.x/initials/svg?seed=${encodeURIComponent(seed)}&radius=8`;
        }

        // 1.2 CTA / Contacto con WhatsApp
        const phoneDigits = String(data.leadPhone || '').replace(/\D/g, '');
        const waUrl = phoneDigits ? `https://wa.me/${phoneDigits}` : '';

        if (!schema.hero) schema.hero = {};
        if (waUrl) {
          schema.hero.ctaUrl = waUrl;
          if (!schema.hero.ctaText) schema.hero.ctaText = 'Escríbenos por WhatsApp';
          schema.hero.ctaColor = schema.colors?.accent || '#F97316';
        }

        if (!schema.contact) schema.contact = {};
        if (waUrl) schema.contact.whatsapp = waUrl;

        if (schema.products && Array.isArray(schema.products.items)) {
          schema.products.items = schema.products.items.map(it => ensureWhatsAppButton(it, waUrl));
        }

        // 1.3 Plantilla (templateId) y ajustes por tipo
        const template = String(data.templateId || 'info').toLowerCase();
        schema.templateId = template; // para el front

        if (template === 'ecommerce') {
          // asegura sección productos con botones WA
          schema.products = schema.products || { title: 'Nuestros productos', items: [] };
          if (!Array.isArray(schema.products.items) || schema.products.items.length === 0) {
            schema.products.items = [
              { title: 'Producto 1', text: 'Descripción breve.', price: 149, imageUrl: '', buttonText: 'Pedir por WhatsApp', buttonUrl: waUrl },
              { title: 'Producto 2', text: 'Descripción breve.', price: 249, imageUrl: '', buttonText: 'Pedir por WhatsApp', buttonUrl: waUrl },
              { title: 'Producto 3', text: 'Descripción breve.', price: 199, imageUrl: '', buttonText: 'Pedir por WhatsApp', buttonUrl: waUrl },
            ];
          } else {
            schema.products.items = schema.products.items.map(it => ensureWhatsAppButton(it, waUrl));
          }
        } else if (template === 'booking') {
          // landing orientada a reservar
          schema.hero.title = schema.hero.title || 'Reserva en minutos';
          schema.hero.subtitle = schema.hero.subtitle || 'Agenda por WhatsApp de forma rápida.';
          schema.hero.ctaText = schema.hero.ctaText || 'Reservar por WhatsApp';

          schema.booking = schema.booking || {
            slots: [
              { id:'s1', label:'Hoy 4:00 PM' },
              { id:'s2', label:'Hoy 6:00 PM' },
              { id:'s3', label:'Mañana 11:00 AM' },
            ]
          };
          schema.booking.slots = (schema.booking.slots || []).map(s => ({
            ...s,
            buttonText: s.buttonText || 'Reservar por WhatsApp',
            buttonUrl: waUrl ? `${waUrl}?text=${encodeURIComponent(`Hola, quiero reservar: ${s.label}`)}` : '#'
          }));
        } else {
          // info (presencia) – dejar que prompt domine y solo asegurar CTAs
          if (schema.info?.features?.length) {
            schema.info.features = schema.info.features.map(x => ensureWhatsAppButton(x, waUrl));
          }
        }

        // 2) Query base para imágenes de stock (usada solo de fallback)
        const sectorOrStory = Array.isArray(data.businessSector)
          ? data.businessSector.join(', ')
          : (data.businessSector || data.businessStory || data.companyInfo || '');
        const searchQuery = (
          (schema.hero?.title ? `${schema.hero.title} ` : '') +
          sectorOrStory
        ).trim() || 'business';

        // 3) Preferir ASSETS DEL CLIENTE y luego hacer FALLBACK a stock
        try {
          // 3.1 Preferir logo del cliente
          if (data.logoURL && (!schema.logoUrl || isPlaceholderUrl(schema.logoUrl))) {
            schema.logoUrl = data.logoURL;
          }

          // 3.2 Preferir fotos del cliente
          const customerPhotos = Array.isArray(data.photoURLs) ? data.photoURLs.filter(Boolean) : [];

          // HERO
          if (!schema.hero) schema.hero = {};
          if (!schema.hero.backgroundImageUrl || isPlaceholderUrl(schema.hero.backgroundImageUrl)) {
            if (customerPhotos[0]) {
              schema.hero.backgroundImageUrl = customerPhotos[0];
            }
          }

          // PRODUCTS
          if (schema.products?.items && Array.isArray(schema.products.items)) {
            let k = 0;
            for (let i = 0; i < schema.products.items.length; i++) {
              const it = schema.products.items[i] || {};
              if ((!it.imageUrl || isPlaceholderUrl(it.imageUrl)) && customerPhotos[k]) {
                it.imageUrl = customerPhotos[k++];
              }
              schema.products.items[i] = ensureWhatsAppButton(it, waUrl);
            }
          }

          // FEATURED gallery (si la manejas)
          if (!schema.gallery && customerPhotos.length > 1) {
            schema.gallery = customerPhotos;
          }

          // 3.3 Fallback a stock (Pexels/Unsplash) **solo si faltan**
          // --- HERO ---
          if (!schema.hero.backgroundImageUrl || isPlaceholderUrl(schema.hero.backgroundImageUrl)) {
            schema.hero.backgroundImageUrl = await getStockImage(searchQuery, { width: 1600, height: 900 });
          }

          // --- PRODUCTS ---
          if (schema.products?.items && Array.isArray(schema.products.items)) {
            for (let i = 0; i < schema.products.items.length; i++) {
              const it = schema.products.items[i] || {};
              if (!it.imageUrl || isPlaceholderUrl(it.imageUrl)) {
                const q = `${searchQuery} ${it.title || 'product'}`;
                it.imageUrl = await getStockImage(q, { width: 1200, height: 800 });
              }
              schema.products.items[i] = ensureWhatsAppButton(it, waUrl);
            }
          }

          // --- TESTIMONIALS / TEAM ---
          if (schema.testimonials?.items && Array.isArray(schema.testimonials.items)) {
            schema.testimonials.items = schema.testimonials.items.map((t, idx) => {
              if (!t.avatarUrl || isPlaceholderUrl(t.avatarUrl)) t.avatarUrl = avatarFor(`${schema.slug}-test-${idx}`);
              return t;
            });
          }
          if (schema.team && Array.isArray(schema.team)) {
            schema.team = schema.team.map((m, idx) => {
              if (!m.imageUrl || isPlaceholderUrl(m.imageUrl)) m.imageUrl = avatarFor(`${schema.slug}-team-${idx}`);
              return m;
            });
          }
        } catch (e) {
          console.warn('[IMAGES] error general:', e?.message);
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


/* ================= Envío por WhatsApp (texto / links / media) ========== */
export async function enviarMensaje(lead, mensaje) {
  try {
    const sock = getWhatsAppSock();
    if (!sock) return;

    // lead.telefono puede venir en e164 (+52...), en 52..., 521..., o 10 dígitos
    const e164 = toE164(lead.telefono);
    const jid = e164ToJid(e164); // asegura 521…@s.whatsapp.net

    switch ((mensaje?.type || 'texto').toLowerCase()) {
      case 'texto': {
        const text = replacePlaceholders(mensaje.contenido, lead).trim();
        if (text) await sock.sendMessage(jid, { text, linkPreview: false });
        break;
      }
      case 'formulario': {
        const raw = String(mensaje.contenido || '');
        const text = raw
          .replace('{{telefono}}', e164.replace(/\D/g, ''))
          .replace('{{nombre}}', encodeURIComponent(lead.nombre || ''))
          .replace(/\r?\n/g, ' ')
          .trim();
        if (text) await sock.sendMessage(jid, { text, linkPreview: false });
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

/* =========== Enviar sitio, activar secuencia y cancelar otras ========== */
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
  const jid = e164ToJid(e164); // normalizado 521…@s.whatsapp.net
  const sitioUrl = `https://negociosweb.mx/site/${slug}`;

  try {
    console.log(`[ENVIANDO WHATSAPP] A: ${e164} | URL: ${sitioUrl}`);
    await enviarMensaje(
      { telefono: e164, nombre: negocio.companyInfo || '' },
      { type: 'texto', contenido: `¡Tu sitio ya está listo! Puedes verlo aquí: ${sitioUrl}` }
    );
    console.log(`[OK] WhatsApp enviado a ${e164}: ${sitioUrl}`);

    // === activar secuencia WebEnviada y cancelar las anteriores ===
    try {
      const leadId = jid; // nuestro ID de lead en WhatsApp es el JID
      if (typeof Q.cancelSequences === 'function') {
        await Q.cancelSequences(leadId, ['NuevoLeadWeb', 'LeadWeb']).catch(() => {});
      }
      if (typeof Q.scheduleSequenceForLead === 'function') {
        await Q.scheduleSequenceForLead(leadId, 'WebEnviada', new Date()).catch(() => {});
      }
      // etiqueta y bandera en el lead (si existe)
      const leadRef = db.collection('leads').doc(leadId);
      await leadRef.set(
        { etiquetas: FieldValue.arrayUnion('WebEnviada') },
        { merge: true }
      ).catch(() => {});
    } catch (seqErr) {
      console.warn('[enviarSitioWebPorWhatsApp] No se pudo activar/cancelar secuencias:', seqErr?.message);
    }
  } catch (err) {
    console.error(`[ERROR] enviando WhatsApp a ${e164}:`, err);
  }
}

/* ============== Buscar “Procesado” y enviar + marcar enviado =========== */
/** 
 * Ahora con **retraso realista**:
 * - Si el negocio está "Procesado" y NO tiene siteReadyAt, se agenda a +15–25 min y se deja para otra corrida.
 * - Si ya tiene siteReadyAt en el futuro: skip.
 * - Si siteReadyAt <= ahora: se envía el link, se marca "Web enviada".
 */
export async function enviarSitiosPendientes() {
  return withTaskLock('enviarSitiosPendientes', 30, async () => {
    console.log('⏳ Buscando negocios procesados para enviar sitio web...');
    const snap = await db.collection('Negocios').where('status', '==', 'Procesado').get();
    console.log(`[DEBUG] Encontrados: ${snap.size} negocios para enviar`);

    const nowMs = Date.now();

    for (const doc of snap.docs) {
      const data = doc.data();
      const hasReady = !!data.siteReadyAt;
      const readyMs = data.siteReadyAt?.toMillis?.() ?? null;

      // 1) Si NO tiene 'siteReadyAt', programarlo a +15–25 min y continuar
      if (!hasReady) {
        const jitter = Math.floor(Math.random() * (10 * 60 * 1000)); // 0–10 min
        const target = nowMs + (15 * 60 * 1000) + jitter;           // 15–25 min
        await doc.ref.update({
          siteReadyAt: Timestamp.fromMillis(target),
          siteScheduleSetAt: FieldValue.serverTimestamp(),
        });
        console.log(`[DEBUG] Programado siteReadyAt para ${doc.id} en ${new Date(target).toISOString()}`);
        continue;
      }

      // 2) Si tiene 'siteReadyAt' pero aún no llega, omitir
      if (readyMs && readyMs > nowMs) {
        console.log(`[DEBUG] ${doc.id} aún no alcanza siteReadyAt (${new Date(readyMs).toISOString()})`);
        continue;
      }

      // 3) Ya es hora: enviar
      console.log(`[DEBUG] Enviando sitio para negocio: ${doc.id}`, {
        leadPhone: data.leadPhone,
        slug: data.slug,
        schemaSlug: data.schema?.slug,
        status: data.status,
      });

      await enviarSitioWebPorWhatsApp(data);

      // 4) Marcar como enviado
      await doc.ref.update({
        status: 'Web enviada',
        siteSentAt: FieldValue.serverTimestamp(),
      });
    }

    return snap.size;
  });
}

/* ====================== Archivar >24h sin plan ========================= */
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

/* ======================= Proceso de secuencias ========================= */
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
