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
// Reemplaza TODO tu generateSiteSchemas() por esto:





// ====================== Generación del site schema (versión "completa") ======================
export async function generateSiteSchemas() {
  const BATCH = 6;
  const snap = await db.collection('Negocios')
    .where('status', '==', 'Sin procesar')
    .limit(BATCH)
    .get();

  if (snap.empty) {
    console.log('generateSiteSchemas: sin pendientes');
    return;
  }

  const safe = (v, def = '') => (v === undefined || v === null ? def : v);

  for (const doc of snap.docs) {
    const id   = doc.id;
    const data = doc.data() || {};

    // Insumos del brief
    const companyName   = safe(data.companyInfo, 'Tu Negocio');
    const businessStory = safe(data.businessStory, 'Ayudamos a nuestros clientes con productos y servicios de calidad.');
    const primaryColor  = safe(data.primaryColor, null);
    const palette       = Array.isArray(data.palette) && data.palette.length ? data.palette : (primaryColor ? [primaryColor] : []);
    const logoURL       = safe(data.logoURL, '');
    const photoURLs     = Array.isArray(data.photoURLs) ? data.photoURLs : [];
    const whatsapp      = safe(data.contactWhatsapp || data.leadPhone, '');
    const email         = safe(data.contactEmail, '');
    const facebook      = safe(data.socialFacebook, '');
    const instagram     = safe(data.socialInstagram, '');
    const youtube       = safe(data.socialYoutube || data.socialYouTube, '');
    const address       = safe(data.businessAddress, '');
    const templateId    = 'info'; // Para este layout completo usamos "info" como base
    const slug          = safe(data.slug, id).toLowerCase();

    // Prompt de salida estricta al esquema "completo" (menú ligero + muchas secciones)
    const prompt = `
Eres un arquitecto UX/UI y copywriter 2025. Devuelve EXCLUSIVAMENTE un objeto JSON válido (sin texto extra).
Genera el contenido de un sitio de información/servicios "completo", moderno, mobile-first, basado en:

- nombre: ${companyName}
- descripcion: ${businessStory}
- whatsapp: ${whatsapp}
- email: ${email}
- redes: facebook=${facebook} instagram=${instagram} youtube=${youtube}
- logoURL: ${logoURL}
- photoURLs: ${photoURLs.join(', ')}

El menú debe ser LIGERO con solo estas entradas en orden:
  hero, about, pricing, faqs, contact

Claves obligatorias del JSON y formato exacto:

{
  "slug": "${slug}",
  "templateId": "info",
  "colors": {
    "primary": "${primaryColor || (palette[0] || '#22C55E')}",
    "secondary": "#FFFFFF",
    "accent": "#DCFCE7",
    "text": "#1F2937"
  },
  "menu": [
    { "id": "hero", "label": "Inicio" },
    { "id": "about", "label": "Nosotros" },
    { "id": "pricing", "label": "Planes" },
    { "id": "faqs", "label": "Preguntas" },
    { "id": "contact", "label": "Contacto" }
  ],

  "hero": {
    "title": "Título potente de máximo 8 palabras",
    "subtitle": "Beneficio claro en una línea",
    "ctaText": "Hablar por WhatsApp",
    "backgroundImageUrl": "${photoURLs[0] || 'https://source.unsplash.com/1600x900/?business'}",
    "kpis": [
      { "label": "Clientes felices", "value": "1.8K+" },
      { "label": "Años", "value": "5+" }
    ],
    "badges": [
      { "text": "Atención rápida" },
      { "text": "Primera consulta gratis" }
    ]
  },

  "about": {
    "title": "Conoce a ${companyName}",
    "text": "Descripción breve (2–3 frases) sobre el negocio, enfoque y propuesta de valor.",
    "features": [
      { "icon": "UserOutlined", "title": "Equipo experto", "text": "Profesionales certificados." },
      { "icon": "HeartOutlined", "title": "Cercanos a ti", "text": "Acompañamiento por WhatsApp." }
    ]
  },

  "benefits": {
    "title": "Beneficios de elegirnos",
    "items": [
      { "icon": "SmileOutlined", "title": "Experiencia positiva", "text": "Atención clara y resultados palpables." },
      { "icon": "ThunderboltOutlined", "title": "Rápidos", "text": "Respuesta y entrega ágil." },
      { "icon": "SafetyOutlined", "title": "Confiables", "text": "Políticas transparentes y soporte real." }
    ]
  },

  "howItWorks": {
    "title": "Cómo funciona",
    "steps": [
      { "icon": "NumberOutlined", "title": "1) Cuéntanos tus metas", "text": "Envíanos un mensaje con lo que necesitas." },
      { "icon": "NumberOutlined", "title": "2) Te proponemos un plan", "text": "Opciones claras y transparentes." },
      { "icon": "NumberOutlined", "title": "3) Empezamos", "text": "Seguimiento por WhatsApp." }
    ],
    "videoUrl": ""
  },

  "stats": [
    { "label": "Clientes", "value": "1.8K+" },
    { "label": "Proyectos", "value": "3.2K+" },
    { "label": "Años", "value": "5+" }
  ],

  "useCases": {
    "title": "Casos de éxito",
    "items": [
      { "title": "Caso A", "text": "Resultado logrado en poco tiempo.", "imageUrl": "${photoURLs[1] || 'https://source.unsplash.com/1200x800/?success'}" },
      { "title": "Caso B", "text": "Mejoras claras y medibles.",       "imageUrl": "${photoURLs[2] || 'https://source.unsplash.com/1200x800/?team'}" }
    ]
  },

  "pricing": {
    "title": "Planes y precios",
    "subtitle": "Elige el ideal para ti",
    "plans": [
      {
        "name": "Básico",
        "price": "$299",
        "period": "por mes",
        "features": ["Asesoría inicial", "Soporte por chat", "Entrega en 48h"],
        "ctaText": "Comenzar ahora"
      },
      {
        "name": "Premium",
        "price": "$499",
        "period": "por mes",
        "features": ["Seguimiento semanal", "Ajustes personalizados", "Atención prioritaria"],
        "ctaText": "Agendar cita"
      }
    ]
  },

  "testimonials": {
    "title": "Lo que dicen",
    "items": [
      { "text": "Excelente servicio, superaron mis expectativas.", "author": "Cliente 1", "imageUrl": "" },
      { "text": "Atención rápida y resultados reales.",             "author": "Cliente 2", "imageUrl": "" }
    ]
  },

  "faqs": [
    { "q": "¿Cómo empiezo?", "a": "Escríbenos por WhatsApp y te guiamos paso a paso." },
    { "q": "¿Puedo pagar mensual?", "a": "Sí, aceptamos pagos mensuales." }
  ],

  "gallery": {
    "images": [
      "${photoURLs[0] || 'https://source.unsplash.com/1200x800/?business,team'}",
      "${photoURLs[1] || 'https://source.unsplash.com/1200x800/?office'}",
      "${photoURLs[2] || 'https://source.unsplash.com/1200x800/?people,work'}"
    ]
  },

  "policies": {
    "shipping": "",
    "payments": "Aceptamos tarjeta y transferencia.",
    "refunds": "Cancelación mensual sin permanencia."
  },

  "contact": {
    "email": "${email}",
    "whatsapp": "${whatsapp}",
    "facebook": "${facebook}",
    "instagram": "${instagram}",
    "youtube": "${youtube}"
  },

  "hours": [
    { "day": "Lun-Vie", "open": "09:00", "close": "19:00" },
    { "day": "Sáb",     "open": "10:00", "close": "14:00" }
  ],

  "location": {
    "address": "${address}",
    "mapEmbed": ""
  },

  "ctaFinal": {
    "title": "¿Listo para empezar?",
    "text": "Escríbenos y te respondemos hoy mismo.",
    "ctaText": "Hablar por WhatsApp"
  },

  "footer": {
    "links": [
      { "label": "Política de privacidad", "href": "#policies" },
      { "label": "Contacto", "href": "#contact" }
    ]
  }
}
    `;

    try {
      const resultText = await chatCompletionCompat({
        model: process.env.OPENAI_MODEL || 'gpt-4.1-mini',
        messages: [
          { role: 'system', content: 'Eres un generador de JSON estricto. Responde SOLO con un objeto JSON válido.' },
          { role: 'user', content: prompt }
        ],
        max_tokens: 1600,
        temperature: 0.55
      });

      let schema = {};
      try {
        const trimmed  = (resultText || '').trim();
        const unfenced = trimmed.replace(/```(?:json)?\s*([\s\S]*?)```/i, '$1').trim();
        const start    = unfenced.indexOf('{');
        const end      = unfenced.lastIndexOf('}');
        const candidate = (start >= 0 && end >= 0 && end > start) ? unfenced.slice(start, end + 1) : unfenced;

        const repaired = jsonrepair(candidate);
        schema = JSON.parse(repaired);
      } catch (parseErr) {
        console.error('[generateSiteSchemas] JSON parse error:', parseErr);
        // Fallback mínimo para no dejar el doc en limbo
        schema = {
          slug,
          templateId,
          colors: { primary: (palette[0] || '#22C55E'), secondary: '#FFFFFF', accent: '#DCFCE7', text: '#1F2937' },
          hero: {
            title: companyName,
            subtitle: businessStory,
            ctaText: 'Hablar por WhatsApp',
            backgroundImageUrl: photoURLs[0] || ''
          },
          menu: [
            { id: 'hero', label: 'Inicio' },
            { id: 'about', label: 'Nosotros' },
            { id: 'pricing', label: 'Planes' },
            { id: 'faqs', label: 'Preguntas' },
            { id: 'contact', label: 'Contacto' }
          ]
        };
      }

      // Normalizaciones críticas
      schema.slug = schema.slug || slug;
      schema.templateId = 'info';

      schema.colors = schema.colors || {};
      schema.colors.primary   = schema.colors.primary   || (palette[0] || '#22C55E');
      schema.colors.secondary = schema.colors.secondary || '#FFFFFF';
      schema.colors.accent    = schema.colors.accent    || '#DCFCE7';
      schema.colors.text      = schema.colors.text      || '#1F2937';

      if (!Array.isArray(schema.menu) || !schema.menu.length) {
        schema.menu = [
          { id: 'hero', label: 'Inicio' },
          { id: 'about', label: 'Nosotros' },
          { id: 'pricing', label: 'Planes' },
          { id: 'faqs', label: 'Preguntas' },
          { id: 'contact', label: 'Contacto' }
        ];
      }

      schema.hero = schema.hero || {};
      schema.hero.title               = schema.hero.title               || companyName;
      schema.hero.subtitle            = schema.hero.subtitle            || businessStory;
      schema.hero.ctaText             = schema.hero.ctaText             || 'Hablar por WhatsApp';
      schema.hero.backgroundImageUrl  = schema.hero.backgroundImageUrl  || (photoURLs[0] || '');

      schema.contact = schema.contact || {};
      schema.contact.whatsapp = schema.contact.whatsapp || whatsapp || '';
      schema.contact.email    = schema.contact.email    || email    || '';
      schema.contact.facebook = schema.contact.facebook || facebook || '';
      schema.contact.instagram= schema.contact.instagram|| instagram|| '';
      if (youtube) schema.contact.youtube = schema.contact.youtube || youtube;

      // Asegurar estructuras opcionales
      if (!schema.about)       schema.about = { title: `Conoce a ${companyName}`, text: businessStory, features: [] };
      if (!schema.benefits)    schema.benefits = { title: 'Beneficios', items: [] };
      if (!schema.howItWorks)  schema.howItWorks = { title: 'Cómo funciona', steps: [] };
      if (!schema.stats)       schema.stats = [];
      if (!schema.useCases)    schema.useCases = { title: 'Casos de éxito', items: [] };
      if (!schema.pricing)     schema.pricing = { title: 'Planes', subtitle: '', plans: [] };
      if (!schema.testimonials)schema.testimonials = { title: 'Lo que dicen', items: [] };
      if (!schema.faqs)        schema.faqs = [];
      if (!schema.gallery)     schema.gallery = { images: photoURLs.slice(0,3) };
      if (!schema.policies)    schema.policies = { shipping: '', payments: '', refunds: '' };
      if (!schema.hours)       schema.hours = [
        { day: 'Lun-Vie', open: '09:00', close: '19:00' },
        { day: 'Sáb',     open: '10:00', close: '14:00' }
      ];
      if (!schema.location)    schema.location = { address, mapEmbed: '' };
      if (!schema.ctaFinal)    schema.ctaFinal = { title: '¿Listo para empezar?', text: 'Escríbenos por WhatsApp.', ctaText: 'Hablar por WhatsApp' };
      if (!schema.footer)      schema.footer = { links: [{ label: 'Política de privacidad', href: '#policies' }, { label: 'Contacto', href: '#contact' }] };

      // Persistencia
      await db.collection('Negocios').doc(id).set({
        status: 'Procesado',
        siteSchema: schema,     // renderer principal
        schema,                 // compat
        colors: schema.colors,
        contact: {
          whatsapp: schema.contact.whatsapp || '',
          email:    schema.contact.email    || '',
          facebook: schema.contact.facebook || '',
          instagram:schema.contact.instagram|| '',
          ...(schema.contact.youtube ? { youtube: schema.contact.youtube } : {})
        },
        hero: {
          title: schema.hero.title,
          subtitle: schema.hero.subtitle,
          backgroundImageUrl: schema.hero.backgroundImageUrl || ''
        },
        updatedAt: Timestamp.now(),
        lastGeneratedAt: Timestamp.now()
      }, { merge: true });

      console.log(`[generateSiteSchemas] OK → ${id} (${slug})`);
    } catch (err) {
      console.error('[generateSiteSchemas] error con negocio', id, err?.message || err);
      await db.collection('Negocios').doc(id).set({
        status: 'Error',
        lastError: String(err?.message || err)
      }, { merge: true });
    }
  }
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
