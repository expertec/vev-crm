// server.js - COMPLETO CON SISTEMA DE PIN + STRIPE WEBHOOK FIX
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
import ffprobeInstaller from '@ffprobe-installer/ffprobe';
import dayjs from 'dayjs';
import slugify from 'slugify';
import axios from 'axios';
import { Timestamp } from 'firebase-admin/firestore';

dotenv.config();

// ================ FFmpeg ================
ffmpeg.setFfmpegPath(ffmpegInstaller.path);
ffmpeg.setFfprobePath(ffprobeInstaller.path);

function getAudioDurationSeconds(filePath) {
  return new Promise((resolve) => {
    ffmpeg.ffprobe(filePath, (error, data) => {
      if (error) return resolve(null);
      const seconds = Number(data?.format?.duration || 0);
      if (!Number.isFinite(seconds) || seconds <= 0) return resolve(null);
      return resolve(seconds);
    });
  });
}

// ================ Firebase / WhatsApp ================
import { admin, db } from './firebaseAdmin.js';
import {
  connectToWhatsApp,
  getLatestQR,
  getConnectionStatus,
  getWhatsAppSock,
  sendMessageToLead,
  sendImageToLead,
  getSessionPhone,
  sendAudioMessage,
  sendVideoNote,
} from './whatsappService.js';

// ================ SUSCRIPCIONES STRIPE ================
import subscriptionRoutes, { subscriptionRedirectSuccess, subscriptionRedirectCancel } from './subscriptionRoutes.js';

// ================ MERCADO PAGO CHECKOUT PRO ================
import mercadopagoRoutes from './mercadopagoRoutes.js';

// ================ STRIPE PAGOS ÚNICOS ================
import stripeOneTimeRoutes from './stripeOneTimeRoutes.js';

// ================ Secuencias / Scheduler (web) ================
import {
  processSequences,
  generateSiteSchemas,
  archivarNegociosAntiguos,
  enviarSitiosPendientes,
} from './scheduler.js';

// ================ 🆕 SISTEMA DE PIN ================
import { activarPlan, reenviarPIN } from './activarPlanRoutes.js';

// ================ 🆕 AUTENTICACIÓN DE CLIENTE ================
import { loginCliente, verificarSesion, logoutCliente } from './clienteAuthRoutes.js';
import { createProcessInformationRouter } from './routes/processInformationRoutes.js';

// (opcional) queue helpers
let cancelSequences = null;
let cancelAllSequences = null;
let scheduleSequenceForLead = null;
try {
  const q = await import('./queue.js');
  cancelSequences = q.cancelSequences || null;
  cancelAllSequences = q.cancelAllSequences || null;
  scheduleSequenceForLead = q.scheduleSequenceForLead || null;
} catch {
  /* noop */
}

// ================ OpenAI compat (para mensajes GPT) ================
import OpenAIImport from 'openai';
const OpenAICtor = OpenAIImport?.OpenAI || OpenAIImport;

import { classifyBusiness } from './utils/businessClassifier.js';

function normalizeJidCandidate(value = '') {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (!raw.includes('@')) return '';
  const [user, domain] = raw.split('@');
  const cleanUser = String(user || '').split(':')[0].replace(/\s+/g, '');
  return `${cleanUser}@${domain}`;
}

function isSuspiciousPseudoPhoneJid(value = '') {
  const normalized = normalizeJidCandidate(value);
  if (!normalized.endsWith('@s.whatsapp.net')) return false;
  const [user] = normalized.split('@');
  const digits = String(user || '').replace(/\D/g, '');
  return digits.length > 13;
}

function isSafeMxPhone(value = '') {
  const digits = String(value || '').replace(/\D/g, '');
  return /^\d{10}$/.test(digits) || /^52\d{10}$/.test(digits) || /^521\d{10}$/.test(digits);
}

function getUnsafeLeadTargetError(leadId, leadData = {}) {
  const resolvedJid = String(leadData.resolvedJid || '').trim();
  const jid = String(leadData.jid || '').trim();
  const lidJid = String(leadData.lidJid || '').trim();
  const phone = String(leadData.telefono || '').replace(/\D/g, '');

  const suspicious = [resolvedJid, jid].find((candidate) => isSuspiciousPseudoPhoneJid(candidate));
  if (!suspicious) return '';
  if (lidJid && /@lid$/i.test(lidJid)) return '';
  if (isSafeMxPhone(phone)) return '';

  return `Lead ${leadId} tiene un destino WhatsApp no confiable (${suspicious}). Espera un mensaje entrante para resolver su JID real.`;
}

function getUnsafeIdentifierTargetError(identifier = '') {
  const raw = String(identifier || '').trim();
  if (!raw) return 'Lead sin identificador de destino.';

  const normalizedJid = normalizeJidCandidate(raw);
  if (normalizedJid) {
    if (isSuspiciousPseudoPhoneJid(normalizedJid)) {
      return `Destino WhatsApp no confiable (${normalizedJid}). Espera un mensaje entrante para resolver su JID real.`;
    }
    return '';
  }

  const digits = raw.replace(/\D/g, '');
  if (!digits) {
    return `Lead ${raw} no existe y no tiene un identificador de WhatsApp válido para registrarlo automáticamente.`;
  }
  if (!isSafeMxPhone(digits)) {
    return `Destino WhatsApp no confiable (${raw}). Usa un número MX válido o espera un mensaje entrante.`;
  }
  return '';
}

function buildUnsplashFeaturedQueries(summary = {}) {
  const objetivoMap = {
    ecommerce: 'tienda online,productos',
    booking: 'reservas,servicios,agenda',
    info: 'negocio local',
  };
  const objetivo =
    objetivoMap[String(summary.templateId || '').toLowerCase()] ||
    'negocio local';

  const nombre = (
    summary.companyName ||
    summary.name ||
    summary.slug ||
    ''
  )
    .toString()
    .trim();

  const descTop = (summary.description || '')
    .toString()
    .replace(/https?:\/\/\S+/g, '')
    .replace(/[^\p{L}\p{N}\s]/gu, '')
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 4)
    .join(' ');

  const terms = [objetivo, nombre, descTop].filter(Boolean).join(',');
  const q = encodeURIComponent(terms);
  const w = 1200,
    h = 800;

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

async function getStockPhotoUrls(summary, count = 3) {
  const { sector, keywords } = await classifyBusiness(summary);

  const objetivoMap = {
    ecommerce: 'tienda online productos',
    booking: 'reservas servicios agenda',
    info: 'negocio local',
  };
  const objetivo =
    objetivoMap[String(summary?.templateId || '').toLowerCase()] ||
    'negocio local';
  const nombre = (
    summary?.companyName ||
    summary?.name ||
    summary?.slug ||
    ''
  )
    .toString()
    .trim();

  const query = [sector, keywords, objetivo, nombre]
    .filter(Boolean)
    .join(' ')
    .trim();

  const apiKey = process.env.PEXELS_API_KEY;
  if (apiKey) {
    try {
      const url = `https://api.pexels.com/v1/search?query=${encodeURIComponent(
        query
      )}&per_page=${count}&orientation=landscape&locale=es-ES`;
      const { data } = await axios.get(url, {
        headers: { Authorization: apiKey },
      });
      const photos = Array.isArray(data?.photos) ? data.photos : [];
      const pexelsUrls = photos
        .slice(0, count)
        .map(
          (p) =>
            p?.src?.landscape ||
            p?.src?.large2x ||
            p?.src?.large ||
            p?.src?.original
        )
        .filter(Boolean);
      if (pexelsUrls.length) return pexelsUrls;
    } catch (e) {
      console.error('[getStockPhotoUrls] Pexels error:', e?.message || e);
    }
  }

  const termsForUnsplash = [sector, keywords, objetivo, nombre]
    .filter(Boolean)
    .join(',');
  const q = encodeURIComponent(termsForUnsplash);
  const w = 1200,
    h = 800;
  const sourceList = [
    `https://source.unsplash.com/featured/${w}x${h}/?${q}&sig=1`,
    `https://source.unsplash.com/featured/${w}x${h}/?${q}&sig=2`,
    `https://source.unsplash.com/featured/${w}x${h}/?${q}&sig=3`,
  ];
  const finals = [];
  for (const u of sourceList) finals.push(await resolveUnsplashFinalUrl(u));
  return finals.filter(Boolean);
}

async function uploadBase64Image({
  base64,
  folder = 'web-assets',
  filenamePrefix = 'img',
  contentType = 'image/png',
}) {
  if (!base64) return null;
  try {
    const matches = String(base64).match(
      /^data:(image\/[a-zA-Z0-9.+-]+);base64,(.*)$/
    );
    const mime = matches ? matches[1] : contentType || 'image/png';
    const b64 = matches ? matches[2] : base64;

    const buffer = Buffer.from(b64, 'base64');
    const ts = Date.now();
    const fileName = `${folder}/${filenamePrefix}_${ts}.png`;
    const file = admin.storage().bucket().file(fileName);

    await file.save(buffer, {
      contentType: mime,
      metadata: { cacheControl: 'public,max-age=31536000' },
      resumable: false,
      public: true,
      validation: false,
    });

    try {
      await file.makePublic();
    } catch {
      /* noop */
    }

    return `https://storage.googleapis.com/${admin.storage().bucket().name}/${fileName}`;
  } catch (err) {
    console.error('[uploadBase64Image] error:', err);
    return null;
  }
}

async function getOpenAI() {
  if (!process.env.OPENAI_API_KEY)
    throw new Error('Falta OPENAI_API_KEY');

  try {
    const client = new OpenAICtor({
      apiKey: process.env.OPENAI_API_KEY,
    });
    const hasChatCompletions =
      !!client?.chat?.completions?.create;
    if (hasChatCompletions)
      return { client, mode: 'v4-chat' };
  } catch {
    /* noop */
  }

  const { Configuration, OpenAIApi } =
    await import('openai');
  const configuration = new Configuration({
    apiKey: process.env.OPENAI_API_KEY,
  });
  const client = new OpenAIApi(configuration);
  return { client, mode: 'v3' };
}

function extractText(resp, mode) {
  try {
    if (mode === 'v4-chat') {
      return (
        resp?.choices?.[0]?.message?.content?.trim() ||
        ''
      );
    }
    if (mode === 'v4-resp') {
      return (
        resp?.output_text?.trim?.() ||
        resp?.output?.[0]?.content?.[0]?.text
          ?.trim?.() ||
        ''
      );
    }
    return (
      resp?.data?.choices?.[0]?.message?.content?.trim() ||
      ''
    );
  } catch {
    return '';
  }
}

async function chatCompletionCompat({
  model,
  messages,
  max_tokens = 300,
  temperature = 0.55,
}) {
  const { client, mode } = await getOpenAI();
  if (mode === 'v4-chat') {
    const resp = await client.chat.completions.create({
      model,
      messages,
      max_tokens,
      temperature,
    });
    return extractText(resp, mode);
  }
  if (mode === 'v4-resp') {
    const input = messages
      .map((m) => `${m.role.toUpperCase()}: ${m.content}`)
      .join('\n\n');
    const resp = await client.responses.create({
      model,
      input,
    });
    return extractText(resp, mode);
  }
  const resp = await client.createChatCompletion({
    model,
    messages,
    max_tokens,
    temperature,
  });
  return extractText(resp, 'v3');
}

// ================ Teléfonos helpers ================
import { parsePhoneNumberFromString } from 'libphonenumber-js';
function toE164(num, defaultCountry = 'MX') {
  const raw = String(num || '').replace(/\D/g, '');
  const p = parsePhoneNumberFromString(raw, defaultCountry);
  if (p && p.isValid()) return p.number;
  if (/^\d{10}$/.test(raw)) return `+52${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('521')) return `+${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('52')) return `+${raw}`;
  return `+${raw}`;
}
function e164ToLeadId(e164) {
  const digits = String(e164 || '').replace(/\D/g, '');
  return `${digits}@s.whatsapp.net`;
}
function firstName(n = '') {
  return String(n).trim().split(/\s+/)[0] || '';
}

// ================== Personalización por giro ==================
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
  otros: ['negocio'],
};

function humanizeGiro(code = '') {
  const c = String(code || '').toLowerCase();
  if (GIRO_ALIAS[c]) return GIRO_ALIAS[c][0];
  return (
    c
      .replace(/([a-z])([A-Z])/g, '$1 $2')
      .replace(/[_-]+/g, ' ')
      .trim() || 'negocio'
  );
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

function normalizeFold(text = '') {
  return String(text || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
}

function uniqueClean(items = []) {
  const out = [];
  const seen = new Set();
  for (const item of items) {
    const clean = String(item || '')
      .replace(/\s+/g, ' ')
      .trim();
    if (!clean) continue;
    const key = clean.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(clean);
  }
  return out;
}

function normalizeKeyItems(keyItems = []) {
  if (!Array.isArray(keyItems)) return [];
  return keyItems
    .map((item) => {
      if (item && typeof item === 'object') {
        return String(
          item.label ||
            item.name ||
            item.title ||
            item.text ||
            item.value ||
            ''
        ).trim();
      }
      return String(item || '').trim();
    })
    .filter(Boolean);
}

function formatTriplet(items = []) {
  const top = uniqueClean(items).slice(0, 3);
  return top
    .map((line, i) => `${i + 1}) ${line}`)
    .join('\n');
}

function parseRecommendations(raw = '') {
  const txt = String(raw || '').replace(/\r/g, '\n').trim();
  if (!txt) return [];

  let lines = txt
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean);

  if (lines.length < 3) {
    lines = txt
      .split(/(?:\n|;\s+|\.\s+)/g)
      .map((l) => l.trim())
      .filter(Boolean);
  }

  return uniqueClean(
    lines.map((line) =>
      line
        .replace(/^\s*\d+\s*[.)-]?\s*/, '')
        .replace(/^\s*[-*•]+\s*/, '')
        .trim()
    )
  ).slice(0, 3);
}

async function buildAfterFormRecommendations({
  summary = {},
  giroHumano = 'negocio',
  fallbackTriplet = [],
}) {
  const probe = normalizeFold(
    [
      summary?.companyName,
      summary?.name,
      summary?.description,
      summary?.businessStory,
      summary?.templateId,
      giroHumano,
    ]
      .filter(Boolean)
      .join(' ')
  );
  let sectorClassified = giroHumano || 'negocio';
  if (/(restaurante|comida|cafeteria|pizza|taquer|bar)/.test(probe)) {
    sectorClassified = 'restaurante';
  } else if (/(spa|belleza|barber|estetica|unas)/.test(probe)) {
    sectorClassified = 'spa y belleza';
  } else if (/(clinica|salud|dent|medic|terapia|psicolog)/.test(probe)) {
    sectorClassified = 'clinica/salud';
  } else if (/(inmobili|bienes raices|departament|casa|renta|venta)/.test(probe)) {
    sectorClassified = 'inmobiliaria';
  } else if (/(tienda|ecommerce|producto|catalogo|ropa|boutique)/.test(probe)) {
    sectorClassified = 'tienda online';
  } else if (/(curso|academ|escuela|clase|capacitacion|taller)/.test(probe)) {
    sectorClassified = 'escuela/cursos';
  } else if (/(abogad|conta|consultor|agencia|marketing|diseno)/.test(probe)) {
    sectorClassified = 'servicios profesionales';
  } else if (/(booking|reserva|agenda|cita)/.test(probe)) {
    sectorClassified = 'servicios con reservas';
  }

  const templateId = String(
    summary?.templateId || ''
  ).toLowerCase();
  const normalizedKeyItems = normalizeKeyItems(summary?.keyItems);
  const keyItems = normalizedKeyItems.slice(0, 4);
  const sectorNorm = normalizeFold(sectorClassified);

  const personalized = [];

  if (keyItems[0]) {
    personalized.push(
      `Destaca "${keyItems[0]}" en portada con beneficio claro y CTA a WhatsApp`
    );
  }
  if (keyItems[1]) {
    personalized.push(
      `Crea bloque específico para "${keyItems[1]}" con precio base y tiempos`
    );
  }

  if (/(restaurante|cafeteria|bar)/.test(sectorNorm)) {
    personalized.push(
      'Menu y precios visibles desde el primer scroll con fotos reales'
    );
    personalized.push(
      'Boton fijo para pedir o reservar por WhatsApp en todo momento'
    );
    personalized.push(
      'Horarios, ubicacion y mapa accesibles sin navegar varias secciones'
    );
  } else if (
    /(clinica|salud|dent|medic|terapia)/.test(
      sectorNorm
    )
  ) {
    personalized.push(
      'Explica cada servicio con sintomas que resuelve y rango de precio'
    );
    personalized.push(
      'Agenda por WhatsApp en 1 paso con horarios disponibles'
    );
    personalized.push(
      'Refuerza confianza con credenciales, reseñas y protocolos visibles'
    );
  } else if (
    /(spa|belleza|barber|estetica)/.test(sectorNorm)
  ) {
    personalized.push(
      'Galeria antes/despues con resultados reales y servicios destacados'
    );
    personalized.push(
      'Reservacion rapida por WhatsApp con promos de primera visita'
    );
    personalized.push(
      'Muestra paquetes, duracion y cuidados posteriores por servicio'
    );
  } else if (
    /(tienda online|ecommerce|tienda de ropa|retail)/.test(
      sectorNorm
    )
  ) {
    personalized.push(
      'Categorias simples para encontrar productos en menos clics'
    );
    personalized.push(
      'Incluye envios, cambios y metodos de pago antes de comprar'
    );
    personalized.push(
      'Productos estrella con prueba social y CTA de compra/WhatsApp'
    );
  } else if (
    /(inmobiliaria|bienes raices|propiedad)/.test(
      sectorNorm
    )
  ) {
    personalized.push(
      'Fichas por propiedad con precio, zona, metraje y fotos reales'
    );
    personalized.push(
      'Boton para agendar visita por WhatsApp desde cada inmueble'
    );
    personalized.push(
      'Filtros por presupuesto, ubicacion y tipo para reducir friccion'
    );
  } else if (
    /(servicios profesionales|consultor|abogad|conta|marketing)/.test(
      sectorNorm
    )
  ) {
    personalized.push(
      'Presenta servicios por resultado esperado, no solo por nombre'
    );
    personalized.push(
      'Incluye casos de exito cortos con metrica de impacto'
    );
    personalized.push(
      'CTA para diagnostico inicial por WhatsApp con respuesta rapida'
    );
  } else {
    personalized.push(
      'Propuesta de valor clara en portada y llamada a la accion inmediata'
    );
    personalized.push(
      'Bloques cortos por servicio/producto con beneficio y prueba social'
    );
    personalized.push(
      'Contacto por WhatsApp visible en toda la navegacion'
    );
  }

  if (templateId === 'ecommerce') {
    personalized.push(
      'Checkout simple: pocos pasos, costos claros y pago confiable'
    );
  }
  if (templateId === 'booking') {
    personalized.push(
      'Agenda de disponibilidad con confirmacion por WhatsApp automatizada'
    );
  }

  const fallback = uniqueClean([
    ...personalized,
    ...(Array.isArray(fallbackTriplet)
      ? fallbackTriplet
      : []),
    ...pickOpportunityTriplet(sectorClassified),
  ]).slice(0, 3);
  const fallbackText = formatTriplet(fallback);

  if (!process.env.OPENAI_API_KEY) {
    return {
      text: fallbackText,
      source: 'fallback_no_api_key',
      sector: sectorClassified,
    };
  }

  const companyName = String(
    summary?.companyName ||
      summary?.name ||
      'Negocio sin nombre'
  ).trim();
  const description = String(
    summary?.description ||
      summary?.businessStory ||
      ''
  ).trim();
  const keyItemsText = normalizedKeyItems
    .slice(0, 6)
    .join(', ');
  try {
    const raw = await chatCompletionCompat({
      model:
        process.env.WEB_AFTER_FORM_GPT_MODEL ||
        process.env.OPENAI_MODEL ||
        'gpt-4o-mini',
      messages: [
        {
          role: 'system',
          content:
            'Eres consultor CRO/UX para sitios de negocio local. Devuelve 3 recomendaciones accionables, concretas y utiles para vender mas. Escribe en espanol neutro, en formato numerado 1) 2) 3), sin saludos ni cierre.',
        },
        {
          role: 'user',
          content: `Genera recomendaciones para una muestra web.
Negocio: ${companyName}
Tipo de negocio: ${sectorClassified}
Template: ${templateId || 'no definido'}
Descripcion: ${description || 'sin descripcion'}
Puntos clave: ${keyItemsText || 'sin puntos clave'}

Contexto: Las recomendaciones se enviaran por WhatsApp a un cliente real.
Necesito recomendaciones faciles de implementar en una landing/muestra.

Devuelve solo los 3 puntos numerados.`,
        },
      ],
      max_tokens: 280,
      temperature: 0.35,
    });

    const parsed = parseRecommendations(raw);
    if (!parsed.length) {
      return {
        text: fallbackText,
        source: 'fallback_empty_gpt',
        sector: sectorClassified,
      };
    }

    const merged = uniqueClean([...parsed, ...fallback]).slice(
      0,
      3
    );
    return {
      text: formatTriplet(merged),
      source: 'gpt',
      sector: sectorClassified,
    };
  } catch (err) {
    console.error(
      '[after-form] recomendaciones GPT fallback:',
      err?.message || err
    );
    return {
      text: fallbackText,
      source: 'fallback_error_gpt',
      sector: sectorClassified,
    };
  }
}

// ================ App base ================
const app = express();
const port = process.env.PORT || 3001;
const upload = multer({ dest: path.resolve('./uploads') });

// 1) CORS primero
app.use(cors());

/**
 * 2) Webhook de Stripe - debe ir ANTES del bodyParser.json
 *    para tener acceso al cuerpo RAW y validar la firma.
 */
app.post(
  '/api/subscription/webhook',
  express.raw({ type: 'application/json' }),
  subscriptionRoutes.stripeWebhook
);

// Redirecciones para limpiar session_id y evitar ModSecurity
app.get('/api/subscription/redirect-success', subscriptionRedirectSuccess);
app.get('/api/subscription/redirect-cancel', subscriptionRedirectCancel);

/**
 * 3) Body parsers para el resto de rutas
 */
app.use(bodyParser.json({ limit: '50mb' }));
app.use(
  bodyParser.urlencoded({ extended: true, limit: '50mb' })
);

// ============== 🆕 RUTAS DEL SISTEMA DE PIN ==============
app.post('/api/activar-plan', activarPlan);
app.post('/api/reenviar-pin', reenviarPIN);

// ============== 🆕 RUTAS DE SUSCRIPCIÓN CON STRIPE ==============
app.post(
  '/api/subscription/create-checkout',
  subscriptionRoutes.createCheckoutSession
);
app.post(
  '/api/subscription/cancel',
  subscriptionRoutes.cancelSubscription
);
app.post(
  '/api/subscription/portal',
  subscriptionRoutes.createPortalSession
);
app.post('/api/subscription/trial', subscriptionRoutes.activateTrial);
app.get(
  '/api/subscription/status/:negocioId',
  subscriptionRoutes.getSubscriptionStatus
);

// ============== 🆕 RUTAS DE MERCADO PAGO ==============
app.use('/api/mp', mercadopagoRoutes);

// ============== 🆕 RUTAS DE STRIPE PAGOS ÚNICOS ==============
app.use('/api/stripe-onetime', stripeOneTimeRoutes);

// ============== 🆕 RUTAS DE AUTENTICACIÓN DE CLIENTE ==============
app.post('/api/cliente/login', loginCliente);
app.post('/api/cliente/verificar-sesion', verificarSesion);
app.post('/api/cliente/logout', logoutCliente);

// ============== 🆕 FLUJO INFORMACION (SIN SECUENCIAS) ==============
app.use('/api/web', createProcessInformationRouter());

// ============== RUTAS EXISTENTES ==============

// Ruta de bienvenida
app.get('/', (req, res) => {
  res.json({ message: 'Servidor activo y corriendo 🚀' });
});

// WhatsApp status / número
app.get('/api/whatsapp/status', (_req, res) => {
  res.json({ status: getConnectionStatus(), qr: getLatestQR() });
});

app.get('/api/whatsapp/number', (_req, res) => {
  const phone = getSessionPhone();
  if (phone) return res.json({ phone });
  return res.status(503).json({ error: 'WhatsApp no conectado' });
});

// Enviar mensaje manual
app.post('/api/whatsapp/send-message', async (req, res) => {
  const {
    leadId,
    message,
    replyToWaMessageId = '',
    replyPreview = '',
    replySender = '',
  } = req.body || {};
  if (!leadId || !message)
    return res.status(400).json({ error: 'Faltan leadId o message' });

  try {
    const leadDoc = await db.collection('leads').doc(leadId).get();
    if (leadDoc.exists) {
      const unsafeTargetError = getUnsafeLeadTargetError(String(leadId), leadDoc.data() || {});
      if (unsafeTargetError) {
        return res.status(409).json({ error: unsafeTargetError });
      }
    } else {
      const unsafeIdentifierError = getUnsafeIdentifierTargetError(String(leadId));
      if (unsafeIdentifierError) {
        return res.status(409).json({ error: unsafeIdentifierError });
      }
    }
    const result = await sendMessageToLead(leadId, message, {
      replyToWaMessageId,
      replyPreview,
      replySender,
    });
    return res.json(result);
  } catch (error) {
    console.error('Error enviando WhatsApp:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Enviar imagen manual
app.post('/api/whatsapp/send-image', async (req, res) => {
  const { leadId, imageUrl, caption = '' } = req.body || {};
  if (!leadId || !imageUrl) {
    return res.status(400).json({ error: 'Faltan leadId o imageUrl' });
  }

  try {
    const leadDoc = await db.collection('leads').doc(String(leadId)).get();
    if (leadDoc.exists) {
      const unsafeTargetError = getUnsafeLeadTargetError(String(leadId), leadDoc.data() || {});
      if (unsafeTargetError) {
        return res.status(409).json({ error: unsafeTargetError });
      }
    } else {
      const unsafeIdentifierError = getUnsafeIdentifierTargetError(String(leadId));
      if (unsafeIdentifierError) {
        return res.status(409).json({ error: unsafeIdentifierError });
      }
    }

    const result = await sendImageToLead(String(leadId), String(imageUrl), String(caption || ''));
    return res.json(result);
  } catch (error) {
    console.error('Error enviando imagen por WhatsApp:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Reenviar audio por URL (sin subir archivo)
app.post('/api/whatsapp/send-audio-url', async (req, res) => {
  const {
    leadId,
    audioUrl,
    ptt = true,
    forwarded = true,
  } = req.body || {};

  if (!leadId || !audioUrl) {
    return res.status(400).json({ error: 'Faltan leadId o audioUrl' });
  }

  try {
    const leadDoc = await db.collection('leads').doc(String(leadId)).get();
    if (leadDoc.exists) {
      const unsafeTargetError = getUnsafeLeadTargetError(String(leadId), leadDoc.data() || {});
      if (unsafeTargetError) {
        return res.status(409).json({ error: unsafeTargetError });
      }
    } else {
      const unsafeIdentifierError = getUnsafeIdentifierTargetError(String(leadId));
      if (unsafeIdentifierError) {
        return res.status(409).json({ error: unsafeIdentifierError });
      }
    }

    await sendAudioMessage(String(leadId), String(audioUrl), {
      ptt: Boolean(ptt),
      forwarded: Boolean(forwarded),
    });
    return res.json({ success: true });
  } catch (error) {
    console.error('Error reenviando audio por URL:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Enviar mensajes masivos (secuencia)
app.post('/api/whatsapp/send-bulk-message', async (req, res) => {
  const { phones, messages } = req.body;
  if (!phones || !Array.isArray(phones) || phones.length === 0 || !messages || !Array.isArray(messages)) {
    return res.status(400).json({ error: 'Faltan phones (array), messages (array)' });
  }

  const results = [];
  for (const phone of phones) {
    try {
      let delayAccum = 0;
      for (const msg of messages) {
        setTimeout(async () => {
          try {
            if (msg.type === 'texto') {
              await sendMessageToLead(phone, msg.contenido);
            } else if (msg.type === 'imagen') {
              const sock = getWhatsAppSock();
              if (!sock) throw new Error('No hay conexión activa con WhatsApp');
              const num = normalizePhoneForWA(phone);
              const jid = `${num}@s.whatsapp.net`;
              await sock.sendMessage(jid, {
                image: { url: msg.contenido },
                caption: msg.caption || ''
              });
            } else if (msg.type === 'audio') {
              await sendAudioMessage(phone, msg.contenido, { ptt: true });
            } else if (msg.type === 'video') {
              const sock = getWhatsAppSock();
              if (!sock) throw new Error('No hay conexión activa con WhatsApp');
              const num = normalizePhoneForWA(phone);
              const jid = `${num}@s.whatsapp.net`;
              await sock.sendMessage(jid, {
                video: { url: msg.contenido },
                caption: msg.caption || ''
              });
            }
          } catch (err) {
            console.error(`Error enviando ${msg.type} a ${phone}:`, err);
          }
        }, delayAccum);
        delayAccum += (msg.delay || 0) * 60 * 1000; // delay en minutos
      }
      results.push({ phone, success: true });
    } catch (error) {
      console.error(`Error programando para ${phone}:`, error);
      results.push({ phone, success: false, error: error.message });
    }
  }

  const successCount = results.filter(r => r.success).length;
  const failCount = results.length - successCount;

  return res.json({
    total: results.length,
    success: successCount,
    failed: failCount,
    results
  });
});
// Enviar secuencia masiva
app.post('/api/whatsapp/send-bulk-sequence', async (req, res) => {
  const { phones, sequenceId } = req.body;
  if (!phones || !Array.isArray(phones) || phones.length === 0 || !sequenceId) {
    return res.status(400).json({ error: 'Faltan phones (array), sequenceId' });
  }

  try {
    const seqDoc = await db.collection('secuencias').doc(sequenceId).get();
    if (!seqDoc.exists) {
      return res.status(404).json({ error: 'Secuencia no encontrada' });
    }
    const sequence = seqDoc.data();
    const messages = sequence.messages || [];

    const results = [];
    for (const phone of phones) {
      try {
        let delayAccum = 0;
        for (const msg of messages) {
          setTimeout(async () => {
            try {
              if (msg.type === 'texto') {
                await sendMessageToLead(phone, msg.contenido);
              } else if (msg.type === 'imagen') {
                const sock = getWhatsAppSock();
                if (!sock) throw new Error('No hay conexión activa con WhatsApp');
                const num = normalizePhoneForWA(phone);
                const jid = `${num}@s.whatsapp.net`;
                await sock.sendMessage(jid, {
                  image: { url: msg.contenido },
                  caption: msg.caption || ''
                });
              } else if (msg.type === 'audio') {
                await sendAudioMessage(phone, msg.contenido, { ptt: true });
              } else if (msg.type === 'video') {
                const sock = getWhatsAppSock();
                if (!sock) throw new Error('No hay conexión activa con WhatsApp');
                const num = normalizePhoneForWA(phone);
                const jid = `${num}@s.whatsapp.net`;
                await sock.sendMessage(jid, {
                  video: { url: msg.contenido },
                  caption: msg.caption || ''
                });
              } else if (msg.type === 'videonota') {
                await sendVideoNote(phone, msg.contenido, msg.seconds || null);
              } else if (msg.type === 'formulario') {
                await sendMessageToLead(phone, msg.contenido);
              }
            } catch (err) {
              console.error(`Error enviando ${msg.type} a ${phone}:`, err);
            }
          }, delayAccum);
          delayAccum += (msg.delay || 0) * 60 * 1000; // delay en minutos
        }
        results.push({ phone, success: true });
      } catch (error) {
        console.error(`Error programando para ${phone}:`, error);
        results.push({ phone, success: false, error: error.message });
      }
    }

    const successCount = results.filter(r => r.success).length;
    const failCount = results.length - successCount;

    return res.json({
      total: results.length,
      success: successCount,
      failed: failCount,
      results
    });
  } catch (error) {
    console.error('Error obteniendo secuencia:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Enviar audio
app.post(
  '/api/whatsapp/send-audio',
  upload.single('audio'),
  async (req, res) => {
    const { phone, leadId, forwarded, ptt } = req.body;
    if (!req.file) {
      return res
        .status(400)
        .json({ success: false, error: 'Falta archivo de audio' });
    }
    if (!phone && !leadId) {
      return res
        .status(400)
        .json({ success: false, error: 'Falta phone o leadId' });
    }

    const uploadPath = req.file.path;
    const oggPath = `${uploadPath}.ogg`;
    const parseBool = (value, defaultValue = false) => {
      if (value === undefined || value === null || value === '') return defaultValue;
      if (typeof value === 'boolean') return value;
      return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
    };

    let target = phone;
    const isPttExplicit = ptt !== undefined && ptt !== null && ptt !== '';
    const shouldPtt = parseBool(ptt, true);
    const shouldForward = parseBool(forwarded, false);

    try {
      if (leadId) {
        const leadDoc = await db.collection('leads').doc(String(leadId)).get();
        if (leadDoc.exists) {
          const unsafeTargetError = getUnsafeLeadTargetError(String(leadId), leadDoc.data() || {});
          if (unsafeTargetError) {
            return res
              .status(409)
              .json({ success: false, error: unsafeTargetError });
          }
        } else {
          const unsafeIdentifierError = getUnsafeIdentifierTargetError(String(leadId));
          if (unsafeIdentifierError) {
            return res
              .status(409)
              .json({ success: false, error: unsafeIdentifierError });
          }
        }
        // Delegar resolución de destino a whatsappService para evitar enviar a IDs @lid convertidos.
        target = String(leadId);
      }
      if (!target) {
        return res
          .status(400)
          .json({ success: false, error: 'Lead sin destino de WhatsApp' });
      }

      let sourcePath = uploadPath;
      let sourceMime = req.file.mimetype || 'audio/ogg; codecs=opus';
      let finalPtt = shouldPtt;

      // Para notas de voz o formatos no compatibles, normalizar a OGG/Opus.
      await new Promise((resolve, reject) => {
        ffmpeg(uploadPath)
          .noVideo()
          .audioCodec('libopus')
          .audioChannels(1)
          .audioFrequency(48000)
          .audioBitrate('32k')
          .outputOptions([
            '-vbr on',
            '-compression_level 10',
            '-frame_duration 20',
            '-application voip',
            '-avoid_negative_ts make_zero',
          ])
          .toFormat('ogg')
          .save(oggPath)
          .on('end', resolve)
          .on('error', reject);
      });
      sourcePath = oggPath;
      sourceMime = 'audio/ogg; codecs=opus';
      finalPtt = isPttExplicit ? shouldPtt : true;
      const sourceSizeBytes = fs.statSync(sourcePath).size;
      console.log('[send-audio] Enviando audio convertido', {
        target: String(target),
        ptt: finalPtt,
        mimetype: sourceMime,
        bytes: sourceSizeBytes,
      });
      await sendAudioMessage(target, sourcePath, {
        ptt: finalPtt,
        forwarded: shouldForward,
        mimetype: sourceMime,
      });
      console.log('[send-audio] Audio enviado OK', { target: String(target) });

      try {
        fs.unlinkSync(uploadPath);
      } catch {}
      try {
        fs.unlinkSync(oggPath);
      } catch {}

      return res.json({ success: true });
    } catch (error) {
      console.error('Error enviando audio:', error);
      try {
        fs.unlinkSync(uploadPath);
      } catch {}
      try {
        fs.unlinkSync(oggPath);
      } catch {}
      return res
        .status(500)
        .json({ success: false, error: error.message });
    }
  }
);

// Crear usuario + bienvenida WA
app.post('/api/crear-usuario', async (req, res) => {
  const { email, negocioId } = req.body;
  if (!email || !negocioId)
    return res
      .status(400)
      .json({ error: 'Faltan email o negocioId' });

  try {
    const tempPassword = Math.random().toString(36).slice(-8);
    let userRecord,
      isNewUser = false;
    try {
      userRecord = await admin.auth().getUserByEmail(email);
    } catch {
      userRecord = await admin
        .auth()
        .createUser({ email, password: tempPassword });
      isNewUser = true;
    }

    await db
      .collection('Negocios')
      .doc(negocioId)
      .update({
        ownerUID: userRecord.uid,
        ownerEmail: email,
      });

    const negocioDoc = await db
      .collection('Negocios')
      .doc(negocioId)
      .get();
    const negocio = negocioDoc.data() || {};
    let telefono = toE164(negocio?.leadPhone);
    const urlAcceso = 'https://negociosweb.mx/login';

    let mensaje = `¡Bienvenido a tu panel de administración de tu página web! 👋

🔗 Accede aquí: ${urlAcceso}
📧 Usuario: ${email}
`;
    if (isNewUser)
      mensaje += `🔑 Contraseña temporal: ${tempPassword}\n`;
    else
      mensaje +=
        `🔄 Si no recuerdas tu contraseña, usa "¿Olvidaste tu contraseña?"\n`;

    let fechaCorte = '-';
    const d = negocio.planRenewalDate;
    if (d?.toDate)
      fechaCorte = dayjs(d.toDate()).format('DD/MM/YYYY');
    else if (d instanceof Date)
      fechaCorte = dayjs(d).format('DD/MM/YYYY');
    else if (
      typeof d === 'string' ||
      typeof d === 'number'
    )
      fechaCorte = dayjs(d).format('DD/MM/YYYY');
    mensaje += `\n🗓️ Tu plan termina el día: ${fechaCorte}\n\nPor seguridad, cambia tu contraseña después de ingresar.\n`;

    if (telefono && telefono.length >= 12) {
      try {
        await sendMessageToLead(telefono, mensaje);
      } catch (waError) {
        console.error('[CREAR USUARIO] Error WA:', waError);
      }
    }

    if (!isNewUser)
      await admin.auth().generatePasswordResetLink(email);
    return res.json({
      success: true,
      uid: userRecord.uid,
      email,
    });
  } catch (err) {
    console.error('Error creando usuario:', err);
    return res.status(500).json({ error: err.message });
  }
});

// Marcar como leídos
app.post(
  '/api/whatsapp/mark-read',
  async (req, res) => {
    const { leadId } = req.body;
    if (!leadId)
      return res
        .status(400)
        .json({ error: 'Falta leadId' });
    try {
      await db
        .collection('leads')
        .doc(leadId)
        .update({ unreadCount: 0 });
      return res.json({ success: true });
    } catch (err) {
      console.error('Error mark-read:', err);
      return res.status(500).json({ error: err.message });
    }
  }
);

// Forzar secuencia para rescate manual (crea/actualiza lead si falta)
app.post('/api/whatsapp/force-sequence', async (req, res) => {
  const {
    leadId: inputLeadId,
    phone,
    leadPhone,
    trigger: rawTrigger = 'LeadWhatsapp',
    markAsMetaAd = true,
    forceRestart = false,
  } = req.body || {};

  const trigger = String(rawTrigger || '').trim() || 'LeadWhatsapp';
  const phoneInput = String(phone || leadPhone || '').trim();

  if (!inputLeadId && !phoneInput) {
    return res.status(400).json({ error: 'Falta leadId o phone' });
  }
  if (typeof scheduleSequenceForLead !== 'function') {
    return res.status(503).json({ error: 'Scheduler de secuencias no disponible' });
  }

  try {
    const e164 = phoneInput ? toE164(phoneInput) : '';
    const phoneDigits = e164 ? e164.replace(/\D/g, '') : '';
    const leadIdFromPhone = e164 ? e164ToLeadId(e164) : '';

    let finalLeadId = String(inputLeadId || leadIdFromPhone || '').trim();
    if (!finalLeadId) {
      return res.status(400).json({ error: 'No se pudo resolver leadId' });
    }

    let leadRef = db.collection('leads').doc(finalLeadId);
    let leadSnap = await leadRef.get();

    if (!leadSnap.exists && phoneDigits) {
      const byPhone = await db
        .collection('leads')
        .where('telefono', '==', phoneDigits)
        .limit(1)
        .get();
      if (!byPhone.empty) {
        leadRef = byPhone.docs[0].ref;
        leadSnap = byPhone.docs[0];
        finalLeadId = byPhone.docs[0].id;
      }
    }

    const existingData = leadSnap.exists ? (leadSnap.data() || {}) : {};
    const existingResolvedJid = String(existingData.resolvedJid || '');
    const existingJid = String(existingData.jid || '');
    const candidateJid =
      (existingResolvedJid.includes('@s.whatsapp.net') && existingResolvedJid) ||
      (existingJid.includes('@s.whatsapp.net') && existingJid) ||
      (String(finalLeadId).includes('@s.whatsapp.net') ? String(finalLeadId) : '') ||
      (phoneDigits ? `${phoneDigits}@s.whatsapp.net` : '');

    if (!phoneDigits && !candidateJid) {
      return res.status(400).json({
        error: 'Lead sin teléfono/JID enrutable. Envía phone para rescatarlo.',
      });
    }

    const now = new Date();
    const tags = markAsMetaAd
      ? [trigger, 'RescateManual', 'MetaAds']
      : [trigger, 'RescateManual'];

    const baseLeadPatch = {
      lastMessageAt: now,
      estado: 'nuevo',
      hasActiveSequences: true,
      needsJidResolution: false,
      ...(phoneDigits ? { telefono: phoneDigits } : {}),
      ...(candidateJid ? { jid: candidateJid, resolvedJid: candidateJid } : {}),
      ...(markAsMetaAd
        ? {
            source: 'meta_ads',
            campaign: 'whatsapp_click_to_chat',
            lastInboundFromAd: true,
            lastInboundAt: now,
          }
        : {}),
    };

    if (!leadSnap.exists) {
      await leadRef.set({
        ...baseLeadPatch,
        nombre: '',
        fecha_creacion: now,
        unreadCount: 0,
        etiquetas: tags,
      });
    } else {
      await leadRef.set(
        {
          ...baseLeadPatch,
          etiquetas: admin.firestore.FieldValue.arrayUnion(...tags),
        },
        { merge: true }
      );
    }

    if (forceRestart && typeof cancelSequences === 'function') {
      await cancelSequences(finalLeadId, [trigger]).catch((err) => {
        console.warn('[force-sequence] cancelSequences:', err?.message || err);
      });
    }

    await scheduleSequenceForLead(finalLeadId, trigger, now);

    await leadRef.set(
      {
        hasActiveSequences: true,
        estado: 'nuevo',
        etiquetas: admin.firestore.FieldValue.arrayUnion(...tags),
      },
      { merge: true }
    );

    return res.json({
      ok: true,
      leadId: finalLeadId,
      trigger,
      createdLead: !leadSnap.exists,
      markAsMetaAd: !!markAsMetaAd,
      forceRestart: !!forceRestart,
    });
  } catch (err) {
    console.error('[force-sequence] Error:', err);
    return res.status(500).json({ error: err.message || String(err) });
  }
});

// Aplicar etapa de embudo: cancela secuencias previas y activa la configurada para la etapa
app.post('/api/whatsapp/apply-stage', async (req, res) => {
  const {
    leadId: inputLeadId,
    phone,
    leadPhone,
    stageId: inputStageId,
    stageName: rawStageName,
    stageKey: rawStageKey,
    stageColor: rawStageColor,
    sequenceTrigger: rawSequenceTrigger,
    stopSequences: rawStopSequences,
    isClosed: rawIsClosed,
    leadStatus: rawLeadStatus,
    clearStage: rawClearStage,
  } = req.body || {};

  const phoneInput = String(phone || leadPhone || '').trim();
  const stageId = String(inputStageId || '').trim();
  const stageNameInput = String(rawStageName || '').trim();
  const stageKeyInput = String(rawStageKey || '').trim();

  if (!inputLeadId && !phoneInput) {
    return res.status(400).json({ error: 'Falta leadId o phone' });
  }
  const parseBool = (value, defaultValue = false) => {
    if (value === undefined || value === null || value === '') return defaultValue;
    if (typeof value === 'boolean') return value;
    return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
  };

  const boolStopInput = parseBool(rawStopSequences, false);
  const boolClosedInput = parseBool(rawIsClosed, false);
  const clearStage = parseBool(rawClearStage, false);
  if (!clearStage && !stageId && !stageNameInput && !stageKeyInput) {
    return res.status(400).json({ error: 'Falta stageId o stageName' });
  }
  const stageStatusInput = String(rawLeadStatus || '').trim();
  const stageColorInput = String(rawStageColor || '').trim();
  const sequenceTriggerInput = String(rawSequenceTrigger || '').trim();
  const normalizeStageKey = (value) => {
    const slug = slugify(String(value || ''), { lower: true, strict: true });
    if (slug) return slug;
    return String(value || '')
      .trim()
      .toLowerCase()
      .replace(/\s+/g, '_');
  };
  const normalizeStageColor = (value, fallback = '#2563eb') => {
    const raw = String(value || '').trim();
    if (!raw) return fallback;
    if (/^#[0-9a-fA-F]{6}$/.test(raw)) return raw.toLowerCase();
    return fallback;
  };

  try {
    const e164 = phoneInput ? toE164(phoneInput) : '';
    const phoneDigits = e164 ? e164.replace(/\D/g, '') : '';
    const leadIdFromPhone = e164 ? e164ToLeadId(e164) : '';

    let finalLeadId = String(inputLeadId || leadIdFromPhone || '').trim();
    if (!finalLeadId) {
      return res.status(400).json({ error: 'No se pudo resolver leadId' });
    }

    let leadRef = db.collection('leads').doc(finalLeadId);
    let leadSnap = await leadRef.get();

    if (!leadSnap.exists && phoneDigits) {
      const byPhone = await db
        .collection('leads')
        .where('telefono', '==', phoneDigits)
        .limit(1)
        .get();
      if (!byPhone.empty) {
        leadRef = byPhone.docs[0].ref;
        leadSnap = byPhone.docs[0];
        finalLeadId = byPhone.docs[0].id;
      }
    }

    let stageDocId = '';
    let stageData = null;
    if (!clearStage && stageId) {
      const stageSnap = await db.collection('funnelStages').doc(stageId).get();
      if (stageSnap.exists) {
        stageDocId = stageSnap.id;
        stageData = stageSnap.data() || {};
      }
    }

    const stageName = clearStage
      ? ''
      : String(stageData?.name || stageNameInput || stageKeyInput || stageId || '').trim();
    const stageKey = clearStage
      ? ''
      : normalizeStageKey(stageData?.key || stageKeyInput || stageName);
    const sequenceTrigger = clearStage
      ? ''
      : String(stageData?.sequenceTrigger || sequenceTriggerInput || '').trim();
    const shouldStopSequences = clearStage
      ? false
      : Boolean(stageData?.stopSequences ?? boolStopInput ?? false);
    const isClosed = clearStage
      ? false
      : Boolean(stageData?.isClosed ?? boolClosedInput ?? false);
    const leadStatus = clearStage
      ? ''
      : String(stageData?.leadStatus || stageStatusInput || '').trim();
    const stageColor = clearStage
      ? ''
      : normalizeStageColor(stageData?.color || stageColorInput || '', '#2563eb');

    if (!clearStage && !stageName && !stageKey) {
      return res.status(400).json({ error: 'No se pudo resolver la etapa' });
    }
    if (!clearStage && sequenceTrigger && typeof scheduleSequenceForLead !== 'function') {
      return res.status(503).json({ error: 'Scheduler de secuencias no disponible' });
    }

    const existingData = leadSnap.exists ? (leadSnap.data() || {}) : {};
    const existingResolvedJid = String(existingData.resolvedJid || '');
    const existingJid = String(existingData.jid || '');
    const candidateJid =
      (existingResolvedJid.includes('@s.whatsapp.net') && existingResolvedJid) ||
      (existingJid.includes('@s.whatsapp.net') && existingJid) ||
      (String(finalLeadId).includes('@s.whatsapp.net') ? String(finalLeadId) : '') ||
      (phoneDigits ? `${phoneDigits}@s.whatsapp.net` : '');

    const now = new Date();

    if (!leadSnap.exists) {
      await leadRef.set({
        nombre: '',
        fecha_creacion: now,
        unreadCount: 0,
        ...(phoneDigits ? { telefono: phoneDigits } : {}),
        ...(candidateJid ? { jid: candidateJid, resolvedJid: candidateJid, needsJidResolution: false } : {}),
      });
      leadSnap = await leadRef.get();
    }

    const currentData = leadSnap.data() || {};
    const activeTriggers = Array.isArray(currentData.secuenciasActivas)
      ? currentData.secuenciasActivas
          .map((item) => String(item?.trigger || '').trim())
          .filter(Boolean)
      : [];

    if (typeof cancelAllSequences === 'function') {
      await cancelAllSequences(finalLeadId).catch((err) => {
        console.warn('[apply-stage] cancelAllSequences:', err?.message || err);
      });
    } else if (typeof cancelSequences === 'function' && activeTriggers.length > 0) {
      await cancelSequences(finalLeadId, activeTriggers).catch((err) => {
        console.warn('[apply-stage] cancelSequences:', err?.message || err);
      });
    }

    const stageTag = clearStage ? 'etapa:leads_nuevos' : `etapa:${stageKey || stageName}`;
    const leadPatch = {
      funnelUpdatedAt: now,
      lastStageChangeAt: now,
      hasActiveSequences: false,
      stopSequences: clearStage ? false : (shouldStopSequences || isClosed),
      etiquetas: admin.firestore.FieldValue.arrayUnion(stageTag, 'Embudo'),
      ...(phoneDigits ? { telefono: phoneDigits } : {}),
      ...(candidateJid ? { jid: candidateJid, resolvedJid: candidateJid, needsJidResolution: false } : {}),
    };

    if (clearStage) {
      leadPatch.etapa = admin.firestore.FieldValue.delete();
      leadPatch.etapaNombre = admin.firestore.FieldValue.delete();
      leadPatch.etapaColor = admin.firestore.FieldValue.delete();
      leadPatch.funnelStageId = admin.firestore.FieldValue.delete();
      if (!String(currentData.estado || '').trim()) {
        leadPatch.estado = 'nuevo';
      }
    } else {
      leadPatch.etapa = stageKey || stageName;
      leadPatch.etapaNombre = stageName || stageKey;
      leadPatch.etapaColor = stageColor;
      if (stageDocId) leadPatch.funnelStageId = stageDocId;

      if (leadStatus) {
        leadPatch.estado = leadStatus;
      } else if (isClosed) {
        leadPatch.estado = 'compro';
      }
    }

    await leadRef.set(leadPatch, { merge: true });

    let scheduled = false;
    if (!clearStage && sequenceTrigger && !shouldStopSequences && !isClosed) {
      await scheduleSequenceForLead(finalLeadId, sequenceTrigger, now);
      await leadRef.set(
        {
          hasActiveSequences: true,
          stopSequences: false,
          etiquetas: admin.firestore.FieldValue.arrayUnion(sequenceTrigger),
        },
        { merge: true }
      );
      scheduled = true;
    }

    return res.json({
      ok: true,
      leadId: finalLeadId,
      stage: {
        id: clearStage ? null : (stageDocId || null),
        name: clearStage ? 'Leads nuevos' : stageName,
        key: clearStage ? 'leads_nuevos' : (stageKey || stageName),
        stopSequences: clearStage ? false : (shouldStopSequences || isClosed),
        isClosed: clearStage ? false : isClosed,
        color: clearStage ? '#60a5fa' : stageColor,
        base: clearStage,
      },
      sequence: {
        trigger: sequenceTrigger || null,
        scheduled,
      },
      canceledPrevious: activeTriggers.length,
    });
  } catch (err) {
    console.error('[apply-stage] Error:', err);
    return res.status(500).json({ error: err.message || String(err) });
  }
});

// after-form (web)
app.post('/api/web/after-form', async (req, res) => {
  try {
    const { leadId, leadPhone, summary, negocioId } =
      req.body || {};
    if (!leadId && !leadPhone)
      return res
        .status(400)
        .json({ error: 'Faltan leadId o leadPhone' });
    if (!summary)
      return res
        .status(400)
        .json({ error: 'Falta summary' });

    const e164 = toE164(
      leadPhone || (leadId || '').split('@')[0]
    );
    const finalLeadId =
      leadId || e164ToLeadId(e164);
    const leadPhoneDigits =
      e164.replace('+', '');

    const leadRef = db
      .collection('leads')
      .doc(finalLeadId);
    const leadSnap = await leadRef.get();
    if (!leadSnap.exists) {
      await leadRef.set(
        {
          telefono: leadPhoneDigits,
          nombre: '',
          source: 'Web',
          fecha_creacion: new Date(),
          estado: 'nuevo',
          etiquetas: ['FormularioCompletado'],
          unreadCount: 0,
          lastMessageAt: new Date(),
        },
        { merge: true }
      );
    }
    await leadRef.set(
      {
        briefWeb: summary || {},
        etiquetas:
          admin.firestore.FieldValue.arrayUnion(
            'FormularioCompletado'
          ),
        lastMessageAt: new Date(),
      },
      { merge: true }
    );

    let uploadedLogoURL = null;
    let uploadedPhotos = [];
    try {
      const assets = summary?.assets || {};
      const { logo, images = [] } = assets;

      if (logo) {
        uploadedLogoURL =
          await uploadBase64Image({
            base64: logo,
            folder: `web-assets/${(
              summary.slug || 'site'
            ).toLowerCase()}`,
            filenamePrefix: 'logo',
          });
      }

      if (Array.isArray(images)) {
        for (
          let i = 0;
          i < Math.min(images.length, 3);
          i++
        ) {
          const b64 = images[i];
          if (!b64) continue;
          const url =
            await uploadBase64Image({
              base64: b64,
              folder: `web-assets/${(
                summary.slug || 'site'
              ).toLowerCase()}`,
              filenamePrefix: `photo_${i + 1}`,
            });
          if (url) uploadedPhotos.push(url);
        }
      }
    } catch (e) {
      console.error(
        '[after-form] error subiendo assets:',
        e
      );
    }

    if (!uploadedPhotos || uploadedPhotos.length === 0) {
      try {
        uploadedPhotos =
          await getStockPhotoUrls(summary);
      } catch (e) {
        console.error(
          '[after-form] stock photos error:',
          e
        );
        uploadedPhotos =
          buildUnsplashFeaturedQueries(summary);
      }
    }

    let negocioDocId = negocioId;
    let finalSlug = summary.slug || '';
    if (!negocioDocId) {
      const existSnap = await db
        .collection('Negocios')
        .where('leadPhone', '==', leadPhoneDigits)
        .limit(1)
        .get();

      if (!existSnap.empty) {
        const exist = existSnap.docs[0];
        const existData = exist.data() || {};
        return res.status(409).json({
          error:
            'Ya existe un negocio con ese WhatsApp.',
          negocioId: exist.id,
          slug:
            existData.slug ||
            existData?.schema?.slug ||
            '',
        });
      }

      const ref = await db
        .collection('Negocios')
        .add({
          leadId: finalLeadId,
          leadPhone: leadPhoneDigits,
          status: 'Sin procesar',
          companyInfo:
            summary.companyName ||
            summary.name ||
            '',
          businessSector: '',
          businessStory:
            summary.description || '',
          templateId: String(
            summary.templateId || 'info'
          ).toLowerCase(),
          primaryColor:
            summary.primaryColor || null,
          palette:
            summary.palette ||
            (summary.primaryColor
              ? [summary.primaryColor]
              : []),
          keyItems: summary.keyItems || [],
          contactWhatsapp:
            summary.contactWhatsapp || '',
          contactEmail:
            summary.contactEmail || '',
          socialFacebook:
            summary.socialFacebook || '',
          socialInstagram:
            summary.socialInstagram || '',
          logoURL:
            uploadedLogoURL ||
            summary.logoURL ||
            '',
          photoURLs:
            uploadedPhotos &&
            uploadedPhotos.length
              ? uploadedPhotos
              : summary.photoURLs || [],
          slug: summary.slug || '',
          createdAt: new Date(),
        });
      negocioDocId = ref.id;
      finalSlug = summary.slug || '';
    }

    const giroBase = (() => {
      const t = String(
        summary?.templateId || ''
      ).toLowerCase();
      if (t === 'ecommerce') return 'tienda online';
      if (t === 'booking')
        return 'servicio con reservas';
      return 'negocio';
    })();

    const giroHumano = humanizeGiro
      ? humanizeGiro(giroBase)
      : giroBase;
    const fallbackTriplet =
      pickOpportunityTriplet
        ? pickOpportunityTriplet(giroHumano)
        : [
            'clarificar propuesta de valor',
            'CTA visible a WhatsApp',
            'pruebas sociales (reseñas)',
          ];
    const recomendacionesData =
      await buildAfterFormRecommendations({
        summary,
        giroHumano,
        fallbackTriplet,
      });
    const recomendacionesGpt = recomendacionesData.text;

    const afterFormRecommendations = {
      source:
        recomendacionesData.source || 'unknown',
      sector:
        recomendacionesData.sector || giroHumano,
      text: recomendacionesGpt,
      generatedAt: new Date(),
    };

    const writes = [
      leadRef.set(
        {
          etapa: 'form_submitted',
          afterFormRecommendations,
          etiquetas:
            admin.firestore.FieldValue.arrayUnion(
              'FormOK'
            ),
        },
        { merge: true }
      ),
    ];

    if (typeof cancelSequences === 'function') {
      const intakeTriggers = [
        'LeadWeb',
        'NuevoLead',
        'NuevoLeadWeb',
        'LeadWhatsapp',
        'WebPromo',
        'leadweb',
        'nuevolead',
        'nuevoleadweb',
        'leadwhatsapp',
        'webpromo',
      ];
      writes.push(
        cancelSequences(finalLeadId, intakeTriggers).catch((err) => {
          console.warn('[after-form] cancelSequences:', err?.message || err);
          return 0;
        })
      );
    }

    if (negocioDocId) {
      writes.push(
        db.collection('Negocios')
          .doc(negocioDocId)
          .set(
            { afterFormRecommendations },
            { merge: true }
          )
      );
    }

    await Promise.all(writes);

    return res.json({
      ok: true,
      negocioId: negocioDocId,
      slug: finalSlug,
    });
  } catch (e) {
    console.error(
      '/api/web/after-form error:',
      e
    );
    return res
      .status(500)
      .json({ error: String(e?.message || e) });
  }
});

// Activar WebEnviada tras mandar link
app.post('/api/web/sample-sent', async (req, res) => {
  try {
    const { leadId, leadPhone } = req.body || {};
    if (!leadId && !leadPhone)
      return res
        .status(400)
        .json({ error: 'Faltan leadId o leadPhone' });

    const e164 = toE164(
      leadPhone || (leadId || '').split('@')[0]
    );
    const finalLeadId =
      leadId || e164ToLeadId(e164);

    if (!scheduleSequenceForLead) {
      return res.status(500).json({
        error:
          'scheduleSequenceForLead no disponible',
      });
    }

    if (cancelSequences) {
      await cancelSequences(finalLeadId, [
        'LeadWhatsapp',
        'NuevoLeadWeb',
        'LeadWeb',
      ]).catch((err) => {
        console.warn(
          '[sample-sent] cancelSequences:',
          err?.message || err
        );
      });
    }

    const startAt = new Date(
      Date.now() + 15 * 60 * 1000
    );
    await scheduleSequenceForLead(
      finalLeadId,
      'WebEnviada',
      startAt
    );

    await db
      .collection('leads')
      .doc(finalLeadId)
      .set(
        {
          webLinkSentAt: new Date(),
          etiquetas:
            admin.firestore.FieldValue.arrayUnion(
              'WebLinkSent'
            ),
        },
        { merge: true }
      );

    return res.json({
      ok: true,
      scheduledAt: startAt.toISOString(),
    });
  } catch (e) {
    console.error(
      '/api/web/sample-sent error:',
      e
    );
    return res
      .status(500)
      .json({ error: String(e?.message || e) });
  }
});

// tracking: link abierto
app.post('/api/track/link-open', async (req, res) => {
  try {
    let { leadId, leadPhone, slug } =
      req.body || {};

    if (slug && !leadPhone && !leadId) {
      const snap = await db
        .collection('Negocios')
        .where('slug', '==', String(slug))
        .limit(1)
        .get();
      if (!snap.empty) {
        const d = snap.docs[0].data() || {};
        leadPhone = d.leadPhone || leadPhone;
      }
    }

    if (!leadId && leadPhone) {
      const e164 = toE164(leadPhone);
      leadId = e164ToLeadId(e164);
    }
    if (!leadId)
      return res.status(400).json({
        error:
          'Falta leadId/leadPhone/slug',
      });

    const leadRef = db
      .collection('leads')
      .doc(leadId);
    const leadSnap = await leadRef.get();
    if (!leadSnap.exists)
      return res.status(404).json({
        error: 'Lead no encontrado',
      });

    const leadData = leadSnap.data() || {};
    if (leadData.linkOpenedAt) {
      return res.json({ ok: true, already: true });
    }

    await leadRef.set(
      {
        linkOpenedAt: new Date(),
        etiquetas:
          admin.firestore.FieldValue.arrayUnion(
            'LinkAbierto'
          ),
      },
      { merge: true }
    );

    try {
      if (cancelSequences) {
        await cancelSequences(leadId, [
          'WebEnviada',
        ]);
      }
      if (scheduleSequenceForLead) {
        await scheduleSequenceForLead(
          leadId,
          'LinkAbierto',
          new Date()
        );
      }
    } catch (seqErr) {
      console.warn(
        '[track/link-open] secuencias:',
        seqErr?.message
      );
    }

    return res.json({ ok: true });
  } catch (err) {
    console.error(
      '/api/track/link-open error:',
      err
    );
    return res
      .status(500)
      .json({ error: err.message });
  }
});

// Enviar video note (PTV)
app.post(
  '/api/whatsapp/send-video-note',
  async (req, res) => {
    try {
      const { phone, url, seconds } =
        req.body || {};
      if (!phone || !url) {
        return res.status(400).json({
          ok: false,
          error: 'Faltan phone y url',
        });
      }

      console.log(
        `[API] send-video-note → ${phone} ${url} s=${
          seconds ?? 'n/a'
        }`
      );
      await sendVideoNote(
        phone,
        url,
        Number.isFinite(+seconds)
          ? +seconds
          : null
      );

      return res.json({ ok: true });
    } catch (e) {
      console.error(
        '/api/whatsapp/send-video-note error:',
        e
      );
      return res.status(500).json({
        ok: false,
        error: String(e?.message || e),
      });
    }
  }
);

// sample-create (turbo)
app.post('/api/web/sample-create', async (req, res) => {
  try {
    const { leadPhone, summary } =
      req.body || {};
    if (!leadPhone)
      return res
        .status(400)
        .json({ error: 'Falta leadPhone' });
    if (
      !summary?.companyName ||
      !summary?.businessStory ||
      !summary?.slug
    ) {
      return res.status(400).json({
        error:
          'Faltan companyName, businessStory o slug',
      });
    }

    const e164 = toE164(leadPhone || '');
    const finalLeadId =
      e164ToLeadId(e164);
    const leadPhoneDigits =
      e164.replace('+', '');

    const existSnap = await db
      .collection('Negocios')
      .where('leadPhone', '==', leadPhoneDigits)
      .limit(1)
      .get();
    if (!existSnap.empty) {
      const exist = existSnap.docs[0];
      const existData = exist.data() || {};
      return res.status(409).json({
        error:
          'Ya existe un negocio con ese WhatsApp.',
        negocioId: exist.id,
        slug:
          existData.slug ||
          existData?.schema?.slug ||
          '',
      });
    }

    const finalSlug =
      await ensureUniqueSlug(
        summary.slug || summary.companyName
      );

    const leadRef = db
      .collection('leads')
      .doc(finalLeadId);
    const leadSnap = await leadRef.get();
    if (!leadSnap.exists) {
      await leadRef.set(
        {
          telefono: leadPhoneDigits,
          nombre: '',
          source: 'WebTurbo',
          fecha_creacion: new Date(),
          estado: 'nuevo',
          etiquetas: ['FormularioTurbo'],
          unreadCount: 0,
          lastMessageAt: new Date(),
        },
        { merge: true }
      );
    }

    let uploadedLogoURL = null;
    let uploadedPhotos = [];
    try {
      const assets = summary?.assets || {};
      const { logo, images = [] } = assets;

      if (logo) {
        uploadedLogoURL =
          await uploadBase64Image({
            base64: logo,
            folder: `web-assets/${(
              finalSlug || 'site'
            ).toLowerCase()}`,
            filenamePrefix: 'logo',
          });
      }

      if (Array.isArray(images)) {
        for (
          let i = 0;
          i < Math.min(images.length, 3);
          i++
        ) {
          const b64 = images[i];
          if (!b64) continue;
          const url =
            await uploadBase64Image({
              base64: b64,
              folder: `web-assets/${(
                finalSlug || 'site'
              ).toLowerCase()}`,
              filenamePrefix: `photo_${
                i + 1
              }`,
            });
          if (url) uploadedPhotos.push(url);
        }
      }
    } catch (e) {
      console.error(
        '[sample-create] error subiendo assets:',
        e
      );
    }

    const ref = await db
      .collection('Negocios')
      .add({
        leadId: finalLeadId,
        leadPhone: leadPhoneDigits,
        status: 'Sin procesar',
        companyInfo: summary.companyName,
        businessSector: '',
        businessStory:
          summary.businessStory,
        templateId:
          summary.templateId || 'info',
        primaryColor:
          summary.primaryColor || null,
        palette: summary.primaryColor
          ? [summary.primaryColor]
          : [],
        keyItems: [],
        contactWhatsapp:
          summary.contactWhatsapp || '',
        contactEmail:
          summary.email || '',
        socialFacebook:
          summary.socialFacebook || '',
        socialInstagram:
          summary.socialInstagram || '',
        logoURL:
          uploadedLogoURL ||
          summary.logoURL || '',
        photoURLs:
          uploadedPhotos &&
          uploadedPhotos.length
            ? uploadedPhotos
            : summary.photoURLs || [],
        slug: finalSlug,
        createdAt: new Date(),
      });

    await leadRef.set(
      {
        briefWeb: {
          companyName:
            summary.companyName,
          businessStory:
            summary.businessStory,
          slug: finalSlug,
          templateId:
            summary.templateId || 'info',
          primaryColor:
            summary.primaryColor || null,
          turbo: true,
        },
        etiquetas:
          admin.firestore.FieldValue.arrayUnion(
            'FormularioTurbo'
          ),
        lastMessageAt: new Date(),
      },
      { merge: true }
    );

    return res.json({
      ok: true,
      negocioId: ref.id,
      slug: finalSlug,
    });
  } catch (e) {
    console.error(
      '/api/web/sample-create error:',
      e
    );
    return res
      .status(500)
      .json({ error: String(e?.message || e) });
  }
});

// ============== Arranque servidor + WA ==============
app.listen(port, () => {
  console.log(`🚀 Servidor corriendo en puerto ${port}`);
  console.log(`✅ Sistema de PIN activado`);
  console.log(`✅ Autenticación de cliente activada`);
  console.log(`✅ Webhook de Stripe configurado con raw body`);
  console.log(`✅ Mercado Pago Checkout Pro activado`);
  console.log(`✅ Stripe Pagos Únicos activado`);
  connectToWhatsApp().catch((err) =>
    console.error(
      'Error al conectar WhatsApp en startup:',
      err
    )
  );
});

// ============== CRON JOBS ==============
cron.schedule('*/30 * * * * *', () => {
  console.log(
    '⏱️ processSequences:',
    new Date().toISOString()
  );
  processSequences().catch((err) =>
    console.error('Error en processSequences:', err)
  );
});

cron.schedule('* * * * *', () => {
  console.log(
    '⏱️ generateSiteSchemas:',
    new Date().toISOString()
  );
  generateSiteSchemas().catch((err) =>
    console.error(
      'Error en generateSiteSchemas:',
      err
    )
  );
});

cron.schedule('*/15 * * * *', () => {
  console.log(
    '⏱️ enviarSitiosPendientes:',
    new Date().toISOString()
  );
  enviarSitiosPendientes().catch((err) =>
    console.error(
      'Error en enviarSitiosPendientes:',
      err
    )
  );
});

cron.schedule('0 * * * *', () => {
  console.log(
    '⏱️ archivarNegociosAntiguos:',
    new Date().toISOString()
  );
  archivarNegociosAntiguos().catch((err) =>
    console.error(
      'Error en archivarNegociosAntiguos:',
      err
    )
  );
});

// Verificar trials expirados cada hora
cron.schedule('0 * * * *', async () => {
  console.log(
    '🔍 Verificando trials expirados...'
  );
  try {
    const now = Timestamp.now();
    const expiredTrials = await db
      .collection('Negocios')
      .where('trialActive', '==', true)
      .where('trialEndDate', '<=', now)
      .get();

    for (const doc of expiredTrials.docs) {
      const negocioData = doc.data() || {};
      await doc.ref.update({
        trialActive: false,
        plan: 'expired',
        websiteArchived: true,
        archivedReason: 'trial_expired',
        updatedAt: Timestamp.now(),
      });

      console.log(
        `⏰ Trial expirado para negocio: ${doc.id}`
      );

      const rawLeadId = String(
        negocioData.leadId || ''
      ).trim();
      let leadId = /@s\.whatsapp\.net$/i.test(rawLeadId)
        ? rawLeadId
        : '';

      if (!leadId) {
        const phoneSource = String(
          negocioData.leadPhone ||
            negocioData.contactWhatsapp ||
            ''
        ).trim();
        const digits = phoneSource.replace(/\D/g, '');
        if (digits.length >= 10) {
          leadId = e164ToLeadId(toE164(phoneSource));
        }
      }

      let usedTrigger = null;
      let scheduled = false;
      const triggerCandidates = [
        '#etapaLevamiento',
        'EtapaLevamiento',
        '#etapalevamiento',
        'etapaLevamiento',
        'etapalevamiento',
      ];

      if (leadId && typeof scheduleSequenceForLead === 'function') {
        for (const trigger of triggerCandidates) {
          try {
            const programmed =
              await scheduleSequenceForLead(
                leadId,
                trigger,
                new Date()
              );
            if (programmed > 0) {
              usedTrigger = trigger;
              scheduled = true;
              break;
            }
          } catch (seqErr) {
            console.warn(
              `[trial-expired] No se pudo activar '${trigger}' para ${leadId}:`,
              seqErr?.message || seqErr
            );
          }
        }
      }

      if (leadId) {
        const etiquetas = ['NegocioArchivado', '#etapaLevamiento'];
        if (usedTrigger) etiquetas.push(usedTrigger);

        const leadPatch = {
          etapa: 'negocio_archivado',
          archivedNegocioAt: new Date(),
          archivedNegocioId: doc.id,
          archivedSequenceTrigger:
            usedTrigger || '#etapaLevamiento',
          archivedSequenceScheduled: scheduled,
          etiquetas:
            admin.firestore.FieldValue.arrayUnion(
              ...etiquetas
            ),
        };
        if (scheduled) leadPatch.hasActiveSequences = true;

        await db
          .collection('leads')
          .doc(leadId)
          .set(leadPatch, { merge: true })
          .catch((err) =>
            console.warn(
              `[trial-expired] No se pudo actualizar lead ${leadId}:`,
              err?.message || err
            )
          );
      } else {
        console.warn(
          `[trial-expired] No se pudo resolver lead para negocio ${doc.id}`
        );
      }
    }
  } catch (err) {
    console.error(
      'Error verificando trials expirados:',
      err
    );
  }
});

// ============== Helpers ==============
async function ensureUniqueSlug(input) {
  const base =
    slugify(String(input || ''), {
      lower: true,
      strict: true,
    }).slice(0, 30) || 'sitio';
  let slug = base;
  let i = 2;
  while (true) {
    const snap = await db
      .collection('Negocios')
      .where('slug', '==', slug)
      .limit(1)
      .get();
    if (snap.empty) return slug;
    slug = `${base}-${String(i).padStart(2, '0')}`;
    i++;
    if (i > 99)
      throw new Error(
        'No fue posible generar un slug único'
      );
  }
}
