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
import { generarPIN, generarMensajeCredenciales } from './pinUtils.js';

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
  if (leadData?.allowUnsafeTarget === true) return '';

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

function toIsoOrNull(value) {
  if (!value) return null;
  if (typeof value?.toDate === 'function') {
    try {
      return value.toDate().toISOString();
    } catch {
      return null;
    }
  }
  if (value instanceof Date) return value.toISOString();
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) return null;
  return parsed.toISOString();
}

function parseBooleanInput(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') return defaultValue;
  if (typeof value === 'boolean') return value;
  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
}

function normalizePhoneDigits(value = '') {
  return String(value || '').replace(/\D/g, '');
}

function expandPhoneCandidates(value = '') {
  const digits = normalizePhoneDigits(value);
  if (!digits) return [];

  const set = new Set([digits]);
  if (/^\d{10}$/.test(digits)) {
    set.add(`52${digits}`);
    set.add(`521${digits}`);
  } else if (/^52\d{10}$/.test(digits) && !digits.startsWith('521')) {
    const tail = digits.slice(2);
    set.add(tail);
    set.add(`521${tail}`);
  } else if (/^521\d{10}$/.test(digits)) {
    const tail = digits.slice(3);
    set.add(tail);
    set.add(`52${tail}`);
  }

  return Array.from(set);
}

function followMergedIntoChain(maxDepth = 5) {
  return async (snap) => {
    if (!snap?.exists) return snap;
    const leadsCol = db.collection('leads');
    let current = snap;
    const visited = new Set([snap.id]);

    for (let depth = 0; depth < maxDepth; depth += 1) {
      const data = current.data() || {};
      const mergedInto = String(data.mergedInto || '').trim();
      if (!mergedInto || mergedInto === current.id || visited.has(mergedInto)) break;
      const next = await leadsCol.doc(mergedInto).get();
      if (!next.exists) break;
      visited.add(mergedInto);
      current = next;
    }

    return current;
  };
}

const resolveCanonicalLeadSnap = followMergedIntoChain(6);

async function resolveLeadByIdentity({ leadId = '', phone = '' } = {}) {
  const leadsCol = db.collection('leads');
  const requestedLeadId = String(leadId || '').trim();
  const phoneRaw = String(phone || '').trim();

  const e164 = phoneRaw ? toE164(phoneRaw) : '';
  let phoneDigits = normalizePhoneDigits(e164 || phoneRaw);

  let leadSnap = null;
  let finalLeadId = requestedLeadId;

  if (requestedLeadId) {
    const byId = await leadsCol.doc(requestedLeadId).get();
    if (byId.exists) {
      leadSnap = await resolveCanonicalLeadSnap(byId);
      finalLeadId = leadSnap.id;
    }
  }

  if (!leadSnap && requestedLeadId.includes('@')) {
    for (const field of ['resolvedJid', 'jid', 'lidJid']) {
      const byJid = await leadsCol.where(field, '==', requestedLeadId).limit(1).get();
      if (!byJid.empty) {
        leadSnap = await resolveCanonicalLeadSnap(byJid.docs[0]);
        finalLeadId = leadSnap.id;
        break;
      }
    }
  }

  const inferredFromLeadId = normalizePhoneDigits(requestedLeadId.split('@')[0] || '');
  const phoneCandidates = expandPhoneCandidates(phoneDigits || inferredFromLeadId);

  if (!leadSnap) {
    for (const candidate of phoneCandidates) {
      const byPhone = await leadsCol.where('telefono', '==', candidate).limit(1).get();
      if (!byPhone.empty) {
        leadSnap = await resolveCanonicalLeadSnap(byPhone.docs[0]);
        finalLeadId = leadSnap.id;
        break;
      }
    }
  }

  if (!leadSnap) {
    for (const candidate of phoneCandidates) {
      const byPhoneId = await leadsCol.doc(`${candidate}@s.whatsapp.net`).get();
      if (byPhoneId.exists) {
        leadSnap = await resolveCanonicalLeadSnap(byPhoneId);
        finalLeadId = leadSnap.id;
        break;
      }
    }
  }

  let leadData = leadSnap?.data?.() || null;
  if (!phoneDigits) {
    phoneDigits = normalizePhoneDigits(leadData?.telefono || phoneCandidates[0] || '');
  }
  if (!finalLeadId && phoneDigits) {
    finalLeadId = `${phoneDigits}@s.whatsapp.net`;
  }

  return {
    requestedLeadId,
    leadId: finalLeadId,
    leadRef: finalLeadId ? leadsCol.doc(finalLeadId) : null,
    leadSnap,
    leadData,
    phoneDigits,
    phoneCandidates: expandPhoneCandidates(phoneDigits),
  };
}

async function resolveNegocioByIdentity({
  negocioId = '',
  leadId = '',
  phoneDigits = '',
} = {}) {
  const negociosCol = db.collection('Negocios');
  const requestedNegocioId = String(negocioId || '').trim();
  const finalLeadId = String(leadId || '').trim();
  const phoneCandidates = expandPhoneCandidates(phoneDigits);

  let negocioSnap = null;

  if (requestedNegocioId) {
    const byId = await negociosCol.doc(requestedNegocioId).get();
    if (byId.exists) negocioSnap = byId;
  }

  if (!negocioSnap && finalLeadId) {
    const byLeadId = await negociosCol.where('leadId', '==', finalLeadId).limit(1).get();
    if (!byLeadId.empty) negocioSnap = byLeadId.docs[0];
  }

  if (!negocioSnap) {
    for (const candidate of phoneCandidates) {
      const byLeadPhone = await negociosCol.where('leadPhone', '==', candidate).limit(1).get();
      if (!byLeadPhone.empty) {
        negocioSnap = byLeadPhone.docs[0];
        break;
      }
    }
  }

  return {
    negocioSnap,
    negocioId: negocioSnap?.id || '',
    negocioRef: negocioSnap?.ref || null,
    negocioData: negocioSnap?.data?.() || null,
  };
}

function getPanelAccessUrl() {
  return process.env.CLIENT_PANEL_URL || 'https://negociosweb.mx/cliente-login';
}

function getDefaultFormUrl() {
  return process.env.WEB_FORM_URL || process.env.CLIENT_FORM_URL || 'https://app.negociosweb.mx';
}

function buildLeadFormUrl(baseUrl, { negocioId = '', leadId = '', phone = '' } = {}) {
  const safeBase = String(baseUrl || '').trim() || getDefaultFormUrl();
  const safeNegocioId = String(negocioId || '').trim();
  const safeLeadId = String(leadId || '').trim();
  const safePhone = normalizePhoneDigits(phone);

  try {
    const url = new URL(safeBase);
    if (safeNegocioId) url.searchParams.set('negocioId', safeNegocioId);
    if (safeLeadId) url.searchParams.set('leadId', safeLeadId);
    if (safePhone) url.searchParams.set('phone', safePhone);
    return url.toString();
  } catch {
    return safeBase;
  }
}

function buildSiteUrl(negocioData = {}) {
  const slug = String(
    negocioData?.slug ||
      negocioData?.schema?.slug ||
      negocioData?.briefWeb?.slug ||
      ''
  ).trim();

  if (!slug) return '';
  return `https://negociosweb.mx/site/${slug}`;
}

function serializeNegocio(negocioId, negocioData = {}, context = {}) {
  if (!negocioId || !negocioData) return null;
  const siteUrl = buildSiteUrl(negocioData);
  const panelUrl = getPanelAccessUrl();
  const planStartAt = negocioData.planStartAt || negocioData.planStartDate || null;
  const expiresAt = negocioData.expiresAt || negocioData.planRenewalDate || null;
  const leadPhone = normalizePhoneDigits(negocioData.leadPhone || context.phoneDigits || '');

  return {
    id: negocioId,
    companyInfo: String(negocioData.companyInfo || ''),
    status: String(negocioData.status || ''),
    leadId: String(negocioData.leadId || context.leadId || ''),
    leadPhone,
    contactWhatsapp: normalizePhoneDigits(negocioData.contactWhatsapp || ''),
    contactEmail: String(negocioData.contactEmail || ''),
    plan: String(negocioData.plan || ''),
    planNombre: String(negocioData.planNombre || ''),
    pin: String(negocioData.pin || ''),
    slug: String(
      negocioData.slug ||
        negocioData?.schema?.slug ||
        negocioData?.briefWeb?.slug ||
        ''
    ),
    dominio: String(negocioData.dominio || ''),
    siteUrl,
    panelUrl,
    sampleUrl: String(negocioData.sampleLinkSentUrl || '') || buildSampleFormUrl(leadPhone),
    trialActive: negocioData.trialActive === true,
    templateId: String(negocioData.templateId || ''),
    schema: (negocioData?.schema && typeof negocioData.schema === 'object') ? negocioData.schema : null,
    businessStory: String(negocioData.businessStory || ''),
    brief: serializeBrief(negocioData),
    createdAt: toIsoOrNull(negocioData.createdAt),
    updatedAt: toIsoOrNull(negocioData.updatedAt),
    planStartDate: toIsoOrNull(planStartAt),
    planActivatedAt: toIsoOrNull(negocioData.planActivatedAt),
    planRenewalDate: toIsoOrNull(expiresAt),
    planStartAt: toIsoOrNull(planStartAt),
    expiresAt: toIsoOrNull(expiresAt),
    planExpiresAt: toIsoOrNull(negocioData.planExpiresAt || expiresAt),
  };
}

async function sendLeadTextMessage({ leadId = '', phoneDigits = '', content = '' } = {}) {
  const target = String(leadId || '').trim() || normalizePhoneDigits(phoneDigits);
  const message = String(content || '').trim();
  if (!target) throw new Error('No se pudo resolver destino para WhatsApp');
  if (!message) throw new Error('Mensaje vacío');
  return sendMessageToLead(target, message);
}

function isPaidPlan(plan = '') {
  const key = String(plan || '').trim().toLowerCase();
  return ['basic', 'pro', 'premium'].includes(key);
}

function parseDateInputToTimestamp(value, fieldLabel = 'fecha') {
  if (value === undefined || value === null || value === '') return null;
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) {
    throw new Error(`Fecha inválida en ${fieldLabel}`);
  }
  return Timestamp.fromDate(parsed);
}

function normalizeDomainInput(value = '') {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';

  const withoutProto = raw.replace(/^https?:\/\//, '');
  const noPath = withoutProto.split('/')[0];
  const noPort = noPath.split(':')[0];
  const clean = noPort.replace(/^\.+|\.+$/g, '');

  if (!clean) return '';
  return clean;
}

function hasUsefulBriefData(negocioData = {}) {
  const brief = negocioData?.advancedBrief;
  if (brief && typeof brief === 'object') {
    const objectValues = Object.values(brief).flatMap((item) => {
      if (Array.isArray(item)) return item;
      if (item && typeof item === 'object') return Object.values(item);
      return [item];
    });
    const hasContent = objectValues.some((item) => String(item || '').trim().length > 0);
    if (hasContent) return true;
  }

  const infoStatus = String(negocioData?.infoStatus || '').toLowerCase();
  if (infoStatus === 'informacion pendiente' || infoStatus === 'informacion procesada') return true;
  if (negocioData?.infoSubmittedAt) return true;
  if (negocioData?.infoProcessedAt) return true;
  return false;
}

function getAppBaseUrl() {
  return (
    process.env.NEXT_PUBLIC_SITE_URL
    || process.env.REACT_APP_SITE_URL
    || process.env.CLIENT_APP_BASE_URL
    || 'https://app.negociosweb.mx'
  );
}

function buildInformationFormUrl(phoneDigits = '') {
  const safePhone = normalizePhoneDigits(phoneDigits);
  if (!safePhone) return '';
  const base = String(getAppBaseUrl()).replace(/\/+$/, '');
  return `${base}/informacion/${safePhone}`;
}

function getSampleFormBaseUrl() {
  return (
    process.env.SAMPLE_FORM_BASE_URL
    || process.env.PUBLIC_SAMPLE_FORM_URL
    || process.env.NEXT_PUBLIC_SITE_URL
    || 'https://negociosweb.mx'
  );
}

function buildSampleFormUrl(phoneDigits = '') {
  const safePhone = normalizePhoneDigits(phoneDigits);
  if (!safePhone) return '';
  const base = String(getSampleFormBaseUrl()).replace(/\/+$/, '');
  return `${base}/muestra/${safePhone}`;
}

function buildBriefInviteMessage({ companyName = '', briefUrl = '' } = {}) {
  const safeName = String(companyName || '').trim() || 'tu negocio';
  const safeUrl = String(briefUrl || '').trim();
  return (
    `Hola ${safeName}.\n\n`
    + `Te comparto tu formulario de información para preparar tu sitio:\n${safeUrl}\n\n`
    + 'Cuando lo completes, te confirmo por este medio.'
  );
}

function buildSampleInviteMessage({ companyName = '', sampleUrl = '' } = {}) {
  const safeName = String(companyName || '').trim() || 'tu negocio';
  const safeUrl = String(sampleUrl || '').trim();
  return (
    `Hola ${safeName}.\n\n`
    + `Aquí tienes tu formulario de muestra express:\n${safeUrl}\n\n`
    + 'Cuando lo completes, te envío tu página por WhatsApp.'
  );
}

function serializeBrief(negocioData = {}) {
  const advancedBrief = (negocioData?.advancedBrief && typeof negocioData.advancedBrief === 'object')
    ? negocioData.advancedBrief
    : null;

  return {
    isFilled: hasUsefulBriefData(negocioData),
    infoStatus: String(negocioData?.infoStatus || ''),
    infoSubmittedAt: toIsoOrNull(negocioData?.infoSubmittedAt),
    infoProcessedAt: toIsoOrNull(negocioData?.infoProcessedAt),
    advancedBrief,
  };
}

async function sendWhatsappFallbackMessage({
  leadId = '',
  phoneDigits = '',
  message = '',
} = {}) {
  const safeMessage = String(message || '').trim();
  if (!safeMessage) throw new Error('Mensaje vacío');

  const safeLeadId = String(leadId || '').trim();
  const safePhone = normalizePhoneDigits(phoneDigits || '');

  let lastError = null;

  if (safePhone) {
    try {
      await sendLeadTextMessage({ phoneDigits: safePhone, content: safeMessage });
      return { sent: true, method: 'send-direct', target: safePhone };
    } catch (error) {
      lastError = error;
    }
  }

  if (safePhone) {
    try {
      await sendLeadTextMessage({ phoneDigits: safePhone, content: safeMessage });
      return { sent: true, method: 'send-bulk-message', target: safePhone };
    } catch (error) {
      lastError = error;
    }
  }

  if (safeLeadId) {
    try {
      await sendLeadTextMessage({ leadId: safeLeadId, content: safeMessage });
      return { sent: true, method: 'send-message', target: safeLeadId };
    } catch (error) {
      lastError = error;
    }
  }

  throw new Error(lastError?.message || 'No se pudo enviar el mensaje por WhatsApp');
}

const FINANCE_SERVICES_COLLECTION = 'CrmFinanceServices';
const FINANCE_TRANSACTIONS_COLLECTION = 'CrmFinanceTransactions';
const FINANCE_CATALOG_COLLECTION = 'CrmFinanceCatalog';
const FINANCE_SERVICE_STATUSES = new Set(['pendiente', 'en_proceso', 'pagado', 'cancelado']);
const FINANCE_TRANSACTION_TYPES = new Set(['income', 'expense']);

function roundMoney(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return 0;
  return Math.round((num + Number.EPSILON) * 100) / 100;
}

function toMoneyNumber(value, fallback = 0) {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const raw = String(value).trim();
  if (!raw) return fallback;

  let normalized = raw.replace(/[^\d,.-]/g, '');
  if (normalized.includes(',') && normalized.includes('.')) {
    normalized = normalized.replace(/,/g, '');
  } else if (normalized.includes(',') && !normalized.includes('.')) {
    normalized = normalized.replace(/,/g, '.');
  }

  const amount = Number(normalized);
  return Number.isFinite(amount) ? amount : fallback;
}

function parseMoneyInput(value, fieldLabel = 'monto', {
  required = false,
  min = 0,
  allowZero = true,
} = {}) {
  if (value === null || value === undefined || value === '') {
    if (required) throw new Error(`Falta ${fieldLabel}`);
    return null;
  }

  const amount = toMoneyNumber(value, Number.NaN);
  if (!Number.isFinite(amount)) throw new Error(`${fieldLabel} inválido`);
  if (amount < min) throw new Error(`${fieldLabel} debe ser mayor o igual a ${min}`);
  if (!allowZero && amount === 0) throw new Error(`${fieldLabel} debe ser mayor a 0`);
  return roundMoney(amount);
}

function normalizeFinanceStatus(value = '') {
  const key = String(value || '').trim().toLowerCase();
  if (!key) return 'pendiente';
  if (key === 'pendiente' || key === 'nuevo') return 'pendiente';
  if (key === 'en proceso' || key === 'en_proceso' || key === 'proceso') return 'en_proceso';
  if (key === 'pagado' || key === 'completado' || key === 'cerrado') return 'pagado';
  if (key === 'cancelado' || key === 'cancelada') return 'cancelado';
  return FINANCE_SERVICE_STATUSES.has(key) ? key : 'pendiente';
}

function normalizeFinanceType(value = '') {
  const key = String(value || '').trim().toLowerCase();
  if (key === 'income' || key === 'ingreso') return 'income';
  if (key === 'expense' || key === 'egreso') return 'expense';
  return '';
}

function parseDateInputToTimestampOrNull(value, fieldLabel = 'fecha') {
  if (value === undefined || value === null || value === '') return null;
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) {
    throw new Error(`${fieldLabel} inválida`);
  }
  return Timestamp.fromDate(parsed);
}

function parseDateRangeBound(value, { endOfDay = false } = {}) {
  if (!value) return null;
  const parsed = new Date(String(value));
  if (!Number.isFinite(parsed.getTime())) return null;
  if (endOfDay) {
    parsed.setHours(23, 59, 59, 999);
  } else {
    parsed.setHours(0, 0, 0, 0);
  }
  return parsed.getTime();
}

function serializeFinanceService(serviceId, serviceData = {}) {
  if (!serviceId || !serviceData) return null;

  const totalAmount = Math.max(0, roundMoney(toMoneyNumber(serviceData.totalAmount, 0)));
  const paidAmount = Math.max(0, roundMoney(toMoneyNumber(serviceData.paidAmount, 0)));
  const pendingAmountStored = roundMoney(toMoneyNumber(serviceData.pendingAmount, Math.max(0, totalAmount - paidAmount)));
  const pendingAmount = Math.max(0, pendingAmountStored);

  const advancePercentRaw = toMoneyNumber(serviceData.advancePercent, 50);
  const advancePercent = Math.max(0, Math.min(100, roundMoney(advancePercentRaw)));
  const advanceAmount = roundMoney(
    toMoneyNumber(serviceData.advanceAmount, totalAmount * (advancePercent / 100))
  );
  const advancePendingAmount = Math.max(0, roundMoney(advanceAmount - paidAmount));

  const estimatedCostAmount = Math.max(0, roundMoney(toMoneyNumber(serviceData.estimatedCostAmount, 0)));
  const actualCostAmount = Math.max(0, roundMoney(toMoneyNumber(serviceData.actualCostAmount, 0)));
  const estimatedProfitAmount = roundMoney(totalAmount - estimatedCostAmount);
  const realizedProfitAmount = roundMoney(paidAmount - actualCostAmount);

  const status = normalizeFinanceStatus(serviceData.status || '');
  const dueDateIso = toIsoOrNull(serviceData.dueDate);
  const dueDateMs = dueDateIso ? new Date(dueDateIso).getTime() : 0;
  const isOverdue = Boolean(
    pendingAmount > 0
      && status !== 'cancelado'
      && Number.isFinite(dueDateMs)
      && dueDateMs > 0
      && dueDateMs < Date.now()
  );

  return {
    id: serviceId,
    clientName: String(serviceData.clientName || '').trim(),
    serviceName: String(serviceData.serviceName || '').trim(),
    category: String(serviceData.category || '').trim(),
    description: String(serviceData.description || '').trim(),
    catalogItemId: String(serviceData.catalogItemId || '').trim(),
    catalogItemName: String(serviceData.catalogItemName || '').trim(),
    currency: String(serviceData.currency || 'MXN'),
    status,
    leadId: String(serviceData.leadId || '').trim(),
    leadPhone: normalizePhoneDigits(serviceData.leadPhone || ''),
    negocioId: String(serviceData.negocioId || '').trim(),
    totalAmount,
    paidAmount,
    pendingAmount,
    advancePercent,
    advanceAmount,
    advancePendingAmount,
    estimatedCostAmount,
    actualCostAmount,
    estimatedProfitAmount,
    realizedProfitAmount,
    lastPaymentAt: toIsoOrNull(serviceData.lastPaymentAt),
    lastExpenseAt: toIsoOrNull(serviceData.lastExpenseAt),
    dueDate: dueDateIso,
    isOverdue,
    createdAt: toIsoOrNull(serviceData.createdAt),
    updatedAt: toIsoOrNull(serviceData.updatedAt),
  };
}

function serializeFinanceCatalogItem(itemId, itemData = {}) {
  if (!itemId || !itemData) return null;
  const unitPrice = Math.max(0, roundMoney(toMoneyNumber(itemData.unitPrice, 0)));
  return {
    id: itemId,
    name: String(itemData.name || '').trim(),
    category: String(itemData.category || '').trim(),
    description: String(itemData.description || '').trim(),
    unitPrice,
    currency: String(itemData.currency || 'MXN').trim().toUpperCase() || 'MXN',
    sku: String(itemData.sku || '').trim(),
    active: itemData.active !== false,
    createdAt: toIsoOrNull(itemData.createdAt),
    updatedAt: toIsoOrNull(itemData.updatedAt),
  };
}

function serializeFinanceTransaction(transactionId, txData = {}) {
  if (!transactionId || !txData) return null;
  const type = normalizeFinanceType(txData.type || '');
  const amount = Math.max(0, roundMoney(toMoneyNumber(txData.amount, 0)));
  const sign = type === 'expense' ? -1 : 1;
  return {
    id: transactionId,
    type,
    amount,
    signedAmount: roundMoney(amount * sign),
    category: String(txData.category || '').trim(),
    paymentMethod: String(txData.paymentMethod || '').trim(),
    notes: String(txData.notes || '').trim(),
    reference: String(txData.reference || '').trim(),
    serviceId: String(txData.serviceId || '').trim(),
    serviceName: String(txData.serviceName || '').trim(),
    clientName: String(txData.clientName || '').trim(),
    leadId: String(txData.leadId || '').trim(),
    leadPhone: normalizePhoneDigits(txData.leadPhone || ''),
    negocioId: String(txData.negocioId || '').trim(),
    isAdvance: txData.isAdvance === true,
    occurredAt: toIsoOrNull(txData.occurredAt),
    createdAt: toIsoOrNull(txData.createdAt),
    updatedAt: toIsoOrNull(txData.updatedAt),
  };
}

function buildFinanceSummary({ services = [], transactions = [] } = {}) {
  const incomeTotal = roundMoney(
    transactions
      .filter((item) => item.type === 'income')
      .reduce((acc, item) => acc + toMoneyNumber(item.amount, 0), 0)
  );
  const expenseTotal = roundMoney(
    transactions
      .filter((item) => item.type === 'expense')
      .reduce((acc, item) => acc + toMoneyNumber(item.amount, 0), 0)
  );
  const netProfit = roundMoney(incomeTotal - expenseTotal);

  const receivableTotal = roundMoney(
    services
      .filter((item) => item.status !== 'cancelado')
      .reduce((acc, item) => acc + toMoneyNumber(item.pendingAmount, 0), 0)
  );
  const advanceReceivableTotal = roundMoney(
    services
      .filter((item) => item.status !== 'cancelado')
      .reduce((acc, item) => acc + toMoneyNumber(item.advancePendingAmount, 0), 0)
  );
  const totalBilled = roundMoney(
    services
      .filter((item) => item.status !== 'cancelado')
      .reduce((acc, item) => acc + toMoneyNumber(item.totalAmount, 0), 0)
  );
  const totalCollected = roundMoney(
    services
      .filter((item) => item.status !== 'cancelado')
      .reduce((acc, item) => acc + toMoneyNumber(item.paidAmount, 0), 0)
  );
  const estimatedProfitTotal = roundMoney(
    services
      .filter((item) => item.status !== 'cancelado')
      .reduce((acc, item) => acc + toMoneyNumber(item.estimatedProfitAmount, 0), 0)
  );
  const marginPercent = incomeTotal > 0 ? roundMoney((netProfit / incomeTotal) * 100) : 0;

  const overdueCount = services.filter((item) => item.isOverdue).length;
  const paidServices = services.filter((item) => item.status === 'pagado').length;
  const pendingServices = services.filter((item) => item.pendingAmount > 0 && item.status !== 'cancelado').length;

  const categoryMap = new Map();
  transactions.forEach((item) => {
    const key = String(item.category || 'sin_categoria').trim() || 'sin_categoria';
    const current = categoryMap.get(key) || { category: key, income: 0, expense: 0, net: 0 };
    if (item.type === 'expense') {
      current.expense = roundMoney(current.expense + toMoneyNumber(item.amount, 0));
    } else {
      current.income = roundMoney(current.income + toMoneyNumber(item.amount, 0));
    }
    current.net = roundMoney(current.income - current.expense);
    categoryMap.set(key, current);
  });

  const monthlyMap = new Map();
  transactions.forEach((item) => {
    const occurredAt = item.occurredAt ? new Date(item.occurredAt) : null;
    if (!occurredAt || !Number.isFinite(occurredAt.getTime())) return;
    const ym = `${occurredAt.getFullYear()}-${String(occurredAt.getMonth() + 1).padStart(2, '0')}`;
    const current = monthlyMap.get(ym) || { month: ym, income: 0, expense: 0, net: 0 };
    if (item.type === 'expense') {
      current.expense = roundMoney(current.expense + toMoneyNumber(item.amount, 0));
    } else {
      current.income = roundMoney(current.income + toMoneyNumber(item.amount, 0));
    }
    current.net = roundMoney(current.income - current.expense);
    monthlyMap.set(ym, current);
  });

  const topReceivables = [...services]
    .filter((item) => item.pendingAmount > 0 && item.status !== 'cancelado')
    .sort((a, b) => b.pendingAmount - a.pendingAmount)
    .slice(0, 15);

  return {
    metrics: {
      incomeTotal,
      expenseTotal,
      netProfit,
      marginPercent,
      receivableTotal,
      advanceReceivableTotal,
      totalBilled,
      totalCollected,
      estimatedProfitTotal,
      totalServices: services.length,
      pendingServices,
      paidServices,
      overdueCount,
    },
    byCategory: Array.from(categoryMap.values()).sort(
      (a, b) => Math.abs(b.net) - Math.abs(a.net)
    ),
    byMonth: Array.from(monthlyMap.values())
      .sort((a, b) => a.month.localeCompare(b.month))
      .slice(-12),
    topReceivables,
  };
}

async function createFinanceTransaction({
  type = '',
  amount = 0,
  category = '',
  paymentMethod = '',
  notes = '',
  reference = '',
  serviceId = '',
  leadId = '',
  leadPhone = '',
  negocioId = '',
  isAdvance = false,
  occurredAt = null,
  createdBy = '',
  affectService = true,
} = {}) {
  const safeType = normalizeFinanceType(type);
  if (!FINANCE_TRANSACTION_TYPES.has(safeType)) {
    throw new Error('Tipo de movimiento inválido. Usa income o expense.');
  }

  const safeAmount = parseMoneyInput(amount, 'amount', {
    required: true,
    allowZero: false,
    min: 0.01,
  });
  const safeServiceId = String(serviceId || '').trim();
  const safeOccurredAt = occurredAt || Timestamp.now();
  const safeCategory = String(category || (safeType === 'income' ? 'ingreso' : 'egreso')).trim().slice(0, 80);
  const safePaymentMethod = String(paymentMethod || '').trim().slice(0, 80);
  const safeNotes = String(notes || '').trim().slice(0, 1000);
  const safeReference = String(reference || '').trim().slice(0, 120);
  const safeLeadId = String(leadId || '').trim();
  const safeLeadPhone = normalizePhoneDigits(leadPhone);
  const safeNegocioId = String(negocioId || '').trim();
  const safeCreatedBy = String(createdBy || '').trim().slice(0, 120);

  const txRef = db.collection(FINANCE_TRANSACTIONS_COLLECTION).doc();
  let updatedService = null;

  await db.runTransaction(async (trx) => {
    let serviceRef = null;
    let serviceSerialized = null;

    if (safeServiceId) {
      serviceRef = db.collection(FINANCE_SERVICES_COLLECTION).doc(safeServiceId);
      const serviceSnap = await trx.get(serviceRef);
      if (!serviceSnap.exists) {
        throw new Error('Servicio no encontrado para registrar movimiento.');
      }
      serviceSerialized = serializeFinanceService(serviceSnap.id, serviceSnap.data() || {});
      if (!serviceSerialized) {
        throw new Error('Servicio inválido para registrar movimiento.');
      }
    }

    const payload = {
      type: safeType,
      amount: safeAmount,
      category: safeCategory,
      paymentMethod: safePaymentMethod,
      notes: safeNotes,
      reference: safeReference,
      serviceId: safeServiceId,
      serviceName: String(serviceSerialized?.serviceName || ''),
      clientName: String(serviceSerialized?.clientName || ''),
      leadId: safeLeadId || String(serviceSerialized?.leadId || ''),
      leadPhone: safeLeadPhone || normalizePhoneDigits(serviceSerialized?.leadPhone || ''),
      negocioId: safeNegocioId || String(serviceSerialized?.negocioId || ''),
      isAdvance: isAdvance === true,
      occurredAt: safeOccurredAt,
      createdBy: safeCreatedBy,
      createdAt: Timestamp.now(),
      updatedAt: Timestamp.now(),
    };
    trx.set(txRef, payload);

    if (serviceRef && affectService) {
      const patch = { updatedAt: Timestamp.now() };

      if (safeType === 'income') {
        const nextPaid = roundMoney(serviceSerialized.paidAmount + safeAmount);
        const nextPending = Math.max(0, roundMoney(serviceSerialized.totalAmount - nextPaid));
        let nextStatus = serviceSerialized.status;
        if (nextStatus !== 'cancelado') {
          if (nextPending <= 0) nextStatus = 'pagado';
          else if (nextPaid > 0 && nextStatus === 'pendiente') nextStatus = 'en_proceso';
        }

        patch.paidAmount = nextPaid;
        patch.pendingAmount = nextPending;
        patch.status = nextStatus;
        patch.lastPaymentAt = safeOccurredAt;
      }

      if (safeType === 'expense') {
        const nextCost = roundMoney(serviceSerialized.actualCostAmount + safeAmount);
        patch.actualCostAmount = nextCost;
        patch.lastExpenseAt = safeOccurredAt;
      }

      trx.set(serviceRef, patch, { merge: true });
      updatedService = { id: safeServiceId };
    }
  });

  const txSnap = await txRef.get();
  const serializedTx = serializeFinanceTransaction(txSnap.id, txSnap.data() || {});

  let serializedService = null;
  if (updatedService?.id) {
    const serviceSnap = await db.collection(FINANCE_SERVICES_COLLECTION).doc(updatedService.id).get();
    if (serviceSnap.exists) {
      serializedService = serializeFinanceService(serviceSnap.id, serviceSnap.data() || {});
    }
  }

  return {
    transaction: serializedTx,
    service: serializedService,
  };
}

function getVercelHeaders() {
  const token = String(process.env.VERCEL_TOKEN || '').trim();
  if (!token) throw new Error('Falta VERCEL_TOKEN');
  return {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
  };
}

async function syncCustomDomain({
  slug = '',
  domain = '',
  previousDomain = '',
} = {}) {
  const projectId = String(process.env.VERCEL_PROJECT_ID || '').trim();
  const edgeConfigId = String(process.env.VERCEL_EDGE_CONFIG || '').trim();
  if (!projectId) throw new Error('Falta VERCEL_PROJECT_ID');
  if (!edgeConfigId) throw new Error('Falta VERCEL_EDGE_CONFIG');

  const headers = getVercelHeaders();
  const nextDomain = normalizeDomainInput(domain);
  const prevDomain = normalizeDomainInput(previousDomain);
  const safeSlug = String(slug || '').trim();

  const edgeItems = [];
  if (prevDomain && prevDomain !== nextDomain) {
    edgeItems.push({ operation: 'delete', key: prevDomain });
  }
  if (nextDomain) {
    edgeItems.push({ operation: 'upsert', key: nextDomain, value: safeSlug });
  }

  if (edgeItems.length > 0) {
    await axios.patch(
      `https://api.vercel.com/v1/edge-config/${edgeConfigId}/items`,
      { items: edgeItems },
      { headers }
    );
  }

  if (prevDomain && prevDomain !== nextDomain) {
    try {
      await axios.delete(
        `https://api.vercel.com/v10/projects/${projectId}/domains/${encodeURIComponent(prevDomain)}`,
        { headers }
      );
    } catch {
      // Best-effort: un dominio previo puede no existir en Vercel.
    }
  }

  if (nextDomain) {
    try {
      await axios.post(
        `https://api.vercel.com/v10/projects/${projectId}/domains`,
        { name: nextDomain },
        { headers }
      );
    } catch (error) {
      const detail = String(error?.response?.data?.error?.message || error?.message || '');
      if (!/already|exists|conflict|in use/i.test(detail)) {
        throw error;
      }
    }
  }

  return { domain: nextDomain, previousDomain: prevDomain };
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

app.post('/api/whatsapp/send-direct', async (req, res) => {
  const { phone = '', message = '', leadId = '' } = req.body || {};
  const safePhone = normalizePhoneDigits(phone);
  const safeLeadId = String(leadId || '').trim();
  const safeMessage = String(message || '').trim();

  if (!safeMessage) {
    return res.status(400).json({ error: 'Falta message.' });
  }
  if (!safePhone && !safeLeadId) {
    return res.status(400).json({ error: 'Falta phone o leadId.' });
  }

  try {
    const result = await sendLeadTextMessage({
      leadId: safeLeadId,
      phoneDigits: safePhone,
      content: safeMessage,
    });
    return res.json({ success: true, result });
  } catch (error) {
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/admin/custom-domain', async (req, res) => {
  const {
    negocioId = '',
    slug = '',
    domain = '',
    previousDomain = '',
  } = req.body || {};

  const safeNegocioId = String(negocioId || '').trim();
  const safeSlug = String(slug || '').trim();
  const safeDomain = normalizeDomainInput(domain);
  const safePreviousDomain = normalizeDomainInput(previousDomain);

  if (!safeNegocioId) {
    return res.status(400).json({ error: 'Falta negocioId.' });
  }
  if (safeDomain && !safeSlug) {
    return res.status(400).json({ error: 'Slug requerido cuando domain tiene valor.' });
  }

  try {
    const sync = await syncCustomDomain({
      slug: safeSlug,
      domain: safeDomain,
      previousDomain: safePreviousDomain,
    });
    return res.json({ success: true, ...sync });
  } catch (error) {
    console.error('[admin/custom-domain] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.get('/api/crm/lead-business', async (req, res) => {
  const { leadId = '', phone = '', negocioId = '' } = req.query || {};

  try {
    const leadCtx = await resolveLeadByIdentity({ leadId, phone });
    const negocioCtx = await resolveNegocioByIdentity({
      negocioId,
      leadId: leadCtx.leadId,
      phoneDigits: leadCtx.phoneDigits,
    });

    const negocio = serializeNegocio(
      negocioCtx.negocioId,
      negocioCtx.negocioData,
      { leadId: leadCtx.leadId, phoneDigits: leadCtx.phoneDigits }
    );

    const formUrl = buildLeadFormUrl(getDefaultFormUrl(), {
      negocioId: negocioCtx.negocioId,
      leadId: leadCtx.leadId,
      phone: leadCtx.phoneDigits,
    });
    const sampleUrl = buildSampleFormUrl(
      normalizePhoneDigits(
        negocioCtx.negocioData?.contactWhatsapp
        || negocioCtx.negocioData?.leadPhone
        || leadCtx.phoneDigits
        || leadCtx.leadData?.telefono
        || ''
      )
    );

    return res.json({
      success: true,
      lead: {
        id: String(leadCtx.leadId || ''),
        phone: normalizePhoneDigits(leadCtx.phoneDigits || leadCtx.leadData?.telefono || ''),
        name: String(leadCtx.leadData?.nombre || ''),
        estado: String(leadCtx.leadData?.estado || ''),
        etapa: String(leadCtx.leadData?.etapa || leadCtx.leadData?.etapaNombre || ''),
      },
      negocio,
      panelUrl: getPanelAccessUrl(),
      formUrl,
      sampleUrl,
      sampleReadyTrigger: String(
        leadCtx.leadData?.sampleFlow?.onReadyTrigger
        || negocioCtx.negocioData?.sampleOnReadyTrigger
        || ''
      ),
      sampleReadyStageKey: String(
        leadCtx.leadData?.sampleFlow?.onReadyStageKey
        || negocioCtx.negocioData?.sampleOnReadyStageKey
        || ''
      ),
      sampleEnabled: leadCtx.leadData?.sampleFlow?.enabled === true,
      sampleExpiresAt: toIsoOrNull(leadCtx.leadData?.sampleFlow?.expiresAt),
    });
  } catch (error) {
    console.error('[crm/lead-business] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/lead-business/activate-plan', async (req, res) => {
  const {
    leadId = '',
    phone = '',
    negocioId = '',
    plan = '',
    email = '',
    sendAccess = true,
    autoCreateBusiness = true,
  } = req.body || {};

  const safePlan = String(plan || '').trim().toLowerCase();
  if (!safePlan || !['basic', 'pro', 'premium'].includes(safePlan)) {
    return res.status(400).json({ error: 'Plan inválido. Usa: basic, pro o premium.' });
  }

  if (!String(leadId || '').trim() && !String(phone || '').trim() && !String(negocioId || '').trim()) {
    return res.status(400).json({ error: 'Falta leadId, phone o negocioId.' });
  }

  try {
    const leadCtx = await resolveLeadByIdentity({ leadId, phone });
    let negocioCtx = await resolveNegocioByIdentity({
      negocioId,
      leadId: leadCtx.leadId,
      phoneDigits: leadCtx.phoneDigits,
    });

    const shouldAutoCreate = parseBooleanInput(autoCreateBusiness, true);
    if (!negocioCtx.negocioRef) {
      if (!shouldAutoCreate) {
        return res.status(404).json({ error: 'No existe negocio vinculado para este lead.' });
      }

      const leadPhone = normalizePhoneDigits(leadCtx.phoneDigits || leadCtx.leadData?.telefono || '');
      if (!leadCtx.leadId && !leadPhone) {
        return res.status(400).json({ error: 'No se pudo resolver lead para crear negocio.' });
      }

      const newData = {
        leadId: String(leadCtx.leadId || ''),
        leadPhone,
        companyInfo: String(leadCtx.leadData?.nombre || '').trim() || 'Cliente',
        status: 'Sin procesar',
        createdAt: Timestamp.now(),
        updatedAt: Timestamp.now(),
      };
      const ref = await db.collection('Negocios').add(newData);
      const snap = await ref.get();
      negocioCtx = {
        negocioRef: ref,
        negocioId: ref.id,
        negocioSnap: snap,
        negocioData: snap.data() || newData,
      };
    }

    const current = negocioCtx.negocioData || {};
    const currentPin = String(current.pin || '').trim();
    const finalPin = /^\d{4}$/.test(currentPin) ? currentPin : generarPIN();

    let planDurationDays = 30;
    if (safePlan === 'premium') planDurationDays = 365;
    if (safePlan === 'basic') planDurationDays = 365;

    const nowDate = new Date();
    const renewalDate = dayjs(nowDate).add(planDurationDays, 'day').toDate();
    const leadPhone = normalizePhoneDigits(
      current.leadPhone || leadCtx.phoneDigits || leadCtx.leadData?.telefono || ''
    );
    const contactWhatsapp = normalizePhoneDigits(current.contactWhatsapp || leadPhone || '');

    const updateData = {
      plan: safePlan,
      pin: finalPin,
      pinCreatedAt: Timestamp.now(),
      planStartDate: Timestamp.fromDate(nowDate),
      planRenewalDate: Timestamp.fromDate(renewalDate),
      planDurationDays,
      planActivatedAt: Timestamp.now(),
      updatedAt: Timestamp.now(),
      subscriptionStatus: 'active',
      websiteArchived: false,
      leadId: String(current.leadId || leadCtx.leadId || ''),
      leadPhone,
      contactWhatsapp,
    };
    if (String(email || '').trim()) {
      updateData.contactEmail = String(email || '').trim();
    }

    await negocioCtx.negocioRef.set(updateData, { merge: true });
    const updatedSnap = await negocioCtx.negocioRef.get();
    const updatedData = updatedSnap.data() || {};

    const shouldSendAccess = parseBooleanInput(sendAccess, true);
    let whatsappSent = false;
    let whatsappError = '';

    if (shouldSendAccess) {
      const companyName = String(updatedData.companyInfo || current.companyInfo || leadCtx.leadData?.nombre || 'Tu negocio');
      const accessPhone = normalizePhoneDigits(updatedData.leadPhone || updatedData.contactWhatsapp || leadPhone);
      const panelUrl = getPanelAccessUrl();
      const message = generarMensajeCredenciales({
        companyName,
        pin: finalPin,
        phone: accessPhone || leadCtx.phoneDigits || '',
        plan: safePlan,
        loginUrl: panelUrl,
      });

      try {
        await sendLeadTextMessage({
          leadId: String(leadCtx.leadId || updatedData.leadId || ''),
          phoneDigits: accessPhone || leadCtx.phoneDigits,
          content: message,
        });
        whatsappSent = true;
      } catch (sendError) {
        whatsappError = String(sendError?.message || sendError);
        console.warn('[crm/activate-plan] No se pudo enviar acceso por WhatsApp:', whatsappError);
      }
    }

    return res.json({
      success: true,
      negocio: serializeNegocio(updatedSnap.id, updatedData, {
        leadId: leadCtx.leadId,
        phoneDigits: leadCtx.phoneDigits,
      }),
      panelUrl: getPanelAccessUrl(),
      formUrl: buildLeadFormUrl(getDefaultFormUrl(), {
        negocioId: updatedSnap.id,
        leadId: String(updatedData.leadId || leadCtx.leadId || ''),
        phone: normalizePhoneDigits(updatedData.leadPhone || leadCtx.phoneDigits || ''),
      }),
      whatsappSent,
      ...(whatsappError ? { whatsappError } : {}),
    });
  } catch (error) {
    console.error('[crm/activate-plan] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/lead-business/send-access', async (req, res) => {
  const {
    leadId = '',
    phone = '',
    negocioId = '',
    autoGeneratePin = true,
  } = req.body || {};

  if (!String(leadId || '').trim() && !String(phone || '').trim() && !String(negocioId || '').trim()) {
    return res.status(400).json({ error: 'Falta leadId, phone o negocioId.' });
  }

  try {
    const leadCtx = await resolveLeadByIdentity({ leadId, phone });
    const negocioCtx = await resolveNegocioByIdentity({
      negocioId,
      leadId: leadCtx.leadId,
      phoneDigits: leadCtx.phoneDigits,
    });

    if (!negocioCtx.negocioRef || !negocioCtx.negocioData) {
      return res.status(404).json({ error: 'No existe negocio vinculado para este lead.' });
    }

    const current = negocioCtx.negocioData || {};
    const shouldAutoGeneratePin = parseBooleanInput(autoGeneratePin, true);
    let finalPin = String(current.pin || '').trim();
    if (!/^\d{4}$/.test(finalPin)) {
      if (!shouldAutoGeneratePin) {
        return res.status(400).json({ error: 'Este negocio no tiene PIN. Activa un plan primero.' });
      }
      finalPin = generarPIN();
      await negocioCtx.negocioRef.set(
        {
          pin: finalPin,
          pinCreatedAt: Timestamp.now(),
          updatedAt: Timestamp.now(),
        },
        { merge: true }
      );
    }

    const companyName = String(current.companyInfo || leadCtx.leadData?.nombre || 'Tu negocio');
    const accessPhone = normalizePhoneDigits(
      current.leadPhone || current.contactWhatsapp || leadCtx.phoneDigits || leadCtx.leadData?.telefono || ''
    );
    const panelUrl = getPanelAccessUrl();
    const message = generarMensajeCredenciales({
      companyName,
      pin: finalPin,
      phone: accessPhone || leadCtx.phoneDigits || '',
      plan: String(current.plan || 'basic'),
      loginUrl: panelUrl,
    });

    await sendLeadTextMessage({
      leadId: String(leadCtx.leadId || current.leadId || ''),
      phoneDigits: accessPhone || leadCtx.phoneDigits,
      content: message,
    });

    const updatedSnap = await negocioCtx.negocioRef.get();
    return res.json({
      success: true,
      negocio: serializeNegocio(updatedSnap.id, updatedSnap.data() || {}, {
        leadId: leadCtx.leadId,
        phoneDigits: leadCtx.phoneDigits,
      }),
      panelUrl,
    });
  } catch (error) {
    console.error('[crm/send-access] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/lead-business/send-form-link', async (req, res) => {
  const {
    leadId = '',
    phone = '',
    negocioId = '',
    formUrl = '',
    message = '',
  } = req.body || {};

  if (!String(leadId || '').trim() && !String(phone || '').trim() && !String(negocioId || '').trim()) {
    return res.status(400).json({ error: 'Falta leadId, phone o negocioId.' });
  }

  try {
    const leadCtx = await resolveLeadByIdentity({ leadId, phone });
    const negocioCtx = await resolveNegocioByIdentity({
      negocioId,
      leadId: leadCtx.leadId,
      phoneDigits: leadCtx.phoneDigits,
    });

    const safeFormUrl = buildLeadFormUrl(formUrl || getDefaultFormUrl(), {
      negocioId: negocioCtx.negocioId || negocioId,
      leadId: leadCtx.leadId,
      phone: leadCtx.phoneDigits,
    });

    const customMessage = String(message || '').trim();
    const finalMessage =
      customMessage ||
      `¡Hola! 👋 Gracias por tu interés.\n\nPara crear tu muestra GRATIS, llena este formulario de 2 minutos:\n${safeFormUrl}\n\nCuando lo termines me avisas por aquí y continuamos.`;

    await sendLeadTextMessage({
      leadId: leadCtx.leadId,
      phoneDigits: leadCtx.phoneDigits,
      content: finalMessage,
    });

    if (leadCtx.leadRef) {
      await leadCtx.leadRef.set(
        {
          formLinkSentAt: new Date(),
          formLinkSentUrl: safeFormUrl,
          etiquetas: admin.firestore.FieldValue.arrayUnion('FormLinkSent'),
        },
        { merge: true }
      );
    }

    return res.json({
      success: true,
      formUrl: safeFormUrl,
      negocioId: negocioCtx.negocioId || '',
    });
  } catch (error) {
    console.error('[crm/send-form-link] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/lead-business/send-sample-link', async (req, res) => {
  const {
    leadId = '',
    phone = '',
    negocioId = '',
    sampleUrl = '',
    message = '',
    onReadyTrigger = '',
    onReadyStageKey = '',
  } = req.body || {};

  if (!String(leadId || '').trim() && !String(phone || '').trim() && !String(negocioId || '').trim()) {
    return res.status(400).json({ error: 'Falta leadId, phone o negocioId.' });
  }

  try {
    const leadCtx = await resolveLeadByIdentity({ leadId, phone });
    const negocioCtx = await resolveNegocioByIdentity({
      negocioId,
      leadId: leadCtx.leadId,
      phoneDigits: leadCtx.phoneDigits,
    });

    const negocio = negocioCtx.negocioData || {};
    const targetPhone = normalizePhoneDigits(
      negocio.contactWhatsapp || negocio.leadPhone || leadCtx.phoneDigits || leadCtx.leadData?.telefono || ''
    );
    if (!targetPhone) {
      return res.status(400).json({ error: 'No se encontró teléfono para enviar la muestra.' });
    }

    const normalizedStageKey = String(onReadyStageKey || '')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '');
    const normalizedTrigger = String(onReadyTrigger || '').trim();
    const resolvedSampleUrl = String(sampleUrl || '').trim() || buildSampleFormUrl(targetPhone);
    if (!resolvedSampleUrl) {
      return res.status(500).json({ error: 'No se pudo construir la URL de muestra.' });
    }

    const finalMessage = String(message || '').trim() || buildSampleInviteMessage({
      companyName: String(negocio.companyInfo || leadCtx.leadData?.nombre || ''),
      sampleUrl: resolvedSampleUrl,
    });

    const sent = await sendWhatsappFallbackMessage({
      leadId: String(leadCtx.leadId || negocio.leadId || ''),
      phoneDigits: targetPhone,
      message: finalMessage,
    });

    const now = new Date();
    const expiresAt = new Date(now.getTime() + 24 * 60 * 60 * 1000);
    if (leadCtx.leadRef) {
      await leadCtx.leadRef.set(
        {
          sampleFlow: {
            enabled: true,
            enabledAt: now,
            source: 'crm_send_sample_link',
            phone: targetPhone,
            sampleUrl: resolvedSampleUrl,
            mode: 'funnel',
            onReadyTrigger: normalizedTrigger,
            onReadyStageKey: normalizedStageKey,
            expiresAt,
          },
          sampleLinkSentAt: now,
          sampleLinkSentUrl: resolvedSampleUrl,
          etiquetas: admin.firestore.FieldValue.arrayUnion('SampleLinkSent', 'MuestraActiva'),
        },
        { merge: true }
      );
    }

    if (negocioCtx.negocioRef) {
      await negocioCtx.negocioRef.set(
        {
          sampleFlowType: 'funnel',
          suppressDefaultFollowups: true,
          sampleEnabledAt: now,
          sampleExpiresAt: expiresAt,
          sampleLinkSentAt: now,
          sampleLinkSentUrl: resolvedSampleUrl,
          sampleOnReadyTrigger: normalizedTrigger || admin.firestore.FieldValue.delete(),
          sampleOnReadyStageKey: normalizedStageKey || admin.firestore.FieldValue.delete(),
          updatedAt: now,
        },
        { merge: true }
      );
    }

    return res.json({
      success: true,
      sampleUrl: resolvedSampleUrl,
      sentVia: sent.method,
      target: sent.target,
      leadId: String(leadCtx.leadId || ''),
      negocioId: negocioCtx.negocioId || '',
    });
  } catch (error) {
    console.error('[crm/send-sample-link] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/lead-business/update', async (req, res) => {
  const {
    leadId = '',
    phone = '',
    negocioId = '',
    plan = '',
    status = '',
    contactEmail = '',
    businessStory = '',
    descripcion = '',
    templateId = '',
    schema = null,
    slug = '',
    planStartAt = '',
    expiresAt = '',
  } = req.body || {};

  if (!String(leadId || '').trim() && !String(phone || '').trim() && !String(negocioId || '').trim()) {
    return res.status(400).json({ error: 'Falta leadId, phone o negocioId.' });
  }

  try {
    const leadCtx = await resolveLeadByIdentity({ leadId, phone });
    const negocioCtx = await resolveNegocioByIdentity({
      negocioId,
      leadId: leadCtx.leadId,
      phoneDigits: leadCtx.phoneDigits,
    });

    if (!negocioCtx.negocioRef || !negocioCtx.negocioData) {
      return res.status(404).json({ error: 'No existe negocio vinculado para este lead.' });
    }

    const startTs = parseDateInputToTimestamp(planStartAt, 'planStartAt');
    const endTs = parseDateInputToTimestamp(expiresAt, 'expiresAt');
    if (startTs && endTs && endTs.toMillis() < startTs.toMillis()) {
      return res.status(400).json({ error: 'La fecha de expiración no puede ser menor al inicio.' });
    }

    const patch = { updatedAt: Timestamp.now() };
    const safePlan = String(plan || '').trim().toLowerCase();
    if (safePlan) patch.plan = safePlan;
    if (String(status || '').trim()) patch.status = String(status || '').trim();
    if (String(contactEmail || '').trim()) patch.contactEmail = String(contactEmail || '').trim();

    const story = String(descripcion || businessStory || '').trim();
    if (story) patch.businessStory = story;
    if (String(templateId || '').trim()) patch.templateId = String(templateId || '').trim();
    if (String(slug || '').trim()) patch.slug = String(slug || '').trim();
    if (schema && typeof schema === 'object') patch.schema = schema;

    if (startTs) {
      patch.planStartAt = startTs;
      patch.planStartDate = startTs;
    }
    if (endTs) {
      patch.expiresAt = endTs;
      patch.planRenewalDate = endTs;
      patch.planExpiresAt = endTs;
    }

    await negocioCtx.negocioRef.set(patch, { merge: true });
    const updatedSnap = await negocioCtx.negocioRef.get();
    const updatedData = updatedSnap.data() || {};

    return res.json({
      success: true,
      negocio: serializeNegocio(updatedSnap.id, updatedData, {
        leadId: leadCtx.leadId,
        phoneDigits: leadCtx.phoneDigits,
      }),
    });
  } catch (error) {
    console.error('[crm/lead-business/update] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/lead-business/domain', async (req, res) => {
  const {
    leadId = '',
    phone = '',
    negocioId = '',
    domain = '',
    slug = '',
  } = req.body || {};

  if (!String(leadId || '').trim() && !String(phone || '').trim() && !String(negocioId || '').trim()) {
    return res.status(400).json({ error: 'Falta leadId, phone o negocioId.' });
  }

  try {
    const leadCtx = await resolveLeadByIdentity({ leadId, phone });
    const negocioCtx = await resolveNegocioByIdentity({
      negocioId,
      leadId: leadCtx.leadId,
      phoneDigits: leadCtx.phoneDigits,
    });

    if (!negocioCtx.negocioRef || !negocioCtx.negocioData) {
      return res.status(404).json({ error: 'No existe negocio vinculado para este lead.' });
    }

    const current = negocioCtx.negocioData || {};
    const safeDomain = normalizeDomainInput(domain);
    const safePreviousDomain = normalizeDomainInput(current.dominio || '');
    const safeSlug = String(slug || current.slug || current?.schema?.slug || '').trim();
    const currentPlan = String(current.plan || '').trim().toLowerCase();

    if (safeDomain && currentPlan !== 'premium') {
      return res.status(400).json({ error: 'Solo el plan premium puede usar dominio personalizado.' });
    }
    if (safeDomain && !safeSlug) {
      return res.status(400).json({ error: 'Slug requerido para asignar dominio personalizado.' });
    }

    const changedDomain = safeDomain !== safePreviousDomain;
    const changedSlug = Boolean(safeSlug) && safeSlug !== String(current.slug || '').trim();
    const shouldSync = Boolean(safeDomain || safePreviousDomain);

    if (changedDomain || changedSlug) {
      await negocioCtx.negocioRef.set(
        {
          dominio: safeDomain || null,
          ...(safeSlug ? { slug: safeSlug } : {}),
          updatedAt: Timestamp.now(),
        },
        { merge: true }
      );
    } else if (!shouldSync) {
      return res.json({
        success: true,
        noChange: true,
        negocio: serializeNegocio(negocioCtx.negocioId, current, {
          leadId: leadCtx.leadId,
          phoneDigits: leadCtx.phoneDigits,
        }),
      });
    }

    if (shouldSync) {
      await syncCustomDomain({
        slug: safeSlug,
        domain: safeDomain,
        previousDomain: safePreviousDomain,
      });
    }

    const updatedSnap = await negocioCtx.negocioRef.get();
    return res.json({
      success: true,
      negocio: serializeNegocio(updatedSnap.id, updatedSnap.data() || {}, {
        leadId: leadCtx.leadId,
        phoneDigits: leadCtx.phoneDigits,
      }),
    });
  } catch (error) {
    console.error('[crm/lead-business/domain] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/lead-business/resend-info', async (req, res) => {
  const {
    leadId = '',
    phone = '',
    negocioId = '',
    message = '',
  } = req.body || {};

  if (!String(leadId || '').trim() && !String(phone || '').trim() && !String(negocioId || '').trim()) {
    return res.status(400).json({ error: 'Falta leadId, phone o negocioId.' });
  }

  try {
    const leadCtx = await resolveLeadByIdentity({ leadId, phone });
    const negocioCtx = await resolveNegocioByIdentity({
      negocioId,
      leadId: leadCtx.leadId,
      phoneDigits: leadCtx.phoneDigits,
    });

    if (!negocioCtx.negocioData) {
      return res.status(404).json({ error: 'No existe negocio vinculado para este lead.' });
    }

    const negocio = negocioCtx.negocioData;
    const targetPhone = normalizePhoneDigits(
      negocio.contactWhatsapp || negocio.leadPhone || leadCtx.phoneDigits || leadCtx.leadData?.telefono || ''
    );
    if (!targetPhone) {
      return res.status(400).json({ error: 'No se encontró teléfono para reenviar información.' });
    }

    const briefUrl = buildInformationFormUrl(targetPhone);
    if (!briefUrl) {
      return res.status(500).json({ error: 'No se pudo construir la URL del formulario.' });
    }

    const finalMessage = String(message || '').trim() || buildBriefInviteMessage({
      companyName: String(negocio.companyInfo || leadCtx.leadData?.nombre || ''),
      briefUrl,
    });

    const sent = await sendWhatsappFallbackMessage({
      leadId: String(leadCtx.leadId || negocio.leadId || ''),
      phoneDigits: targetPhone,
      message: finalMessage,
    });

    return res.json({
      success: true,
      briefUrl,
      sentVia: sent.method,
      target: sent.target,
    });
  } catch (error) {
    console.error('[crm/lead-business/resend-info] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/lead-business/delete', async (req, res) => {
  const {
    leadId = '',
    phone = '',
    negocioId = '',
    force = false,
  } = req.body || {};

  if (!String(leadId || '').trim() && !String(phone || '').trim() && !String(negocioId || '').trim()) {
    return res.status(400).json({ error: 'Falta leadId, phone o negocioId.' });
  }

  try {
    const leadCtx = await resolveLeadByIdentity({ leadId, phone });
    const negocioCtx = await resolveNegocioByIdentity({
      negocioId,
      leadId: leadCtx.leadId,
      phoneDigits: leadCtx.phoneDigits,
    });

    if (!negocioCtx.negocioRef || !negocioCtx.negocioData) {
      return res.status(404).json({ error: 'No existe negocio vinculado para este lead.' });
    }

    const negocio = negocioCtx.negocioData || {};
    const paidActive = isPaidPlan(negocio.plan) && String(negocio.status || '').toLowerCase() === 'activo';
    if (paidActive && !parseBooleanInput(force, false)) {
      return res.status(409).json({
        error: 'Este negocio tiene plan pagado activo. Confirma eliminación con force=true.',
      });
    }

    const domain = normalizeDomainInput(negocio.dominio || '');
    const slug = String(negocio.slug || negocio?.schema?.slug || '').trim();
    let customDomainCleared = false;
    let archivoDeleted = false;

    if (domain && slug) {
      try {
        await syncCustomDomain({
          slug,
          domain: '',
          previousDomain: domain,
        });
        customDomainCleared = true;
      } catch (error) {
        console.warn('[crm/lead-business/delete] No se pudo limpiar custom domain:', error?.message || error);
      }
    }

    await negocioCtx.negocioRef.delete();

    try {
      await db.collection('ArchivoNegocios').doc(negocioCtx.negocioId).delete();
      archivoDeleted = true;
    } catch (error) {
      console.warn('[crm/lead-business/delete] ArchivoNegocios best-effort falló:', error?.message || error);
    }

    return res.json({
      success: true,
      negocioId: negocioCtx.negocioId,
      customDomainCleared,
      archivoDeleted,
    });
  } catch (error) {
    console.error('[crm/lead-business/delete] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.get('/api/crm/finance/catalog', async (req, res) => {
  try {
    const onlyActive = parseBooleanInput(req.query?.active, false);
    const maxDocs = Math.max(20, Math.min(1000, Number(req.query?.maxDocs || 300)));

    const snap = await db
      .collection(FINANCE_CATALOG_COLLECTION)
      .orderBy('updatedAt', 'desc')
      .limit(maxDocs)
      .get();

    let items = snap.docs
      .map((docSnap) => serializeFinanceCatalogItem(docSnap.id, docSnap.data() || {}))
      .filter(Boolean);

    if (onlyActive) {
      items = items.filter((item) => item.active !== false);
    }

    return res.json({
      success: true,
      items,
    });
  } catch (error) {
    console.error('[crm/finance/catalog] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/finance/catalog', async (req, res) => {
  const {
    name = '',
    category = '',
    description = '',
    unitPrice = '',
    currency = 'MXN',
    sku = '',
    active = true,
  } = req.body || {};

  try {
    const safeName = String(name || '').trim();
    if (!safeName) {
      return res.status(400).json({ error: 'Falta name.' });
    }

    const safePrice = parseMoneyInput(unitPrice, 'unitPrice', {
      required: true,
      allowZero: true,
      min: 0,
    });
    const safeCategory = String(category || '').trim().slice(0, 120);
    const safeDescription = String(description || '').trim().slice(0, 2000);
    const safeCurrency = String(currency || 'MXN').trim().toUpperCase().slice(0, 8) || 'MXN';
    const safeSku = String(sku || '').trim().slice(0, 60);
    const safeActive = parseBooleanInput(active, true);

    const ref = await db.collection(FINANCE_CATALOG_COLLECTION).add({
      name: safeName,
      category: safeCategory,
      description: safeDescription,
      unitPrice: safePrice,
      currency: safeCurrency,
      sku: safeSku,
      active: safeActive,
      createdAt: Timestamp.now(),
      updatedAt: Timestamp.now(),
    });

    const created = await ref.get();
    return res.status(201).json({
      success: true,
      item: serializeFinanceCatalogItem(created.id, created.data() || {}),
    });
  } catch (error) {
    console.error('[crm/finance/catalog:create] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/finance/catalog/:itemId', async (req, res) => {
  const itemId = String(req.params?.itemId || '').trim();
  if (!itemId) return res.status(400).json({ error: 'Falta itemId.' });

  const {
    name,
    category,
    description,
    unitPrice,
    currency,
    sku,
    active,
  } = req.body || {};

  try {
    const ref = db.collection(FINANCE_CATALOG_COLLECTION).doc(itemId);
    const snap = await ref.get();
    if (!snap.exists) return res.status(404).json({ error: 'Item de catálogo no encontrado.' });

    const patch = { updatedAt: Timestamp.now() };
    if (name !== undefined) patch.name = String(name || '').trim().slice(0, 160);
    if (category !== undefined) patch.category = String(category || '').trim().slice(0, 120);
    if (description !== undefined) patch.description = String(description || '').trim().slice(0, 2000);
    if (unitPrice !== undefined) {
      patch.unitPrice = parseMoneyInput(unitPrice, 'unitPrice', {
        required: true,
        allowZero: true,
        min: 0,
      });
    }
    if (currency !== undefined) {
      patch.currency = String(currency || 'MXN').trim().toUpperCase().slice(0, 8) || 'MXN';
    }
    if (sku !== undefined) patch.sku = String(sku || '').trim().slice(0, 60);
    if (active !== undefined) patch.active = parseBooleanInput(active, true);

    await ref.set(patch, { merge: true });
    const updated = await ref.get();
    return res.json({
      success: true,
      item: serializeFinanceCatalogItem(updated.id, updated.data() || {}),
    });
  } catch (error) {
    console.error('[crm/finance/catalog:update] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.delete('/api/crm/finance/catalog/:itemId', async (req, res) => {
  const itemId = String(req.params?.itemId || '').trim();
  if (!itemId) return res.status(400).json({ error: 'Falta itemId.' });

  try {
    const ref = db.collection(FINANCE_CATALOG_COLLECTION).doc(itemId);
    const snap = await ref.get();
    if (!snap.exists) return res.status(404).json({ error: 'Item de catálogo no encontrado.' });
    await ref.delete();
    return res.json({ success: true, itemId });
  } catch (error) {
    console.error('[crm/finance/catalog:delete] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.get('/api/crm/finance/overview', async (req, res) => {
  try {
    const fromMs = parseDateRangeBound(req.query?.from);
    const toMs = parseDateRangeBound(req.query?.to, { endOfDay: true });
    const maxServices = Math.max(100, Math.min(2500, Number(req.query?.maxServices || 1200)));
    const maxTransactions = Math.max(100, Math.min(6000, Number(req.query?.maxTransactions || 2000)));

    const [servicesSnap, transactionsSnap] = await Promise.all([
      db.collection(FINANCE_SERVICES_COLLECTION).orderBy('updatedAt', 'desc').limit(maxServices).get(),
      db.collection(FINANCE_TRANSACTIONS_COLLECTION).orderBy('occurredAt', 'desc').limit(maxTransactions).get(),
    ]);

    const services = servicesSnap.docs
      .map((docSnap) => serializeFinanceService(docSnap.id, docSnap.data() || {}))
      .filter(Boolean);

    const transactionsRaw = transactionsSnap.docs
      .map((docSnap) => serializeFinanceTransaction(docSnap.id, docSnap.data() || {}))
      .filter(Boolean);

    const transactions = transactionsRaw.filter((item) => {
      const ms = item.occurredAt ? new Date(item.occurredAt).getTime() : 0;
      if (!Number.isFinite(ms) || ms <= 0) return false;
      if (Number.isFinite(fromMs) && ms < fromMs) return false;
      if (Number.isFinite(toMs) && ms > toMs) return false;
      return true;
    });

    const summary = buildFinanceSummary({ services, transactions });

    return res.json({
      success: true,
      range: {
        from: Number.isFinite(fromMs) ? new Date(fromMs).toISOString() : null,
        to: Number.isFinite(toMs) ? new Date(toMs).toISOString() : null,
      },
      summary,
      sampled: {
        services: services.length,
        transactions: transactions.length,
      },
    });
  } catch (error) {
    console.error('[crm/finance/overview] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.get('/api/crm/finance/services', async (req, res) => {
  try {
    const page = Math.max(1, Number(req.query?.page || 1));
    const pageSize = Math.max(5, Math.min(200, Number(req.query?.pageSize || 30)));
    const maxDocs = Math.max(100, Math.min(4000, Number(req.query?.maxDocs || 1800)));
    const safeStatusRaw = String(req.query?.status || '').trim().toLowerCase();
    const safeSearch = String(req.query?.search || '').trim().toLowerCase();
    const safeLeadId = String(req.query?.leadId || '').trim();
    const safeLeadPhone = normalizePhoneDigits(req.query?.leadPhone || '');
    const safeNegocioId = String(req.query?.negocioId || '').trim();
    const onlyPending = parseBooleanInput(req.query?.onlyPending, false);

    const snap = await db
      .collection(FINANCE_SERVICES_COLLECTION)
      .orderBy('updatedAt', 'desc')
      .limit(maxDocs)
      .get();

    let items = snap.docs
      .map((docSnap) => serializeFinanceService(docSnap.id, docSnap.data() || {}))
      .filter(Boolean);

    if (safeStatusRaw && safeStatusRaw !== 'all') {
      const normalizedStatus = normalizeFinanceStatus(safeStatusRaw);
      items = items.filter((item) => item.status === normalizedStatus);
    }

    if (safeLeadId) {
      items = items.filter((item) => String(item.leadId || '').trim() === safeLeadId);
    }
    if (safeLeadPhone) {
      items = items.filter((item) => normalizePhoneDigits(item.leadPhone || '') === safeLeadPhone);
    }
    if (safeNegocioId) {
      items = items.filter((item) => String(item.negocioId || '').trim() === safeNegocioId);
    }

    if (onlyPending) {
      items = items.filter((item) => item.pendingAmount > 0 && item.status !== 'cancelado');
    }

    if (safeSearch) {
      items = items.filter((item) => {
        const haystack = [
          item.clientName,
          item.serviceName,
          item.description,
          item.category,
          item.leadPhone,
          item.leadId,
          item.negocioId,
        ]
          .join(' ')
          .toLowerCase();
        return haystack.includes(safeSearch);
      });
    }

    items.sort((a, b) => {
      const aMs = new Date(a.updatedAt || a.createdAt || 0).getTime();
      const bMs = new Date(b.updatedAt || b.createdAt || 0).getTime();
      return bMs - aMs;
    });

    const totalItems = items.length;
    const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
    const safePage = Math.min(page, totalPages);
    const start = (safePage - 1) * pageSize;
    const pagedItems = items.slice(start, start + pageSize);

    const totals = {
      billed: roundMoney(items.reduce((acc, item) => acc + item.totalAmount, 0)),
      collected: roundMoney(items.reduce((acc, item) => acc + item.paidAmount, 0)),
      pending: roundMoney(items.reduce((acc, item) => acc + item.pendingAmount, 0)),
      advancePending: roundMoney(items.reduce((acc, item) => acc + item.advancePendingAmount, 0)),
      estimatedProfit: roundMoney(items.reduce((acc, item) => acc + item.estimatedProfitAmount, 0)),
    };

    return res.json({
      success: true,
      items: pagedItems,
      totals,
      pagination: {
        page: safePage,
        pageSize,
        totalItems,
        totalPages,
      },
    });
  } catch (error) {
    console.error('[crm/finance/services] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/finance/services', async (req, res) => {
  const {
    clientName = '',
    serviceName = '',
    catalogItemId = '',
    catalogItemName = '',
    category = '',
    description = '',
    leadId = '',
    leadPhone = '',
    negocioId = '',
    currency = 'MXN',
    totalAmount = '',
    advancePercent = 50,
    estimatedCostAmount = 0,
    status = 'pendiente',
    dueDate = '',
    initialPaymentAmount = 0,
    initialPaymentMethod = '',
    initialPaymentNotes = '',
    initialPaymentDate = '',
    createdBy = '',
  } = req.body || {};

  try {
    const safeClientName = String(clientName || '').trim();
    const safeServiceName = String(serviceName || '').trim();
    if (!safeServiceName) {
      return res.status(400).json({ error: 'Falta serviceName.' });
    }
    if (!safeClientName && !String(leadId || '').trim() && !normalizePhoneDigits(leadPhone)) {
      return res.status(400).json({ error: 'Falta clientName o referencia del lead.' });
    }

    const safeTotalAmount = parseMoneyInput(totalAmount, 'totalAmount', {
      required: true,
      allowZero: false,
      min: 0.01,
    });
    const safeAdvancePercent = Math.max(0, Math.min(100, roundMoney(toMoneyNumber(advancePercent, 50))));
    const safeAdvanceAmount = roundMoney(safeTotalAmount * (safeAdvancePercent / 100));
    const safeEstimatedCostAmount = Math.max(
      0,
      parseMoneyInput(estimatedCostAmount, 'estimatedCostAmount', {
        required: false,
        allowZero: true,
        min: 0,
      }) || 0
    );
    const safeStatus = normalizeFinanceStatus(status);
    const safeDueDate = parseDateInputToTimestampOrNull(dueDate, 'dueDate');
    const safeLeadId = String(leadId || '').trim();
    const safeLeadPhone = normalizePhoneDigits(leadPhone);
    const safeNegocioId = String(negocioId || '').trim();
    const safeCatalogItemId = String(catalogItemId || '').trim();
    const safeCatalogItemName = String(catalogItemName || '').trim();
    const safeCurrency = String(currency || 'MXN').trim().toUpperCase().slice(0, 8) || 'MXN';
    const safeCategory = String(category || '').trim().slice(0, 120);
    const safeDescription = String(description || '').trim().slice(0, 2000);
    const safeCreatedBy = String(createdBy || '').trim().slice(0, 120);

    const safeInitialPayment = Math.max(
      0,
      parseMoneyInput(initialPaymentAmount, 'initialPaymentAmount', {
        required: false,
        allowZero: true,
        min: 0,
      }) || 0
    );
    if (safeInitialPayment > safeTotalAmount) {
      return res.status(400).json({ error: 'El anticipo inicial no puede ser mayor al total del servicio.' });
    }
    const initialPaymentAt = parseDateInputToTimestampOrNull(initialPaymentDate, 'initialPaymentDate')
      || Timestamp.now();

    const serviceRef = db.collection(FINANCE_SERVICES_COLLECTION).doc();
    const initialTxRef = safeInitialPayment > 0 ? db.collection(FINANCE_TRANSACTIONS_COLLECTION).doc() : null;

    await db.runTransaction(async (trx) => {
      const basePayload = {
        clientName: safeClientName || safeLeadPhone || safeLeadId || 'Cliente',
        serviceName: safeServiceName,
        catalogItemId: safeCatalogItemId,
        catalogItemName: safeCatalogItemName,
        category: safeCategory,
        description: safeDescription,
        leadId: safeLeadId,
        leadPhone: safeLeadPhone,
        negocioId: safeNegocioId,
        currency: safeCurrency,
        totalAmount: safeTotalAmount,
        advancePercent: safeAdvancePercent,
        advanceAmount: safeAdvanceAmount,
        paidAmount: 0,
        pendingAmount: safeTotalAmount,
        estimatedCostAmount: safeEstimatedCostAmount,
        actualCostAmount: 0,
        status: safeStatus,
        dueDate: safeDueDate,
        createdBy: safeCreatedBy,
        createdAt: Timestamp.now(),
        updatedAt: Timestamp.now(),
      };
      trx.set(serviceRef, basePayload);

      if (initialTxRef) {
        const pendingAfterPayment = Math.max(0, roundMoney(safeTotalAmount - safeInitialPayment));
        const nextStatus = pendingAfterPayment <= 0
          ? 'pagado'
          : safeStatus === 'pendiente'
            ? 'en_proceso'
            : safeStatus;

        trx.set(initialTxRef, {
          type: 'income',
          amount: safeInitialPayment,
          category: 'anticipo',
          paymentMethod: String(initialPaymentMethod || '').trim().slice(0, 80),
          notes: String(initialPaymentNotes || '').trim().slice(0, 1000),
          reference: '',
          serviceId: serviceRef.id,
          serviceName: safeServiceName,
          clientName: safeClientName || safeLeadPhone || safeLeadId || 'Cliente',
          leadId: safeLeadId,
          leadPhone: safeLeadPhone,
          negocioId: safeNegocioId,
          isAdvance: true,
          occurredAt: initialPaymentAt,
          createdBy: safeCreatedBy,
          createdAt: Timestamp.now(),
          updatedAt: Timestamp.now(),
        });

        trx.set(
          serviceRef,
          {
            paidAmount: safeInitialPayment,
            pendingAmount: pendingAfterPayment,
            status: nextStatus,
            lastPaymentAt: initialPaymentAt,
            updatedAt: Timestamp.now(),
          },
          { merge: true }
        );
      }
    });

    const createdServiceSnap = await serviceRef.get();
    const createdService = serializeFinanceService(serviceRef.id, createdServiceSnap.data() || {});

    let initialPayment = null;
    if (initialTxRef) {
      const initialTxSnap = await initialTxRef.get();
      initialPayment = serializeFinanceTransaction(initialTxRef.id, initialTxSnap.data() || {});
    }

    return res.status(201).json({
      success: true,
      service: createdService,
      ...(initialPayment ? { initialPayment } : {}),
    });
  } catch (error) {
    console.error('[crm/finance/services:create] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/finance/services/:serviceId', async (req, res) => {
  const serviceId = String(req.params?.serviceId || '').trim();
  if (!serviceId) {
    return res.status(400).json({ error: 'Falta serviceId.' });
  }

  try {
    const serviceRef = db.collection(FINANCE_SERVICES_COLLECTION).doc(serviceId);
    const serviceSnap = await serviceRef.get();
    if (!serviceSnap.exists) {
      return res.status(404).json({ error: 'Servicio no encontrado.' });
    }

    const current = serializeFinanceService(serviceSnap.id, serviceSnap.data() || {});
    const body = req.body || {};
    const patch = { updatedAt: Timestamp.now() };

    if (Object.prototype.hasOwnProperty.call(body, 'clientName')) {
      patch.clientName = String(body.clientName || '').trim().slice(0, 120);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'serviceName')) {
      patch.serviceName = String(body.serviceName || '').trim().slice(0, 160);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'catalogItemId')) {
      patch.catalogItemId = String(body.catalogItemId || '').trim();
    }
    if (Object.prototype.hasOwnProperty.call(body, 'catalogItemName')) {
      patch.catalogItemName = String(body.catalogItemName || '').trim().slice(0, 180);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'category')) {
      patch.category = String(body.category || '').trim().slice(0, 120);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'description')) {
      patch.description = String(body.description || '').trim().slice(0, 2000);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'leadId')) {
      patch.leadId = String(body.leadId || '').trim();
    }
    if (Object.prototype.hasOwnProperty.call(body, 'leadPhone')) {
      patch.leadPhone = normalizePhoneDigits(body.leadPhone);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'negocioId')) {
      patch.negocioId = String(body.negocioId || '').trim();
    }
    if (Object.prototype.hasOwnProperty.call(body, 'currency')) {
      patch.currency = String(body.currency || 'MXN').trim().toUpperCase().slice(0, 8) || 'MXN';
    }
    if (Object.prototype.hasOwnProperty.call(body, 'status')) {
      patch.status = normalizeFinanceStatus(body.status);
    }
    if (Object.prototype.hasOwnProperty.call(body, 'dueDate')) {
      patch.dueDate = parseDateInputToTimestampOrNull(body.dueDate, 'dueDate');
    }

    const totalAmountInput = Object.prototype.hasOwnProperty.call(body, 'totalAmount')
      ? parseMoneyInput(body.totalAmount, 'totalAmount', {
          required: true,
          allowZero: false,
          min: 0.01,
        })
      : current.totalAmount;
    const advancePercentInput = Object.prototype.hasOwnProperty.call(body, 'advancePercent')
      ? Math.max(0, Math.min(100, roundMoney(toMoneyNumber(body.advancePercent, current.advancePercent))))
      : current.advancePercent;
    const estimatedCostInput = Object.prototype.hasOwnProperty.call(body, 'estimatedCostAmount')
      ? Math.max(
          0,
          parseMoneyInput(body.estimatedCostAmount, 'estimatedCostAmount', {
            required: true,
            allowZero: true,
            min: 0,
          }) || 0
        )
      : current.estimatedCostAmount;

    if (current.paidAmount > totalAmountInput) {
      return res.status(400).json({
        error: 'El total del servicio no puede ser menor al monto ya cobrado.',
      });
    }

    const nextPending = Math.max(0, roundMoney(totalAmountInput - current.paidAmount));
    const nextAdvanceAmount = roundMoney(totalAmountInput * (advancePercentInput / 100));
    const autoStatus = nextPending <= 0 ? 'pagado' : (patch.status || current.status || 'pendiente');

    patch.totalAmount = totalAmountInput;
    patch.pendingAmount = nextPending;
    patch.advancePercent = advancePercentInput;
    patch.advanceAmount = nextAdvanceAmount;
    patch.estimatedCostAmount = estimatedCostInput;
    patch.status = normalizeFinanceStatus(autoStatus);

    await serviceRef.set(patch, { merge: true });
    const updatedSnap = await serviceRef.get();

    return res.json({
      success: true,
      service: serializeFinanceService(updatedSnap.id, updatedSnap.data() || {}),
    });
  } catch (error) {
    console.error('[crm/finance/services:update] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.delete('/api/crm/finance/services/:serviceId', async (req, res) => {
  const serviceId = String(req.params?.serviceId || '').trim();
  if (!serviceId) {
    return res.status(400).json({ error: 'Falta serviceId.' });
  }

  try {
    const serviceRef = db.collection(FINANCE_SERVICES_COLLECTION).doc(serviceId);
    const serviceSnap = await serviceRef.get();
    if (!serviceSnap.exists) {
      return res.status(404).json({ error: 'Servicio no encontrado.' });
    }

    const cascade = parseBooleanInput(req.query?.cascade, false);
    const probe = await db
      .collection(FINANCE_TRANSACTIONS_COLLECTION)
      .where('serviceId', '==', serviceId)
      .limit(1)
      .get();

    if (!probe.empty && !cascade) {
      return res.status(409).json({
        error: 'Este servicio tiene movimientos financieros. Usa cascade=true para eliminar todo.',
      });
    }

    let deletedTransactions = 0;
    if (cascade) {
      while (true) {
        const snap = await db
          .collection(FINANCE_TRANSACTIONS_COLLECTION)
          .where('serviceId', '==', serviceId)
          .limit(400)
          .get();
        if (snap.empty) break;

        const batch = db.batch();
        snap.docs.forEach((docSnap) => {
          batch.delete(docSnap.ref);
        });
        deletedTransactions += snap.docs.length;
        await batch.commit();

        if (snap.docs.length < 400) break;
      }
    }

    await serviceRef.delete();

    return res.json({
      success: true,
      serviceId,
      deletedTransactions,
    });
  } catch (error) {
    console.error('[crm/finance/services:delete] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/finance/services/:serviceId/payments', async (req, res) => {
  const serviceId = String(req.params?.serviceId || '').trim();
  if (!serviceId) {
    return res.status(400).json({ error: 'Falta serviceId.' });
  }

  const {
    amount = '',
    paymentMethod = '',
    notes = '',
    reference = '',
    occurredAt = '',
    category = 'cobro_servicio',
    isAdvance = false,
    createdBy = '',
  } = req.body || {};

  try {
    const paymentTimestamp = parseDateInputToTimestampOrNull(occurredAt, 'occurredAt') || Timestamp.now();
    const result = await createFinanceTransaction({
      type: 'income',
      amount,
      category,
      paymentMethod,
      notes,
      reference,
      serviceId,
      occurredAt: paymentTimestamp,
      isAdvance: parseBooleanInput(isAdvance, false),
      createdBy,
      affectService: true,
    });

    return res.status(201).json({
      success: true,
      ...result,
    });
  } catch (error) {
    console.error('[crm/finance/services:payment] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.get('/api/crm/finance/transactions', async (req, res) => {
  try {
    const page = Math.max(1, Number(req.query?.page || 1));
    const pageSize = Math.max(5, Math.min(200, Number(req.query?.pageSize || 40)));
    const maxDocs = Math.max(100, Math.min(8000, Number(req.query?.maxDocs || 3000)));
    const safeType = normalizeFinanceType(req.query?.type || '');
    const safeServiceId = String(req.query?.serviceId || '').trim();
    const safeSearch = String(req.query?.search || '').trim().toLowerCase();
    const fromMs = parseDateRangeBound(req.query?.from);
    const toMs = parseDateRangeBound(req.query?.to, { endOfDay: true });

    const snap = await db
      .collection(FINANCE_TRANSACTIONS_COLLECTION)
      .orderBy('occurredAt', 'desc')
      .limit(maxDocs)
      .get();

    let items = snap.docs
      .map((docSnap) => serializeFinanceTransaction(docSnap.id, docSnap.data() || {}))
      .filter(Boolean);

    if (safeType) {
      items = items.filter((item) => item.type === safeType);
    }
    if (safeServiceId) {
      items = items.filter((item) => item.serviceId === safeServiceId);
    }
    if (safeSearch) {
      items = items.filter((item) => {
        const haystack = [
          item.category,
          item.paymentMethod,
          item.notes,
          item.reference,
          item.clientName,
          item.serviceName,
          item.leadPhone,
          item.negocioId,
        ]
          .join(' ')
          .toLowerCase();
        return haystack.includes(safeSearch);
      });
    }

    items = items.filter((item) => {
      const ms = item.occurredAt ? new Date(item.occurredAt).getTime() : 0;
      if (!Number.isFinite(ms) || ms <= 0) return false;
      if (Number.isFinite(fromMs) && ms < fromMs) return false;
      if (Number.isFinite(toMs) && ms > toMs) return false;
      return true;
    });

    items.sort((a, b) => {
      const aMs = new Date(a.occurredAt || a.createdAt || 0).getTime();
      const bMs = new Date(b.occurredAt || b.createdAt || 0).getTime();
      return bMs - aMs;
    });

    const totalItems = items.length;
    const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
    const safePage = Math.min(page, totalPages);
    const start = (safePage - 1) * pageSize;
    const pagedItems = items.slice(start, start + pageSize);

    const totals = {
      income: roundMoney(items.filter((item) => item.type === 'income').reduce((acc, item) => acc + item.amount, 0)),
      expense: roundMoney(items.filter((item) => item.type === 'expense').reduce((acc, item) => acc + item.amount, 0)),
    };
    totals.net = roundMoney(totals.income - totals.expense);

    return res.json({
      success: true,
      items: pagedItems,
      totals,
      pagination: {
        page: safePage,
        pageSize,
        totalItems,
        totalPages,
      },
    });
  } catch (error) {
    console.error('[crm/finance/transactions] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/crm/finance/transactions', async (req, res) => {
  const {
    type = '',
    amount = '',
    category = '',
    paymentMethod = '',
    notes = '',
    reference = '',
    serviceId = '',
    leadId = '',
    leadPhone = '',
    negocioId = '',
    isAdvance = false,
    occurredAt = '',
    createdBy = '',
    affectService = true,
  } = req.body || {};

  try {
    const occurredTimestamp = parseDateInputToTimestampOrNull(occurredAt, 'occurredAt') || Timestamp.now();
    const result = await createFinanceTransaction({
      type,
      amount,
      category,
      paymentMethod,
      notes,
      reference,
      serviceId,
      leadId,
      leadPhone,
      negocioId,
      isAdvance: parseBooleanInput(isAdvance, false),
      occurredAt: occurredTimestamp,
      createdBy,
      affectService: parseBooleanInput(affectService, true),
    });

    return res.status(201).json({
      success: true,
      ...result,
    });
  } catch (error) {
    console.error('[crm/finance/transactions:create] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
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
    const leadCtx = await resolveLeadByIdentity({
      leadId: inputLeadId,
      phone: phoneInput,
    });

    let phoneDigits = normalizePhoneDigits(leadCtx.phoneDigits || phoneInput);
    let finalLeadId = String(leadCtx.leadId || '').trim();
    if (!finalLeadId) {
      return res.status(400).json({ error: 'No se pudo resolver leadId' });
    }

    if (String(inputLeadId || '').trim() && String(inputLeadId || '').trim() !== finalLeadId) {
      console.log(`[force-sequence] lead canonicalizado ${String(inputLeadId || '').trim()} -> ${finalLeadId}`);
    }

    let leadRef = leadCtx.leadRef || db.collection('leads').doc(finalLeadId);
    let leadSnap = leadCtx.leadSnap || await leadRef.get();
    if (!phoneDigits) phoneDigits = normalizePhoneDigits(leadSnap.data()?.telefono || '');

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

    await scheduleSequenceForLead(finalLeadId, trigger, now, {
      allowReschedule: Boolean(forceRestart),
      debug: true,
      source: 'force-sequence',
    });

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
    const leadCtx = await resolveLeadByIdentity({
      leadId: inputLeadId,
      phone: phoneInput,
    });

    let phoneDigits = normalizePhoneDigits(leadCtx.phoneDigits || phoneInput);
    let finalLeadId = String(leadCtx.leadId || '').trim();
    if (!finalLeadId) {
      return res.status(400).json({ error: 'No se pudo resolver leadId' });
    }

    if (String(inputLeadId || '').trim() && String(inputLeadId || '').trim() !== finalLeadId) {
      console.log(`[apply-stage] lead canonicalizado ${String(inputLeadId || '').trim()} -> ${finalLeadId}`);
    }

    let leadRef = leadCtx.leadRef || db.collection('leads').doc(finalLeadId);
    let leadSnap = leadCtx.leadSnap || await leadRef.get();
    if (!phoneDigits) phoneDigits = normalizePhoneDigits(leadSnap.data()?.telefono || '');

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
    const hasReachableDestination = Boolean(candidateJid || phoneDigits);

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
    if (!clearStage && sequenceTrigger && !shouldStopSequences && !isClosed && hasReachableDestination) {
      const scheduledSteps = await scheduleSequenceForLead(
        finalLeadId,
        sequenceTrigger,
        now,
        { allowReschedule: true, debug: true, source: 'apply-stage' }
      );
      if (scheduledSteps > 0) {
        await leadRef.set(
          {
            hasActiveSequences: true,
            stopSequences: false,
            etiquetas: admin.firestore.FieldValue.arrayUnion(sequenceTrigger),
          },
          { merge: true }
        );
        scheduled = true;
      } else {
        console.warn(
          `[apply-stage] no se programo secuencia lead=${finalLeadId} stage=${stageDocId || stageKey || stageName} trigger=${sequenceTrigger}`
        );
      }
    } else if (!clearStage && sequenceTrigger && !hasReachableDestination) {
      console.warn(
        `[apply-stage] sin destino enrutable lead=${finalLeadId} stage=${stageDocId || stageKey || stageName} trigger=${sequenceTrigger}`
      );
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

app.get('/api/web/sample-access/:phone', async (req, res) => {
  try {
    const phoneDigits = normalizePhoneDigits(req.params?.phone || '');
    if (!phoneDigits) {
      return res.status(400).json({ error: 'Teléfono inválido.' });
    }

    const leadCtx = await resolveLeadByIdentity({ phone: phoneDigits });
    if (!leadCtx?.leadRef || !leadCtx?.leadId) {
      return res.status(404).json({ error: 'Lead no encontrado para este teléfono.' });
    }

    const leadSnap = leadCtx.leadSnap || await leadCtx.leadRef.get();
    if (!leadSnap.exists) {
      return res.status(404).json({ error: 'Lead no encontrado para este teléfono.' });
    }

    const leadData = leadSnap.data() || {};
    const sampleFlow = (leadData.sampleFlow && typeof leadData.sampleFlow === 'object')
      ? leadData.sampleFlow
      : {};

    if (sampleFlow.enabled !== true) {
      return res.status(403).json({ error: 'Este número no tiene muestra habilitada.' });
    }

    const sampleExpiresAt = (() => {
      const value = sampleFlow.expiresAt;
      if (!value) return null;
      if (value instanceof Date) return value;
      if (typeof value?.toDate === 'function') return value.toDate();
      const parsed = new Date(value);
      return Number.isFinite(parsed.getTime()) ? parsed : null;
    })();
    if (sampleExpiresAt && sampleExpiresAt.getTime() <= Date.now()) {
      return res.status(403).json({ error: 'La muestra expiró. Solicita un nuevo enlace.' });
    }

    const expectedPhone = normalizePhoneDigits(sampleFlow.phone || leadCtx.phoneDigits || leadData.telefono || '');
    const candidates = new Set(expandPhoneCandidates(phoneDigits));
    if (expectedPhone && !candidates.has(expectedPhone)) {
      return res.status(403).json({ error: 'Este enlace no coincide con la muestra habilitada.' });
    }

    const negocioCtx = await resolveNegocioByIdentity({
      leadId: leadCtx.leadId,
      phoneDigits: expectedPhone || phoneDigits,
    });
    const negocio = negocioCtx.negocioData || {};

    return res.json({
      success: true,
      allowed: true,
      leadId: String(leadCtx.leadId || ''),
      negocioId: String(negocioCtx.negocioId || ''),
      phone: expectedPhone || phoneDigits,
      sampleUrl: String(sampleFlow.sampleUrl || buildSampleFormUrl(expectedPhone || phoneDigits)),
      sampleExpiresAt: sampleExpiresAt ? sampleExpiresAt.toISOString() : null,
      prefill: {
        companyName: String(negocio.companyInfo || leadData.nombre || ''),
        objective: String(negocio.businessObjective || ''),
        businessStory: String(negocio.businessStory || leadData?.briefWeb?.description || ''),
        keyItems: Array.isArray(negocio.keyItems) ? negocio.keyItems : [],
        primaryColor: String(negocio.primaryColor || '#2563eb'),
        contactEmail: String(negocio.contactEmail || ''),
        contactWhatsapp: normalizePhoneDigits(negocio.contactWhatsapp || negocio.leadPhone || expectedPhone || phoneDigits),
        logoURL: String(negocio.logoURL || ''),
        photoURLs: Array.isArray(negocio.photoURLs) ? negocio.photoURLs : [],
      },
    });
  } catch (error) {
    console.error('[web/sample-access] Error:', error);
    return res.status(500).json({ error: error.message || String(error) });
  }
});

app.post('/api/web/sample-submit', async (req, res) => {
  try {
    const {
      phone = '',
      leadId = '',
      negocioId = '',
      summary = {},
    } = req.body || {};

    const safeSummary = (summary && typeof summary === 'object') ? summary : null;
    if (!safeSummary) {
      return res.status(400).json({ error: 'Falta summary.' });
    }

    const phoneDigits = normalizePhoneDigits(phone || safeSummary.contactWhatsapp || '');
    const leadCtx = await resolveLeadByIdentity({ leadId, phone: phoneDigits });
    if (!leadCtx?.leadRef || !leadCtx?.leadId) {
      return res.status(404).json({ error: 'No se pudo resolver el lead de la muestra.' });
    }

    const leadSnap = leadCtx.leadSnap || await leadCtx.leadRef.get();
    if (!leadSnap.exists) {
      return res.status(404).json({ error: 'Lead no encontrado para esta muestra.' });
    }

    const leadData = leadSnap.data() || {};
    const sampleFlow = (leadData.sampleFlow && typeof leadData.sampleFlow === 'object')
      ? leadData.sampleFlow
      : {};
    if (sampleFlow.enabled !== true) {
      return res.status(403).json({ error: 'Este número no tiene muestra activa.' });
    }

    const sampleExpiresAt = (() => {
      const value = sampleFlow.expiresAt;
      if (!value) return null;
      if (value instanceof Date) return value;
      if (typeof value?.toDate === 'function') return value.toDate();
      const parsed = new Date(value);
      return Number.isFinite(parsed.getTime()) ? parsed : null;
    })();
    if (sampleExpiresAt && sampleExpiresAt.getTime() <= Date.now()) {
      return res.status(403).json({ error: 'La muestra expiró. Solicita un nuevo enlace.' });
    }

    const expectedPhone = normalizePhoneDigits(
      sampleFlow.phone || leadCtx.phoneDigits || leadData.telefono || phoneDigits
    );
    if (!expectedPhone) {
      return res.status(400).json({ error: 'No se pudo resolver teléfono para la muestra.' });
    }

    const allowedCandidates = new Set(expandPhoneCandidates(expectedPhone));
    if (phoneDigits && !allowedCandidates.has(phoneDigits)) {
      return res.status(403).json({ error: 'Este enlace no corresponde a la muestra activa.' });
    }

    let uploadedLogoURL = '';
    let uploadedPhotos = [];
    try {
      const assets = safeSummary?.assets || {};
      const { logo, images = [] } = assets;

      if (logo) {
        uploadedLogoURL = await uploadBase64Image({
          base64: logo,
          folder: `web-assets/${(safeSummary.slug || 'sample').toLowerCase()}`,
          filenamePrefix: 'logo',
        });
      }

      if (Array.isArray(images)) {
        for (let i = 0; i < Math.min(images.length, 3); i += 1) {
          const base64 = images[i];
          if (!base64) continue;
          const imageUrl = await uploadBase64Image({
            base64,
            folder: `web-assets/${(safeSummary.slug || 'sample').toLowerCase()}`,
            filenamePrefix: `photo_${i + 1}`,
          });
          if (imageUrl) uploadedPhotos.push(imageUrl);
        }
      }
    } catch (error) {
      console.warn('[web/sample-submit] error subiendo assets:', error?.message || error);
    }

    const fallbackPhotos = Array.isArray(safeSummary.photoURLs)
      ? safeSummary.photoURLs.map((item) => String(item || '').trim()).filter(Boolean).slice(0, 3)
      : [];
    if (uploadedPhotos.length === 0) {
      uploadedPhotos = fallbackPhotos;
    }
    if (uploadedPhotos.length === 0) {
      try {
        uploadedPhotos = await getStockPhotoUrls(safeSummary);
      } catch {
        uploadedPhotos = buildUnsplashFeaturedQueries(safeSummary);
      }
    }

    const negocioCtx = await resolveNegocioByIdentity({
      negocioId,
      leadId: leadCtx.leadId,
      phoneDigits: expectedPhone,
    });

    const now = new Date();
    const currentNegocio = negocioCtx.negocioData || {};
    const currentSlug = String(currentNegocio.slug || '').trim();
    const requestedSlug = String(safeSummary.slug || '').trim();
    let finalSlug = currentSlug;
    if (requestedSlug && requestedSlug !== currentSlug) {
      finalSlug = await ensureUniqueSlug(requestedSlug);
    }
    if (!finalSlug) {
      const fallbackSlug = String(
        safeSummary.companyName || currentNegocio.companyInfo || `muestra-${expectedPhone}`
      ).trim();
      finalSlug = await ensureUniqueSlug(fallbackSlug);
    }
    const normalizedReadyStageKey = String(sampleFlow.onReadyStageKey || '')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '');
    const readyTrigger = String(sampleFlow.onReadyTrigger || '').trim();

    const negocioPatch = {
      leadId: String(leadCtx.leadId || ''),
      leadPhone: expectedPhone,
      contactWhatsapp: normalizePhoneDigits(safeSummary.contactWhatsapp || currentNegocio.contactWhatsapp || expectedPhone),
      contactEmail: String(safeSummary.contactEmail || currentNegocio.contactEmail || ''),
      companyInfo: String(safeSummary.companyName || currentNegocio.companyInfo || ''),
      businessStory: String(safeSummary.businessStory || safeSummary.description || currentNegocio.businessStory || ''),
      businessObjective: String(safeSummary.objective || currentNegocio.businessObjective || ''),
      keyItems: Array.isArray(safeSummary.keyItems) ? safeSummary.keyItems : (currentNegocio.keyItems || []),
      primaryColor: String(safeSummary.primaryColor || currentNegocio.primaryColor || '#2563eb'),
      templateId: String(safeSummary.templateId || currentNegocio.templateId || 'info').toLowerCase(),
      logoURL: String(uploadedLogoURL || safeSummary.logoURL || currentNegocio.logoURL || ''),
      photoURLs: uploadedPhotos,
      slug: finalSlug,
      status: 'Sin procesar',
      sampleFlowType: 'funnel',
      suppressDefaultFollowups: true,
      sampleSubmittedAt: now,
      sampleOnReadyTrigger: readyTrigger || admin.firestore.FieldValue.delete(),
      sampleOnReadyStageKey: normalizedReadyStageKey || admin.firestore.FieldValue.delete(),
      updatedAt: now,
      createdAt: currentNegocio.createdAt || now,
    };

    let finalNegocioId = '';
    if (negocioCtx.negocioRef) {
      await negocioCtx.negocioRef.set(negocioPatch, { merge: true });
      finalNegocioId = negocioCtx.negocioId;
    } else {
      const created = await db.collection('Negocios').add(negocioPatch);
      finalNegocioId = created.id;
    }

    await leadCtx.leadRef.set(
      {
        briefWeb: safeSummary,
        sampleFlow: {
          ...sampleFlow,
          enabled: true,
          phone: expectedPhone,
          submittedAt: now,
          lastNegocioId: finalNegocioId,
        },
        sampleSubmittedAt: now,
        sampleLastNegocioId: finalNegocioId,
        etiquetas: admin.firestore.FieldValue.arrayUnion('MuestraFormularioEnviado'),
        lastMessageAt: now,
      },
      { merge: true }
    );

    return res.json({
      ok: true,
      leadId: String(leadCtx.leadId || ''),
      negocioId: finalNegocioId,
      slug: finalSlug,
    });
  } catch (error) {
    console.error('[web/sample-submit] Error:', error);
    return res.status(500).json({ error: String(error?.message || error) });
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
