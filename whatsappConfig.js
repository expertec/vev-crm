// whatsappConfig.js
//
// Configuración del WhatsApp CRM por negocio (bienvenida, ausencia/horario,
// auto-respuestas por palabra clave). Guardado en:
//   Negocios/{negocioId}/integraciones/whatsappConfig
//
// Solo server-side (firebase-admin). El panel lo lee/escribe vía endpoints del BFF.

import admin from 'firebase-admin';
import { db } from './firebaseAdmin.js';

const { FieldValue } = admin.firestore;

const DAYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

function configRef(negocioId) {
  return db.collection('Negocios').doc(negocioId).collection('integraciones').doc('whatsappConfig');
}

export const DEFAULT_PIPELINE_STAGES = [
  { id: 'nuevo', name: 'Nuevo', color: '#3b82f6' },
  { id: 'contactado', name: 'Contactado', color: '#8b5cf6' },
  { id: 'en_proceso', name: 'En proceso', color: '#f59e0b' },
  { id: 'ganado', name: 'Ganado', color: '#16a34a' },
  { id: 'perdido', name: 'Perdido', color: '#ef4444' },
];

export function defaultWhatsAppConfig() {
  return {
    pipelineStages: DEFAULT_PIPELINE_STAGES.map((s) => ({ ...s })),
    welcome: { enabled: false, message: '¡Hola! Gracias por escribirnos. En breve te atendemos. 🙌' },
    away: {
      enabled: false,
      message: 'Gracias por tu mensaje. Ahora estamos fuera de horario; te responderemos lo antes posible.',
      timezone: 'America/Mexico_City',
      // Por día: { enabled, open 'HH:MM', close 'HH:MM' }
      schedule: DAYS.reduce((acc, d) => {
        acc[d] = { enabled: d !== 'sun', open: '09:00', close: '18:00' };
        return acc;
      }, {}),
    },
    autoReplies: [], // [{ id, enabled, keywords: [..], response }]
  };
}

function normalizeAutoReplies(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.slice(0, 100).map((r, i) => ({
    id: String(r?.id || `ar_${i}_${Date.now()}`),
    enabled: r?.enabled !== false,
    keywords: Array.isArray(r?.keywords)
      ? r.keywords.map((k) => String(k || '').trim().toLowerCase()).filter(Boolean)
      : String(r?.keywords || '').split(',').map((k) => k.trim().toLowerCase()).filter(Boolean),
    response: String(r?.response || '').slice(0, 2000),
  })).filter((r) => r.keywords.length && r.response);
}

export function sanitizeWhatsAppConfig(patch = {}) {
  const def = defaultWhatsAppConfig();
  const src = patch && typeof patch === 'object' ? patch : {};
  const out = {};

  if (src.welcome && typeof src.welcome === 'object') {
    out.welcome = {
      enabled: Boolean(src.welcome.enabled),
      message: String(src.welcome.message || def.welcome.message).slice(0, 2000),
    };
  }
  if (src.away && typeof src.away === 'object') {
    const schedule = {};
    const inSched = src.away.schedule && typeof src.away.schedule === 'object' ? src.away.schedule : {};
    for (const d of DAYS) {
      const day = inSched[d] || {};
      schedule[d] = {
        enabled: day.enabled !== false,
        open: /^\d{2}:\d{2}$/.test(day.open) ? day.open : '09:00',
        close: /^\d{2}:\d{2}$/.test(day.close) ? day.close : '18:00',
      };
    }
    out.away = {
      enabled: Boolean(src.away.enabled),
      message: String(src.away.message || def.away.message).slice(0, 2000),
      timezone: String(src.away.timezone || def.away.timezone),
      schedule,
    };
  }
  if (src.autoReplies !== undefined) {
    out.autoReplies = normalizeAutoReplies(src.autoReplies);
  }
  if (src.pipelineStages !== undefined) {
    out.pipelineStages = normalizePipelineStages(src.pipelineStages);
  }
  return out;
}

function slugifyStage(name, fallback) {
  const s = String(name || '').trim().toLowerCase()
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
  return s || fallback;
}

function normalizePipelineStages(raw) {
  if (!Array.isArray(raw) || raw.length === 0) {
    return DEFAULT_PIPELINE_STAGES.map((s) => ({ ...s }));
  }
  const seen = new Set();
  const stages = [];
  raw.slice(0, 30).forEach((s, i) => {
    const name = String(s?.name || '').trim();
    if (!name) return;
    let id = String(s?.id || slugifyStage(name, `etapa_${i}`));
    while (seen.has(id)) id = `${id}_${i}`;
    seen.add(id);
    stages.push({ id, name: name.slice(0, 40), color: /^#[0-9a-fA-F]{6}$/.test(s?.color) ? s.color : '#64748b' });
  });
  return stages.length ? stages : DEFAULT_PIPELINE_STAGES.map((s) => ({ ...s }));
}

export async function getWhatsAppConfig(negocioId) {
  const snap = await configRef(negocioId).get();
  const def = defaultWhatsAppConfig();
  if (!snap.exists) return def;
  const d = snap.data() || {};
  return {
    welcome: { ...def.welcome, ...(d.welcome || {}) },
    away: { ...def.away, ...(d.away || {}), schedule: { ...def.away.schedule, ...((d.away || {}).schedule || {}) } },
    autoReplies: Array.isArray(d.autoReplies) ? d.autoReplies : [],
  };
}

export async function saveWhatsAppConfig(negocioId, patch) {
  const clean = sanitizeWhatsAppConfig(patch);
  await configRef(negocioId).set({ ...clean, updatedAt: FieldValue.serverTimestamp() }, { merge: true });
  return getWhatsAppConfig(negocioId);
}

// ¿La hora actual cae dentro del horario de atención?
export function isWithinBusinessHours(config, now = new Date()) {
  const away = config?.away;
  if (!away?.schedule) return true;
  const tz = away.timezone || 'America/Mexico_City';
  try {
    const fmt = new Intl.DateTimeFormat('en-US', {
      timeZone: tz, weekday: 'short', hour: '2-digit', minute: '2-digit', hour12: false,
    });
    const parts = fmt.formatToParts(now);
    const wd = parts.find((p) => p.type === 'weekday')?.value || '';
    const hh = parts.find((p) => p.type === 'hour')?.value || '00';
    const mm = parts.find((p) => p.type === 'minute')?.value || '00';
    const dayKey = { Sun: 'sun', Mon: 'mon', Tue: 'tue', Wed: 'wed', Thu: 'thu', Fri: 'fri', Sat: 'sat' }[wd];
    const day = away.schedule[dayKey];
    if (!day || !day.enabled) return false;
    const cur = Number(hh) * 60 + Number(mm);
    const [oH, oM] = String(day.open || '09:00').split(':').map(Number);
    const [cH, cM] = String(day.close || '18:00').split(':').map(Number);
    return cur >= oH * 60 + oM && cur <= cH * 60 + cM;
  } catch (_e) {
    return true; // ante la duda, no marcamos ausencia
  }
}

// Busca una auto-respuesta cuyo keyword aparezca en el texto.
export function findKeywordReply(config, text) {
  const t = String(text || '').toLowerCase();
  if (!t) return null;
  const rules = Array.isArray(config?.autoReplies) ? config.autoReplies : [];
  for (const rule of rules) {
    if (rule?.enabled === false) continue;
    const kws = Array.isArray(rule?.keywords) ? rule.keywords : [];
    if (kws.some((k) => k && t.includes(k))) return rule.response;
  }
  return null;
}
