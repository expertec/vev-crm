import crypto from 'node:crypto';
import { admin, db as defaultDb } from '../../firebaseAdmin.js';
import {
  WEB_SALES_EVENTS,
  buildWebSalesStateAfterEvent,
  ensureWebSalesState,
  getWebSalesSequenceCancellations,
  isRepeatableWebSalesEvent,
  isWebSalesEventType,
} from './state.js';

const { FieldValue, Timestamp } = admin.firestore;

function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function normalizeEventKey(value = '') {
  return cleanText(value, 180)
    .toLowerCase()
    .replace(/[^a-z0-9_.:-]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function toDateSafe(value) {
  if (!value) return new Date();
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? new Date() : value;
  if (typeof value?.toDate === 'function') return value.toDate();
  if (typeof value?.toMillis === 'function') return new Date(value.toMillis());
  const parsed = new Date(value);
  return Number.isFinite(parsed.getTime()) ? parsed : new Date();
}

function sanitizeMetadata(value, depth = 0) {
  if (depth > 4) return null;
  if (value === undefined) return undefined;
  if (value === null) return null;
  if (value instanceof Date) return value;
  if (typeof value?.toDate === 'function') return value;
  if (Array.isArray(value)) {
    return value
      .map((item) => sanitizeMetadata(item, depth + 1))
      .filter((item) => item !== undefined)
      .slice(0, 50);
  }
  if (typeof value !== 'object') {
    if (typeof value === 'string') return cleanText(value, 1200);
    if (typeof value === 'number' || typeof value === 'boolean') return value;
    return String(value || '');
  }
  const out = {};
  for (const [key, nested] of Object.entries(value).slice(0, 80)) {
    const safeKey = cleanText(key, 80).replace(/[.$[\]/#]/g, '_');
    if (!safeKey) continue;
    const cleaned = sanitizeMetadata(nested, depth + 1);
    if (cleaned !== undefined) out[safeKey] = cleaned;
  }
  return out;
}

function hashMetadata(metadata = {}) {
  const json = JSON.stringify(sanitizeMetadata(metadata) || {});
  return crypto.createHash('sha1').update(json).digest('hex').slice(0, 16);
}

export function webSalesEventId({ type = '', metadata = {}, timestamp = new Date() } = {}) {
  const safeType = normalizeEventKey(type);
  if (!safeType) return '';
  const explicit = normalizeEventKey(metadata.idempotencyKey || metadata.eventId || metadata.messageId || metadata.requestId || '');
  if (explicit) return `${safeType}_${explicit}`.slice(0, 220);
  if (safeType === WEB_SALES_EVENTS.SAMPLE_FORM_STARTED) {
    const step = normalizeEventKey(metadata.step || metadata.formLastStep || '1');
    return `${safeType}_step_${step || '1'}`.slice(0, 220);
  }
  if (isRepeatableWebSalesEvent(safeType)) {
    const ts = toDateSafe(timestamp).getTime();
    return `${safeType}_${ts}_${hashMetadata(metadata)}`.slice(0, 220);
  }
  return safeType;
}

export function shouldIgnoreWebSalesOpenEvent({ userAgent = '', source = '', metadata = {} } = {}) {
  const ua = cleanText(userAgent || metadata.userAgent || '', 500).toLowerCase();
  const src = cleanText(source || metadata.source || '', 80).toLowerCase();
  if (src === 'system' || src === 'preview' || src === 'internal') return true;
  if (!ua) return false;
  return /bot|crawler|spider|preview|facebookexternalhit|whatsapp|slackbot|discordbot|headless|puppeteer|lighthouse|pagespeed/i.test(ua);
}

export async function recordWebSalesEvent({
  db = defaultDb,
  leadId = '',
  leadRef = null,
  type = '',
  source = 'system',
  metadata = {},
  timestamp = new Date(),
  requestContext = {},
} = {}) {
  const safeLeadId = cleanText(leadId, 220);
  const safeType = cleanText(type, 80);
  if (!safeType || !isWebSalesEventType(safeType)) {
    throw new Error(`Evento webSales invalido: ${safeType || '(vacio)'}`);
  }
  const ref = leadRef || (safeLeadId ? db.collection('leads').doc(safeLeadId) : null);
  if (!ref) throw new Error('Falta leadId o leadRef para registrar evento webSales.');

  const eventTime = toDateSafe(timestamp);
  const safeSource = cleanText(source, 80) || 'system';
  const safeMetadata = sanitizeMetadata(metadata) || {};
  const eventId = webSalesEventId({ type: safeType, metadata: safeMetadata, timestamp: eventTime });
  const eventRef = ref.collection('webSalesEvents').doc(eventId);
  const openEventIgnored = safeType === WEB_SALES_EVENTS.SAMPLE_OPENED
    && shouldIgnoreWebSalesOpenEvent({
      userAgent: requestContext.userAgent || safeMetadata.userAgent || '',
      source: safeSource,
      metadata: safeMetadata,
    });

  const result = await db.runTransaction(async (tx) => {
    const leadSnap = await tx.get(ref);
    if (!leadSnap.exists) throw new Error('Lead no encontrado para webSales.');
    const lead = { id: leadSnap.id, ...(leadSnap.data() || {}) };
    const existingEvent = await tx.get(eventRef);

    if (existingEvent.exists) {
      const currentState = ensureWebSalesState(lead, eventTime);
      return {
        ok: true,
        idempotent: true,
        eventId,
        leadId: leadSnap.id,
        webSales: currentState,
        sequenceCancellations: getWebSalesSequenceCancellations(safeType),
      };
    }

    const current = ensureWebSalesState(lead, eventTime);
    const next = buildWebSalesStateAfterEvent({
      current,
      lead,
      type: safeType,
      metadata: safeMetadata,
      timestamp: eventTime,
      incrementOpenCount: !openEventIgnored,
    });

    tx.set(eventRef, {
      type: safeType,
      timestamp: Timestamp.fromDate(eventTime),
      source: safeSource,
      metadata: safeMetadata,
      ignored: openEventIgnored,
      ignoredReason: openEventIgnored ? 'likely_bot_or_internal_open' : '',
      stageBefore: current.stage || '',
      stageAfter: next.stage || '',
      nextConversionGoal: next.nextConversionGoal || '',
      createdAt: FieldValue.serverTimestamp(),
    }, { merge: false });

    const patch = {
      webSales: next,
      productInterest: 'web',
      nextConversionGoal: next.nextConversionGoal,
      'salesState.productStrategy': lead?.salesState?.productStrategy || 'web',
      updatedAt: FieldValue.serverTimestamp(),
    };

    if (safeType === WEB_SALES_EVENTS.SAMPLE_FORM_SENT) {
      patch.formLinkSentAt = lead.formLinkSentAt || Timestamp.fromDate(eventTime);
    }
    if (safeType === WEB_SALES_EVENTS.SAMPLE_FORM_COMPLETED) {
      patch.sampleSubmittedAt = lead.sampleSubmittedAt || Timestamp.fromDate(eventTime);
      patch.etapa = lead.etapa || 'form_submitted';
      patch.etiquetas = FieldValue.arrayUnion('MuestraFormularioEnviado', 'FormOK');
    }
    if (safeType === WEB_SALES_EVENTS.SAMPLE_GENERATED) {
      patch.sampleGeneratedAt = lead.sampleGeneratedAt || Timestamp.fromDate(eventTime);
    }
    if (safeType === WEB_SALES_EVENTS.SAMPLE_SENT) {
      patch.webLinkSentAt = lead.webLinkSentAt || Timestamp.fromDate(eventTime);
      patch.etiquetas = FieldValue.arrayUnion('WebLinkSent');
    }
    if (safeType === WEB_SALES_EVENTS.SAMPLE_OPENED && !openEventIgnored) {
      patch.linkOpenedAt = lead.linkOpenedAt || Timestamp.fromDate(eventTime);
      patch.etiquetas = FieldValue.arrayUnion('LinkAbierto');
    }
    if (safeType === WEB_SALES_EVENTS.SAMPLE_FEEDBACK_RECEIVED) {
      patch.sampleFeedbackReceivedAt = lead.sampleFeedbackReceivedAt || Timestamp.fromDate(eventTime);
    }
    if (safeType === WEB_SALES_EVENTS.PURCHASE_COMPLETED) {
      patch.estado = 'compro';
      patch.stopSequences = true;
      patch.hasActiveSequences = false;
      patch.etiquetas = FieldValue.arrayUnion('Compro');
    }

    tx.set(ref, patch, { merge: true });

    return {
      ok: true,
      idempotent: false,
      eventId,
      leadId: leadSnap.id,
      webSales: next,
      ignored: openEventIgnored,
      sequenceCancellations: getWebSalesSequenceCancellations(safeType),
    };
  });

  console.log(`[webSales] event:${safeType} lead=${result.leadId} stage=${result.webSales?.stage || ''} goal=${result.webSales?.nextConversionGoal || ''}${result.idempotent ? ' idempotent' : ''}${result.ignored ? ' ignored' : ''}`);
  return result;
}

export { WEB_SALES_EVENTS } from './state.js';
