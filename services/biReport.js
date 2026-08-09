// services/biReport.js
//
// Genera un informe de Business Intelligence del CRM a partir de la coleccion
// `leads` (+ `tasks`). Devuelve datos estructurados y un Markdown listo para
// pegar en Claude y pedir analisis de estrategia de producto y seguimiento.
//
// Diseno: una sola pasada sobre los leads, usando campos a nivel documento
// (sin leer subcolecciones de mensajes) para que sea barato.
//
import { getReactivationMessageCatalog } from './leadReactivationService.js';
import { getFollowupMessageCatalog } from './followupActions.js';

const WON_STATUSES = new Set(['compro', 'cliente', 'ganado', 'closed_won', 'cerrado_ganado', 'pagado']);
const LOST_STATUSES = new Set(['no_interesa', 'nointeresa', 'perdido', 'descartado', 'closed_lost']);

function safeStr(value = '') {
  return String(value || '').trim();
}

function lower(value = '') {
  return safeStr(value).toLowerCase();
}

function normToken(value = '') {
  return String(value || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function toMillis(value) {
  if (value === null || value === undefined || value === '') return 0;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (value instanceof Date) return value.getTime() || 0;
  if (typeof value?.toMillis === 'function') return value.toMillis() || 0;
  if (typeof value?.toDate === 'function') return value.toDate()?.getTime?.() || 0;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function pct(n, d) {
  if (!d) return '—';
  return `${((100 * n) / d).toFixed(1)}%`;
}

// Minutos (offset desde el inicio de la secuencia) → texto legible.
function formatDelay(min) {
  const m = Number(min || 0);
  if (m <= 0) return 'inmediato';
  if (m < 60) return `${m} min`;
  if (m < 1440) return `${(m / 60).toFixed(m % 60 ? 1 : 0)} h`;
  return `${(m / 1440).toFixed(m % 1440 ? 1 : 0)} d`;
}

function tagSet(lead = {}) {
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas : [];
  return new Set(tags.map((t) => normToken(t)));
}

function hasSlug(lead = {}) {
  return Boolean(
    safeStr(lead?.slug) || safeStr(lead?.webSlug) || safeStr(lead?.siteSlug)
    || safeStr(lead?.briefWeb?.slug) || safeStr(lead?.schema?.slug)
  );
}

function hasCompletedForm(lead = {}, tags) {
  if (lower(lead?.etapa) === 'form_submitted' || lower(lead?.etapaNombre) === 'form_submitted') return true;
  return tags.has('formok') || tags.has('formulariocompletado');
}

function incr(map, key, by = 1) {
  const k = key || '(sin dato)';
  map.set(k, (map.get(k) || 0) + by);
}

function mapToSortedRows(map) {
  return [...map.entries()].sort((a, b) => b[1] - a[1]);
}

function renderTable(headers, rows) {
  const head = `| ${headers.join(' | ')} |`;
  const sep = `| ${headers.map(() => '---').join(' | ')} |`;
  const body = rows.map((r) => `| ${r.join(' | ')} |`).join('\n');
  return [head, sep, body].join('\n');
}

function monthKey(ms) {
  if (!ms) return '(sin fecha)';
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}

function isArchivedLead(lead = {}) {
  return lead?.isArchived === true || lead?.archived === true || Boolean(lead?.archivedAt);
}

function isWonLead(lead = {}, tags = tagSet(lead)) {
  const status = lower(lead?.estado);
  return WON_STATUSES.has(status) || tags.has('compro');
}

function formatDateTime(ms) {
  if (!ms) return '(sin fecha)';
  return new Date(ms).toLocaleString('es-MX');
}

function leadName(lead = {}) {
  return (
    safeStr(lead?.nombre)
    || safeStr(lead?.name)
    || safeStr(lead?.cliente)
    || safeStr(lead?.empresa)
    || safeStr(lead?.businessName)
    || safeStr(lead?.telefono)
    || safeStr(lead?.id)
    || 'Cliente sin nombre'
  );
}

function messageSenderLabel(message = {}) {
  const sender = lower(message?.sender || message?.from || message?.role);
  if (sender === 'lead' || sender === 'customer' || sender === 'cliente' || sender === 'client') return 'Cliente';
  if (sender === 'business' || sender === 'owner' || sender === 'agent' || sender === 'user' || sender === 'seller') return 'Negocio';
  if (sender === 'system' || sender === 'bot') return 'Sistema';
  if (message?.fromMe === true) return 'Negocio';
  if (message?.fromMe === false) return 'Cliente';
  return sender || 'Mensaje';
}

function messageBody(message = {}) {
  const content = safeStr(message?.content || message?.text || message?.body || message?.message);
  const mediaType = safeStr(message?.mediaType);
  const mediaUrl = safeStr(message?.mediaUrl || message?.url);

  const parts = [];
  if (content) parts.push(content);
  if (mediaType || mediaUrl) {
    const mediaLabel = mediaType ? `media: ${mediaType}` : 'media';
    parts.push(mediaUrl ? `[${mediaLabel}: ${mediaUrl}]` : `[${mediaLabel}]`);
  }
  return parts.join('\n').trim();
}

function renderIndentedMessage(text = '') {
  return String(text || '')
    .split('\n')
    .map((line, index) => (index === 0 ? line : `  ${line}`))
    .join('\n');
}

function isoOrNull(value) {
  const ms = toMillis(value);
  return ms ? new Date(ms).toISOString() : null;
}

function firstValue(source = {}, paths = []) {
  for (const path of paths) {
    const parts = String(path || '').split('.').filter(Boolean);
    let cursor = source;
    for (const part of parts) {
      if (!cursor || typeof cursor !== 'object') {
        cursor = undefined;
        break;
      }
      cursor = cursor[part];
    }
    if (cursor !== undefined && cursor !== null && cursor !== '') return cursor;
  }
  return '';
}

function cleanPhone(value = '') {
  return String(value || '').replace(/\D/g, '');
}

function validCommercialPhone(lead = {}) {
  const phone = cleanPhone(lead?.telefono || lead?.phone || lead?.whatsapp || lead?.leadPhone || lead?.id);
  return /^\d{10}$/.test(phone) || /^52\d{10}$/.test(phone) || /^521\d{10}$/.test(phone);
}

function hasAnyToken(lead = {}, needles = []) {
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas : [];
  const haystack = [
    lead?.estado,
    lead?.etapa,
    lead?.etapaNombre,
    lead?.source,
    lead?.campaign,
    lead?.campaignName,
    lead?.metaCampaignName,
    lead?.nombre,
    lead?.name,
    lead?.telefono,
    ...tags,
  ].map((v) => normToken(v)).filter(Boolean).join(' ');

  return needles.some((needle) => haystack.includes(normToken(needle)));
}

function shouldExcludeFromConversionDataset(lead = {}, tags = tagSet(lead)) {
  if (lead?.mergedInto || isArchivedLead(lead)) return true;
  if (!validCommercialPhone(lead)) return true;
  if (lead?.autoResponder?.detected === true || tags.has('respuestaautomatica')) return true;
  return hasAnyToken(lead, [
    'spam',
    'prueba',
    'test',
    'testing',
    'interno',
    'internal',
    'empleado',
    'employee',
    'personal',
    'bot',
  ]);
}

function purchaseTimestamp(lead = {}) {
  return Math.max(
    toMillis(lead?.purchaseTimestamp),
    toMillis(lead?.purchasedAt),
    toMillis(lead?.paidAt),
    toMillis(lead?.paymentReceivedAt),
    toMillis(lead?.convertedAt),
    toMillis(lead?.closedAt),
    toMillis(lead?.wonAt),
    toMillis(lead?.planActivatedAt),
    toMillis(lead?.lastPaymentAt),
    toMillis(lead?.paymentReference?.paidAt),
    toMillis(lead?.paymentReference?.confirmedAt),
    toMillis(lead?.paymentReference?.updatedAt)
  );
}

function purchaseAmount(lead = {}) {
  const value = firstValue(lead, [
    'purchaseAmount',
    'amount',
    'monto',
    'lastPaymentAmount',
    'paymentReference.amount',
    'paymentReference.monto',
    'paymentReference.bankInstructions.amountRemaining',
  ]);
  const amount = Number(String(value || '').replace(/[$,\s]/g, ''));
  return Number.isFinite(amount) && amount > 0 ? amount : null;
}

function purchasedProduct(lead = {}) {
  return safeStr(firstValue(lead, [
    'productPurchased',
    'product',
    'productName',
    'paymentReference.productName',
    'paymentReference.planNombre',
    'planNombre',
    'plan',
    'serviceName',
  ]));
}

function leadAttribution(lead = {}) {
  const meta = lead?.metaAttribution && typeof lead.metaAttribution === 'object' ? lead.metaAttribution : {};
  return {
    source: safeStr(lead?.source) || '(sin fuente)',
    campaignId: safeStr(lead?.campaignId || lead?.metaCampaignId || meta.campaignId),
    campaignName: safeStr(lead?.campaignName || lead?.metaCampaignName || lead?.campaign || meta.campaignName),
    adsetId: safeStr(lead?.adsetId || lead?.adSetId || lead?.metaAdSetId || meta.adSetId || meta.adsetId),
    adsetName: safeStr(lead?.adsetName || lead?.adSetName || lead?.metaAdSetName || meta.adSetName || meta.adsetName),
    adId: safeStr(lead?.adId || lead?.metaAdId || meta.adId || meta.sourceId),
    adName: safeStr(lead?.adName || lead?.metaAdName || meta.adName),
    ctwaClid: safeStr(lead?.ctwaClid || lead?.metaCtwaClid || meta.ctwaClid),
  };
}

function leadFinalStage(lead = {}) {
  return safeStr(lead?.etapa || lead?.etapaNombre || lead?.funnelStageId || lead?.estado);
}

function leadInitialStage(lead = {}) {
  const history = Array.isArray(lead?.stageHistory)
    ? lead.stageHistory
    : (Array.isArray(lead?.historialEtapas) ? lead.historialEtapas : []);
  if (history.length > 0) {
    return safeStr(history[0]?.stage || history[0]?.etapa || history[0]?.name);
  }
  return safeStr(lead?.etapaInicial || lead?.initialStage || lead?.sourceStage || 'nuevo');
}

function normalizeStageHistory(lead = {}) {
  const history = Array.isArray(lead?.stageHistory)
    ? lead.stageHistory
    : (Array.isArray(lead?.historialEtapas) ? lead.historialEtapas : []);
  const rows = history.map((item) => ({
    stage: safeStr(item?.stage || item?.etapa || item?.name || item?.to || item?.value),
    timestamp: isoOrNull(item?.timestamp || item?.createdAt || item?.at || item?.date),
  })).filter((item) => item.stage || item.timestamp);

  if (rows.length === 0 && leadFinalStage(lead)) {
    rows.push({
      stage: leadFinalStage(lead),
      timestamp: isoOrNull(lead?.lastStageChangeAt || lead?.funnelUpdatedAt || lead?.fecha_creacion),
    });
  }
  return rows;
}

function messageDirection(message = {}) {
  const sender = lower(message?.sender || message?.from || message?.role);
  if (sender === 'lead' || sender === 'customer' || sender === 'cliente' || sender === 'client') return 'inbound';
  if (sender === 'business' || sender === 'owner' || sender === 'agent' || sender === 'user' || sender === 'seller') return 'outbound';
  if (sender === 'system' || sender === 'bot' || sender === 'automation') return 'system';
  if (message?.fromMe === true) return 'outbound';
  if (message?.fromMe === false) return 'inbound';
  return 'system';
}

function datasetSender(message = {}) {
  const direction = messageDirection(message);
  if (direction === 'inbound') return 'customer';
  if (direction === 'system') return 'system';
  if (message?.automationType || message?.sequenceTrigger || message?.sequenceStep) return 'automation';
  return 'seller';
}

function messageType(message = {}) {
  const type = lower(message?.mediaType || message?.type);
  if (type) return type;
  return messageBody(message) ? 'text' : 'unknown';
}

function messageText(message = {}) {
  return safeStr(message?.content || message?.text || message?.body || message?.message);
}

function messageTranscription(message = {}) {
  return safeStr(
    message?.transcription
    || message?.transcript
    || message?.audioTranscription
    || message?.audioTranscript
    || message?.speechToText
  );
}

function parseSequenceSystemMessage(text = '') {
  const match = String(text || '').match(/\[sequence:([^\]]+)\]\s*step\s*(\d+)\s*enviado/i);
  if (!match) return null;
  return {
    automationType: 'sequence',
    sequenceTrigger: match[1],
    sequenceStep: Number(match[2]),
  };
}

function normalizeMessage(doc) {
  const raw = { id: doc.id, ...(doc.data() || {}) };
  const systemSeq = parseSequenceSystemMessage(raw.content || raw.text || '');
  const timestampMs = Math.max(toMillis(raw?.timestamp), toMillis(raw?.createdAt), toMillis(raw?.sentAt));
  const type = messageType(raw);
  return {
    messageId: safeStr(raw?.waMessageId || raw?.messageId || raw?.id),
    timestamp: isoOrNull(timestampMs),
    timestampMs,
    direction: messageDirection(raw),
    sender: systemSeq ? 'automation' : datasetSender(raw),
    type,
    text: messageText(raw),
    transcription: messageTranscription(raw),
    mediaUrl: safeStr(raw?.mediaUrl || raw?.url),
    automationType: safeStr(raw?.automationType) || systemSeq?.automationType || '',
    sequenceTrigger: safeStr(raw?.sequenceTrigger) || systemSeq?.sequenceTrigger || '',
    sequenceStep: raw?.sequenceStep ?? systemSeq?.sequenceStep ?? null,
  };
}

function dedupeMessages(messages = []) {
  const seen = new Set();
  const deduped = [];
  for (const message of messages) {
    const key = message.messageId
      || [message.timestamp, message.direction, message.sender, message.type, message.text, message.mediaUrl].join('|');
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(message);
  }
  return deduped.sort((a, b) => a.timestampMs - b.timestampMs);
}

function addEvent(events, type, timestamp, metadata = {}) {
  const ts = isoOrNull(timestamp);
  if (!type || !ts) return;
  events.push({ type, timestamp: ts, metadata });
}

function inferEvents(lead = {}, messages = [], purchaseMs = 0) {
  const events = [];
  addEvent(events, 'lead_created', lead?.fecha_creacion, { source: safeStr(lead?.source) });
  const attr = leadAttribution(lead);
  if (attr.campaignId || attr.adId || attr.ctwaClid || lower(attr.source) === 'meta_ads') {
    addEvent(events, 'meta_ad_received', lead?.fecha_creacion || lead?.lastInboundAt, attr);
  }
  addEvent(events, 'form_sent', lead?.formLinkSentAt || lead?.sampleLinkSentAt || lead?.webSentAt);
  addEvent(events, 'form_completed', lead?.formCompletedAt || lead?.briefCompletedAt || lead?.sampleFlow?.completedAt);
  addEvent(events, 'sample_requested', lead?.sampleRequestedAt || lead?.sampleFlow?.requestedAt);
  addEvent(events, 'sample_created', lead?.sampleCreatedAt || lead?.sampleFlow?.generatedAt);
  addEvent(events, 'sample_sent', lead?.sampleSentAt || lead?.sampleFlow?.sentAt);
  addEvent(events, 'website_sent', lead?.websiteSentAt || lead?.webSentAt);
  addEvent(events, 'payment_link_sent', lead?.paymentReference?.createdAt || lead?.paymentReference?.sentAt);
  addEvent(events, 'payment_instructions_sent', lead?.paymentReference?.createdAt || lead?.paymentReference?.sentAt);
  addEvent(events, 'payment_received', purchaseMs, {
    amount: purchaseAmount(lead),
    product: purchasedProduct(lead),
  });
  addEvent(events, 'hot_lead_detected', lead?.aiReply?.classifiedAt, {
    source: 'stored_ai_reply',
  });

  const stageHistory = normalizeStageHistory(lead);
  stageHistory.forEach((item) => addEvent(events, 'stage_changed', item.timestamp, { stage: item.stage }));

  messages.forEach((message) => {
    if (message.sender === 'automation' || message.sequenceTrigger) {
      addEvent(events, 'sequence_message_sent', message.timestamp, {
        sequenceTrigger: message.sequenceTrigger,
        sequenceStep: message.sequenceStep,
      });
    } else if (message.direction === 'outbound') {
      addEvent(events, 'manual_reply', message.timestamp, {});
    }
  });

  return dedupeEvents(events);
}

function dedupeEvents(events = []) {
  const seen = new Set();
  return events
    .filter((event) => event.timestamp)
    .sort((a, b) => toMillis(a.timestamp) - toMillis(b.timestamp))
    .filter((event) => {
      const key = `${event.type}|${event.timestamp}|${JSON.stringify(event.metadata || {})}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

function computeTemporalMetrics({ lead = {}, messages = [], purchaseMs = 0 } = {}) {
  const firstContactMs = Math.min(
    ...[
      toMillis(lead?.fecha_creacion),
      ...messages.map((m) => m.timestampMs).filter(Boolean),
    ].filter(Boolean)
  );
  const firstInboundMs = messages.find((m) => m.direction === 'inbound')?.timestampMs || 0;
  const firstOutboundMs = messages.find((m) => m.direction === 'outbound')?.timestampMs || 0;
  const inboundMessages = messages.filter((m) => m.direction === 'inbound').length;
  const outboundMessages = messages.filter((m) => m.direction === 'outbound').length;
  const followups = messages.filter((m) => m.sender === 'automation' || m.sequenceTrigger).length;

  const sorted = messages.map((m) => m.timestampMs).filter(Boolean).sort((a, b) => a - b);
  let silencePeriods = 0;
  let longestSilenceHours = null;
  for (let i = 1; i < sorted.length; i += 1) {
    const hours = (sorted[i] - sorted[i - 1]) / (60 * 60 * 1000);
    if (hours >= 24) silencePeriods += 1;
    if (longestSilenceHours === null || hours > longestSilenceHours) longestSilenceHours = hours;
  }

  const formMs = Math.max(toMillis(lead?.formCompletedAt), toMillis(lead?.briefCompletedAt), toMillis(lead?.sampleFlow?.completedAt));
  const sampleMs = Math.max(toMillis(lead?.sampleCreatedAt), toMillis(lead?.sampleFlow?.generatedAt), toMillis(lead?.sampleSentAt));

  return {
    minutesToFirstResponse: firstInboundMs && firstContactMs ? (firstInboundMs - firstContactMs) / 60000 : null,
    minutesToFirstSellerReply: firstOutboundMs && firstContactMs ? (firstOutboundMs - firstContactMs) / 60000 : null,
    hoursToFormCompletion: formMs && firstContactMs ? (formMs - firstContactMs) / 3600000 : null,
    hoursToSample: sampleMs && firstContactMs ? (sampleMs - firstContactMs) / 3600000 : null,
    hoursFromSampleToPurchase: sampleMs && purchaseMs ? (purchaseMs - sampleMs) / 3600000 : null,
    hoursFromFirstContactToPurchase: firstContactMs && purchaseMs ? (purchaseMs - firstContactMs) / 3600000 : null,
    numberOfInboundMessages: inboundMessages,
    numberOfOutboundMessages: outboundMessages,
    numberOfFollowups: followups,
    numberOfCustomerReplies: inboundMessages,
    numberOfSilencePeriods: silencePeriods,
    longestSilenceHours,
    daysUntilPurchase: firstContactMs && purchaseMs ? (purchaseMs - firstContactMs) / (24 * 3600000) : null,
  };
}

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  const text = String(value);
  if (/[",\n\r]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function renderLeadsCsv(rows = []) {
  const headers = [
    'leadId',
    'converted',
    'createdAt',
    'lastActivityAt',
    'purchaseTimestamp',
    'purchaseAmount',
    'product',
    'plan',
    'source',
    'campaignId',
    'campaignName',
    'adsetId',
    'adsetName',
    'adId',
    'adName',
    'ctwaClid',
    'initialStage',
    'finalStage',
    'assignedSeller',
    'inboundMessages',
    'outboundMessages',
    'followups',
    'customerReplies',
    'silencePeriods',
    'longestSilenceHours',
    'daysUntilPurchase',
    'tags',
  ];
  const lines = [headers.join(',')];
  rows.forEach((row) => {
    lines.push(headers.map((header) => csvEscape(row[header])).join(','));
  });
  return `${lines.join('\n')}\n`;
}

async function loadSellerTaskEvents(db) {
  const byLead = new Map();
  try {
    const snap = await db.collection('tasks').limit(8000).get();
    snap.forEach((doc) => {
      const task = doc.data() || {};
      const leadId = safeStr(task?.leadId);
      if (!leadId) return;
      const timestamp = isoOrNull(task?.createdAt || task?.timestamp || task?.dueAt || task?.updatedAt);
      if (!timestamp) return;
      const event = {
        type: 'seller_task_created',
        timestamp,
        metadata: {
          taskId: doc.id,
          source: safeStr(task?.source),
          status: safeStr(task?.status),
          title: safeStr(task?.title || task?.titulo),
        },
      };
      if (!byLead.has(leadId)) byLead.set(leadId, []);
      byLead.get(leadId).push(event);
    });
  } catch {
    /* tasks opcional */
  }
  return byLead;
}

async function getDb(dbOverride) {
  if (dbOverride) return dbOverride;
  const { db } = await import('../firebaseAdmin.js');
  return db;
}

export async function exportWonConversationHistory({
  dbOverride = null,
  now = new Date(),
  maxLeads = 1000,
} = {}) {
  const db = await getDb(dbOverride);
  const nowMs = now.getTime();
  const leadLimit = Number.isFinite(Number(maxLeads)) ? Math.max(0, Math.floor(Number(maxLeads))) : 1000;
  const leadsSnap = await db.collection('leads').get();
  const wonLeads = [];

  leadsSnap.forEach((doc) => {
    const lead = { id: doc.id, ...(doc.data() || {}) };
    if (lead?.mergedInto || isArchivedLead(lead)) return;

    const tags = tagSet(lead);
    if (!isWonLead(lead, tags)) return;

    const lastActivityMs = Math.max(
      toMillis(lead?.lastMessageAt),
      toMillis(lead?.lastInboundAt),
      toMillis(lead?.lastOutboundAt),
      toMillis(lead?.fecha_creacion)
    );

    wonLeads.push({
      ...lead,
      _lastActivityMs: lastActivityMs,
      _createdMs: toMillis(lead?.fecha_creacion),
    });
  });

  wonLeads.sort((a, b) => b._lastActivityMs - a._lastActivityMs);
  const selectedLeads = leadLimit > 0 ? wonLeads.slice(0, leadLimit) : wonLeads;

  const conversations = [];
  for (const lead of selectedLeads) {
    const messages = [];
    let readError = '';

    try {
      const messagesSnap = await db.collection('leads')
        .doc(String(lead.id))
        .collection('messages')
        .orderBy('timestamp', 'asc')
        .get();

      messagesSnap.forEach((doc) => {
        const raw = { id: doc.id, ...(doc.data() || {}) };
        const body = messageBody(raw);
        if (!body) return;
        messages.push({
          id: raw.id,
          sender: messageSenderLabel(raw),
          timestampMs: Math.max(toMillis(raw?.timestamp), toMillis(raw?.createdAt), toMillis(raw?.sentAt)),
          content: body,
        });
      });
    } catch (error) {
      readError = error?.message || String(error);
    }

    messages.sort((a, b) => a.timestampMs - b.timestampMs);
    conversations.push({
      lead: {
        id: String(lead.id || ''),
        name: leadName(lead),
        phone: safeStr(lead?.telefono || lead?.phone || lead?.whatsapp),
        status: safeStr(lead?.estado) || '(sin estado)',
        stage: safeStr(lead?.etapa || lead?.etapaNombre) || '(sin etapa)',
        source: safeStr(lead?.source) || '(sin fuente)',
        tags: Array.isArray(lead?.etiquetas) ? lead.etiquetas.map((t) => safeStr(t)).filter(Boolean) : [],
        createdAt: lead._createdMs,
        lastActivityAt: lead._lastActivityMs,
      },
      messages,
      readError,
    });
  }

  const lines = [];
  lines.push('# Historial de conversaciones - Clientes que compraron');
  lines.push(`Generado: ${formatDateTime(nowMs)}`);
  lines.push(`Clientes compradores encontrados: ${wonLeads.length}`);
  lines.push(`Clientes incluidos en este archivo: ${selectedLeads.length}`);
  lines.push('');
  lines.push('Filtro usado: leads activos/no archivados con estado de compra (`compro`, `cliente`, `ganado`, `closed_won`, `cerrado_ganado`, `pagado`) o etiqueta `compro`.');
  lines.push('');

  if (conversations.length === 0) {
    lines.push('_(No se encontraron clientes marcados como compra.)_');
  } else {
    conversations.forEach((item, index) => {
      const lead = item.lead;
      lines.push(`## ${index + 1}. ${lead.name}`);
      lines.push(`- Lead ID: ${lead.id}`);
      if (lead.phone) lines.push(`- Telefono: ${lead.phone}`);
      lines.push(`- Estado: ${lead.status}`);
      lines.push(`- Etapa: ${lead.stage}`);
      lines.push(`- Fuente: ${lead.source}`);
      if (lead.tags.length) lines.push(`- Etiquetas: ${lead.tags.join(', ')}`);
      lines.push(`- Fecha de creacion: ${formatDateTime(lead.createdAt)}`);
      lines.push(`- Ultima actividad: ${formatDateTime(lead.lastActivityAt)}`);
      lines.push(`- Mensajes exportados: ${item.messages.length}`);
      if (item.readError) lines.push(`- Error al leer mensajes: ${item.readError}`);
      lines.push('');
      lines.push('### Conversacion');

      if (item.messages.length === 0) {
        lines.push('_(Sin mensajes visibles en el historial.)_');
      } else {
        item.messages.forEach((message) => {
          lines.push(`- [${formatDateTime(message.timestampMs)}] ${message.sender}: ${renderIndentedMessage(message.content)}`);
        });
      }
      lines.push('');
    });
  }

  return {
    data: {
      generatedAt: new Date(nowMs).toISOString(),
      totalWon: wonLeads.length,
      exported: conversations.length,
      conversations,
    },
    markdown: lines.join('\n'),
  };
}

function nonBuyerScore(candidate = {}, buyers = []) {
  const attr = leadAttribution(candidate);
  const createdMs = toMillis(candidate?.fecha_creacion);
  const product = normToken(purchasedProduct(candidate));
  let best = 0;

  buyers.forEach((buyer) => {
    const buyerAttr = leadAttribution(buyer);
    let score = 0;
    if (attr.campaignId && attr.campaignId === buyerAttr.campaignId) score += 10;
    if (attr.campaignName && attr.campaignName === buyerAttr.campaignName) score += 8;
    if (attr.adId && attr.adId === buyerAttr.adId) score += 7;
    if (attr.adsetId && attr.adsetId === buyerAttr.adsetId) score += 6;
    if (attr.source && attr.source === buyerAttr.source) score += 5;
    const buyerProduct = normToken(purchasedProduct(buyer));
    if (product && buyerProduct && product === buyerProduct) score += 5;

    const buyerCreatedMs = toMillis(buyer?.fecha_creacion);
    if (createdMs && buyerCreatedMs) {
      const diffDays = Math.abs(createdMs - buyerCreatedMs) / (24 * 60 * 60 * 1000);
      if (diffDays <= 30) score += 5;
      else if (diffDays <= 90) score += 3;
      else if (diffDays <= 180) score += 1;
    }
    if (score > best) best = score;
  });

  return best;
}

async function loadLeadMessages(db, leadId, cutoffMs = 0) {
  try {
    const snap = await db.collection('leads')
      .doc(String(leadId))
      .collection('messages')
      .orderBy('timestamp', 'asc')
      .get();

    const messages = [];
    snap.forEach((doc) => {
      const message = normalizeMessage(doc);
      if (!message.timestampMs) return;
      if (cutoffMs && message.timestampMs > cutoffMs) return;
      if (!message.text && !message.transcription && !message.mediaUrl) return;
      messages.push(message);
    });
    return { messages: dedupeMessages(messages), error: '' };
  } catch (error) {
    return { messages: [], error: error?.message || String(error) };
  }
}

function serializeLeadDatasetRow({ lead = {}, converted = false, messages = [], events = [], stageHistory = [] } = {}) {
  const attr = leadAttribution(lead);
  const purchaseMs = converted ? purchaseTimestamp(lead) : 0;
  const metrics = computeTemporalMetrics({ lead, messages, purchaseMs });
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas.map((t) => safeStr(t)).filter(Boolean) : [];
  const product = purchasedProduct(lead);

  const base = {
    leadId: safeStr(lead?.id),
    telefono: safeStr(lead?.telefono || lead?.phone || lead?.whatsapp),
    nombre: leadName(lead),
    fechaCreacion: isoOrNull(lead?.fecha_creacion),
    fechaUltimaActividad: isoOrNull(Math.max(
      toMillis(lead?.lastMessageAt),
      toMillis(lead?.lastInboundAt),
      toMillis(lead?.lastOutboundAt),
      toMillis(lead?.fecha_creacion)
    )),
    converted,
    purchaseTimestamp: purchaseMs ? new Date(purchaseMs).toISOString() : null,
    purchaseAmount: converted ? purchaseAmount(lead) : null,
    productPurchased: converted ? product : '',
    planPurchased: converted ? safeStr(lead?.plan || lead?.planNombre || lead?.paymentReference?.planId) : '',
    source: attr.source,
    campaignId: attr.campaignId,
    campaignName: attr.campaignName,
    adsetId: attr.adsetId,
    adsetName: attr.adsetName,
    adId: attr.adId,
    adName: attr.adName,
    ctwaClid: attr.ctwaClid,
    etapaInicial: leadInitialStage(lead),
    etapaFinal: leadFinalStage(lead),
    etiquetas: tags,
    vendedorAsignado: safeStr(lead?.assignedTo || lead?.assignedSeller || lead?.vendedorAsignado),
    messages,
    events,
    stageHistory,
    metrics,
  };

  return base;
}

function serializeCsvLeadRow(datasetLead = {}) {
  const metrics = datasetLead.metrics || {};
  return {
    leadId: datasetLead.leadId,
    converted: datasetLead.converted ? 'true' : 'false',
    createdAt: datasetLead.fechaCreacion,
    lastActivityAt: datasetLead.fechaUltimaActividad,
    purchaseTimestamp: datasetLead.purchaseTimestamp,
    purchaseAmount: datasetLead.purchaseAmount,
    product: datasetLead.productPurchased,
    plan: datasetLead.planPurchased,
    source: datasetLead.source,
    campaignId: datasetLead.campaignId,
    campaignName: datasetLead.campaignName,
    adsetId: datasetLead.adsetId,
    adsetName: datasetLead.adsetName,
    adId: datasetLead.adId,
    adName: datasetLead.adName,
    ctwaClid: datasetLead.ctwaClid,
    initialStage: datasetLead.etapaInicial,
    finalStage: datasetLead.etapaFinal,
    assignedSeller: datasetLead.vendedorAsignado,
    inboundMessages: metrics.numberOfInboundMessages,
    outboundMessages: metrics.numberOfOutboundMessages,
    followups: metrics.numberOfFollowups,
    customerReplies: metrics.numberOfCustomerReplies,
    silencePeriods: metrics.numberOfSilencePeriods,
    longestSilenceHours: metrics.longestSilenceHours,
    daysUntilPurchase: metrics.daysUntilPurchase,
    tags: Array.isArray(datasetLead.etiquetas) ? datasetLead.etiquetas.join('|') : '',
  };
}

export async function generateConversationConversionDataset({
  dbOverride = null,
  now = new Date(),
  nonBuyersPerBuyer = 4,
  minNonBuyerAgeDays = 30,
  maxBuyers = 0,
  maxNonBuyers = 220,
} = {}) {
  const db = await getDb(dbOverride);
  const nowMs = now.getTime();
  const DAY = 24 * 60 * 60 * 1000;
  const leadsSnap = await db.collection('leads').get();
  const buyers = [];
  const nonBuyerCandidates = [];

  leadsSnap.forEach((doc) => {
    const lead = { id: doc.id, ...(doc.data() || {}) };
    const tags = tagSet(lead);
    if (shouldExcludeFromConversionDataset(lead, tags)) return;

    const converted = isWonLead(lead, tags);
    const createdMs = toMillis(lead?.fecha_creacion);
    if (converted) {
      buyers.push(lead);
      return;
    }

    if (!createdMs || (nowMs - createdMs) < Number(minNonBuyerAgeDays || 30) * DAY) return;
    nonBuyerCandidates.push(lead);
  });

  buyers.sort((a, b) => Math.max(purchaseTimestamp(b), toMillis(b?.lastMessageAt), toMillis(b?.fecha_creacion))
    - Math.max(purchaseTimestamp(a), toMillis(a?.lastMessageAt), toMillis(a?.fecha_creacion)));
  const selectedBuyers = Number(maxBuyers) > 0 ? buyers.slice(0, Number(maxBuyers)) : buyers;

  const targetNonBuyerCount = Math.min(
    Number(maxNonBuyers) > 0 ? Number(maxNonBuyers) : nonBuyerCandidates.length,
    Math.max(0, selectedBuyers.length * Math.max(1, Number(nonBuyersPerBuyer) || 4))
  );

  const selectedNonBuyers = nonBuyerCandidates
    .map((lead) => ({
      lead,
      score: nonBuyerScore(lead, selectedBuyers),
      lastActivityMs: Math.max(toMillis(lead?.lastMessageAt), toMillis(lead?.lastInboundAt), toMillis(lead?.lastOutboundAt), toMillis(lead?.fecha_creacion)),
    }))
    .sort((a, b) => (b.score - a.score) || (b.lastActivityMs - a.lastActivityMs))
    .slice(0, targetNonBuyerCount)
    .map((item) => item.lead);

  const selected = [
    ...selectedBuyers.map((lead) => ({ lead, converted: true })),
    ...selectedNonBuyers.map((lead) => ({ lead, converted: false })),
  ];

  const taskEventsByLead = await loadSellerTaskEvents(db);
  const conversations = [];
  const csvRows = [];
  for (const item of selected) {
    const purchaseMs = item.converted ? purchaseTimestamp(item.lead) : 0;
    const cutoffMs = item.converted && purchaseMs
      ? purchaseMs
      : Math.max(toMillis(item.lead?.lastMessageAt), toMillis(item.lead?.lastInboundAt), toMillis(item.lead?.lastOutboundAt), toMillis(item.lead?.fecha_creacion));
    const loaded = await loadLeadMessages(db, item.lead.id, cutoffMs);
    const stageHistory = normalizeStageHistory(item.lead);
    const events = dedupeEvents([
      ...inferEvents(item.lead, loaded.messages, purchaseMs),
      ...(taskEventsByLead.get(String(item.lead.id)) || []),
    ]);
    const row = serializeLeadDatasetRow({
      lead: item.lead,
      converted: item.converted,
      messages: loaded.messages,
      events,
      stageHistory,
    });
    row.messages = row.messages.map(({ timestampMs, ...message }) => message);
    if (loaded.error) {
      row.messageReadError = loaded.error;
    }
    conversations.push(row);
    csvRows.push(serializeCsvLeadRow(row));
  }

  const leadsCsv = renderLeadsCsv(csvRows);
  return {
    data: {
      generatedAt: new Date(nowMs).toISOString(),
      cohortSummary: {
        buyersAvailable: buyers.length,
        buyersExported: selectedBuyers.length,
        nonBuyerCandidates: nonBuyerCandidates.length,
        nonBuyersExported: selectedNonBuyers.length,
        minNonBuyerAgeDays: Number(minNonBuyerAgeDays || 30),
      },
      conversations,
    },
    leadsCsv,
  };
}

export async function generateBiReport({ dbOverride = null, now = new Date() } = {}) {
  const db = await getDb(dbOverride);
  const nowMs = now.getTime();
  const DAY = 24 * 60 * 60 * 1000;

  const leadsSnap = await db.collection('leads').get();

  const totals = {
    all: 0,
    active: 0,
    archived: 0,
    merged: 0,
  };

  const bySource = new Map();
  const byStatus = new Map();
  const byStage = new Map();
  const byMonth = new Map();
  const wonByMonth = new Map();
  const touchBuckets = new Map([['0', 0], ['1-2', 0], ['3-5', 0], ['6+', 0]]);
  const recencyBuckets = new Map([['< 24h', 0], ['1-3 dias', 0], ['3-7 dias', 0], ['7-30 dias', 0], ['> 30 dias', 0], ['sin actividad', 0]]);
  const sourceStats = new Map(); // source -> { total, replied, formCompleted, won }
  const activeSeqByStage = new Map(); // stage -> Map(trigger -> count)
  const intentDist = new Map();
  const interestDist = new Map();
  const responseSamples = []; // { text, interest, intent, automated, ms, estado, stage }

  let replied = 0;
  let neverContacted = 0;
  let contactedToday = 0;
  let silent7d = 0;
  let formLinkSent = 0;
  let formCompleted = 0;
  let sampleGenerated = 0;
  let hot = 0;
  let bot = 0;
  let won = 0;
  let lost = 0;
  let stopped = 0;
  let withUnread = 0;

  const startOfToday = new Date(now);
  startOfToday.setHours(0, 0, 0, 0);
  const startOfTodayMs = startOfToday.getTime();

  leadsSnap.forEach((doc) => {
    const lead = doc.data() || {};
    totals.all += 1;

    if (lead?.mergedInto) {
      totals.merged += 1;
      return; // los merged no cuentan para metricas
    }
    const archived = lead?.isArchived === true || lead?.archived === true || Boolean(lead?.archivedAt);
    if (archived) {
      totals.archived += 1;
      return;
    }
    totals.active += 1;

    const tags = tagSet(lead);
    const source = safeStr(lead?.source) || '(sin fuente)';
    const status = lower(lead?.estado) || '(sin estado)';
    const stage = normToken(lead?.etapa || lead?.etapaNombre) || 'leads_nuevos';

    const createdMs = toMillis(lead?.fecha_creacion);
    const inboundMs = toMillis(lead?.lastInboundAt);
    const outboundMs = Math.max(
      toMillis(lead?.lastOutboundAt),
      toMillis(lead?.lastManualFollowupAt),
      toMillis(lead?.aiFollowup?.lastSentAt)
    );
    const lastActivityMs = Math.max(inboundMs, toMillis(lead?.lastMessageAt), outboundMs, createdMs);

    incr(bySource, source);
    incr(byStatus, status);
    incr(byStage, stage);
    incr(byMonth, monthKey(createdMs));

    if (!sourceStats.has(source)) sourceStats.set(source, { total: 0, replied: 0, formCompleted: 0, won: 0 });
    const ss = sourceStats.get(source);
    ss.total += 1;

    const didReply = inboundMs > 0;
    if (didReply) { replied += 1; ss.replied += 1; }

    if (outboundMs <= 0) neverContacted += 1;
    if (outboundMs >= startOfTodayMs) contactedToday += 1;
    if (!didReply && createdMs > 0 && (nowMs - lastActivityMs) > 7 * DAY) silent7d += 1;
    if (Number(lead?.unreadCount || 0) > 0) withUnread += 1;

    const linkSent = tags.has('formlinksent') || tags.has('samplelinksent') || tags.has('webenviada') || tags.has('muestralista') || tags.has('muestraactiva');
    const completed = hasCompletedForm(lead, tags);
    const sample = hasSlug(lead) || tags.has('samplelinksent') || tags.has('muestraactiva');
    if (linkSent) formLinkSent += 1;
    if (completed) formCompleted += 1;
    if (sample) sampleGenerated += 1;

    if (tags.has('respuestacaliente') || lead?.aiReply?.hot === true) hot += 1;
    if (tags.has('respuestaautomatica') || lead?.autoResponder?.detected === true) bot += 1;
    if (lead?.stopSequences === true || tags.has('detenersecuencia') || tags.has('stopsequences')) stopped += 1;

    const isWon = WON_STATUSES.has(status) || tags.has('compro');
    const isLost = LOST_STATUSES.has(status) || tags.has('no_interesa') || tags.has('nointeresa');
    if (isWon) { won += 1; ss.won += 1; incr(wonByMonth, monthKey(createdMs)); }
    if (isLost) lost += 1;
    if (completed) ss.formCompleted += 1;

    const touch = Number(lead?.aiFollowup?.touchCount || 0);
    if (touch <= 0) incr(touchBuckets, '0');
    else if (touch <= 2) incr(touchBuckets, '1-2');
    else if (touch <= 5) incr(touchBuckets, '3-5');
    else incr(touchBuckets, '6+');

    const sinceActivity = nowMs - lastActivityMs;
    if (lastActivityMs <= 0) incr(recencyBuckets, 'sin actividad');
    else if (sinceActivity < DAY) incr(recencyBuckets, '< 24h');
    else if (sinceActivity < 3 * DAY) incr(recencyBuckets, '1-3 dias');
    else if (sinceActivity < 7 * DAY) incr(recencyBuckets, '3-7 dias');
    else if (sinceActivity < 30 * DAY) incr(recencyBuckets, '7-30 dias');
    else incr(recencyBuckets, '> 30 dias');

    // Secuencias activas en este lead, agrupadas por etapa.
    const activeSeqs = Array.isArray(lead?.secuenciasActivas) ? lead.secuenciasActivas : [];
    activeSeqs.forEach((sq) => {
      if (!sq || sq.completed === true) return;
      const trig = safeStr(sq.trigger);
      if (!trig) return;
      if (!activeSeqByStage.has(stage)) activeSeqByStage.set(stage, new Map());
      incr(activeSeqByStage.get(stage), trig);
    });

    // Respuestas clasificadas (para muestra cualitativa + distribuciones).
    const aiReply = lead?.aiReply;
    if (aiReply && safeStr(aiReply.lastText)) {
      incr(intentDist, lower(aiReply.intent) || 'other');
      incr(interestDist, lower(aiReply.interestLevel) || 'cold');
      responseSamples.push({
        text: safeStr(aiReply.lastText).slice(0, 240),
        interest: lower(aiReply.interestLevel) || 'cold',
        intent: lower(aiReply.intent) || 'other',
        automated: aiReply.automated === true,
        ms: toMillis(aiReply.classifiedAt),
        estado: status,
        stage,
      });
    }
  });

  // Tareas (alertas del detector + manuales)
  const taskStats = { total: 0, open: 0, bySource: new Map() };
  try {
    const tasksSnap = await db.collection('tasks').limit(8000).get();
    tasksSnap.forEach((doc) => {
      const t = doc.data() || {};
      taskStats.total += 1;
      const open = lower(t?.status) !== 'completada';
      if (open) {
        taskStats.open += 1;
        incr(taskStats.bySource, safeStr(t?.source) || 'manual');
      }
    });
  } catch {
    /* tasks opcional */
  }

  // Definiciones de secuencias (mensajes programados por trigger).
  const sequenceDefs = [];
  try {
    const seqSnap = await db.collection('secuencias').get();
    seqSnap.forEach((doc) => {
      const d = doc.data() || {};
      const steps = Array.isArray(d.messages) ? d.messages : [];
      sequenceDefs.push({
        trigger: safeStr(d.trigger) || doc.id,
        active: d.active !== false,
        steps: steps.map((m) => ({
          delay: Number(m?.delay || 0),
          type: safeStr(m?.type) || 'texto',
          content: safeStr(m?.contenido || m?.texto || m?.caption || ''),
        })),
      });
    });
  } catch {
    /* secuencias opcional */
  }

  // Muestra de respuestas: las 25 más recientes con clasificación.
  responseSamples.sort((a, b) => b.ms - a.ms);
  const responseSample = responseSamples.slice(0, 25);

  const data = {
    generatedAt: new Date(nowMs).toISOString(),
    totals,
    active: totals.active,
    bySource: Object.fromEntries(mapToSortedRows(bySource)),
    byStatus: Object.fromEntries(mapToSortedRows(byStatus)),
    byStage: Object.fromEntries(mapToSortedRows(byStage)),
    sampleFunnel: { formLinkSent, formCompleted, sampleGenerated },
    engagement: { replied, neverContacted, contactedToday, silent7d, withUnread, hot, bot, stopped },
    outcome: { won, lost },
    touchBuckets: Object.fromEntries(touchBuckets),
    recencyBuckets: Object.fromEntries(recencyBuckets),
    byMonth: Object.fromEntries(mapToSortedRows(byMonth)),
    tasks: { total: taskStats.total, open: taskStats.open, bySource: Object.fromEntries(mapToSortedRows(taskStats.bySource)) },
    sequences: sequenceDefs,
    activeSeqByStage: Object.fromEntries(
      [...activeSeqByStage.entries()].map(([st, m]) => [st, Object.fromEntries(mapToSortedRows(m))])
    ),
    responses: {
      intent: Object.fromEntries(mapToSortedRows(intentDist)),
      interest: Object.fromEntries(mapToSortedRows(interestDist)),
      sample: responseSample,
    },
  };

  // ----------------------------- Markdown -----------------------------
  const A = totals.active || 1;
  const lines = [];
  lines.push('# Informe BI — CRM NegociosWeb');
  lines.push(`Generado: ${new Date(nowMs).toLocaleString('es-MX')}`);
  lines.push('');
  lines.push('## 1. Resumen general');
  lines.push(renderTable(['Metrica', 'Valor'], [
    ['Leads totales (incluye archivados/merged)', String(totals.all)],
    ['Leads activos (base de analisis)', String(totals.active)],
    ['Archivados', String(totals.archived)],
    ['Merged/duplicados', String(totals.merged)],
    ['Ganados (compraron)', `${won} (${pct(won, A)})`],
    ['Perdidos / no interesa', `${lost} (${pct(lost, A)})`],
  ]));
  lines.push('');

  lines.push('## 2. Embudo de muestra (clave del negocio)');
  lines.push('Flujo: se envia link de formulario → cliente lo llena → se genera su muestra → compra.');
  lines.push(renderTable(['Etapa', 'Leads', '% de activos', '% del paso previo'], [
    ['Recibieron link de formulario/muestra', String(formLinkSent), pct(formLinkSent, A), '—'],
    ['Llenaron el formulario', String(formCompleted), pct(formCompleted, A), pct(formCompleted, formLinkSent)],
    ['Tienen muestra generada', String(sampleGenerated), pct(sampleGenerated, A), pct(sampleGenerated, formCompleted)],
    ['Compraron', String(won), pct(won, A), pct(won, sampleGenerated)],
  ]));
  lines.push('');

  lines.push('## 3. Seguimiento y engagement');
  lines.push(renderTable(['Metrica', 'Leads', '% de activos'], [
    ['Respondieron al menos una vez', String(replied), pct(replied, A)],
    ['Nunca se les ha escrito (saliente)', String(neverContacted), pct(neverContacted, A)],
    ['Contactados hoy', String(contactedToday), pct(contactedToday, A)],
    ['Silenciosos >7 dias (no respondieron)', String(silent7d), pct(silent7d, A)],
    ['Con mensajes sin leer', String(withUnread), pct(withUnread, A)],
    ['Respuestas calientes detectadas', String(hot), pct(hot, A)],
    ['Bots/IA contestando detectados', String(bot), pct(bot, A)],
    ['Marcados como detener/stop', String(stopped), pct(stopped, A)],
  ]));
  lines.push('');

  lines.push('## 4. Por fuente de lead');
  lines.push(renderTable(['Fuente', 'Leads', '% respondio', '% lleno form', '% compro'],
    mapToSortedRows(bySource).map(([src]) => {
      const s = sourceStats.get(src) || { total: 0, replied: 0, formCompleted: 0, won: 0 };
      return [src, String(s.total), pct(s.replied, s.total), pct(s.formCompleted, s.total), pct(s.won, s.total)];
    })
  ));
  lines.push('');

  lines.push('## 5. Por estado en el CRM');
  lines.push(renderTable(['Estado', 'Leads', '%'], mapToSortedRows(byStatus).map(([k, v]) => [k, String(v), pct(v, A)])));
  lines.push('');

  lines.push('## 6. Por etapa del embudo');
  lines.push(renderTable(['Etapa', 'Leads', '%'], mapToSortedRows(byStage).map(([k, v]) => [k, String(v), pct(v, A)])));
  lines.push('');

  lines.push('## 7. Recencia (ultima actividad)');
  lines.push(renderTable(['Antiguedad', 'Leads', '%'], [...recencyBuckets.entries()].map(([k, v]) => [k, String(v), pct(v, A)])));
  lines.push('');

  lines.push('## 8. Intensidad de seguimiento IA (toques)');
  lines.push(renderTable(['Toques de reactivacion', 'Leads'], [...touchBuckets.entries()].map(([k, v]) => [k, String(v)])));
  lines.push('');

  lines.push('## 9. Cohortes por mes de creacion');
  lines.push(renderTable(['Mes', 'Leads creados', 'Compraron', '% conversion'],
    mapToSortedRows(byMonth).map(([m, count]) => {
      const w = wonByMonth.get(m) || 0;
      return [m, String(count), String(w), pct(w, count)];
    })
  ));
  lines.push('');

  lines.push('## 10. Tareas (alertas del sistema)');
  lines.push(renderTable(['Metrica', 'Valor'], [
    ['Tareas abiertas', String(taskStats.open)],
    ...mapToSortedRows(taskStats.bySource).map(([k, v]) => [`  - origen: ${k}`, String(v)]),
  ]));
  lines.push('');

  lines.push('## 11. Secuencias automaticas activas por etapa');
  lines.push('Cuantos leads tienen cada secuencia (trigger) corriendo, por etapa del embudo.');
  const stageSeqRows = [];
  for (const [stage, m] of [...activeSeqByStage.entries()]) {
    for (const [trig, count] of mapToSortedRows(m)) {
      stageSeqRows.push([stage, trig, String(count)]);
    }
  }
  lines.push(stageSeqRows.length
    ? renderTable(['Etapa', 'Secuencia (trigger)', 'Leads activos'], stageSeqRows)
    : '_(Ningun lead con secuencia activa en este momento.)_');
  lines.push('');

  lines.push('## 12. Mensajes que YO configure en las secuencias (texto literal)');
  lines.push('Revisar el copy y el timing de cada paso.');
  lines.push('');
  if (sequenceDefs.length === 0) {
    lines.push('_(No se encontraron definiciones de secuencias.)_');
  } else {
    for (const seq of sequenceDefs) {
      lines.push(`### Secuencia: \`${seq.trigger}\` ${seq.active ? '(activa)' : '(inactiva)'} — ${seq.steps.length} paso(s)`);
      if (seq.steps.length === 0) {
        lines.push('_(Sin pasos.)_');
      } else {
        seq.steps.forEach((st, i) => {
          lines.push(`**Paso ${i + 1}** · envío: ${formatDelay(st.delay)} · tipo: ${st.type}`);
          const body = (st.content || '(media/sin texto)').slice(0, 2000);
          body.split('\n').forEach((ln) => lines.push(`> ${ln}`));
          lines.push('');
        });
      }
    }
  }
  lines.push('');

  lines.push('## 13. Mensajes AUTOMATIZADOS del sistema (texto literal)');
  lines.push('Plantillas que el CRM usa solo. El sistema rota/varia estas frases y rellena {{nombre}} y {{link}}.');
  lines.push('');

  let reactCatalog = null;
  let buttonsCatalog = null;
  try { reactCatalog = getReactivationMessageCatalog(); } catch { /* opcional */ }
  try { buttonsCatalog = getFollowupMessageCatalog(); } catch { /* opcional */ }

  if (reactCatalog) {
    lines.push('### 13.1 Seguimiento diario de reactivación (un ángulo distinto por día)');
    reactCatalog.dailyAngles.forEach((ang) => {
      lines.push(`**Ángulo \`${ang.key}\`:**`);
      ang.variants.forEach((v) => lines.push(`> ${v}`));
      lines.push('');
    });
    lines.push('**Variantes cuando YA tiene muestra (reenvía el sitio):**');
    reactCatalog.sampleReadyVariants.forEach((v) => lines.push(`> ${v}`));
    lines.push('');
    lines.push('**Variantes cuando NO llenó el formulario (invita a llenarlo):**');
    reactCatalog.formInviteVariants.forEach((v) => lines.push(`> ${v}`));
    lines.push('');
  }

  if (buttonsCatalog) {
    lines.push('### 13.2 Botones de seguimiento manual (de un clic en el chat)');
    buttonsCatalog.forEach((b) => {
      lines.push(`**${b.label}** (\`${b.key}\`) — ${b.description}`);
      const all = [...b.variants, ...b.sampleVariants, ...b.formVariants];
      all.forEach((v) => lines.push(`> ${v}`));
      lines.push('');
    });
  }

  lines.push('## 14. Respuestas de los clientes');
  lines.push(renderTable(['Nivel de interes', 'Respuestas'], mapToSortedRows(interestDist).map(([k, v]) => [k, String(v)])));
  lines.push('');
  lines.push('Intencion detectada:');
  lines.push(renderTable(['Intencion', 'Respuestas'], mapToSortedRows(intentDist).map(([k, v]) => [k, String(v)])));
  lines.push('');
  lines.push('Muestra de respuestas recientes (texto real del cliente + clasificacion):');
  if (responseSample.length === 0) {
    lines.push('_(Aun no hay respuestas clasificadas.)_');
  } else {
    lines.push(renderTable(['Interes', 'Intencion', 'Bot?', 'Respuesta del cliente'],
      responseSample.map((r) => [
        r.interest,
        r.intent,
        r.automated ? 'si' : 'no',
        r.text.replace(/\n/g, ' ').replace(/\|/g, '/'),
      ])
    ));
  }
  lines.push('');

  lines.push('---');
  lines.push('## Contexto para el analisis');
  lines.push('Negocio: agencia que vende paginas web, campanas de Meta Ads y software a la medida en Mexico.');
  lines.push('Gancho principal: muestra de pagina GRATIS (el cliente llena un formulario corto y se le genera).');
  lines.push('Canal principal: WhatsApp. Estrategia: seguimiento constante (recordar a diario) sin que baneen el numero.');
  lines.push('');
  lines.push('## Preguntas que quiero que analices');
  lines.push('1. ¿Donde se cae mas el embudo de muestra y que harias para subir esa conversion?');
  lines.push('2. ¿Que fuente de leads conviene mas (responde/convierte) y donde invertir?');
  lines.push('3. ¿Que segmentos de leads (por estado/etapa/recencia) debo priorizar esta semana?');
  lines.push('4. ¿La estrategia de seguimiento esta funcionando? ¿Que mensajes/acciones probar?');
  lines.push('5. ¿Que mejoras de PRODUCTO (oferta, muestra, formulario, precios) sugieres con estos numeros?');
  lines.push('6. Revisa TODO el copy (secciones 12 mis secuencias y 13 mensajes automatizados) frente a las RESPUESTAS reales (seccion 14): ¿que textos y timing cambiarias, palabra por palabra, y por que?');
  lines.push('7. Dame un plan de accion concreto para las proximas 2 semanas.');

  return { data, markdown: lines.join('\n') };
}
