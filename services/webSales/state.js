export const WEB_SALES_STAGES = Object.freeze({
  NEW_LEAD: 'new_lead',
  EXAMPLES_SENT: 'examples_sent',
  SAMPLE_OFFERED: 'sample_offered',
  SAMPLE_FORM_SENT: 'sample_form_sent',
  SAMPLE_FORM_STARTED: 'sample_form_started',
  SAMPLE_FORM_COMPLETED: 'sample_form_completed',
  SAMPLE_GENERATED: 'sample_generated',
  SAMPLE_SENT: 'sample_sent',
  SAMPLE_OPENED: 'sample_opened',
  SAMPLE_FEEDBACK: 'sample_feedback',
  CLOSING: 'closing',
  PAYMENT_SENT: 'payment_sent',
  DEPOSIT_PAID: 'deposit_paid',
  WON: 'won',
  LOST: 'lost',
});

export const WEB_CONVERSION_GOALS = Object.freeze({
  GET_REPLY: 'get_reply',
  GET_BUSINESS_CONTEXT: 'get_business_context',
  OFFER_SAMPLE: 'offer_sample',
  GET_SAMPLE_FORM_COMPLETED: 'get_sample_form_completed',
  GET_SAMPLE_OPEN: 'get_sample_open',
  GET_SAMPLE_FEEDBACK: 'get_sample_feedback',
  RESOLVE_OBJECTION: 'resolve_objection',
  GET_PURCHASE_DECISION: 'get_purchase_decision',
  GET_DEPOSIT: 'get_deposit',
  WAIT: 'wait',
  STOP: 'stop',
});

export const WEB_SALES_EVENTS = Object.freeze({
  SAMPLE_OFFERED: 'sample_offered',
  SAMPLE_FORM_SENT: 'sample_form_sent',
  SAMPLE_FORM_OPENED: 'sample_form_opened',
  SAMPLE_FORM_STARTED: 'sample_form_started',
  SAMPLE_FORM_COMPLETED: 'sample_form_completed',
  SAMPLE_GENERATED: 'sample_generated',
  SAMPLE_SENT: 'sample_sent',
  SAMPLE_OPENED: 'sample_opened',
  SAMPLE_FEEDBACK_RECEIVED: 'sample_feedback_received',
  CLOSING_STARTED: 'closing_started',
  PAYMENT_OFFERED: 'payment_offered',
  PAYMENT_LINK_SENT: 'payment_link_sent',
  PAYMENT_LINK_OPENED: 'payment_link_opened',
  DEPOSIT_PAID: 'deposit_paid',
  PURCHASE_COMPLETED: 'purchase_completed',
  LOST: 'lost',
});

const WEB_STAGE_ORDER = Object.freeze({
  [WEB_SALES_STAGES.NEW_LEAD]: 0,
  [WEB_SALES_STAGES.EXAMPLES_SENT]: 5,
  [WEB_SALES_STAGES.SAMPLE_OFFERED]: 10,
  [WEB_SALES_STAGES.SAMPLE_FORM_SENT]: 20,
  [WEB_SALES_STAGES.SAMPLE_FORM_STARTED]: 30,
  [WEB_SALES_STAGES.SAMPLE_FORM_COMPLETED]: 40,
  [WEB_SALES_STAGES.SAMPLE_GENERATED]: 50,
  [WEB_SALES_STAGES.SAMPLE_SENT]: 60,
  [WEB_SALES_STAGES.SAMPLE_OPENED]: 70,
  [WEB_SALES_STAGES.SAMPLE_FEEDBACK]: 80,
  [WEB_SALES_STAGES.CLOSING]: 90,
  [WEB_SALES_STAGES.PAYMENT_SENT]: 100,
  [WEB_SALES_STAGES.DEPOSIT_PAID]: 110,
  [WEB_SALES_STAGES.WON]: 120,
  [WEB_SALES_STAGES.LOST]: 999,
});

const REPEATABLE_EVENT_TYPES = new Set([
  WEB_SALES_EVENTS.SAMPLE_FORM_STARTED,
  WEB_SALES_EVENTS.SAMPLE_OPENED,
  WEB_SALES_EVENTS.PAYMENT_LINK_OPENED,
]);

export const WEB_EVENT_SEQUENCE_CANCELLATIONS = Object.freeze({
  [WEB_SALES_EVENTS.SAMPLE_FORM_COMPLETED]: ['Web_FormPending', 'Web_FormAbandoned'],
  [WEB_SALES_EVENTS.SAMPLE_OPENED]: ['Web_SampleNotOpened'],
  [WEB_SALES_EVENTS.SAMPLE_FEEDBACK_RECEIVED]: ['Web_SampleOpenedNoReply'],
  [WEB_SALES_EVENTS.DEPOSIT_PAID]: ['*'],
  [WEB_SALES_EVENTS.PURCHASE_COMPLETED]: ['*'],
});

function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function objectOr(value, fallback = {}) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : fallback;
}

function isTimestampLike(value) {
  return value instanceof Date || typeof value?.toDate === 'function' || typeof value?.toMillis === 'function';
}

function keepFirst(currentValue, nextValue) {
  if (currentValue) return currentValue;
  return nextValue;
}

function maxStep(previous, next) {
  const prev = Number(previous || 0);
  const incoming = Number(next || 0);
  if (!Number.isFinite(incoming) || incoming <= 0) return previous || null;
  if (!Number.isFinite(prev) || incoming > prev) return incoming;
  return previous || null;
}

function shouldAdvanceStage(currentStage = '', nextStage = '') {
  const currentOrder = WEB_STAGE_ORDER[currentStage] ?? -1;
  const nextOrder = WEB_STAGE_ORDER[nextStage] ?? -1;
  if (currentStage === WEB_SALES_STAGES.LOST || currentStage === WEB_SALES_STAGES.WON) {
    return nextStage === WEB_SALES_STAGES.LOST || nextStage === WEB_SALES_STAGES.WON;
  }
  return nextOrder >= currentOrder;
}

export function isRepeatableWebSalesEvent(type = '') {
  return REPEATABLE_EVENT_TYPES.has(cleanText(type, 80));
}

export function isWebSalesEventType(type = '') {
  return Object.values(WEB_SALES_EVENTS).includes(cleanText(type, 80));
}

export function buildInitialWebSalesState(seed = {}, now = new Date()) {
  const safe = objectOr(seed);
  return {
    stage: cleanText(safe.stage || WEB_SALES_STAGES.NEW_LEAD, 80),
    updatedAt: safe.updatedAt || now,
    nextConversionGoal: cleanText(safe.nextConversionGoal || WEB_CONVERSION_GOALS.GET_REPLY, 80),
    productInterest: cleanText(safe.productInterest || 'web', 40),
    experiment: objectOr(safe.experiment, { id: 'web_funnel_v2', variant: 'A' }),
    sample: {
      offeredAt: safe.sample?.offeredAt || null,
      formSentAt: safe.sample?.formSentAt || null,
      formOpenedAt: safe.sample?.formOpenedAt || null,
      formStartedAt: safe.sample?.formStartedAt || null,
      formLastStep: safe.sample?.formLastStep || null,
      formCompletedAt: safe.sample?.formCompletedAt || null,
      generatedAt: safe.sample?.generatedAt || null,
      sentAt: safe.sample?.sentAt || null,
      firstOpenedAt: safe.sample?.firstOpenedAt || null,
      lastOpenedAt: safe.sample?.lastOpenedAt || null,
      openCount: Number(safe.sample?.openCount || 0),
      feedbackReceivedAt: safe.sample?.feedbackReceivedAt || null,
    },
    closing: {
      startedAt: safe.closing?.startedAt || null,
      paymentOfferedAt: safe.closing?.paymentOfferedAt || null,
      paymentLinkSentAt: safe.closing?.paymentLinkSentAt || null,
      paymentLinkOpenedAt: safe.closing?.paymentLinkOpenedAt || null,
      depositPaidAt: safe.closing?.depositPaidAt || null,
      paidAt: safe.closing?.paidAt || null,
    },
  };
}

export function inferWebStageFromLegacyLead(lead = {}) {
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas.map((tag) => cleanText(tag, 80).toLowerCase()) : [];
  if (lead?.estado === 'compro' || tags.includes('compro')) return WEB_SALES_STAGES.WON;
  if (lead?.linkOpenedAt) return WEB_SALES_STAGES.SAMPLE_OPENED;
  if (lead?.webLinkSentAt || lead?.sampleLinkSentAt) return WEB_SALES_STAGES.SAMPLE_SENT;
  if (lead?.sampleSubmittedAt || lead?.etapa === 'form_submitted' || tags.includes('formok') || tags.includes('formulariocompletado')) {
    return WEB_SALES_STAGES.SAMPLE_FORM_COMPLETED;
  }
  if (lead?.formLinkSentAt || tags.includes('formlinksent') || lead?.sampleFlow?.enabled === true) {
    return WEB_SALES_STAGES.SAMPLE_FORM_SENT;
  }
  return WEB_SALES_STAGES.NEW_LEAD;
}

export function ensureWebSalesState(lead = {}, now = new Date()) {
  const existing = buildInitialWebSalesState(objectOr(lead?.webSales), now);
  if (lead?.webSales && typeof lead.webSales === 'object') return existing;
  const stage = inferWebStageFromLegacyLead(lead);
  return {
    ...existing,
    stage,
    nextConversionGoal: nextGoalForStage(stage, existing),
  };
}

export function nextGoalForStage(stage = WEB_SALES_STAGES.NEW_LEAD, webSales = {}) {
  const sample = objectOr(webSales.sample);
  if (stage === WEB_SALES_STAGES.LOST || stage === WEB_SALES_STAGES.WON) return WEB_CONVERSION_GOALS.STOP;
  if (stage === WEB_SALES_STAGES.DEPOSIT_PAID) return WEB_CONVERSION_GOALS.GET_DEPOSIT;
  if (stage === WEB_SALES_STAGES.PAYMENT_SENT) return WEB_CONVERSION_GOALS.GET_DEPOSIT;
  if (stage === WEB_SALES_STAGES.CLOSING) return WEB_CONVERSION_GOALS.GET_PURCHASE_DECISION;
  if (stage === WEB_SALES_STAGES.SAMPLE_FEEDBACK) return WEB_CONVERSION_GOALS.GET_PURCHASE_DECISION;
  if (stage === WEB_SALES_STAGES.SAMPLE_OPENED) return WEB_CONVERSION_GOALS.GET_SAMPLE_FEEDBACK;
  if (stage === WEB_SALES_STAGES.SAMPLE_SENT) return WEB_CONVERSION_GOALS.GET_SAMPLE_OPEN;
  if (stage === WEB_SALES_STAGES.SAMPLE_GENERATED) return sample.sentAt ? WEB_CONVERSION_GOALS.GET_SAMPLE_OPEN : WEB_CONVERSION_GOALS.WAIT;
  if (stage === WEB_SALES_STAGES.SAMPLE_FORM_COMPLETED) return WEB_CONVERSION_GOALS.WAIT;
  if (stage === WEB_SALES_STAGES.SAMPLE_FORM_STARTED) return WEB_CONVERSION_GOALS.GET_SAMPLE_FORM_COMPLETED;
  if (stage === WEB_SALES_STAGES.SAMPLE_FORM_SENT) return WEB_CONVERSION_GOALS.GET_SAMPLE_FORM_COMPLETED;
  if (stage === WEB_SALES_STAGES.SAMPLE_OFFERED) return WEB_CONVERSION_GOALS.GET_SAMPLE_FORM_COMPLETED;
  if (stage === WEB_SALES_STAGES.EXAMPLES_SENT) return WEB_CONVERSION_GOALS.OFFER_SAMPLE;
  return WEB_CONVERSION_GOALS.GET_REPLY;
}

function applyStage(currentStage, nextStage) {
  return shouldAdvanceStage(currentStage, nextStage) ? nextStage : currentStage;
}

export function buildWebSalesStateAfterEvent({
  current = {},
  lead = {},
  type = '',
  metadata = {},
  timestamp = new Date(),
  incrementOpenCount = true,
} = {}) {
  const safeType = cleanText(type, 80);
  const next = buildInitialWebSalesState(current?.stage ? current : ensureWebSalesState(lead, timestamp), timestamp);
  const sample = { ...next.sample };
  const closing = { ...next.closing };
  let stage = next.stage || WEB_SALES_STAGES.NEW_LEAD;

  switch (safeType) {
    case WEB_SALES_EVENTS.SAMPLE_OFFERED:
      sample.offeredAt = keepFirst(sample.offeredAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.SAMPLE_OFFERED);
      break;
    case WEB_SALES_EVENTS.SAMPLE_FORM_SENT:
      sample.offeredAt = keepFirst(sample.offeredAt, timestamp);
      sample.formSentAt = keepFirst(sample.formSentAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.SAMPLE_FORM_SENT);
      break;
    case WEB_SALES_EVENTS.SAMPLE_FORM_OPENED:
      sample.formOpenedAt = keepFirst(sample.formOpenedAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.SAMPLE_FORM_SENT);
      break;
    case WEB_SALES_EVENTS.SAMPLE_FORM_STARTED:
      sample.formOpenedAt = keepFirst(sample.formOpenedAt, timestamp);
      sample.formStartedAt = keepFirst(sample.formStartedAt, timestamp);
      sample.formLastStep = maxStep(sample.formLastStep, metadata.step || metadata.formLastStep || 1);
      stage = applyStage(stage, WEB_SALES_STAGES.SAMPLE_FORM_STARTED);
      break;
    case WEB_SALES_EVENTS.SAMPLE_FORM_COMPLETED:
      sample.formOpenedAt = keepFirst(sample.formOpenedAt, timestamp);
      sample.formStartedAt = keepFirst(sample.formStartedAt, timestamp);
      sample.formLastStep = maxStep(sample.formLastStep, metadata.step || metadata.formLastStep || 2);
      sample.formCompletedAt = keepFirst(sample.formCompletedAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.SAMPLE_FORM_COMPLETED);
      break;
    case WEB_SALES_EVENTS.SAMPLE_GENERATED:
      sample.generatedAt = keepFirst(sample.generatedAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.SAMPLE_GENERATED);
      break;
    case WEB_SALES_EVENTS.SAMPLE_SENT:
      sample.generatedAt = keepFirst(sample.generatedAt, metadata.generatedAt && isTimestampLike(metadata.generatedAt) ? metadata.generatedAt : sample.generatedAt);
      sample.sentAt = keepFirst(sample.sentAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.SAMPLE_SENT);
      break;
    case WEB_SALES_EVENTS.SAMPLE_OPENED:
      sample.firstOpenedAt = keepFirst(sample.firstOpenedAt, timestamp);
      sample.lastOpenedAt = timestamp;
      sample.openCount = Math.max(0, Number(sample.openCount || 0)) + (incrementOpenCount ? 1 : 0);
      stage = applyStage(stage, WEB_SALES_STAGES.SAMPLE_OPENED);
      break;
    case WEB_SALES_EVENTS.SAMPLE_FEEDBACK_RECEIVED:
      sample.feedbackReceivedAt = keepFirst(sample.feedbackReceivedAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.SAMPLE_FEEDBACK);
      break;
    case WEB_SALES_EVENTS.CLOSING_STARTED:
      closing.startedAt = keepFirst(closing.startedAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.CLOSING);
      break;
    case WEB_SALES_EVENTS.PAYMENT_OFFERED:
      closing.startedAt = keepFirst(closing.startedAt, timestamp);
      closing.paymentOfferedAt = keepFirst(closing.paymentOfferedAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.CLOSING);
      break;
    case WEB_SALES_EVENTS.PAYMENT_LINK_SENT:
      closing.startedAt = keepFirst(closing.startedAt, timestamp);
      closing.paymentLinkSentAt = keepFirst(closing.paymentLinkSentAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.PAYMENT_SENT);
      break;
    case WEB_SALES_EVENTS.PAYMENT_LINK_OPENED:
      closing.paymentLinkOpenedAt = keepFirst(closing.paymentLinkOpenedAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.PAYMENT_SENT);
      break;
    case WEB_SALES_EVENTS.DEPOSIT_PAID:
      closing.depositPaidAt = keepFirst(closing.depositPaidAt, timestamp);
      stage = applyStage(stage, WEB_SALES_STAGES.DEPOSIT_PAID);
      break;
    case WEB_SALES_EVENTS.PURCHASE_COMPLETED:
      closing.paidAt = keepFirst(closing.paidAt, timestamp);
      stage = WEB_SALES_STAGES.WON;
      break;
    case WEB_SALES_EVENTS.LOST:
      stage = WEB_SALES_STAGES.LOST;
      break;
    default:
      break;
  }

  return {
    ...next,
    stage,
    updatedAt: timestamp,
    productInterest: 'web',
    nextConversionGoal: nextGoalForStage(stage, { ...next, sample, closing }),
    sample,
    closing,
  };
}

export function getWebSalesSequenceCancellations(type = '') {
  const rules = WEB_EVENT_SEQUENCE_CANCELLATIONS[cleanText(type, 80)];
  return Array.isArray(rules) ? [...rules] : [];
}

export function isLikelyWebLead(lead = {}) {
  if (lead?.productInterest === 'web' || lead?.webSales?.productInterest === 'web') return true;
  const productStrategy = cleanText(lead?.salesState?.productStrategy || lead?.salesState?.qualification?.productStrategy || '', 80);
  if (productStrategy === 'web') return true;
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas.map((tag) => cleanText(tag, 80).toLowerCase()) : [];
  if (tags.some((tag) => ['webpromo', 'webenviada', 'leadweb', 'leadwhatsapp', 'nuevoleadweb', 'formulariocompletado', 'formlinksent', 'muestraactiva'].includes(tag))) return true;
  return Boolean(lead?.sampleFlow || lead?.briefWeb || lead?.sampleSubmittedAt || lead?.webLinkSentAt || lead?.sampleLinkSentAt || lead?.linkOpenedAt);
}
