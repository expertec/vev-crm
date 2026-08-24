import { QUEUE_PRIORITY_WEIGHTS } from './config.js';
import { withLeadDefaults } from './leadDefaults.js';

function toMillis(value) {
  if (!value) return 0;
  if (value instanceof Date) return value.getTime() || 0;
  if (typeof value?.toMillis === 'function') return value.toMillis() || 0;
  if (typeof value?.toDate === 'function') return value.toDate()?.getTime?.() || 0;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function clampPriority(value) {
  return Math.max(0, Math.min(100, Math.round(Number(value || 0))));
}

function hasSignal(analysis = {}, key = '') {
  return Array.isArray(analysis?.signals) && analysis.signals.includes(key);
}

export function buildCommercialSignals({ lead = {}, analysis = {}, latestText = '' } = {}) {
  const text = String(latestText || analysis?.summary || '').toLowerCase();
  const state = lead?.salesState || {};
  const interest = String(analysis?.interestLevel || state?.interestLevel || '').toLowerCase();
  const intent = String(analysis?.intent || state?.intent || '').toLowerCase();
  const objection = String(analysis?.objection || state?.objection || '').toLowerCase();

  const payment = /pago|pagar|transferencia|deposito|dep[oó]sito|anticipo|tarjeta|link de pago|datos de pago/.test(text)
    || hasSignal(analysis, 'asked_payment_method')
    || hasSignal(analysis, 'ready_for_deposit')
    || intent === 'ready_for_deposit';
  const price = intent === 'wants_price' || hasSignal(analysis, 'asked_price') || /precio|costo|cu[aá]nto|cotiza|presupuesto/.test(text);
  const ready = intent === 'ready_to_buy' || intent === 'asks_how_to_start' || intent === 'ready_for_deposit' || hasSignal(analysis, 'ready_to_buy') || hasSignal(analysis, 'asks_how_to_start') || hasSignal(analysis, 'ready_for_deposit');
  const webSales = lead?.webSales || {};
  const sampleOpenCount = Number(webSales?.sample?.openCount || 0);
  const sampleEngagement = Boolean(
    webSales?.sample?.feedbackReceivedAt
    || hasSignal(analysis, 'sample_feedback')
    || hasSignal(analysis, 'sample_change_request')
    || hasSignal(analysis, 'likes_sample')
    || intent === 'sample_feedback'
    || intent === 'sample_change_request'
    || intent === 'likes_sample'
  );
  const sampleEvaluation = Boolean(
    webSales?.sample?.firstOpenedAt
    || hasSignal(analysis, 'asks_domain')
    || hasSignal(analysis, 'asks_delivery_time')
    || hasSignal(analysis, 'asks_editing')
    || intent === 'asks_domain'
    || intent === 'asks_delivery_time'
    || intent === 'asks_editing'
  );

  return {
    buyingIntent: ready ? 100 : (interest === 'hot' ? 78 : interest === 'warm' ? 46 : 12),
    urgency: /hoy|ahora|urgente|ya|esta semana|cu[aá]ndo empezamos/.test(text) ? 85 : (ready ? 60 : 20),
    priceIntent: price ? 85 : 0,
    trustNeed: objection === 'trust' || objection === 'bad_previous_experience' || hasSignal(analysis, 'trust_objection') ? 80 : 0,
    objectionLevel: objection && objection !== 'none' ? (objection === 'price' ? 60 : 45) : 0,
    engagement: hasSignal(analysis, 'answered') ? (interest === 'hot' ? 90 : interest === 'warm' ? 70 : 45) : 0,
    paymentIntent: payment ? 100 : 0,
    commercialQuestion: intent === 'question' || intent === 'wants_information' || hasSignal(analysis, 'commercial_question') ? 65 : 0,
    sampleEngagement: sampleEngagement ? 95 : (sampleOpenCount >= 3 ? 82 : 0),
    sampleEvaluation: sampleEvaluation ? 72 : 0,
    sampleOpenCount,
  };
}

export function calculateQueuePriority({
  lead = {},
  analysis = {},
  commercialSignals = null,
  now = new Date(),
  latestText = '',
  weights = QUEUE_PRIORITY_WEIGHTS,
} = {}) {
  const safeLead = withLeadDefaults(lead);
  const signals = commercialSignals || buildCommercialSignals({ lead: safeLead, analysis, latestText });
  const nowMs = toMillis(now) || Date.now();
  const salesScore = Number(safeLead?.salesState?.leadScore || safeLead?.leadScore || 0);
  const enteredMs = toMillis(safeLead.queue.enteredAt || safeLead.lastMessageAt || safeLead.fecha_creacion);
  const lastMessageMs = toMillis(safeLead.lastMessageAt || safeLead.fecha_creacion);
  const followUpMs = toMillis(safeLead.followUp.nextAt);

  const waitingHours = enteredMs ? Math.max(0, (nowMs - enteredMs) / 3_600_000) : 0;
  const waitingTime = Math.min(
    weights.waitingMax,
    (waitingHours > 0 ? weights.waitingFirstHour : 0) + Math.max(0, waitingHours - 1) * weights.waitingAdditionalHour
  );
  const inactivityDays = lastMessageMs ? Math.max(0, (nowMs - lastMessageMs) / 86_400_000) : 0;
  const overdueFollowUp = followUpMs && followUpMs <= nowMs ? weights.overdueFollowUp : 0;
  const unansweredMessage = Number(safeLead.unreadCount || 0) > 0 ? weights.unansweredMessage : 0;

  const raw =
    salesScore * weights.salesScoreMultiplier
    + unansweredMessage
    + waitingTime
    + overdueFollowUp
    + (Number(signals.buyingIntent || 0) / 100) * weights.buyingIntent
    + (Number(signals.paymentIntent || 0) / 100) * weights.paymentQuestion
    + (Number(signals.priceIntent || 0) / 100) * weights.priceQuestion
    + (Number(signals.sampleEngagement || 0) / 100) * 18
    + (Number(signals.sampleEvaluation || 0) / 100) * 10
    + (Number(signals.commercialQuestion || 0) / 100) * weights.commercialQuestion
    + (Number(signals.urgency || 0) / 100) * weights.urgency
    + (Number(signals.engagement || 0) / 100) * weights.engagement
    - (Number(signals.objectionLevel || 0) / 100) * weights.objectionPenalty
    - Math.min(weights.inactivityPenaltyMax, inactivityDays * weights.inactivityPenaltyPerDay)
    - (safeLead.queue.status === 'claimed' ? weights.claimedPenalty : 0);

  return {
    priority: clampPriority(raw),
    raw,
    factors: {
      salesScore,
      unansweredMessage,
      waitingTime,
      overdueFollowUp,
      buyingIntent: signals.buyingIntent,
      paymentIntent: signals.paymentIntent,
      priceIntent: signals.priceIntent,
      sampleEngagement: signals.sampleEngagement,
      sampleEvaluation: signals.sampleEvaluation,
      sampleOpenCount: signals.sampleOpenCount,
      commercialQuestion: signals.commercialQuestion,
      urgency: signals.urgency,
      engagement: signals.engagement,
      inactivityDays: Math.round(inactivityDays * 10) / 10,
    },
    weights,
  };
}
