import { ROUTING_REASONS, ROUTING_STATUSES, QUEUE_STATUSES } from './config.js';
import { buildCommercialSignals, calculateQueuePriority } from './priority.js';
import { withLeadDefaults } from './leadDefaults.js';
import { getTerminalLeadReason } from './eligibility.js';
import { humanQueueReason } from './reasons.js';

function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function toMillis(value) {
  if (!value) return 0;
  if (value instanceof Date) return value.getTime() || 0;
  if (typeof value?.toMillis === 'function') return value.toMillis() || 0;
  if (typeof value?.toDate === 'function') return value.toDate()?.getTime?.() || 0;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function hasSignal(analysis = {}, signal = '') {
  return Array.isArray(analysis?.signals) && analysis.signals.includes(signal);
}

function isClosedLead(lead = {}, analysis = {}) {
  if (getTerminalLeadReason(lead)) return true;
  if (analysis?.intent === 'no_interest' || hasSignal(analysis, 'stop_requested')) return true;
  return false;
}

export function decideRouting({ lead = {}, analysis = {}, latestText = '', commercialSignals = null, now = new Date() } = {}) {
  const safeLead = withLeadDefaults(lead);
  const signals = commercialSignals || buildCommercialSignals({ lead: safeLead, analysis, latestText });
  const interest = cleanText(analysis?.interestLevel || safeLead?.salesState?.interestLevel || '', 40).toLowerCase();
  const intent = cleanText(analysis?.intent || safeLead?.salesState?.intent || '', 80).toLowerCase();
  const followUpMs = toMillis(safeLead.followUp.nextAt);
  const nowMs = toMillis(now) || Date.now();

  if (isClosedLead(safeLead, analysis)) {
    return { status: ROUTING_STATUSES.CLOSED, reason: ROUTING_REASONS.CLOSED, humanRequired: false };
  }
  if (followUpMs && followUpMs <= nowMs) {
    return { status: ROUTING_STATUSES.FOLLOWUP, reason: ROUTING_REASONS.FOLLOWUP_OVERDUE, humanRequired: true };
  }
  if (intent === 'ready_to_buy' || intent === 'asks_how_to_start' || hasSignal(analysis, 'ready_to_buy')) {
    return { status: ROUTING_STATUSES.READY_FOR_AGENT, reason: ROUTING_REASONS.READY_TO_BUY, humanRequired: true };
  }
  if (Number(signals.paymentIntent || 0) >= 70) {
    return { status: ROUTING_STATUSES.READY_FOR_AGENT, reason: ROUTING_REASONS.ASKED_PAYMENT_METHOD, humanRequired: true };
  }
  if ((intent === 'wants_price' || Number(signals.priceIntent || 0) >= 70) && ['hot', 'warm'].includes(interest)) {
    return { status: ROUTING_STATUSES.READY_FOR_AGENT, reason: ROUTING_REASONS.ASKED_PRICE, humanRequired: true };
  }
  if (Number(signals.commercialQuestion || 0) >= 60 && ['hot', 'warm'].includes(interest)) {
    return { status: ROUTING_STATUSES.READY_FOR_AGENT, reason: ROUTING_REASONS.COMMERCIAL_QUESTION, humanRequired: true };
  }
  if (interest === 'hot' || analysis?.hot === true) {
    return { status: ROUTING_STATUSES.READY_FOR_AGENT, reason: ROUTING_REASONS.HIGH_INTEREST, humanRequired: true };
  }
  if (interest === 'lost') {
    return { status: ROUTING_STATUSES.DORMANT, reason: ROUTING_REASONS.DORMANT, humanRequired: false };
  }
  return {
    status: ROUTING_STATUSES.AUTOMATION,
    reason: interest === 'cold' ? ROUTING_REASONS.LOW_INTEREST : ROUTING_REASONS.NO_SIGNAL,
    humanRequired: false,
  };
}

export async function updateRoutingAfterInbound({
  db = null,
  leadRef = null,
  leadId = '',
  leadData = {},
  analysis = {},
  latestText = '',
  salesBrain = null,
} = {}) {
  const { admin, db: defaultDb } = await import('../../firebaseAdmin.js');
  const { recordSalesActivity } = await import('./activity.js');
  const { FieldValue } = admin.firestore;
  const targetDb = db || defaultDb;
  const ref = leadRef || targetDb.collection('leads').doc(String(leadId || ''));
  const snap = await ref.get();
  if (!snap.exists) return { ok: false, reason: 'missing_lead' };

  const currentLead = withLeadDefaults({ id: snap.id, ...(snap.data() || {}), ...leadData });
  const effectiveAnalysis = analysis && Object.keys(analysis).length ? analysis : (salesBrain?.analysis || {});
  const commercialSignals = buildCommercialSignals({ lead: currentLead, analysis: effectiveAnalysis, latestText });
  const routing = decideRouting({ lead: currentLead, analysis: effectiveAnalysis, latestText, commercialSignals });
  const priorityResult = calculateQueuePriority({
    lead: currentLead,
    analysis: effectiveAnalysis,
    commercialSignals,
    latestText,
  });

  const previousQueueStatus = currentLead.queue.status;
  const previousRoutingStatus = currentLead.salesBrainCurrent?.routing?.status || currentLead.routing?.status || null;
  const requiresAgent = routing.status === ROUTING_STATUSES.READY_FOR_AGENT || routing.status === ROUTING_STATUSES.FOLLOWUP;
  const ownerId = cleanText(currentLead.salesOwner || currentLead.assignedTo || '', 180) || null;
  const queueStatus = routing.status === ROUTING_STATUSES.FOLLOWUP
    ? QUEUE_STATUSES.FOLLOWUP
    : requiresAgent
      ? QUEUE_STATUSES.WAITING
      : routing.status === ROUTING_STATUSES.DORMANT
        ? QUEUE_STATUSES.DORMANT
        : routing.status === ROUTING_STATUSES.CLOSED
          ? QUEUE_STATUSES.CLOSED
          : QUEUE_STATUSES.AUTOMATION;

  const humanReason = humanQueueReason({
    reasonCode: routing.reason,
    lead: currentLead,
    analysis: effectiveAnalysis,
  });

  const patch = {
    salesOwner: ownerId,
    commercialSignals,
    routing,
    'salesBrainCurrent.routing': {
      status: routing.status,
      priority: priorityResult.priority,
      reason: routing.reason,
      updatedAt: FieldValue.serverTimestamp(),
    },
    'queue.status': queueStatus,
    'queue.priority': priorityResult.priority,
    'queue.reasonCode': routing.reason,
    'queue.reason': humanReason,
    'queue.priorityFactors': priorityResult.factors,
    'queue.updatedAt': FieldValue.serverTimestamp(),
  };

  if (requiresAgent) {
    patch['queue.enteredAt'] = previousQueueStatus === QUEUE_STATUSES.WAITING && currentLead.queue.enteredAt
      ? currentLead.queue.enteredAt
      : FieldValue.serverTimestamp();
    patch['aiFollowup.paused'] = true;
  }

  await ref.set(patch, { merge: true });

  if (previousRoutingStatus !== routing.status) {
    await recordSalesActivity({
      db: targetDb,
      leadRef: ref,
      type: 'routing_changed',
      metadata: {
        previousStatus: previousRoutingStatus,
        nextStatus: routing.status,
        reason: routing.reason,
        priority: priorityResult.priority,
      },
    }).catch(() => {});
  }
  if (requiresAgent && previousQueueStatus !== queueStatus) {
    await recordSalesActivity({
      db: targetDb,
      leadRef: ref,
      type: 'queue_entered',
      agentId: ownerId || '',
      metadata: {
        queueStatus,
        reason: routing.reason,
        priority: priorityResult.priority,
        ownerId,
      },
    }).catch(() => {});
  }

  return {
    ok: true,
    routing,
    queue: {
      status: queueStatus,
      priority: priorityResult.priority,
      reasonCode: routing.reason,
      reason: humanReason,
      ownerId,
    },
    commercialSignals,
  };
}
