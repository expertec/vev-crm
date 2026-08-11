import { QUEUE_STATUSES } from './config.js';

function ownObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}

function numberOr(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export function normalizeQueue(queue = {}) {
  const safe = ownObject(queue);
  return {
    status: String(safe.status || QUEUE_STATUSES.AUTOMATION),
    priority: numberOr(safe.priority, 0),
    reason: safe.reason || null,
    reasonCode: safe.reasonCode || null,
    enteredAt: safe.enteredAt || null,
    claimedAt: safe.claimedAt || null,
    firstAgentActionAt: safe.firstAgentActionAt || null,
    outcomeAt: safe.outcomeAt || null,
  };
}

export function normalizeFollowUp(followUp = {}) {
  const safe = ownObject(followUp);
  return {
    status: safe.status || null,
    nextAt: safe.nextAt || null,
    reason: safe.reason || null,
  };
}

export function normalizeSalesContext(salesContext = {}) {
  const safe = ownObject(salesContext);
  return {
    businessType: safe.businessType || null,
    currentSituation: safe.currentSituation || null,
    primaryGoal: safe.primaryGoal || safe.primaryNeed || null,
    painPoint: safe.painPoint || null,
    hasWebsite: safe.hasWebsite ?? null,
    runsAds: safe.runsAds ?? safe.currentlyAdvertising ?? null,
    previousExperience: safe.previousExperience || null,
    customerAcquisition: safe.customerAcquisition || null,
    targetAudience: safe.targetAudience || null,
    productsServices: safe.productsServices || null,
    mainOffer: safe.mainOffer || null,
  };
}

export function withLeadDefaults(lead = {}) {
  return {
    ...lead,
    assignedTo: lead.assignedTo || null,
    assignedToName: lead.assignedToName || null,
    assignedAt: lead.assignedAt || null,
    assignedBy: lead.assignedBy || null,
    salesOwner: lead.salesOwner || lead.assignedTo || null,
    queue: normalizeQueue(lead.queue),
    followUp: normalizeFollowUp(lead.followUp),
    salesContext: normalizeSalesContext(lead.salesContext),
  };
}
