import { QUEUE_STATUSES, ROUTING_STATUSES } from './config.js';
import { withLeadDefaults } from './leadDefaults.js';

function clean(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim().toLowerCase();
}

function hasTag(lead = {}, tag = '') {
  const target = clean(tag);
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas.map(clean) : [];
  return tags.includes(target);
}

export function getTerminalLeadReason(lead = {}) {
  const safe = withLeadDefaults(lead);
  const status = clean(safe.estado || '');
  const routingStatus = clean(safe.routing?.status || safe.salesBrainCurrent?.routing?.status || '');
  const queueStatus = clean(safe.queue?.status || '');

  if (safe.stopSequences === true || hasTag(safe, 'stopsequences') || hasTag(safe, 'detenersecuencia')) return 'stop_requested';
  if (['compro', 'cliente', 'ganado', 'won', 'closed_won', 'cerrado_ganado', 'pagado'].includes(status) || hasTag(safe, 'compro')) return 'sold';
  if (['no interesado', 'no_interesado', 'not_interested', 'perdido', 'lost'].includes(status) || hasTag(safe, 'no_interesado')) return 'not_interested';
  if (queueStatus === QUEUE_STATUSES.CLOSED || routingStatus === ROUTING_STATUSES.CLOSED) return 'closed';
  if (safe.isArchived === true || safe.archived === true || safe.archivedAt) return 'archived';
  return '';
}

export function isTerminalLead(lead = {}) {
  return Boolean(getTerminalLeadReason(lead));
}

export function canClaimGeneralLead(lead = {}) {
  const safe = withLeadDefaults(lead);
  const routingStatus = clean(safe.routing?.status || safe.salesBrainCurrent?.routing?.status || '');
  const score = Number(safe.salesState?.leadScore || safe.leadScore || 0);
  const queueReady = safe.queue.status === QUEUE_STATUSES.WAITING;
  const routingReady = routingStatus === ROUTING_STATUSES.READY_FOR_AGENT;
  const scoreReady = score >= 70 && safe.queue.status === QUEUE_STATUSES.AUTOMATION;
  return (queueReady || routingReady || scoreReady)
    && !safe.salesOwner
    && !safe.assignedTo
    && !isTerminalLead(safe);
}

export function canShowPersonalWork(lead = {}, agentUid = '') {
  const safe = withLeadDefaults(lead);
  const owner = String(safe.salesOwner || safe.assignedTo || '').trim();
  const statuses = new Set([QUEUE_STATUSES.WAITING, QUEUE_STATUSES.FOLLOWUP, QUEUE_STATUSES.CLAIMED]);
  const routingStatus = clean(safe.routing?.status || safe.salesBrainCurrent?.routing?.status || '');
  const score = Number(safe.salesState?.leadScore || safe.leadScore || 0);
  const routingReady = routingStatus === ROUTING_STATUSES.READY_FOR_AGENT;
  const scoreReady = score >= 70 && safe.queue.status === QUEUE_STATUSES.AUTOMATION;
  return owner === String(agentUid || '').trim()
    && (statuses.has(safe.queue.status) || routingReady || scoreReady)
    && !isTerminalLead(safe);
}
