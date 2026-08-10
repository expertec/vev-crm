import { admin, db as defaultDb } from '../../firebaseAdmin.js';
import { OUTCOME_TYPES, QUEUE_STATUSES, ROUTING_STATUSES } from './config.js';
import { calculateQueuePriority } from './priority.js';
import { withLeadDefaults } from './leadDefaults.js';
import { recordSalesActivity } from './activity.js';

const { FieldValue, Timestamp } = admin.firestore;

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

function timestampFromInput(value) {
  if (!value) return null;
  if (typeof value?.toDate === 'function') return value;
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return Timestamp.fromDate(date);
}

function serializeLead(docSnap) {
  if (!docSnap?.exists) return null;
  return withLeadDefaults({ id: docSnap.id, ...(docSnap.data() || {}) });
}

function sortByWorkPriority(a, b) {
  const ap = Number(a?.queue?.priority || 0);
  const bp = Number(b?.queue?.priority || 0);
  if (bp !== ap) return bp - ap;
  return toMillis(a?.queue?.enteredAt || a?.lastMessageAt) - toMillis(b?.queue?.enteredAt || b?.lastMessageAt);
}

function isClosedOrStopped(lead = {}) {
  const status = cleanText(lead.estado || '', 80).toLowerCase();
  return ['compro', 'cliente', 'ganado', 'won', 'no interesado', 'not_interested'].includes(status)
    || lead.stopSequences === true
    || lead.queue.status === QUEUE_STATUSES.CLOSED
    || lead.routing?.status === ROUTING_STATUSES.CLOSED;
}

function canClaimGeneralLead(lead = {}) {
  const safe = withLeadDefaults(lead);
  return safe.queue.status === QUEUE_STATUSES.WAITING
    && !safe.salesOwner
    && !safe.assignedTo
    && !isClosedOrStopped(safe);
}

async function loadCandidateDocs(db, constraints = []) {
  const query = constraints.reduce((ref, constraint) => constraint(ref), db.collection('leads'));
  const snap = await query.limit(60).get();
  return snap.docs.map(serializeLead).filter(Boolean);
}

export async function getAgentQueueStats({ db = defaultDb, agentUid = '' } = {}) {
  const waitingSnap = await db.collection('leads')
    .where('queue.status', '==', QUEUE_STATUSES.WAITING)
    .limit(200)
    .get();
  const waiting = waitingSnap.docs.map(serializeLead).filter((lead) => !lead.salesOwner && !lead.assignedTo);

  let mine = [];
  const safeAgent = cleanText(agentUid, 180);
  if (safeAgent) {
    const personalWaiting = await db.collection('leads')
      .where('salesOwner', '==', safeAgent)
      .limit(200)
      .get()
      .catch(() => ({ docs: [] }));
    const statuses = new Set([QUEUE_STATUSES.WAITING, QUEUE_STATUSES.FOLLOWUP, QUEUE_STATUSES.CLAIMED]);
    mine = personalWaiting.docs.map(serializeLead).filter((lead) => lead && statuses.has(lead.queue.status));
  }

  return {
    waiting: waiting.length,
    personal: mine.length,
  };
}

export async function claimNextLead({ db = defaultDb, agentUid = '', agentName = '' } = {}) {
  const safeAgent = cleanText(agentUid, 180);
  if (!safeAgent) {
    const error = new Error('Falta agentUid.');
    error.statusCode = 400;
    throw error;
  }

  const candidates = await loadCandidateDocs(db, [
    (ref) => ref.where('queue.status', '==', QUEUE_STATUSES.WAITING),
  ]);
  const sorted = candidates.filter(canClaimGeneralLead).sort(sortByWorkPriority).slice(0, 12);

  for (const lead of sorted) {
    const leadRef = db.collection('leads').doc(lead.id);
    const claimed = await db.runTransaction(async (tx) => {
      const snap = await tx.get(leadRef);
      if (!snap.exists) return null;
      const latest = serializeLead(snap);
      if (!canClaimGeneralLead(latest)) return null;

      const priority = calculateQueuePriority({ lead: latest }).priority || Number(latest.queue.priority || 0);
      tx.set(leadRef, {
        assignedTo: safeAgent,
        assignedToName: cleanText(agentName, 180) || safeAgent,
        assignedAt: FieldValue.serverTimestamp(),
        assignedBy: 'sales_queue',
        salesOwner: safeAgent,
        'queue.status': QUEUE_STATUSES.CLAIMED,
        'queue.priority': priority,
        'queue.claimedAt': FieldValue.serverTimestamp(),
        'queue.lastAgentId': safeAgent,
      }, { merge: true });

      return { ...latest, assignedTo: safeAgent, salesOwner: safeAgent, queue: { ...latest.queue, status: QUEUE_STATUSES.CLAIMED, priority } };
    });

    if (claimed) {
      await recordSalesActivity({
        db,
        leadRef,
        type: 'queue_claimed',
        agentId: safeAgent,
        metadata: { priority: claimed.queue.priority, reason: claimed.queue.reason || null },
      }).catch(() => {});
      return { claimed: true, lead: claimed };
    }
  }

  return { claimed: false, lead: null };
}

export async function getNextAgentWork({ db = defaultDb, agentUid = '', agentName = '' } = {}) {
  const safeAgent = cleanText(agentUid, 180);
  if (!safeAgent) {
    const error = new Error('Falta agentUid.');
    error.statusCode = 400;
    throw error;
  }

  const statuses = [QUEUE_STATUSES.WAITING, QUEUE_STATUSES.FOLLOWUP, QUEUE_STATUSES.CLAIMED];
  const statusSet = new Set(statuses);
  const personalSnap = await db.collection('leads')
    .where('salesOwner', '==', safeAgent)
    .limit(80)
    .get()
    .catch(() => ({ docs: [] }));
  const personal = personalSnap.docs
    .map(serializeLead)
    .filter((lead) => lead && statusSet.has(lead.queue.status) && !isClosedOrStopped(lead))
    .map((lead) => ({
      ...lead,
      queue: {
        ...lead.queue,
        priority: calculateQueuePriority({ lead }).priority || Number(lead.queue.priority || 0),
      },
    }))
    .sort(sortByWorkPriority);

  if (personal[0]) {
    const leadRef = db.collection('leads').doc(personal[0].id);
    await recordSalesActivity({
      db,
      leadRef,
      type: 'agent_opened',
      agentId: safeAgent,
      metadata: { source: 'personal_queue' },
    }).catch(() => {});
    return { source: 'personal', lead: personal[0] };
  }

  const claimed = await claimNextLead({ db, agentUid: safeAgent, agentName });
  if (claimed.claimed && claimed.lead) {
    await recordSalesActivity({
      db,
      leadRef: db.collection('leads').doc(claimed.lead.id),
      type: 'agent_opened',
      agentId: safeAgent,
      metadata: { source: 'general_claim' },
    }).catch(() => {});
    return { source: 'claimed', lead: claimed.lead };
  }

  return { source: 'empty', lead: null };
}

export async function registerAgentOutcome({
  db = defaultDb,
  leadId = '',
  agentUid = '',
  agentName = '',
  outcome = '',
  notes = '',
  followUpAt = null,
  followUpReason = '',
} = {}) {
  const safeLeadId = cleanText(leadId, 220);
  const safeAgent = cleanText(agentUid, 180);
  const safeOutcome = cleanText(outcome, 80);
  if (!safeLeadId) {
    const error = new Error('Falta leadId.');
    error.statusCode = 400;
    throw error;
  }
  if (!safeAgent) {
    const error = new Error('Falta agentUid.');
    error.statusCode = 400;
    throw error;
  }
  if (!OUTCOME_TYPES.includes(safeOutcome)) {
    const error = new Error('Resultado invalido.');
    error.statusCode = 400;
    throw error;
  }

  const leadRef = db.collection('leads').doc(safeLeadId);
  const followUpTimestamp = timestampFromInput(followUpAt);
  const result = await db.runTransaction(async (tx) => {
    const snap = await tx.get(leadRef);
    if (!snap.exists) {
      const error = new Error('Lead no encontrado.');
      error.statusCode = 404;
      throw error;
    }
    const lead = serializeLead(snap);
    const owner = cleanText(lead.salesOwner || lead.assignedTo || safeAgent, 180);
    if (owner && owner !== safeAgent) {
      const error = new Error('Este lead pertenece a otro vendedor.');
      error.statusCode = 403;
      throw error;
    }

    const patch = {
      assignedTo: safeAgent,
      assignedToName: cleanText(agentName, 180) || lead.assignedToName || safeAgent,
      salesOwner: safeAgent,
      lastSalesOutcome: {
        outcome: safeOutcome,
        notes: cleanText(notes, 1200),
        agentId: safeAgent,
        agentName: cleanText(agentName, 180) || null,
        createdAt: FieldValue.serverTimestamp(),
      },
      'queue.lastOutcome': safeOutcome,
      'queue.updatedAt': FieldValue.serverTimestamp(),
    };

    if (safeOutcome === 'followup') {
      if (!followUpTimestamp) {
        const error = new Error('Falta fecha de seguimiento.');
        error.statusCode = 400;
        throw error;
      }
      patch.followUp = {
        status: 'pending',
        nextAt: followUpTimestamp,
        reason: cleanText(followUpReason || notes, 500) || 'agent_followup',
      };
      patch['queue.status'] = QUEUE_STATUSES.FOLLOWUP;
      patch['queue.reason'] = 'agent_followup';
      patch['routing.status'] = ROUTING_STATUSES.FOLLOWUP;
    } else if (safeOutcome === 'sale') {
      patch.followUp = { status: 'completed', nextAt: null, reason: null };
      patch['queue.status'] = QUEUE_STATUSES.CLOSED;
      patch['queue.reason'] = 'sale';
      patch['routing.status'] = ROUTING_STATUSES.CLOSED;
      patch.estado = lead.estado || 'Compro';
    } else if (safeOutcome === 'not_interested') {
      patch.followUp = { status: 'completed', nextAt: null, reason: null };
      patch['queue.status'] = QUEUE_STATUSES.CLOSED;
      patch['queue.reason'] = 'not_interested';
      patch['routing.status'] = ROUTING_STATUSES.CLOSED;
      patch.estado = lead.estado || 'No interesado';
    } else if (safeOutcome === 'no_response') {
      patch['queue.status'] = QUEUE_STATUSES.AUTOMATION;
      patch['queue.reason'] = 'agent_no_response';
      patch['routing.status'] = ROUTING_STATUSES.AUTOMATION;
    } else {
      patch['queue.status'] = QUEUE_STATUSES.AUTOMATION;
      patch['queue.reason'] = 'agent_interested';
      patch['routing.status'] = ROUTING_STATUSES.AUTOMATION;
    }

    tx.set(leadRef, patch, { merge: true });
    const activityRef = leadRef.collection('salesActivity').doc();
    tx.set(activityRef, {
      type: 'agent_outcome',
      agentId: safeAgent,
      createdAt: FieldValue.serverTimestamp(),
      metadata: {
        outcome: safeOutcome,
        notes: cleanText(notes, 1200),
        followUpAt: followUpTimestamp || null,
        followUpReason: cleanText(followUpReason, 500) || null,
      },
    });
    if (safeOutcome === 'followup') {
      const followupRef = leadRef.collection('salesActivity').doc();
      tx.set(followupRef, {
        type: 'followup_created',
        agentId: safeAgent,
        createdAt: FieldValue.serverTimestamp(),
        metadata: {
          nextAt: followUpTimestamp,
          reason: cleanText(followUpReason || notes, 500) || 'agent_followup',
        },
      });
    }
    if (safeOutcome === 'sale') {
      const saleRef = leadRef.collection('salesActivity').doc();
      tx.set(saleRef, {
        type: 'sale',
        agentId: safeAgent,
        createdAt: FieldValue.serverTimestamp(),
        metadata: { notes: cleanText(notes, 1200) },
      });
    }

    return { lead, patch };
  });

  return {
    ok: true,
    leadId: safeLeadId,
    outcome: safeOutcome,
    previousLead: result.lead,
  };
}
