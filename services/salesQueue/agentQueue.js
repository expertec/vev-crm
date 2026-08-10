import { OUTCOME_TYPES, QUEUE_STATUSES, ROUTING_STATUSES } from './config.js';
import { calculateQueuePriority } from './priority.js';
import { withLeadDefaults } from './leadDefaults.js';
import {
  canClaimGeneralLead,
  canShowPersonalWork,
  isTerminalLead,
} from './eligibility.js';
import { humanQueueReason } from './reasons.js';

let firebaseDeps = null;

async function resolveDeps({ db = null, firestore = null, recordActivity = null } = {}) {
  if (db && firestore) {
    return {
      db,
      FieldValue: firestore.FieldValue,
      Timestamp: firestore.Timestamp,
      recordActivity: recordActivity || (async () => null),
    };
  }
  if (!firebaseDeps) {
    const firebase = await import('../../firebaseAdmin.js');
    const activity = await import('./activity.js');
    firebaseDeps = {
      db: firebase.db,
      FieldValue: firebase.admin.firestore.FieldValue,
      Timestamp: firebase.admin.firestore.Timestamp,
      recordActivity: activity.recordSalesActivity,
    };
  }
  return {
    db: db || firebaseDeps.db,
    FieldValue: firestore?.FieldValue || firebaseDeps.FieldValue,
    Timestamp: firestore?.Timestamp || firebaseDeps.Timestamp,
    recordActivity: recordActivity || firebaseDeps.recordActivity,
  };
}

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

function timestampFromInput(value, Timestamp) {
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

async function loadCandidateDocs(db, constraints = []) {
  const query = constraints.reduce((ref, constraint) => constraint(ref), db.collection('leads'));
  const snap = await query.limit(60).get();
  return snap.docs.map(serializeLead).filter(Boolean);
}

function dedupeLeads(rows = []) {
  const map = new Map();
  rows.filter(Boolean).forEach((lead) => map.set(lead.id, lead));
  return Array.from(map.values());
}

async function loadGeneralOpportunityCandidates(db) {
  const batches = await Promise.all([
    loadCandidateDocs(db, [(ref) => ref.where('queue.status', '==', QUEUE_STATUSES.WAITING)]),
    loadCandidateDocs(db, [(ref) => ref.where('routing.status', '==', ROUTING_STATUSES.READY_FOR_AGENT)]).catch(() => []),
    loadCandidateDocs(db, [(ref) => ref.where('salesBrainCurrent.routing.status', '==', ROUTING_STATUSES.READY_FOR_AGENT)]).catch(() => []),
    loadCandidateDocs(db, [(ref) => ref.where('salesState.leadScore', '>=', 70)]).catch(() => []),
  ]);
  return dedupeLeads(batches.flat()).filter(canClaimGeneralLead);
}

export async function getAgentQueueStats({ db = null, firestore = null, agentUid = '' } = {}) {
  const deps = await resolveDeps({ db, firestore });
  db = deps.db;
  const waiting = await loadGeneralOpportunityCandidates(db);

  let mine = [];
  const safeAgent = cleanText(agentUid, 180);
  if (safeAgent) {
    const personalWaiting = await db.collection('leads')
      .where('salesOwner', '==', safeAgent)
      .limit(200)
      .get()
      .catch(() => ({ docs: [] }));
    mine = personalWaiting.docs.map(serializeLead).filter((lead) => lead && canShowPersonalWork(lead, safeAgent));
  }

  return {
    waiting: waiting.length,
    personal: mine.length,
  };
}

export async function claimNextLead({ db = null, firestore = null, recordActivity = null, agentUid = '', agentName = '' } = {}) {
  const deps = await resolveDeps({ db, firestore, recordActivity });
  db = deps.db;
  const { FieldValue } = deps;
  const safeAgent = cleanText(agentUid, 180);
  if (!safeAgent) {
    const error = new Error('Falta agentUid.');
    error.statusCode = 400;
    throw error;
  }

  const candidates = await loadGeneralOpportunityCandidates(db);
  const sorted = candidates.sort(sortByWorkPriority).slice(0, 12);

  for (const lead of sorted) {
    const leadRef = db.collection('leads').doc(lead.id);
    const claimed = await db.runTransaction(async (tx) => {
      const snap = await tx.get(leadRef);
      if (!snap.exists) return null;
      const latest = serializeLead(snap);
      if (!canClaimGeneralLead(latest)) return null;

      const priority = calculateQueuePriority({ lead: latest }).priority || Number(latest.queue.priority || 0);
      const reasonCode = latest.queue.reasonCode || latest.queue.reason || 'manual_claim';
      const reason = humanQueueReason({ reasonCode, lead: latest });
      tx.set(leadRef, {
        assignedTo: safeAgent,
        assignedToName: cleanText(agentName, 180) || safeAgent,
        assignedAt: FieldValue.serverTimestamp(),
        assignedBy: 'sales_queue',
        salesOwner: safeAgent,
        'queue.status': QUEUE_STATUSES.CLAIMED,
        'queue.priority': priority,
        'queue.reasonCode': reasonCode,
        'queue.reason': reason,
        'queue.claimedAt': FieldValue.serverTimestamp(),
        'queue.firstAgentActionAt': FieldValue.serverTimestamp(),
        'queue.lastAgentId': safeAgent,
      }, { merge: true });

      return { ...latest, assignedTo: safeAgent, salesOwner: safeAgent, queue: { ...latest.queue, status: QUEUE_STATUSES.CLAIMED, priority, reasonCode, reason } };
    });

    if (claimed) {
      await deps.recordActivity({
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

export async function getNextAgentWork({ db = null, firestore = null, recordActivity = null, agentUid = '', agentName = '' } = {}) {
  const deps = await resolveDeps({ db, firestore, recordActivity });
  db = deps.db;
  const { FieldValue } = deps;
  const safeAgent = cleanText(agentUid, 180);
  if (!safeAgent) {
    const error = new Error('Falta agentUid.');
    error.statusCode = 400;
    throw error;
  }

  const personalSnap = await db.collection('leads')
    .where('salesOwner', '==', safeAgent)
    .limit(80)
    .get()
    .catch(() => ({ docs: [] }));
  const personal = personalSnap.docs
    .map(serializeLead)
    .filter((lead) => lead && canShowPersonalWork(lead, safeAgent))
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
    if (!personal[0].queue?.firstAgentActionAt) {
      await leadRef.set({
        'queue.firstAgentActionAt': FieldValue.serverTimestamp(),
      }, { merge: true }).catch(() => {});
    }
    await deps.recordActivity({
      db,
      leadRef,
      type: 'agent_opened',
      agentId: safeAgent,
      metadata: { source: 'personal_queue' },
    }).catch(() => {});
    return { source: 'personal', lead: personal[0] };
  }

  const claimed = await claimNextLead({ db, firestore, recordActivity: deps.recordActivity, agentUid: safeAgent, agentName });
  if (claimed.claimed && claimed.lead) {
    await deps.recordActivity({
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
  db = null,
  firestore = null,
  recordActivity = null,
  leadId = '',
  agentUid = '',
  agentName = '',
  outcome = '',
  notes = '',
  followUpAt = null,
  followUpReason = '',
} = {}) {
  const deps = await resolveDeps({ db, firestore, recordActivity });
  db = deps.db;
  const { FieldValue, Timestamp } = deps;
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
  const followUpTimestamp = timestampFromInput(followUpAt, Timestamp);
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
      'queue.outcomeAt': FieldValue.serverTimestamp(),
      'queue.updatedAt': FieldValue.serverTimestamp(),
    };
    if (!lead.queue?.firstAgentActionAt) {
      patch['queue.firstAgentActionAt'] = FieldValue.serverTimestamp();
    }

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
      patch['queue.reasonCode'] = 'agent_followup';
      patch['queue.reason'] = 'Seguimiento programado por el vendedor';
      patch['routing.status'] = ROUTING_STATUSES.FOLLOWUP;
    } else if (safeOutcome === 'sale') {
      patch.followUp = { status: 'completed', nextAt: null, reason: null };
      patch['queue.status'] = QUEUE_STATUSES.CLOSED;
      patch['queue.reasonCode'] = 'sale';
      patch['queue.reason'] = 'Venta registrada';
      patch['routing.status'] = ROUTING_STATUSES.CLOSED;
      patch.estado = 'compro';
      patch.wonAt = FieldValue.serverTimestamp();
      patch.hasActiveSequences = false;
      patch.stopSequences = true;
      patch.secuenciasActivas = [];
      patch.nextSequenceRunAt = FieldValue.delete();
      patch.etiquetas = FieldValue.arrayUnion('Compro');
    } else if (safeOutcome === 'not_interested') {
      patch.followUp = { status: 'completed', nextAt: null, reason: null };
      patch['queue.status'] = QUEUE_STATUSES.CLOSED;
      patch['queue.reasonCode'] = 'not_interested';
      patch['queue.reason'] = 'Marcado como no interesado';
      patch['routing.status'] = ROUTING_STATUSES.CLOSED;
      patch.estado = 'No interesado';
      patch.lostAt = FieldValue.serverTimestamp();
      patch.hasActiveSequences = false;
      patch.stopSequences = true;
      patch.secuenciasActivas = [];
      patch.nextSequenceRunAt = FieldValue.delete();
      patch.etiquetas = FieldValue.arrayUnion('No interesado');
    } else if (safeOutcome === 'no_response') {
      patch['queue.status'] = QUEUE_STATUSES.AUTOMATION;
      patch['queue.reasonCode'] = 'agent_no_response';
      patch['queue.reason'] = 'Sin respuesta: vuelve a automatizacion y reactivacion';
      patch['routing.status'] = ROUTING_STATUSES.AUTOMATION;
    } else {
      patch['queue.status'] = QUEUE_STATUSES.AUTOMATION;
      patch['queue.reasonCode'] = 'agent_interested';
      patch['queue.reason'] = 'Atendido: mantiene propietario para futuras respuestas';
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
