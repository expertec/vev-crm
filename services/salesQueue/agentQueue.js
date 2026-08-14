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

function cleanEmail(value = '') {
  const email = cleanText(value, 180).toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : '';
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

function serializeAgent(docSnap) {
  if (!docSnap?.exists) return null;
  const data = docSnap.data() || {};
  const permissions = data.permissions && typeof data.permissions === 'object' ? data.permissions : {};
  return {
    uid: cleanText(data.uid || docSnap.id, 180),
    id: docSnap.id,
    name: cleanText(data.name || data.displayName || data.email || docSnap.id, 180),
    email: cleanEmail(data.email || ''),
    phone: cleanText(data.phone || '', 40),
    role: cleanText(data.role || 'sales_agent', 80),
    active: data.active !== false,
    permissions: {
      canViewAllLeads: permissions.canViewAllLeads === true,
      canAssignLeads: permissions.canAssignLeads === true,
      canUseSharedWhatsapp: permissions.canUseSharedWhatsapp !== false,
    },
    createdAt: data.createdAt || null,
    updatedAt: data.updatedAt || null,
  };
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
    loadCandidateDocs(db, [(ref) => ref.where('salesState.qualification.readyForSales', '==', true)]).catch(() => []),
    loadCandidateDocs(db, [(ref) => ref.where('salesState.readyForSales', '==', true)]).catch(() => []),
  ]);
  return dedupeLeads(batches.flat()).filter(canClaimGeneralLead);
}

async function loadPersonalWorkCandidates(db, agentUid = '', max = 200) {
  const safeAgent = cleanText(agentUid, 180);
  if (!safeAgent) return [];
  const batches = await Promise.all([
    db.collection('leads')
      .where('salesOwner', '==', safeAgent)
      .limit(max)
      .get()
      .catch(() => ({ docs: [] })),
    db.collection('leads')
      .where('assignedTo', '==', safeAgent)
      .limit(max)
      .get()
      .catch(() => ({ docs: [] })),
  ]);
  return dedupeLeads(
    batches.flatMap((snap) => snap.docs.map(serializeLead).filter(Boolean))
  ).filter((lead) => canShowPersonalWork(lead, safeAgent));
}

export async function getAgentQueueStats({ db = null, firestore = null, agentUid = '', includeGeneral = true } = {}) {
  const deps = await resolveDeps({ db, firestore });
  db = deps.db;
  const waiting = includeGeneral === false ? [] : await loadGeneralOpportunityCandidates(db);

  const mine = await loadPersonalWorkCandidates(db, agentUid, 200);

  return {
    waiting: waiting.length,
    personal: mine.length,
  };
}

export async function listSalesAgents({ db = null, firestore = null, includeInactive = false } = {}) {
  const deps = await resolveDeps({ db, firestore });
  db = deps.db;

  const snap = await db.collection('salesAgents')
    .limit(300)
    .get()
    .catch(() => ({ docs: [] }));

  return snap.docs
    .map(serializeAgent)
    .filter(Boolean)
    .filter((agent) => includeInactive || agent.active)
    .sort((a, b) => {
      if (a.active !== b.active) return a.active ? -1 : 1;
      return String(a.name || a.email || a.uid).localeCompare(String(b.name || b.email || b.uid));
    });
}

export async function upsertSalesAgent({
  db = null,
  firestore = null,
  agentUid = '',
  name = '',
  email = '',
  phone = '',
  role = 'sales_agent',
  active = true,
  permissions = {},
  updatedBy = '',
} = {}) {
  const deps = await resolveDeps({ db, firestore });
  db = deps.db;
  const { FieldValue } = deps;
  const safeUid = cleanText(agentUid, 180);
  const safeName = cleanText(name, 180);
  const safeEmail = cleanEmail(email);
  if (!safeUid) {
    const error = new Error('Falta agentUid.');
    error.statusCode = 400;
    throw error;
  }
  if (!safeName && !safeEmail) {
    const error = new Error('Falta nombre o email del agente.');
    error.statusCode = 400;
    throw error;
  }

  const agentRef = db.collection('salesAgents').doc(safeUid);
  const snap = await agentRef.get();
  const patch = {
    uid: safeUid,
    name: safeName || safeEmail || safeUid,
    email: safeEmail,
    phone: cleanText(phone, 40),
    role: cleanText(role, 80) || 'sales_agent',
    active: active !== false,
    permissions: {
      canViewAllLeads: permissions?.canViewAllLeads === true,
      canAssignLeads: permissions?.canAssignLeads === true,
      canUseSharedWhatsapp: permissions?.canUseSharedWhatsapp !== false,
    },
    updatedAt: FieldValue.serverTimestamp(),
    updatedBy: cleanText(updatedBy, 180),
  };
  if (!snap.exists) {
    patch.createdAt = FieldValue.serverTimestamp();
    patch.createdBy = cleanText(updatedBy, 180);
  }
  await agentRef.set(patch, { merge: true });
  const next = await agentRef.get();
  return serializeAgent(next);
}

async function loadActiveSalesAgent(db, agentUid = '') {
  const safeUid = cleanText(agentUid, 180);
  if (!safeUid) return null;
  const snap = await db.collection('salesAgents').doc(safeUid).get().catch(() => null);
  const agent = serializeAgent(snap);
  return agent?.active ? agent : null;
}

export async function assignLeadToAgent({
  db = null,
  firestore = null,
  recordActivity = null,
  leadId = '',
  agentUid = '',
  assignedBy = '',
  assignedByName = '',
  reason = 'manual_assignment',
} = {}) {
  const deps = await resolveDeps({ db, firestore, recordActivity });
  db = deps.db;
  const { FieldValue } = deps;
  const safeLeadId = cleanText(leadId, 220);
  const safeAgent = cleanText(agentUid, 180);
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

  const agent = await loadActiveSalesAgent(db, safeAgent);
  if (!agent) {
    const error = new Error('Agente de venta no encontrado o inactivo.');
    error.statusCode = 404;
    throw error;
  }

  const leadRef = db.collection('leads').doc(safeLeadId);
  const assigned = await db.runTransaction(async (tx) => {
    const snap = await tx.get(leadRef);
    if (!snap.exists) {
      const error = new Error('Lead no encontrado.');
      error.statusCode = 404;
      throw error;
    }
    const lead = serializeLead(snap);
    const priority = calculateQueuePriority({ lead }).priority || Number(lead.queue.priority || 0);
    const cleanReason = cleanText(reason, 240) || 'manual_assignment';
    tx.set(leadRef, {
      assignedTo: agent.uid,
      assignedToName: agent.name,
      assignedToEmail: agent.email || '',
      assignedAt: FieldValue.serverTimestamp(),
      assignedBy: cleanText(assignedBy, 180) || 'manual',
      assignedByName: cleanText(assignedByName, 180),
      salesOwner: agent.uid,
      salesOwnerName: agent.name,
      'queue.status': QUEUE_STATUSES.CLAIMED,
      'queue.priority': priority,
      'queue.reasonCode': cleanReason,
      'queue.reason': cleanReason === 'manual_assignment'
        ? `Asignado manualmente a ${agent.name}`
        : cleanReason,
      'queue.claimedAt': FieldValue.serverTimestamp(),
      'queue.lastAgentId': agent.uid,
      'queue.updatedAt': FieldValue.serverTimestamp(),
      'routing.status': ROUTING_STATUSES.READY_FOR_AGENT,
    }, { merge: true });
    return { ...lead, assignedTo: agent.uid, assignedToName: agent.name, salesOwner: agent.uid };
  });

  await deps.recordActivity({
    db,
    leadRef,
    type: 'lead_assigned',
    agentId: agent.uid,
    metadata: {
      agentName: agent.name,
      assignedBy: cleanText(assignedBy, 180) || null,
      reason: cleanText(reason, 240) || 'manual_assignment',
    },
  }).catch(() => {});

  return { ok: true, lead: assigned, agent };
}

export async function unassignLead({
  db = null,
  firestore = null,
  recordActivity = null,
  leadId = '',
  assignedBy = '',
  reason = 'manual_unassignment',
} = {}) {
  const deps = await resolveDeps({ db, firestore, recordActivity });
  db = deps.db;
  const { FieldValue } = deps;
  const safeLeadId = cleanText(leadId, 220);
  if (!safeLeadId) {
    const error = new Error('Falta leadId.');
    error.statusCode = 400;
    throw error;
  }

  const leadRef = db.collection('leads').doc(safeLeadId);
  const previous = await db.runTransaction(async (tx) => {
    const snap = await tx.get(leadRef);
    if (!snap.exists) {
      const error = new Error('Lead no encontrado.');
      error.statusCode = 404;
      throw error;
    }
    const lead = serializeLead(snap);
    tx.set(leadRef, {
      assignedTo: FieldValue.delete(),
      assignedToName: FieldValue.delete(),
      assignedToEmail: FieldValue.delete(),
      assignedAt: FieldValue.delete(),
      assignedBy: FieldValue.delete(),
      assignedByName: FieldValue.delete(),
      salesOwner: FieldValue.delete(),
      salesOwnerName: FieldValue.delete(),
      'queue.status': QUEUE_STATUSES.WAITING,
      'queue.reasonCode': cleanText(reason, 240) || 'manual_unassignment',
      'queue.reason': 'Asignacion removida manualmente',
      'queue.claimedAt': FieldValue.delete(),
      'queue.lastAgentId': FieldValue.delete(),
      'queue.updatedAt': FieldValue.serverTimestamp(),
    }, { merge: true });
    return lead;
  });

  await deps.recordActivity({
    db,
    leadRef,
    type: 'lead_unassigned',
    agentId: cleanText(previous?.salesOwner || previous?.assignedTo || '', 180) || null,
    metadata: {
      assignedBy: cleanText(assignedBy, 180) || null,
      reason: cleanText(reason, 240) || 'manual_unassignment',
    },
  }).catch(() => {});

  return { ok: true, previousLead: previous };
}

export async function claimNextLead({ db = null, firestore = null, recordActivity = null, agentUid = '', agentName = '', allowGeneralClaim = true } = {}) {
  const deps = await resolveDeps({ db, firestore, recordActivity });
  db = deps.db;
  const { FieldValue } = deps;
  const safeAgent = cleanText(agentUid, 180);
  if (!safeAgent) {
    const error = new Error('Falta agentUid.');
    error.statusCode = 400;
    throw error;
  }
  if (allowGeneralClaim === false) {
    return { claimed: false, lead: null };
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

export async function getNextAgentWork({ db = null, firestore = null, recordActivity = null, agentUid = '', agentName = '', allowGeneralClaim = true } = {}) {
  const deps = await resolveDeps({ db, firestore, recordActivity });
  db = deps.db;
  const { FieldValue } = deps;
  const safeAgent = cleanText(agentUid, 180);
  if (!safeAgent) {
    const error = new Error('Falta agentUid.');
    error.statusCode = 400;
    throw error;
  }

  const personal = (await loadPersonalWorkCandidates(db, safeAgent, 80))
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

  const claimed = await claimNextLead({ db, firestore, recordActivity: deps.recordActivity, agentUid: safeAgent, agentName, allowGeneralClaim });
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
