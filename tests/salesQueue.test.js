import test from 'node:test';
import assert from 'node:assert/strict';
import {
  calculateQueuePriority,
} from '../services/salesQueue/priority.js';
import {
  decideRouting,
} from '../services/salesQueue/routing.js';
import {
  claimNextLead,
  getAgentQueueStats,
  getNextAgentWork,
  registerAgentOutcome,
} from '../services/salesQueue/agentQueue.js';
import { QUEUE_STATUSES, ROUTING_STATUSES } from '../services/salesQueue/config.js';

function clone(value) {
  if (value instanceof Date) return new Date(value.getTime());
  if (!value || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(clone);
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, clone(item)]));
}

function getPath(obj, path = '') {
  return String(path || '').split('.').reduce((acc, key) => (
    acc && typeof acc === 'object' ? acc[key] : undefined
  ), obj);
}

function setPath(obj, path = '', value) {
  const parts = String(path || '').split('.').filter(Boolean);
  let cursor = obj;
  while (parts.length > 1) {
    const key = parts.shift();
    cursor[key] = cursor[key] && typeof cursor[key] === 'object' ? cursor[key] : {};
    cursor = cursor[key];
  }
  const key = parts[0];
  if (!key) return;
  if (value?.__delete) {
    delete cursor[key];
  } else if (value?.__arrayUnion) {
    const current = Array.isArray(cursor[key]) ? cursor[key] : [];
    cursor[key] = Array.from(new Set([...current, ...value.values]));
  } else {
    cursor[key] = clone(value);
  }
}

function applyPatch(target, patch = {}) {
  for (const [key, value] of Object.entries(patch)) {
    if (key.includes('.')) setPath(target, key, value);
    else if (value?.__delete) delete target[key];
    else if (value?.__arrayUnion) target[key] = Array.from(new Set([...(Array.isArray(target[key]) ? target[key] : []), ...value.values]));
    else target[key] = clone(value);
  }
}

class MemoryDocSnap {
  constructor(id, data) {
    this.id = id;
    this._data = data ? clone(data) : null;
    this.exists = Boolean(data);
  }

  data() {
    return clone(this._data);
  }
}

class MemoryDocRef {
  constructor(db, collectionName, id) {
    this.db = db;
    this.collectionName = collectionName;
    this.id = id;
  }

  async get() {
    return new MemoryDocSnap(this.id, this.db.store.get(this.id));
  }

  async set(patch, options = {}) {
    this.db.applySet(this.id, patch, options);
  }

  collection(name) {
    return {
      add: async (payload) => {
        this.db.activities.push({ leadId: this.id, collection: name, payload: clone(payload) });
        return { id: `activity-${this.db.activities.length}` };
      },
      doc: () => ({
        set: async (payload) => {
          this.db.activities.push({ leadId: this.id, collection: name, payload: clone(payload) });
        },
      }),
    };
  }
}

class MemoryQuery {
  constructor(db, filters = [], max = 999) {
    this.db = db;
    this.filters = filters;
    this.max = max;
  }

  where(field, op, value) {
    return new MemoryQuery(this.db, [...this.filters, { field, op, value }], this.max);
  }

  limit(max) {
    return new MemoryQuery(this.db, this.filters, max);
  }

  async get() {
    let docs = Array.from(this.db.store.entries()).map(([id, data]) => new MemoryDocSnap(id, data));
    docs = docs.filter((snap) => this.filters.every((filter) => {
      const actual = getPath(snap.data(), filter.field);
      if (filter.op === '==') return actual === filter.value;
      if (filter.op === 'in') return Array.isArray(filter.value) && filter.value.includes(actual);
      if (filter.op === '>=') return Number(actual) >= Number(filter.value);
      return false;
    }));
    return { empty: docs.length === 0, docs: docs.slice(0, this.max) };
  }

  doc(id) {
    return new MemoryDocRef(this.db, 'leads', id);
  }
}

class MemoryDb {
  constructor(seed = {}) {
    this.store = new Map(Object.entries(seed).map(([id, data]) => [id, clone(data)]));
    this.activities = [];
    this.beforeTransactionGet = null;
    this.txQueue = Promise.resolve();
  }

  collection(name) {
    assert.equal(name, 'leads');
    return new MemoryQuery(this);
  }

  applySet(id, patch, options = {}) {
    const base = options.merge ? clone(this.store.get(id) || {}) : {};
    applyPatch(base, patch);
    this.store.set(id, base);
  }

  async runTransaction(callback) {
    const run = this.txQueue.then(async () => {
      const tx = {
        get: async (ref) => {
          if (this.beforeTransactionGet) this.beforeTransactionGet(ref.id);
          return ref.get();
        },
        set: (ref, patch, options = {}) => this.applySet(ref.id, patch, options),
      };
      return callback(tx);
    });
    this.txQueue = run.catch(() => {});
    return run;
  }

  lead(id) {
    return this.store.get(id);
  }
}

const fakeFirestore = {
  FieldValue: {
    serverTimestamp: () => new Date('2026-01-01T12:00:00.000Z'),
    delete: () => ({ __delete: true }),
    arrayUnion: (...values) => ({ __arrayUnion: true, values }),
  },
  Timestamp: {
    fromDate: (date) => new Date(date.getTime()),
  },
};

const noActivity = async () => null;

function waitingLead(priority, extra = {}) {
  return {
    nombre: 'Lead',
    queue: {
      status: QUEUE_STATUSES.WAITING,
      priority,
      reasonCode: 'ready_to_buy',
      reason: 'Mostro intencion alta de compra',
      enteredAt: new Date('2026-01-01T10:00:00.000Z'),
    },
    salesState: { leadScore: priority },
    ...extra,
  };
}

test('queue priority es distinta del sales score y sube por pago pendiente', () => {
  const lead = {
    salesState: { leadScore: 50 },
    unreadCount: 1,
    lastMessageAt: new Date(),
  };
  const analysis = {
    intent: 'ready_to_buy',
    interestLevel: 'hot',
    signals: ['answered', 'asked_payment_method', 'ready_to_buy'],
  };

  const result = calculateQueuePriority({
    lead,
    analysis,
    latestText: 'Listo, pasame los datos de pago para hacer el anticipo.',
  });

  assert.notEqual(result.priority, 50);
  assert.ok(result.priority > 75);
  assert.equal(result.factors.salesScore, 50);
});

test('routing manda a agente cuando hay pregunta de pago', () => {
  const routing = decideRouting({
    lead: { salesState: { leadScore: 42 } },
    analysis: {
      intent: 'wants_information',
      interestLevel: 'warm',
      signals: ['answered', 'asked_payment_method'],
    },
    latestText: 'Como puedo pagar?',
  });

  assert.equal(routing.status, ROUTING_STATUSES.READY_FOR_AGENT);
  assert.equal(routing.humanRequired, true);
});

test('routing mantiene automatizacion con interes bajo sin senal fuerte', () => {
  const routing = decideRouting({
    lead: { salesState: { leadScore: 12 } },
    analysis: {
      intent: 'other',
      interestLevel: 'cold',
      signals: ['answered'],
    },
    latestText: 'ok gracias',
  });

  assert.equal(routing.status, ROUTING_STATUSES.AUTOMATION);
  assert.equal(routing.humanRequired, false);
});

test('claim concurrente: dos agentes no obtienen el mismo mejor lead', async () => {
  const db = new MemoryDb({
    leadA: waitingLead(92),
  });

  const [a, b] = await Promise.all([
    claimNextLead({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'agent-a' }),
    claimNextLead({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'agent-b' }),
  ]);

  const claimed = [a, b].filter((item) => item.claimed);
  assert.equal(claimed.length, 1);
  assert.equal(claimed[0].lead.id, 'leadA');
  assert.equal(db.lead('leadA').salesOwner, claimed[0].lead.salesOwner);
});

test('claim concurrente: dos leads disponibles se asignan a agentes distintos', async () => {
  const db = new MemoryDb({
    leadA: waitingLead(90),
    leadB: waitingLead(80),
  });

  const [a, b] = await Promise.all([
    claimNextLead({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'agent-a' }),
    claimNextLead({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'agent-b' }),
  ]);

  assert.equal(a.claimed, true);
  assert.equal(b.claimed, true);
  assert.notEqual(a.lead.id, b.lead.id);
  assert.notEqual(db.lead(a.lead.id).salesOwner, db.lead(b.lead.id).salesOwner);
});

test('claim prueba siguiente candidato si el mejor fue reclamado antes de la transaccion', async () => {
  const db = new MemoryDb({
    leadA: waitingLead(95),
    leadB: waitingLead(75),
  });
  let stolen = false;
  db.beforeTransactionGet = (id) => {
    if (!stolen && id === 'leadA') {
      stolen = true;
      db.applySet('leadA', {
        salesOwner: 'other-agent',
        assignedTo: 'other-agent',
        'queue.status': QUEUE_STATUSES.CLAIMED,
      }, { merge: true });
    }
  };

  const result = await claimNextLead({
    db,
    firestore: fakeFirestore,
    recordActivity: noActivity,
    agentUid: 'agent-a',
  });

  assert.equal(result.claimed, true);
  assert.equal(result.lead.id, 'leadB');
  assert.equal(db.lead('leadA').salesOwner, 'other-agent');
});

test('propiedad: lead sin salesOwner puede entrar a cola general', async () => {
  const db = new MemoryDb({
    leadA: waitingLead(60),
  });

  const result = await claimNextLead({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'agent-a' });

  assert.equal(result.claimed, true);
  assert.equal(result.lead.id, 'leadA');
});

test('cola general incluye lead ready_for_agent aunque queue siga en automation', async () => {
  const db = new MemoryDb({
    leadA: waitingLead(40, {
      queue: { status: QUEUE_STATUSES.AUTOMATION, priority: 40 },
      routing: { status: ROUTING_STATUSES.READY_FOR_AGENT, reason: 'high_intent' },
    }),
  });

  const stats = await getAgentQueueStats({ db, firestore: fakeFirestore });
  const result = await claimNextLead({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'agent-a' });

  assert.equal(stats.waiting, 1);
  assert.equal(result.claimed, true);
  assert.equal(result.lead.id, 'leadA');
  assert.equal(db.lead('leadA').queue.status, QUEUE_STATUSES.CLAIMED);
});

test('cola general no incluye lead solo por score alto si no esta ready_for_sales', async () => {
  const db = new MemoryDb({
    leadA: waitingLead(100, {
      queue: { status: QUEUE_STATUSES.AUTOMATION, priority: 0 },
      salesState: { leadScore: 100 },
    }),
  });

  const stats = await getAgentQueueStats({ db, firestore: fakeFirestore });
  const result = await claimNextLead({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'agent-a' });

  assert.equal(stats.waiting, 0);
  assert.equal(result.claimed, false);
  assert.equal(result.lead, null);
});

test('cola general incluye lead con qualification ready_for_sales aunque queue siga en automation', async () => {
  const db = new MemoryDb({
    leadA: waitingLead(100, {
      queue: { status: QUEUE_STATUSES.AUTOMATION, priority: 0 },
      salesState: {
        leadScore: 100,
        qualification: { readyForSales: true, qualificationStatus: 'ready_for_sales' },
      },
    }),
  });

  const stats = await getAgentQueueStats({ db, firestore: fakeFirestore });
  const result = await claimNextLead({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'agent-a' });

  assert.equal(stats.waiting, 1);
  assert.equal(result.claimed, true);
  assert.equal(result.lead.id, 'leadA');
});

test('propiedad: lead con salesOwner no puede ser reclamado por otro agente', async () => {
  const db = new MemoryDb({
    leadA: waitingLead(90, { salesOwner: 'owner-a', assignedTo: 'owner-a' }),
  });

  const result = await claimNextLead({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'agent-b' });

  assert.equal(result.claimed, false);
});

test('propiedad: lead propio ready_for_sales vuelve al trabajo personal aunque queue siga en automation', async () => {
  const db = new MemoryDb({
    leadA: waitingLead(100, {
      salesOwner: 'owner-a',
      assignedTo: 'owner-a',
      queue: { status: QUEUE_STATUSES.AUTOMATION, priority: 0 },
      salesState: {
        leadScore: 100,
        qualification: { readyForSales: true, qualificationStatus: 'ready_for_sales' },
      },
    }),
  });

  const stats = await getAgentQueueStats({ db, firestore: fakeFirestore, agentUid: 'owner-a' });
  const ownerWork = await getNextAgentWork({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'owner-a' });

  assert.equal(stats.personal, 1);
  assert.equal(ownerWork.source, 'personal');
  assert.equal(ownerWork.lead.id, 'leadA');
});

test('propiedad: lead con propietario vuelve al trabajo personal del propietario', async () => {
  const db = new MemoryDb({
    leadA: waitingLead(90, { salesOwner: 'owner-a', assignedTo: 'owner-a', unreadCount: 1 }),
  });

  const ownerWork = await getNextAgentWork({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'owner-a' });
  const otherWork = await getNextAgentWork({ db, firestore: fakeFirestore, recordActivity: noActivity, agentUid: 'agent-b' });

  assert.equal(ownerWork.lead.id, 'leadA');
  assert.equal(otherWork.lead, null);
  assert.equal(db.lead('leadA').salesOwner, 'owner-a');
});

test('outcome followup requiere nextAt y mantiene propietario', async () => {
  const db = new MemoryDb({
    leadA: waitingLead(70, { salesOwner: 'agent-a', assignedTo: 'agent-a' }),
  });

  await assert.rejects(
    registerAgentOutcome({
      db,
      firestore: fakeFirestore,
      recordActivity: noActivity,
      leadId: 'leadA',
      agentUid: 'agent-a',
      outcome: 'followup',
    }),
    /Falta fecha/
  );

  await registerAgentOutcome({
    db,
    firestore: fakeFirestore,
    recordActivity: noActivity,
    leadId: 'leadA',
    agentUid: 'agent-a',
    outcome: 'followup',
    followUpAt: '2026-01-02T12:00:00.000Z',
    followUpReason: 'Revisar manana',
  });

  assert.equal(db.lead('leadA').salesOwner, 'agent-a');
  assert.equal(db.lead('leadA').followUp.status, 'pending');
  assert.equal(db.lead('leadA').queue.status, QUEUE_STATUSES.FOLLOWUP);
});

test('outcome sale y not_interested son terminales y salen de cola', async () => {
  const db = new MemoryDb({
    saleLead: waitingLead(90, { salesOwner: 'agent-a', assignedTo: 'agent-a', hasActiveSequences: true }),
    lostLead: waitingLead(80, { salesOwner: 'agent-a', assignedTo: 'agent-a', hasActiveSequences: true }),
  });

  await registerAgentOutcome({
    db,
    firestore: fakeFirestore,
    recordActivity: noActivity,
    leadId: 'saleLead',
    agentUid: 'agent-a',
    outcome: 'sale',
  });
  await registerAgentOutcome({
    db,
    firestore: fakeFirestore,
    recordActivity: noActivity,
    leadId: 'lostLead',
    agentUid: 'agent-a',
    outcome: 'not_interested',
  });

  assert.equal(db.lead('saleLead').estado, 'compro');
  assert.equal(db.lead('saleLead').queue.status, QUEUE_STATUSES.CLOSED);
  assert.equal(db.lead('saleLead').hasActiveSequences, false);
  assert.equal(db.lead('lostLead').estado, 'No interesado');
  assert.equal(db.lead('lostLead').queue.status, QUEUE_STATUSES.CLOSED);
  assert.equal(db.lead('lostLead').stopSequences, true);
});
