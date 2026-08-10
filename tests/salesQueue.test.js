import test from 'node:test';
import assert from 'node:assert/strict';
import {
  calculateQueuePriority,
} from '../services/salesQueue/priority.js';
import {
  decideRouting,
} from '../services/salesQueue/routing.js';
import { ROUTING_STATUSES } from '../services/salesQueue/config.js';

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
