import test from 'node:test';
import assert from 'node:assert/strict';

import { decideNextAction } from '../services/salesBrain/decision.js';
import { calculateLeadScore } from '../services/salesBrain/leadScore.js';
import { mergeConversationMemory } from '../services/salesBrain/memory.js';
import { eventIdForInputMessage } from '../services/salesBrain/catalog.js';
import { buildSalesBrainEventPayload } from '../services/salesBrain/eventPayload.js';
import { generateReply } from '../services/salesBrain/replyGenerator.js';

test('Sales Brain decision respeta prioridad: cierre gana sobre precio', () => {
  const decision = decideNextAction({
    lead: { telefono: '5215551112233' },
    analysis: {
      intent: 'ready_to_buy',
      signals: ['asked_price', 'ready_to_buy'],
      objection: 'none',
    },
    conversationMemory: {
      facts: {
        businessType: { value: 'restaurant' },
        primaryNeed: { value: 'more_customers' },
      },
    },
  });

  assert.equal(decision.nextBestAction, 'START_CLOSING');
});

test('Sales Brain decision usa objecion de confianza antes que descubrimiento', () => {
  const decision = decideNextAction({
    lead: { telefono: '5215551112233' },
    analysis: {
      intent: 'other',
      signals: ['previous_bad_agency_experience'],
      objection: 'bad_previous_experience',
    },
    conversationMemory: { facts: {} },
  });

  assert.equal(decision.nextBestAction, 'HANDLE_TRUST_OBJECTION');
});

test('Sales Brain decision pregunta tipo de negocio si no hay datos ni objeciones', () => {
  const decision = decideNextAction({
    lead: { telefono: '5215551112233' },
    analysis: {
      intent: 'other',
      signals: [],
      objection: 'none',
    },
    conversationMemory: { facts: {} },
  });

  assert.equal(decision.nextBestAction, 'ASK_BUSINESS_TYPE');
});

test('PlanRedes con negocio conocido descubre objetivo antes de vender', () => {
  const decision = decideNextAction({
    lead: { telefono: '5215551112233', etiquetas: ['PlanRedes'] },
    analysis: {
      intent: 'other',
      businessType: 'travel_agency',
      signals: ['answered', 'business_identified'],
      objection: 'none',
    },
    conversationMemory: {
      facts: {
        businessType: { value: 'travel_agency' },
      },
    },
  });

  assert.equal(decision.productStrategy, 'redes_sociales');
  assert.equal(decision.conversationObjective, 'DISCOVER_GOAL');
  assert.equal(decision.nextBestAction, 'ASK_PRIMARY_GOAL');
  assert.equal(decision.readyForSales, false);
});

test('PlanRedes demuestra entendimiento despues de conocer objetivo', () => {
  const decision = decideNextAction({
    lead: { telefono: '5215551112233', etiquetas: ['PlanRedes'] },
    analysis: {
      intent: 'other',
      primaryNeed: 'more_customers',
      signals: ['answered', 'primary_need_identified'],
      objection: 'none',
    },
    conversationMemory: {
      facts: {
        businessType: { value: 'travel_agency' },
        primaryNeed: { value: 'more_customers' },
      },
    },
  });

  assert.equal(decision.conversationObjective, 'DEMONSTRATE_UNDERSTANDING');
  assert.equal(decision.nextBestAction, 'DEMONSTRATE_UNDERSTANDING');
});

test('PlanRedes no repite objetivo y pregunta situacion actual tras entregar entendimiento', () => {
  const decision = decideNextAction({
    lead: { telefono: '5215551112233', etiquetas: ['PlanRedes'] },
    analysis: {
      intent: 'other',
      signals: ['answered'],
      objection: 'none',
    },
    salesState: {
      qualification: {
        productStrategy: 'redes_sociales',
        delivered: { understanding: true },
      },
    },
    conversationMemory: {
      facts: {
        businessType: { value: 'travel_agency' },
        primaryNeed: { value: 'more_customers' },
      },
    },
  });

  assert.equal(decision.conversationObjective, 'DISCOVER_CURRENT_SITUATION');
  assert.equal(decision.nextBestAction, 'ASK_CURRENT_SITUATION');
});

test('PlanRedes con contexto completo y pregunta de precio presenta oferta sin marcar ready_for_sales si no recibio valor', () => {
  const decision = decideNextAction({
    lead: { telefono: '5215551112233', etiquetas: ['PlanRedes'] },
    analysis: {
      intent: 'wants_price',
      signals: ['answered', 'asked_price'],
      objection: 'none',
    },
    conversationMemory: {
      facts: {
        businessType: { value: 'travel_agency' },
        primaryNeed: { value: 'more_customers' },
        currentSituation: { value: 'runs_ads_no_results' },
        painPoint: { value: 'no_results' },
      },
    },
  });

  assert.equal(decision.conversationObjective, 'PRESENT_OFFER');
  assert.equal(decision.nextBestAction, 'PRESENT_OFFER');
  assert.equal(decision.readyForSales, false);
});

test('PlanRedes genera pregunta natural de objetivo por template sin API', async () => {
  const reply = await generateReply({
    action: 'ASK_PRIMARY_GOAL',
    conversationObjective: 'DISCOVER_GOAL',
    productStrategy: 'redes_sociales',
    qualification: {
      business: { known: true, value: 'travel_agency' },
      primaryGoal: { known: false, value: null },
    },
  });

  assert.equal(reply.model, 'template');
  assert.match(reply.message, /cotizaciones|informacion/i);
  assert.doesNotMatch(reply.message, /nuestro plan|diseñado|disenado/i);
});

test('Sales Brain score devuelve breakdown auditable y normalizado', () => {
  const result = calculateLeadScore({
    lead: { source: 'meta_ads', telefono: '5215551112233' },
    analysis: {
      intent: 'ready_to_buy',
      interestLevel: 'hot',
      signals: ['answered', 'business_identified', 'asked_price', 'ready_to_buy'],
    },
    memory: {
      facts: {
        businessType: { value: 'restaurant' },
        primaryNeed: { value: 'more_customers' },
      },
    },
  });

  assert.equal(result.breakdown.meta_ad, 10);
  assert.equal(result.breakdown.answered, 5);
  assert.equal(result.breakdown.asked_price, 15);
  assert.equal(result.breakdown.ready_to_buy, 30);
  assert.equal(result.total, 90);
});

test('Sales Brain score no duplica señales ya aplicadas', () => {
  const first = calculateLeadScore({
    lead: { source: 'meta_ads', telefono: '5215551112233' },
    analysis: {
      intent: 'wants_price',
      interestLevel: 'hot',
      signals: ['answered', 'asked_price'],
    },
    memory: { facts: {} },
  });

  const second = calculateLeadScore({
    lead: {
      source: 'meta_ads',
      telefono: '5215551112233',
      salesScoreState: { appliedSignals: first.appliedSignals },
    },
    analysis: {
      intent: 'wants_price',
      interestLevel: 'hot',
      signals: ['answered', 'asked_price'],
    },
    memory: { facts: {} },
  });

  assert.deepEqual(second.newlyApplied, {});
  assert.equal(second.breakdown.asked_price, 15);
  assert.equal(second.total, first.total);
});

test('Sales Brain memory no sobrescribe hecho explicito con inferencia', () => {
  const previous = {
    summary: 'Tiene restaurante.',
    facts: {
      businessType: {
        value: 'restaurant',
        confidence: 0.95,
        source: 'explicit',
        sourceMessageId: 'wa_a',
        updatedAt: new Date('2026-01-01T00:00:00.000Z'),
      },
    },
  };

  const next = mergeConversationMemory(previous, {
    summary: 'Podria ser barberia.',
    facts: {
      businessType: {
        value: 'barbershop',
        confidence: 0.8,
        source: 'inferred',
      },
    },
  }, {
    inputMessageId: 'wa_b',
    now: new Date('2026-01-02T00:00:00.000Z'),
  });

  assert.equal(next.facts.businessType.value, 'restaurant');
  assert.equal(next.facts.businessType.sourceMessageId, 'wa_a');
});

test('Sales Brain event id es estable por inputMessageId', () => {
  assert.equal(eventIdForInputMessage('wa_123'), 'inbound_wa_123');
  assert.equal(eventIdForInputMessage('wa:abc/123'), 'inbound_wa_abc_123');
});

test('Sales Brain event payload conserva scoreBreakdown y versiones', () => {
  const event = buildSalesBrainEventPayload({
    inputMessageId: 'wa_123',
    analysis: { intent: 'wants_examples', model: 'keyword' },
    scoreBreakdown: { answered: 5, asked_examples: 10 },
    leadScore: 15,
    nextBestAction: 'SEND_EXAMPLES',
    reason: 'Pidio ejemplos.',
    suggestedReply: 'Claro, te mando ejemplos.',
    createdAt: new Date('2026-08-08T12:00:00.000Z'),
  });

  assert.equal(event.status, 'pending');
  assert.equal(event.scoreBreakdown.answered, 5);
  assert.equal(event.leadScore, 15);
  assert.equal(event.analysisVersion, 'v1');
  assert.equal(event.decisionVersion, 'v2');
  assert.equal(event.replyPromptVersion, 'v2');
  assert.equal(event.agentVersion, 'sales-brain-mvp-v1');
});

test('Sales Brain event payload marca failed si falla generateReply', () => {
  const event = buildSalesBrainEventPayload({
    inputMessageId: 'wa_124',
    analysis: { intent: 'wants_examples', model: 'keyword' },
    scoreBreakdown: { answered: 5 },
    leadScore: 5,
    nextBestAction: 'SEND_EXAMPLES',
    reason: 'Pidio ejemplos.',
    suggestedReply: '',
    replyGenerationStatus: 'failed',
    createdAt: new Date('2026-08-08T12:00:00.000Z'),
  });

  assert.equal(event.status, 'failed');
  assert.equal(event.replyGenerationStatus, 'failed');
  assert.equal(event.nextBestAction, 'SEND_EXAMPLES');
});
