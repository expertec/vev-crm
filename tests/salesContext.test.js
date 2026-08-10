import test from 'node:test';
import assert from 'node:assert/strict';

import { decideNextAction } from '../services/salesBrain/decision.js';
import { buildSalesContextPatch } from '../services/salesBrain/salesContext.js';
import { calculateLeadScore } from '../services/salesBrain/leadScore.js';
import { decideRouting } from '../services/salesQueue/routing.js';
import { ROUTING_STATUSES } from '../services/salesQueue/config.js';
import { CONVERSATIONAL_WELCOME_SEQUENCE } from '../services/salesQueue/welcomeSequence.js';

test('bienvenida conversacional define cuatro preguntas naturales con waitForReply', () => {
  assert.equal(CONVERSATIONAL_WELCOME_SEQUENCE.messages.length, 4);
  assert.deepEqual(
    CONVERSATIONAL_WELCOME_SEQUENCE.messages.map((step) => step.saveTo),
    [
      'salesContext.businessType',
      'salesContext.customerAcquisition',
      'salesContext.primaryGoal',
      'salesContext.previousExperience',
    ]
  );
  assert.equal(CONVERSATIONAL_WELCOME_SEQUENCE.messages.every((step) => step.type === 'question' && step.waitForReply === true), true);
  const fullCopy = CONVERSATIONAL_WELCOME_SEQUENCE.messages.map((step) => step.message).join(' ').toLowerCase();
  for (const forbidden of ['lead', 'funnel', 'roas', 'pipeline', 'conversión', 'cac']) {
    assert.equal(fullCopy.includes(forbidden), false);
  }
});

test('cliente normal completa contexto sin volverse hot automaticamente', () => {
  let state = {};
  let raw = {};
  let confidence = {};

  for (const item of [
    ['salesContext.businessType', 'Tengo una estética.', { businessType: { value: 'estetica', confidence: 0.95, source: 'explicit' } }],
    ['salesContext.customerAcquisition', 'Casi todo es recomendado.', { customerAcquisition: { value: 'referrals', confidence: 0.92, source: 'explicit' } }],
    ['salesContext.primaryGoal', 'Quiero más clientes.', { primaryGoal: { value: 'more_customers', confidence: 0.92, source: 'explicit' } }],
    ['salesContext.previousExperience', 'Nunca he hecho publicidad.', { previousExperience: { value: 'never_tried', confidence: 0.9, source: 'explicit' } }],
  ]) {
    const patch = buildSalesContextPatch({
      previousSalesContext: state,
      previousSalesContextRaw: raw,
      previousConfidence: confidence,
      analysis: { facts: item[2], intent: 'other', interestLevel: 'cold', signals: ['answered'] },
      latestText: item[1],
      saveTo: item[0],
    });
    state = patch.salesContext;
    raw = patch.salesContextRaw;
    confidence = patch.salesContextConfidence;
  }

  assert.deepEqual(state, {
    businessType: 'estetica',
    customerAcquisition: 'referrals',
    primaryGoal: 'more_customers',
    previousExperience: 'never_tried',
  });
  assert.equal(raw.businessType, 'Tengo una estética.');
  assert.equal(raw.customerAcquisition, 'Casi todo es recomendado.');

  const score = calculateLeadScore({
    lead: { salesContext: state },
    analysis: { intent: 'other', interestLevel: 'cold', signals: ['answered'] },
    memory: { facts: { businessType: { value: state.businessType }, primaryNeed: { value: state.primaryGoal } } },
  });
  const routing = decideRouting({
    lead: { salesContext: state, salesState: { leadScore: score.total } },
    analysis: { intent: 'other', interestLevel: 'cold', signals: ['answered'] },
  });

  assert.ok(score.total < 40);
  assert.equal(routing.status, ROUTING_STATUSES.AUTOMATION);
});

test('mala experiencia previa alimenta objecion de confianza', () => {
  const patch = buildSalesContextPatch({
    analysis: {
      facts: {
        previousExperience: { value: 'bad_experience', confidence: 0.95, source: 'explicit' },
      },
      intent: 'other',
      interestLevel: 'cold',
      signals: ['answered'],
    },
    latestText: 'Ya pagué anuncios y no me sirvió de nada.',
    saveTo: 'salesContext.previousExperience',
  });

  const decision = decideNextAction({
    lead: { telefono: '5215551112233', salesContext: patch.salesContext },
    analysis: { intent: 'other', objection: 'none', signals: ['answered'] },
    conversationMemory: { facts: {} },
  });

  assert.equal(patch.salesContext.previousExperience, 'bad_experience');
  assert.equal(patch.salesContextRaw.previousExperience, 'Ya pagué anuncios y no me sirvió de nada.');
  assert.equal(decision.nextBestAction, 'HANDLE_TRUST_OBJECTION');
});

test('cliente caliente durante bienvenida se enruta a agente', () => {
  const routing = decideRouting({
    lead: { sequenceQuestionPending: { status: 'waiting_for_reply', saveTo: 'salesContext.businessType' } },
    analysis: {
      intent: 'ready_to_buy',
      interestLevel: 'hot',
      signals: ['answered', 'ready_to_buy', 'asked_payment_method'],
    },
    latestText: 'Sí quiero hacerlo, pásame los datos para pagar.',
  });

  assert.equal(routing.status, ROUTING_STATUSES.READY_FOR_AGENT);
  assert.equal(routing.humanRequired, true);
});

test('respuesta ambigua preserva texto crudo pero no inventa contexto', () => {
  const patch = buildSalesContextPatch({
    analysis: { facts: {}, intent: 'other', interestLevel: 'cold', signals: ['answered'] },
    latestText: 'Ok',
    saveTo: 'salesContext.businessType',
  });

  assert.equal(patch.salesContext.businessType, undefined);
  assert.equal(patch.salesContextRaw.businessType, 'Ok');
});
