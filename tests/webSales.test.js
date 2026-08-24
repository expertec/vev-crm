import test from 'node:test';
import assert from 'node:assert/strict';

process.env.SALES_BRAIN_AI = 'off';

const {
  WEB_CONVERSION_GOALS,
  WEB_SALES_EVENTS,
  WEB_SALES_STAGES,
  buildWebSalesStateAfterEvent,
  ensureWebSalesState,
} = await import('../services/webSales/state.js');
const { determineNextWebSalesAction } = await import('../services/webSales/orchestrator.js');
const { analyzeConversation } = await import('../services/salesBrain/analyzeConversation.js');
const { calculateLeadScore } = await import('../services/salesBrain/leadScore.js');
const { decideRouting } = await import('../services/salesQueue/routing.js');

test('webSales inicializa desde campos legacy sin migracion masiva', () => {
  const state = ensureWebSalesState({
    sampleSubmittedAt: new Date('2026-08-20T10:00:00.000Z'),
    etiquetas: ['FormOK'],
  }, new Date('2026-08-21T10:00:00.000Z'));

  assert.equal(state.stage, WEB_SALES_STAGES.SAMPLE_FORM_COMPLETED);
  assert.equal(state.nextConversionGoal, WEB_CONVERSION_GOALS.WAIT);
});

test('sample_form_sent deja objetivo en completar formulario', () => {
  const state = buildWebSalesStateAfterEvent({
    current: {},
    type: WEB_SALES_EVENTS.SAMPLE_FORM_SENT,
    timestamp: new Date('2026-08-21T10:00:00.000Z'),
  });

  assert.equal(state.stage, WEB_SALES_STAGES.SAMPLE_FORM_SENT);
  assert.equal(state.nextConversionGoal, WEB_CONVERSION_GOALS.GET_SAMPLE_FORM_COMPLETED);
  assert.ok(state.sample.formSentAt);
});

test('sample_form_opened no marca formulario como completado', () => {
  const current = buildWebSalesStateAfterEvent({
    type: WEB_SALES_EVENTS.SAMPLE_FORM_SENT,
    timestamp: new Date('2026-08-21T10:00:00.000Z'),
  });
  const state = buildWebSalesStateAfterEvent({
    current,
    type: WEB_SALES_EVENTS.SAMPLE_FORM_OPENED,
    timestamp: new Date('2026-08-21T10:05:00.000Z'),
  });

  assert.equal(state.stage, WEB_SALES_STAGES.SAMPLE_FORM_SENT);
  assert.ok(state.sample.formOpenedAt);
  assert.equal(state.sample.formCompletedAt, null);
});

test('sample_form_started guarda ultimo paso relevante', () => {
  const state = buildWebSalesStateAfterEvent({
    type: WEB_SALES_EVENTS.SAMPLE_FORM_STARTED,
    metadata: { step: 2 },
    timestamp: new Date('2026-08-21T10:10:00.000Z'),
  });

  assert.equal(state.stage, WEB_SALES_STAGES.SAMPLE_FORM_STARTED);
  assert.equal(state.sample.formLastStep, 2);
});

test('sample_form_completed avanza y mantiene objetivo en wait mientras se genera', () => {
  const state = buildWebSalesStateAfterEvent({
    type: WEB_SALES_EVENTS.SAMPLE_FORM_COMPLETED,
    metadata: { step: 2 },
    timestamp: new Date('2026-08-21T10:20:00.000Z'),
  });

  assert.equal(state.stage, WEB_SALES_STAGES.SAMPLE_FORM_COMPLETED);
  assert.equal(state.nextConversionGoal, WEB_CONVERSION_GOALS.WAIT);
  assert.ok(state.sample.formCompletedAt);
});

test('sample_generated y sample_sent separan lista vs enviada', () => {
  const generated = buildWebSalesStateAfterEvent({
    type: WEB_SALES_EVENTS.SAMPLE_GENERATED,
    timestamp: new Date('2026-08-21T10:30:00.000Z'),
  });
  const sent = buildWebSalesStateAfterEvent({
    current: generated,
    type: WEB_SALES_EVENTS.SAMPLE_SENT,
    timestamp: new Date('2026-08-21T10:35:00.000Z'),
  });

  assert.equal(generated.stage, WEB_SALES_STAGES.SAMPLE_GENERATED);
  assert.equal(sent.stage, WEB_SALES_STAGES.SAMPLE_SENT);
  assert.equal(sent.nextConversionGoal, WEB_CONVERSION_GOALS.GET_SAMPLE_OPEN);
});

test('sample_opened incrementa contador y cambia objetivo a feedback', () => {
  let state = buildWebSalesStateAfterEvent({
    type: WEB_SALES_EVENTS.SAMPLE_SENT,
    timestamp: new Date('2026-08-21T10:35:00.000Z'),
  });
  state = buildWebSalesStateAfterEvent({
    current: state,
    type: WEB_SALES_EVENTS.SAMPLE_OPENED,
    timestamp: new Date('2026-08-21T10:40:00.000Z'),
  });
  state = buildWebSalesStateAfterEvent({
    current: state,
    type: WEB_SALES_EVENTS.SAMPLE_OPENED,
    timestamp: new Date('2026-08-21T10:45:00.000Z'),
  });
  state = buildWebSalesStateAfterEvent({
    current: state,
    type: WEB_SALES_EVENTS.SAMPLE_OPENED,
    timestamp: new Date('2026-08-21T10:50:00.000Z'),
  });

  assert.equal(state.stage, WEB_SALES_STAGES.SAMPLE_OPENED);
  assert.equal(state.nextConversionGoal, WEB_CONVERSION_GOALS.GET_SAMPLE_FEEDBACK);
  assert.equal(state.sample.openCount, 3);

  const score = calculateLeadScore({
    lead: { webSales: state },
    analysis: { signals: [] },
  });
  assert.equal(score.breakdown.web_sample_opened, 10);
  assert.equal(score.breakdown.web_sample_opened_multiple, 6);
});

test('feedback de muestra manda a cola de ventas con sample_engagement', async () => {
  const analysis = await analyzeConversation({
    latestText: 'Me gustaría cambiar algunas imágenes de la muestra',
  });
  const routing = decideRouting({
    lead: {
      telefono: '5215551112233',
      webSales: buildWebSalesStateAfterEvent({ type: WEB_SALES_EVENTS.SAMPLE_OPENED }),
    },
    analysis,
    latestText: 'Me gustaría cambiar algunas imágenes de la muestra',
  });

  assert.equal(analysis.intent, 'sample_change_request');
  assert.ok(analysis.signals.includes('sample_feedback'));
  assert.equal(routing.status, 'ready_for_agent');
  assert.equal(routing.reason, 'sample_engagement');
});

test('preguntas de pago y anticipo son cierre de alta prioridad', async () => {
  const card = await analyzeConversation({ latestText: 'Aceptan tarjeta?' });
  const deposit = await analyzeConversation({ latestText: 'Puedo dar el 50% y empezar?' });

  assert.ok(card.signals.includes('asked_payment_method'));
  assert.equal(deposit.intent, 'ready_for_deposit');
  assert.ok(deposit.signals.includes('ready_for_deposit'));
});

test('falta de fotos se interpreta como friccion, no como perdido', async () => {
  const analysis = await analyzeConversation({ latestText: 'No tengo todavía las imágenes ni logo' });

  assert.equal(analysis.intent, 'missing_assets');
  assert.equal(analysis.interestLevel, 'warm');
  assert.notEqual(analysis.intent, 'no_interest');
});

test('orquestador respeta un solo objetivo activo', () => {
  const webSales = buildWebSalesStateAfterEvent({
    type: WEB_SALES_EVENTS.SAMPLE_FORM_SENT,
    timestamp: new Date('2026-08-21T10:00:00.000Z'),
  });
  const action = determineNextWebSalesAction({ webSales, productInterest: 'web' });

  assert.equal(action.action, 'send_sequence');
  assert.equal(action.reason, 'form_pending');
  assert.equal(action.sequenceTrigger, 'Web_FormPending');
});
