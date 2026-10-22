import test from 'node:test';
import assert from 'node:assert/strict';
import { scheduleAiSampleFollowup } from '../services/aiSampleJobs.js';
import { shouldBlockSequenceByLeadContext } from '../utils/sequenceTriggerGuards.js';

const ref = (lead) => ({ id: 'lead', get: async () => ({ data: () => lead }) });

test('la muestra entregada por el agente programa el recordatorio configurado', async () => {
  const calls = [];
  await scheduleAiSampleFollowup({ leadRef: ref({ sampleAgent: { enabled: false, status: 'completed' } }), schedule: async (...args) => calls.push(args) });
  assert.equal(calls.length, 1);
  assert.equal(calls[0][0], 'lead'); assert.equal(calls[0][1], 'Web_SampleNotOpened');
  assert.equal(calls[0][3].source, 'ai-sample-ready');
  assert.ok(calls[0][2] instanceof Date);
});

test('no programa recordatorios con muestra abierta, agente activo, cancelación o atención humana', async () => {
  for (const lead of [{ linkOpenedAt: new Date() }, { sampleAgent: { enabled: true } }, { stopSequences: true }, { humanControl: true }, { salesBrainHumanControl: true }, { archived: true }, { estado: 'compro' }, { estado: 'no interesado' }]) {
    await scheduleAiSampleFollowup({ leadRef: ref(lead), schedule: async () => assert.fail('No debe programar') });
  }
});

test('un fallo del seguimiento no vuelve a fallar la entrega de la muestra', async () => {
  const warnings = [];
  await scheduleAiSampleFollowup({ leadRef: ref({}), schedule: async () => { throw Error('Temporal'); }, logger: { warn: (...args) => warnings.push(args) } });
  assert.equal(warnings.length, 1);
});

test('la protección transaccional impide programar no-abierta después de registrar la apertura', () => {
  assert.deepEqual(shouldBlockSequenceByLeadContext({ linkOpenedAt: new Date() }, 'Web_SampleNotOpened'), { blocked: true, reason: 'sample_already_opened' });
  assert.equal(shouldBlockSequenceByLeadContext({ linkOpenedAt: new Date() }, 'LinkAbierto').blocked, false);
  assert.equal(shouldBlockSequenceByLeadContext({}, 'Web_SampleNotOpened').blocked, false);
});
