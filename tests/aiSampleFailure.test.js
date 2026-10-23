import test from 'node:test';
import assert from 'node:assert/strict';
import { syncAiSampleFailure } from '../services/aiSampleFailure.js';
import { summarizeSampleProgress } from '../services/sampleProgress.js';

function harness() {
  let writes = 0;
  const data = { lead: { telefono: '5211234567890', sampleAgent: { status: 'generating', negocioId: 'business', enabled: true, brief: { companyName: 'Sonido KALIZ CON Z', activity: 'Sonido para eventos' }, images: [{ url: 'https://storage.googleapis.com/test/photo.jpg' }] } }, business: { leadId: 'lead', aiSampleJob: { state: 'failed', error: 'You have no credits remaining.', finishedAt: new Date('2026-10-23T12:00:00Z') }, aiSampleDraft: { design: 'keep' } } };
  const leadRef = { id: 'lead', key: 'lead' };
  const db = { collection: () => ({ doc: () => ({ key: 'business' }) }), runTransaction: async (fn) => fn({ get: async (ref) => ({ data: () => structuredClone(data[ref.key]) }), update: (ref, patch) => { writes++; Object.assign(data[ref.key], patch); } }) };
  return { data, leadRef, db, get writes() { return writes; } };
}
test('repairs an old generating state and enables retry without generating or deleting assets', async () => {
  const h = harness();
  const result = await syncAiSampleFailure(h.db, h.leadRef);
  assert.equal(result.sampleAgent.status, 'error');
  assert.equal(result.sampleAgent.enabled, false);
  assert.match(result.sampleAgent.error, /credits/);
  assert.equal(result.sampleAgent.images.length, 1);
  assert.equal(result.sampleAgent.brief.companyName, 'Sonido KALIZ CON Z');
  assert.equal(h.data.business.aiSampleDraft.design, 'keep');
  assert.equal(summarizeSampleProgress('lead', result).canForce, true);
  await syncAiSampleFailure(h.db, h.leadRef);
  assert.equal(h.writes, 1);
});
test('does not change active jobs, completed/paused contacts, mismatched owners or a newer manual retry', async () => {
  for (const change of [h => { h.data.business.aiSampleJob.state = 'running'; }, h => { h.data.lead.sampleAgent.status = 'completed'; }, h => { h.data.lead.sampleAgent.status = 'paused'; }, h => { h.data.business.leadId = 'another'; }, h => { h.data.lead.sampleAgent.manualRequestedAt = new Date('2026-10-23T13:00:00Z'); }]) {
    const h = harness(); change(h); await syncAiSampleFailure(h.db, h.leadRef); assert.equal(h.writes, 0);
  }
  const h = harness(); await syncAiSampleFailure(h.db, h.leadRef, 'other-business'); assert.equal(h.writes, 0);
});
