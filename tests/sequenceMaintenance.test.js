import test from 'node:test';
import assert from 'node:assert/strict';
import { createSequenceProcessor, createLegacySequenceScanner } from '../services/sequenceMaintenance.js';

test('entrega cada 30 segundos y mantenimiento histórico solo cada cinco minutos', async () => {
  let time = 0; const calls = [];
  const names = ['hydrateNextSequenceRun', 'pauseLegacyRecoveredMissingDestinationSequences', 'recoverMissingDestinationSequences', 'resumeSalesQueuePausedSequences'];
  const queue = Object.fromEntries(names.map(name => [name, async options => calls.push({ name, options })]));
  queue.processSequenceLeadsBatch = async () => { calls.push({ name: 'deliver' }); return 2; };
  queue.processQueue = async () => { calls.push({ name: 'jobs' }); return 1; };
  const run = createSequenceProcessor(queue, { now: () => time });
  for (let n = 0; n < 10; n++) { assert.equal(await run(), 3); time += 30000; }
  for (const name of names) assert.equal(calls.filter(c => c.name === name).length, 1);
  assert.equal(calls.filter(c => c.name === 'deliver').length, 10);
  await run(); for (const name of names) assert.equal(calls.filter(c => c.name === name).length, 2);
});
test('un mantenimiento fallido no detiene entregas ni se reintenta en cada cron', async () => {
  let repair = 0, delivered = 0, warnings = 0;
  const run = createSequenceProcessor({ hydrateNextSequenceRun: async () => { repair++; throw Error('temporarily unavailable'); },
    processSequenceLeadsBatch: async () => ++delivered }, { now: () => 0, logger: { warn: () => warnings++ } });
  await run(); await run(); assert.equal(repair, 1); assert.equal(delivered, 2); assert.equal(warnings, 1);
});
test('un cron superpuesto no procesa la misma cola dos veces', async () => {
  let release, calls = 0;
  const run = createSequenceProcessor({ processQueue: async () => { calls++; await new Promise(resolve => { release = resolve; }); return 1; } });
  const first = run(); assert.equal(await run(), 0); release(); assert.equal(await first, 1); assert.equal(calls, 1);
});
test('mantiene el alias de entrega y libera el bloqueo local si la entrega falla', async () => {
  let calls = 0;
  const run = createSequenceProcessor({ processDueSequenceJobs: async () => { if (++calls === 1) throw Error('WA down'); return 3; } });
  await assert.rejects(run(), /WA down/); assert.equal(await run(), 3);
});
test('la hidratación avanza de página y solo reinicia al terminar el recorrido', async () => {
  const ids = ['a', 'b', 'c', 'd', 'e']; const selects = [];
  const make = (cursor = '') => ({ where: () => make(cursor), orderBy: () => make(cursor),
    select: (...fields) => { selects.push(fields); return make(cursor); }, startAfter: doc => make(doc.id),
    limit: count => ({ get: async () => { const docs = ids.filter(id => id > cursor).slice(0, count).map(id => ({ id })); return { docs, empty: !docs.length }; } }) });
  const scan = createLegacySequenceScanner({ collection: () => make() });
  assert.deepEqual((await scan(2)).docs.map(d => d.id), ['a', 'b']);
  assert.deepEqual((await scan(2)).docs.map(d => d.id), ['c', 'd']);
  assert.deepEqual((await scan(2)).docs.map(d => d.id), ['e']);
  assert.deepEqual((await scan(2)).docs.map(d => d.id), ['a', 'b']);
  assert.ok(selects.every(fields => fields.includes('secuenciasActivas') && !fields.includes('sampleAgent')));
});
