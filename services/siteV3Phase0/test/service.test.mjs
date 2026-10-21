import test from 'node:test';
import assert from 'node:assert/strict';
import { firebaseIdTokenVerifier } from '../access.mjs';
import { SiteV3Service } from '../service.mjs';
import { MemoryStore, FirestoreStore } from '../store.mjs';
import { BIZ, OTHER, billing, candidate, editor, errorCode, fixture, foreign, harness, owner, prepared, queued, ready, request, worker, worker2 } from './helpers.mjs';

test('disabled service and unconfigured verification fail closed', async () => {
  const h = harness({ enabled: false }); await assert.rejects(() => h.service.createProject(owner, BIZ), errorCode('V3_DISABLED'));
  const closed = new SiteV3Service({ ...h.config, enabled: true, verifySession: undefined });
  await assert.rejects(() => closed.createProject(owner, BIZ), errorCode('AUTH_NOT_CONFIGURED'));
});
test('Firebase adapter requests revocation checks and never decodes an unsigned token', async () => {
  let params;
  const verify = firebaseIdTokenVerifier({ verifyIdToken: async (...args) => { params = args; return { uid: 'demo' }; } });
  await verify('synthetic'); assert.deepEqual(params, ['synthetic', true]);
});
for (const operation of ['create', 'read', 'edit', 'process', 'images', 'activate', 'approve', 'publish']) test(`${operation}: no session, invalid/expired/revoked token and tenant B cannot act on tenant A`, async () => {
  const h = harness();
  await assert.rejects(() => h.service.access({}, BIZ, operation), errorCode('AUTH_REQUIRED'));
  for (const token of ['tampered', 'expired', 'revoked', 'missingexp']) await assert.rejects(() => h.service.access(request(token), BIZ, operation), errorCode('INVALID_SESSION'));
  await assert.rejects(() => h.service.access(foreign, BIZ, operation), errorCode('FORBIDDEN'));
  const valid = operation === 'activate' ? billing : owner;
  assert.equal((await h.service.access(valid, BIZ, operation)).negocioId, BIZ);
});
test('actual cross-tenant mutation and job reads are rejected without effects', async () => {
  const h = await prepared(), before = h.store.snapshot();
  await assert.rejects(() => h.service.saveBrief(foreign, BIZ, h.brief), errorCode('FORBIDDEN'));
  await assert.rejects(() => h.service.requestGeneration(foreign, BIZ, { expectedInputRevision: 2 }), errorCode('FORBIDDEN'));
  await assert.rejects(() => h.service.createProject(owner, OTHER), errorCode('FORBIDDEN'));
  assert.deepEqual(h.store.snapshot(), before);
  const job = await queued(h);
  await assert.rejects(() => h.service.getJob(foreign, BIZ, job.id), errorCode('FORBIDDEN'));
});
test('ID/path traversal, browser permission grants and fabricated verification fields are rejected', async () => {
  const h = harness(); await assert.rejects(() => h.service.createProject(owner, '../other'), errorCode('INVALID_ID'));
  await h.service.createProject(owner, BIZ);
  const b = fixture('business-brief'); b.facts[0].verified = true;
  await assert.rejects(() => h.service.saveBrief(owner, BIZ, b), errorCode('INVALID_CONTRACT'));
});
test('duplicate creates are idempotent; stale concurrent edits have exactly one winner', async () => {
  const h = harness(); const creates = await Promise.all([h.service.createProject(owner, BIZ), h.service.createProject(owner, BIZ)]);
  assert.equal(creates.filter(x => !x.duplicate).length, 1);
  const b = fixture('business-brief');
  const edits = await Promise.allSettled([h.service.saveBrief(owner, BIZ, b), h.service.saveBrief(owner, BIZ, b)]);
  assert.equal(edits.filter(x => x.status === 'fulfilled').length, 1);
  assert.equal(edits.find(x => x.status === 'rejected').reason.code, 'STALE_INPUT');
});
test('editors cannot confirm private evidence, and legacy-unverified facts remain unconfirmed', async () => {
  const h = await prepared();
  await assert.rejects(() => h.service.confirmFacts(editor, BIZ, { expectedInputRevision: 2, factIds: ['fact-business-name'] }), errorCode('FORBIDDEN'));
  const b = structuredClone(h.brief); b.expectedInputRevision = 2; b.facts[0].origin = 'legacy_unverified';
  await h.service.saveBrief(owner, BIZ, b);
  await assert.rejects(() => h.service.confirmFacts(owner, BIZ, { expectedInputRevision: 3, factIds: ['fact-cat-materiales'] }), errorCode('FACT_NOT_CONFIRMABLE'));
});
test('equivalent simultaneous generation requests persist one durable job and revision reservation', async () => {
  const h = await prepared();
  const calls = await Promise.all(Array.from({ length: 12 }, () => h.service.requestGeneration(owner, BIZ, { expectedInputRevision: 2 })));
  assert.equal(new Set(calls.map(x => x.job.id)).size, 1); assert.equal(calls.filter(x => !x.duplicate).length, 1);
  assert.equal(Object.keys(h.store.snapshot()).filter(x => x.startsWith('SiteV3Jobs/')).length, 1);
  assert.equal((await h.store.get(`SiteV3Projects/${BIZ}`)).nextRevision, 2);
});
test('concurrent workers cannot share a live lease; heartbeat extends exclusion', async () => {
  const h = await prepared(), job = await queued(h);
  const { job: lease } = await h.service.claimJob(worker, BIZ, job.id);
  await assert.rejects(() => h.service.claimJob(worker2, BIZ, job.id), errorCode('LEASE_BUSY'));
  h.advance(900); await h.service.heartbeat(worker, BIZ, job.id, lease.fencingToken); h.advance(200);
  await assert.rejects(() => h.service.claimJob(worker2, BIZ, job.id), errorCode('LEASE_BUSY'));
});
test('expired lease gets a higher fencing token and rejects late worker commit', async () => {
  const h = await prepared(), job = await queued(h), first = await candidate(h, job);
  h.advance(1001); const second = await h.service.claimJob(worker2, BIZ, job.id);
  assert.ok(second.job.fencingToken > first.fencingToken);
  await assert.rejects(() => h.service.completeJob(worker, BIZ, job.id, first), errorCode('STALE_WORKER'));
  const site = await h.service.buildCandidate(worker2, BIZ, job.id, second.job.fencingToken);
  await h.service.completeJob(worker2, BIZ, job.id, { site, fencingToken: second.job.fencingToken });
});
test('interrupted job recovers after process restart from persisted state', async () => {
  const h = await prepared(), job = await queued(h); await candidate(h, job);
  h.advance(1001);
  const store = new MemoryStore(h.store.snapshot()), restarted = new SiteV3Service({ ...h.config, store });
  const recovered = await restarted.recoverExpiredJobs(worker2);
  assert.equal(recovered.length, 1); assert.equal(recovered[0].outcome, 'claimed');
  const lease = recovered[0].job;
  const site = await restarted.buildCandidate(worker2, BIZ, job.id, lease.fencingToken);
  await restarted.completeJob(worker2, BIZ, job.id, { site, fencingToken: lease.fencingToken });
  assert.equal((await restarted.getJob(owner, BIZ, job.id)).state, 'ready');
});
test('recovery terminates explicitly after configured maximum attempts', async () => {
  const h = await prepared({ maxAttempts: 1 }), job = await queued(h); await candidate(h, job); h.advance(1001);
  const result = await h.service.recoverExpiredJobs(worker2);
  assert.equal(result[0].outcome, 'failed'); assert.equal((await h.service.getJob(owner, BIZ, job.id)).state, 'failed');
  assert.equal(await h.store.get(`PublicSitesV3/${BIZ}`), null);
});
test('editing while a job runs supersedes it and preserves any previous publication', async () => {
  const h = await prepared(), job = await queued(h), c = await candidate(h, job);
  const b = structuredClone(h.brief); b.expectedInputRevision = 2; b.business.name = 'Nueva propuesta ficticia';
  await h.service.saveBrief(owner, BIZ, b);
  await assert.rejects(() => h.service.completeJob(worker, BIZ, job.id, c), errorCode('STALE_WORKER'));
  assert.equal((await h.service.claimJob(worker2, BIZ, job.id)).outcome, 'superseded');
  assert.equal(await h.store.get(`PublicSitesV3/${BIZ}`), null);
});
test('incomplete and fabricated result never becomes ready or public', async () => {
  const h = await prepared(), job = await queued(h), c = await candidate(h, job);
  const bad = structuredClone(c); bad.site.content.offerings[0].description = 'Garantía ilimitada y stock disponible';
  await assert.rejects(() => h.service.completeJob(worker, BIZ, job.id, bad), errorCode('UNCONFIRMED_PUBLIC_CONTENT'));
  assert.equal((await h.service.getJob(owner, BIZ, job.id)).state, 'planning');
  assert.equal(await h.store.get(`PublicSitesV3/${BIZ}`), null);
  assert.equal(await h.store.get(`SiteV3Projects/${BIZ}/revisions/${c.site.revisionId}`), null);
});
test('complete is idempotent but cannot overwrite an immutable revision', async () => {
  const h = await prepared(), c = await ready(h);
  assert.equal((await h.service.completeJob(worker, BIZ, c.job.id, c)).duplicate, true);
  c.site.business.description = 'Otra cosa';
  await assert.rejects(() => h.service.completeJob(worker, BIZ, c.job.id, c), errorCode('STALE_WORKER'));
});
test('only a ready, approved revision publishes; retry does not duplicate and projection omits private state', async () => {
  const h = await prepared(), c = await ready(h);
  const input = { revisionId: c.site.revisionId, expectedInputRevision: 2, expectedPublishedRevisionId: null };
  await assert.rejects(() => h.service.publish(owner, BIZ, input), errorCode('REVISION_NOT_APPROVED'));
  await h.service.approve(owner, BIZ, input);
  const both = await Promise.all([h.service.publish(owner, BIZ, input), h.service.publish(owner, BIZ, input)]);
  assert.equal(both.filter(x => !x.duplicate).length, 1);
  const projection = await h.store.get(`PublicSitesV3/${BIZ}`);
  assert.deepEqual(projection, c.site);
  for (const key of ['facts', 'brief', 'leadPhone', 'pin', 'leaseOwner', 'approvedAt']) assert.equal(Object.hasOwn(projection, key), false);
});
test('new draft leaves approved/public snapshot unchanged and rejects stale publication', async () => {
  const h = await prepared(), c = await ready(h), input = { revisionId: c.site.revisionId, expectedInputRevision: 2, expectedPublishedRevisionId: null };
  await h.service.approve(owner, BIZ, input); await h.service.publish(owner, BIZ, input);
  const before = await h.store.get(`PublicSitesV3/${BIZ}`), b = structuredClone(h.brief); b.expectedInputRevision = 2;
  await h.service.saveBrief(owner, BIZ, b);
  await assert.rejects(() => h.service.publish(owner, BIZ, input), errorCode('STALE_INPUT'));
  assert.deepEqual(await h.store.get(`PublicSitesV3/${BIZ}`), before);
});
test('publication transaction failure rolls back both safe projection and pointer', async () => {
  const h = await prepared(), c = await ready(h), input = { revisionId: c.site.revisionId, expectedInputRevision: 2, expectedPublishedRevisionId: null };
  await h.service.approve(owner, BIZ, input);
  const run = h.store.runTransaction.bind(h.store);
  h.store.runTransaction = fn => run(tx => fn({ ...tx, set: (path, value) => { if (path.startsWith('SiteV3Projects/')) throw new Error('simulated commit failure'); tx.set(path, value); } }));
  await assert.rejects(() => h.service.publish(owner, BIZ, input), /simulated commit failure/);
  assert.equal(await h.store.get(`PublicSitesV3/${BIZ}`), null);
  assert.equal((await h.store.get(`SiteV3Projects/${BIZ}`)).publishedRevisionId, null);
});
test('plan activation and image executor cannot spend or mutate legacy state in F0', async () => {
  const h = await prepared(), before = h.store.snapshot();
  await assert.rejects(() => h.service.activate(owner, BIZ, { paid: true }), errorCode('FORBIDDEN'));
  await assert.rejects(() => h.service.activate(billing, BIZ, { paid: true }), errorCode('VERIFIED_PAYMENT_ADAPTER_REQUIRED'));
  await assert.rejects(() => h.service.validateImage(foreign, BIZ, fixture('image-request')), errorCode('FORBIDDEN'));
  await assert.rejects(() => h.service.validateImage(owner, BIZ, fixture('image-request')), errorCode('TENANT_MISMATCH'));
  assert.deepEqual(h.store.snapshot(), before);
});
test('duplicate click transports create one owner event and no visitor/payment event', async () => {
  const h = await prepared({ phone: true }), c = await ready(h);
  const e = { ...fixture('commercial-event'), revisionId: c.site.revisionId };
  const context = { sampleId: e.sampleId, audience: e.audience, variantId: e.variantId };
  const results = await Promise.all([h.service.recordEvent(owner, BIZ, e, context), h.service.recordEvent(owner, BIZ, e, context)]);
  assert.equal(results.filter(x => !x.duplicate).length, 1);
  assert.equal(Object.keys(h.store.snapshot()).filter(k => k.startsWith('WebSampleEvents/')).length, 1);
  assert.equal(Object.keys(h.store.snapshot()).filter(k => k.startsWith('BusinessSiteEvents/')).length, 0);
});
test('Firestore adapter is injected, performs no initialization, and uses native atomic create/set', async () => {
  const calls = [], docs = new Map();
  const db = { doc: path => ({ path, get: async () => ({ exists: docs.has(path), data: () => docs.get(path) }) }), runTransaction: async fn => fn({
    get: async ref => ({ exists: docs.has(ref.path), data: () => docs.get(ref.path) }),
    create: (ref, data) => { calls.push(['create', ref.path]); docs.set(ref.path, data); },
    set: (ref, data) => { calls.push(['set', ref.path]); docs.set(ref.path, data); },
  }) };
  const adapter = new FirestoreStore(db);
  await adapter.runTransaction(async tx => { assert.equal(await tx.get('demo/one'), null); tx.create('demo/one', { ok: true }); });
  assert.deepEqual(await adapter.get('demo/one'), { ok: true }); assert.deepEqual(calls, [['create', 'demo/one']]);
});
test('an enabled service still cannot publish without an available renderer manifest', async () => {
  const h = await prepared(), c = await ready(h), input = { revisionId: c.site.revisionId, expectedInputRevision: 2, expectedPublishedRevisionId: null };
  await h.service.approve(owner, BIZ, input);
  const withoutRenderer = new SiteV3Service({ ...h.config, rendererVersions: [] });
  await assert.rejects(() => withoutRenderer.publish(owner, BIZ, input), errorCode('RENDERER_NOT_AVAILABLE'));
  assert.equal(await h.store.get(`PublicSitesV3/${BIZ}`), null);
});
