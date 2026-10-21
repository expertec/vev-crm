import { fail, VERSIONS, validateContract } from './contracts.mjs';
import { createAuthorizer, safeId } from './access.mjs';
import { assertPublishable, compileFactualSite, digest, PHASE0_GRANTS, proposeFacts } from './facts.mjs';
import { validateBrief, validateCommercialEvent, validateImageRequest } from './validation.mjs';

const projectPath = id => `SiteV3Projects/${safeId(id)}`;
const jobPath = id => `SiteV3Jobs/${safeId(id)}`;
const revisionPath = (id, revisionId) => `${projectPath(id)}/revisions/${safeId(revisionId)}`;
const publicPath = id => `PublicSitesV3/${safeId(id)}`;
const approvalPath = (id, revisionId) => `${projectPath(id)}/approvals/${safeId(revisionId)}`;
const active = job => ['queued', 'planning', 'generating_copy', 'resolving_assets', 'validating'].includes(job?.state);

export class SiteV3Service {
  constructor({ store, verifySession, enabled = false, now = Date.now, leaseMs = 30000, maxAttempts = 3, grants = PHASE0_GRANTS, rendererVersions = [] } = {}) {
    fail(store, 'STORE_NOT_CONFIGURED', 503);
    fail(Number.isSafeInteger(leaseMs) && leaseMs >= 1000 && leaseMs <= 300000 && Number.isSafeInteger(maxAttempts) && maxAttempts >= 1 && maxAttempts <= 5, 'INVALID_WORKER_CONFIG');
    this.store = store; this.now = now; this.enabled = enabled === true; this.leaseMs = leaseMs; this.maxAttempts = maxAttempts;
    this.grants = { ...PHASE0_GRANTS, ...grants };
    fail(Array.isArray(rendererVersions) && rendererVersions.every(v => typeof v === 'string'), 'INVALID_RENDERER_MANIFEST');
    this.rendererVersions = [...rendererVersions];
    this.authorize = createAuthorizer({ store, verifySession, now });
  }
  async access(request, id, operation) { fail(this.enabled, 'V3_DISABLED', 503); return this.authorize(request, id, operation); }
  async createProject(request, negocioId) {
    const actor = await this.access(request, negocioId, 'create');
    return this.store.runTransaction(async tx => {
      const path = projectPath(negocioId), old = await tx.get(path);
      if (old) return { duplicate: true, inputRevision: old.inputRevision };
      tx.create(path, { negocioId, createdBy: actor.uid, inputRevision: 0, brief: null, facts: [], grants: this.grants, fence: 0, nextRevision: 1, activeJobId: null, draftRevisionId: null, approvedRevisionId: null, publishedRevisionId: null });
      return { duplicate: false, inputRevision: 0 };
    });
  }
  async saveBrief(request, negocioId, body) {
    await this.access(request, negocioId, 'edit'); validateBrief(body); proposeFacts(body);
    return this.store.runTransaction(async tx => {
      const path = projectPath(negocioId), p = await tx.get(path);
      fail(p, 'NOT_FOUND', 404); fail(body.expectedInputRevision === p.inputRevision, 'STALE_INPUT', 409);
      const next = { ...p, brief: structuredClone(body), facts: [], inputRevision: p.inputRevision + 1, fence: p.fence + 1, activeJobId: null, draftRevisionId: null, approvedRevisionId: null };
      tx.set(path, next); return { inputRevision: next.inputRevision, proposedFacts: proposeFacts(body) };
    });
  }
  async confirmFacts(request, negocioId, { expectedInputRevision, factIds }) {
    const actor = await this.access(request, negocioId, 'confirm');
    fail(Array.isArray(factIds) && factIds.length > 0 && factIds.length <= 300 && new Set(factIds).size === factIds.length, 'INVALID_FACT_SELECTION', 422);
    return this.store.runTransaction(async tx => {
      const path = projectPath(negocioId), p = await tx.get(path);
      fail(p?.brief, 'NOT_FOUND', 404); fail(expectedInputRevision === p.inputRevision, 'STALE_INPUT', 409);
      const proposed = proposeFacts(p.brief), selected = proposed.filter(f => factIds.includes(f.id));
      fail(selected.length === factIds.length && selected.every(f => f.origin === 'owner_declared'), 'FACT_NOT_CONFIRMABLE', 422);
      const facts = selected.map(f => ({ ...f, negocioId, source: 'user_confirmed', actorId: actor.uid, confirmedAt: this.now() }));
      const ids = new Set(factIds);
      tx.set(path, { ...p, facts: [...p.facts.filter(f => !ids.has(f.id)), ...facts], inputRevision: p.inputRevision + 1, fence: p.fence + 1, activeJobId: null, draftRevisionId: null, approvedRevisionId: null });
      return { inputRevision: p.inputRevision + 1 };
    });
  }
  async requestGeneration(request, negocioId, { expectedInputRevision }) {
    await this.access(request, negocioId, 'process');
    return this.store.runTransaction(async tx => {
      const ppath = projectPath(negocioId), p = await tx.get(ppath);
      fail(p?.brief, 'NOT_FOUND', 404); fail(expectedInputRevision === p.inputRevision, 'STALE_INPUT', 409);
      const inputHash = digest({ negocioId, inputRevision: p.inputRevision, brief: p.brief, facts: p.facts, grants: p.grants, versions: VERSIONS, compiler: 'phase0-factual-1' });
      const id = `job_${inputHash}`, path = jobPath(id), old = await tx.get(path);
      if (old) return { duplicate: true, job: old };
      fail(!p.activeJobId, 'JOB_ALREADY_ACTIVE', 409);
      // Validate before reserving the job; missing critical facts never become ready.
      compileFactualSite({ ...p, revisionId: `rev_${inputHash}`, revision: p.nextRevision });
      const job = { id, negocioId, inputRevision: p.inputRevision, inputHash, revisionId: `rev_${inputHash}`, revision: p.nextRevision, state: 'queued', leaseOwner: null, leaseExpiresAt: 0, fencingToken: 0, attempts: 0, createdAt: this.now() };
      tx.create(path, job); tx.set(ppath, { ...p, activeJobId: id, nextRevision: p.nextRevision + 1 });
      return { duplicate: false, job };
    });
  }
  async getJob(request, negocioId, jobId) {
    await this.access(request, negocioId, 'read');
    const job = await this.store.get(jobPath(jobId));
    fail(job && job.negocioId === negocioId, 'NOT_FOUND', 404);
    // Public status deliberately excludes facts, input, worker identity and tokens.
    return { jobId: job.id, revisionId: job.revisionId, state: job.state, attempts: job.attempts };
  }
  async claimJob(request, negocioId, jobId) {
    const actor = await this.access(request, negocioId, 'work');
    return this.store.runTransaction(async tx => {
      const ppath = projectPath(negocioId), p = await tx.get(ppath), path = jobPath(jobId), job = await tx.get(path);
      fail(p && job?.negocioId === negocioId, 'NOT_FOUND', 404);
      fail(active(job), 'JOB_NOT_ACTIVE', 409);
      if (job.inputRevision !== p.inputRevision || p.activeJobId !== jobId) {
        tx.set(path, { ...job, state: 'superseded', leaseExpiresAt: 0 }); return { outcome: 'superseded' };
      }
      fail(job.leaseExpiresAt <= this.now(), 'LEASE_BUSY', 423);
      if (job.attempts >= this.maxAttempts) {
        tx.set(path, { ...job, state: 'failed', errorCode: 'RECOVERY_EXHAUSTED', leaseExpiresAt: 0 });
        tx.set(ppath, { ...p, activeJobId: null }); return { outcome: 'failed' };
      }
      const claimed = { ...job, state: 'planning', leaseOwner: actor.uid, leaseExpiresAt: this.now() + this.leaseMs, fencingToken: p.fence + 1, attempts: job.attempts + 1 };
      tx.set(path, claimed); tx.set(ppath, { ...p, fence: claimed.fencingToken }); return { outcome: 'claimed', job: claimed };
    });
  }
  assertLease(p, job, actor, token) {
    fail(p && job && job.negocioId === actor.negocioId, 'NOT_FOUND', 404);
    fail(active(job) && p.activeJobId === job.id && job.inputRevision === p.inputRevision && job.fencingToken === token && p.fence === token && job.leaseOwner === actor.uid && job.leaseExpiresAt > this.now(), 'STALE_WORKER', 409);
  }
  async heartbeat(request, negocioId, jobId, fencingToken) {
    const actor = await this.access(request, negocioId, 'work');
    return this.store.runTransaction(async tx => {
      const p = await tx.get(projectPath(negocioId)), path = jobPath(jobId), job = await tx.get(path);
      this.assertLease(p, job, actor, fencingToken);
      const next = { ...job, leaseExpiresAt: this.now() + this.leaseMs }; tx.set(path, next); return next.leaseExpiresAt;
    });
  }
  async buildCandidate(request, negocioId, jobId, fencingToken) {
    const actor = await this.access(request, negocioId, 'work');
    const p = await this.store.get(projectPath(negocioId)), job = await this.store.get(jobPath(jobId));
    this.assertLease(p, job, actor, fencingToken);
    return compileFactualSite({ ...p, revisionId: job.revisionId, revision: job.revision });
  }
  async completeJob(request, negocioId, jobId, { fencingToken, site }) {
    const actor = await this.access(request, negocioId, 'work');
    return this.store.runTransaction(async tx => {
      const ppath = projectPath(negocioId), p = await tx.get(ppath), path = jobPath(jobId), job = await tx.get(path);
      fail(p && job?.negocioId === negocioId, 'NOT_FOUND', 404);
      const rpath = revisionPath(negocioId, job.revisionId), old = await tx.get(rpath);
      if (job.state === 'ready') {
        fail(job.leaseOwner === actor.uid && job.fencingToken === fencingToken && old?.contentHash === digest(site), 'STALE_WORKER', 409);
        return { duplicate: true, revisionId: old.id };
      }
      this.assertLease(p, job, actor, fencingToken);
      fail(site.revisionId === job.revisionId && site.revision === job.revision, 'REVISION_MISMATCH', 409);
      assertPublishable(site, p);
      tx.create(rpath, { id: job.revisionId, negocioId, inputRevision: p.inputRevision, state: 'ready', compilerVersion: 'phase0-factual-1', schema: structuredClone(site), contentHash: digest(site), createdAt: this.now() });
      tx.set(path, { ...job, state: 'ready', leaseExpiresAt: 0, finishedAt: this.now() });
      tx.set(ppath, { ...p, activeJobId: null, draftRevisionId: job.revisionId });
      return { duplicate: false, revisionId: job.revisionId };
    });
  }
  async recoverExpiredJobs(request, { limit = 25 } = {}) {
    fail(Number.isSafeInteger(limit) && limit > 0 && limit <= 100, 'INVALID_LIMIT');
    fail(this.enabled, 'V3_DISABLED', 503);
    const jobs = await this.store.expiredJobs(this.now(), limit), results = [];
    for (const job of jobs) {
      try {
        await this.access(request, job.negocioId, 'recover');
        results.push({ jobId: job.id, ...(await this.claimJob(request, job.negocioId, job.id)) });
      } catch (error) {
        if (!['FORBIDDEN', 'LEASE_BUSY', 'JOB_NOT_ACTIVE'].includes(error.code)) throw error;
      }
    }
    return results;
  }
  async approve(request, negocioId, { revisionId, expectedInputRevision }) {
    const actor = await this.access(request, negocioId, 'approve');
    return this.store.runTransaction(async tx => {
      const ppath = projectPath(negocioId), p = await tx.get(ppath), r = await tx.get(revisionPath(negocioId, revisionId));
      const path = approvalPath(negocioId, revisionId), old = await tx.get(path);
      fail(p && r?.state === 'ready', 'REVISION_NOT_READY', 409);
      fail(p.inputRevision === expectedInputRevision && r.inputRevision === p.inputRevision && p.draftRevisionId === revisionId, 'STALE_INPUT', 409);
      assertPublishable(r.schema, p); fail(digest(r.schema) === r.contentHash, 'REVISION_TAMPERED', 409);
      if (!old) tx.create(path, { revisionId, negocioId, actorId: actor.uid, inputRevision: p.inputRevision, contentHash: r.contentHash, approvedAt: this.now() });
      tx.set(ppath, { ...p, approvedRevisionId: revisionId }); return { duplicate: Boolean(old), revisionId };
    });
  }
  async publish(request, negocioId, { revisionId, expectedInputRevision, expectedPublishedRevisionId }) {
    await this.access(request, negocioId, 'publish');
    return this.store.runTransaction(async tx => {
      const ppath = projectPath(negocioId), p = await tx.get(ppath), r = await tx.get(revisionPath(negocioId, revisionId));
      const approval = await tx.get(approvalPath(negocioId, revisionId));
      fail(p && r?.state === 'ready' && approval, 'REVISION_NOT_APPROVED', 409);
      fail(this.rendererVersions.includes(r.schema.rendererVersion), 'RENDERER_NOT_AVAILABLE', 503);
      fail(p.inputRevision === expectedInputRevision && r.inputRevision === p.inputRevision && p.approvedRevisionId === revisionId, 'STALE_INPUT', 409);
      fail(approval.contentHash === r.contentHash && digest(r.schema) === r.contentHash, 'REVISION_TAMPERED', 409);
      assertPublishable(r.schema, p);
      if (p.publishedRevisionId === revisionId) return { duplicate: true, revisionId };
      fail(p.publishedRevisionId === expectedPublishedRevisionId, 'PUBLISH_CONFLICT', 409);
      // One atomic commit writes both pointer and the complete safe projection.
      tx.set(publicPath(negocioId), structuredClone(r.schema));
      tx.set(ppath, { ...p, publishedRevisionId: revisionId }); return { duplicate: false, revisionId };
    });
  }
  async validateImage(request, negocioId, imageRequest) {
    await this.access(request, negocioId, 'images');
    validateContract('ImageRequestV3', imageRequest);
    const p = await this.store.get(projectPath(negocioId));
    fail(p?.brief, 'NOT_FOUND', 404);
    const reservation = await this.store.get(`${projectPath(negocioId)}/budgets/${safeId(imageRequest.budgetReservationId)}`);
    validateImageRequest(imageRequest, { negocioId, inputRevision: p.inputRevision, revisionId: p.draftRevisionId, sampleId: p.sampleId, contentIds: ['business', ...p.brief.offerings.map(o => o.id)], reservation });
    fail(false, 'IMAGE_EXECUTOR_DISABLED', 503); // no spend or remote generation in F0
  }
  async activate(request, negocioId) {
    await this.access(request, negocioId, 'activate');
    fail(false, 'VERIFIED_PAYMENT_ADAPTER_REQUIRED', 503); // body.plan/body.paid never authorize billing
  }
  async recordEvent(request, negocioId, event, trustedContext) {
    // Internal authenticated path only in F0. Future public ingestion must resolve
    // signed context itself, rather than forwarding browser experiment labels.
    await this.access(request, negocioId, 'event');
    validateContract('CommercialEventV1', event);
    return this.store.runTransaction(async tx => {
      const r = await tx.get(revisionPath(negocioId, event.revisionId));
      fail(r, 'NOT_FOUND', 404);
      validateCommercialEvent(event, { ...trustedContext, site: r.schema, now: this.now(), serverEvidence: false });
      const collection = event.audience === 'sample_owner' ? 'WebSampleEvents' : 'BusinessSiteEvents';
      const path = `${collection}/${digest({ negocioId, eventId: event.eventId })}`, old = await tx.get(path);
      if (old) { fail(old.payloadHash === digest(event), 'EVENT_ID_REUSED', 409); return { duplicate: true }; }
      tx.create(path, { negocioId, event: structuredClone(event), payloadHash: digest(event), receivedAt: this.now() }); return { duplicate: false };
    });
  }
}
