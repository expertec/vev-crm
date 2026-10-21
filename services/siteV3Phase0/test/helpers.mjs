import { readFileSync } from 'node:fs';
import { MemoryStore } from '../store.mjs';
import { SiteV3Service } from '../service.mjs';
import { proposeFacts } from '../facts.mjs';

export const fixture = name => JSON.parse(readFileSync(new URL(`../fixtures/${name}.json`, import.meta.url), 'utf8'));
export const BIZ = 'biz_demo_hardware';
export const OTHER = 'biz_demo_other';
export const request = token => ({ headers: { authorization: `Bearer ${token}` } });
export const owner = request('owner');
export const worker = request('worker');
export const worker2 = request('worker2');
export const foreign = request('foreign');
export const editor = request('editor');
export const billing = request('billing');
export const errorCode = code => error => error.code === code;
export function harness({ enabled = true, snapshot, grants, maxAttempts = 3 } = {}) {
  let time = Date.parse('2026-10-21T18:00:00.000Z');
  const members = Object.fromEntries([['owner', 'owner'], ['editor', 'editor'], ['worker', 'worker'], ['worker2', 'worker'], ['billing', 'billing'], ['viewer', 'viewer']].map(([uid, role]) => [`SiteV3Access/${BIZ}/members/${uid}`, { active: true, roles: [role] }]));
  members[`SiteV3Access/${OTHER}/members/foreign`] = { active: true, roles: ['owner'] };
  const store = new MemoryStore(snapshot ?? members);
  const verifySession = async token => {
    if (token === 'tampered') throw new Error('invalid signature');
    if (token === 'missingexp') return { uid: 'owner' };
    return { uid: token, exp: token === 'expired' ? time / 1000 - 1 : time / 1000 + 3600, revoked: token === 'revoked' };
  };
  // A synthetic manifest exercises publication protocol; no renderer bundle exists in F0.
  const config = { store, verifySession, enabled, now: () => time, leaseMs: 1000, maxAttempts, rendererVersions: ['3.0.0'], grants: grants ?? { catalog: true, whatsappLink: true, cartToWhatsApp: true } };
  const service = new SiteV3Service(config);
  return { store, service, config, now: () => time, advance: ms => { time += ms; } };
}
export async function prepared(options = {}) {
  const h = harness(options), brief = fixture('business-brief');
  if (options.phone) brief.contactDraft = { ...brief.contactDraft, country: 'US', phone: '+1 202 555 0100' }; // synthetic unit data; never contacted
  if (options.price) brief.offerings[0].price = { amountMinor: 25000, currency: 'MXN' };
  if (options.family === 'info') brief.offerings[0].kind = 'service';
  if (options.family === 'booking') { brief.offerings[0].kind = 'service'; brief.goal = 'appointment_request'; }
  await h.service.createProject(owner, BIZ);
  await h.service.saveBrief(owner, BIZ, brief);
  await h.service.confirmFacts(owner, BIZ, { expectedInputRevision: 1, factIds: proposeFacts(brief).map(f => f.id) });
  return { ...h, brief, inputRevision: 2 };
}
export async function queued(h) { return (await h.service.requestGeneration(owner, BIZ, { expectedInputRevision: h.inputRevision })).job; }
export async function candidate(h, job) {
  const claim = await h.service.claimJob(worker, BIZ, job.id);
  const site = await h.service.buildCandidate(worker, BIZ, job.id, claim.job.fencingToken);
  return { site, fencingToken: claim.job.fencingToken, job: claim.job };
}
export async function ready(h) {
  const job = await queued(h), result = await candidate(h, job);
  await h.service.completeJob(worker, BIZ, job.id, result);
  return result;
}
