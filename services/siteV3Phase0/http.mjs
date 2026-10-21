import { fail, SiteV3Error } from './contracts.mjs';

// Framework-neutral Express-compatible adapters. Deliberately NOT mounted by
// server.js or Next. Composition must inject the verified-session service.
// No handler accepts membership, grants, worker results or billing evidence.
export function createSiteV3Handlers(service) {
  const wrap = (keys, operation) => async (req, res) => {
    try {
      const body = req.body ?? {};
      fail(body && typeof body === 'object' && !Array.isArray(body) && Object.keys(body).every(key => keys.includes(key)) && keys.every(key => Object.hasOwn(body, key)), 'INVALID_REQUEST', 422);
      const data = await operation(req, req.params?.negocioId, body);
      return res.status(200).json({ success: true, data });
    } catch (error) {
      const known = error instanceof SiteV3Error;
      return res.status(known ? error.status : 500).json({ success: false, error: { code: known ? error.code : 'INTERNAL_ERROR' } });
    }
  };
  return {
    create: wrap([], (req, id) => service.createProject(req, id)),
    edit: wrap(['brief'], (req, id, body) => service.saveBrief(req, id, body.brief)),
    confirm: wrap(['expectedInputRevision', 'factIds'], (req, id, body) => service.confirmFacts(req, id, body)),
    process: wrap(['expectedInputRevision'], async (req, id, body) => {
      const result = await service.requestGeneration(req, id, body);
      return { duplicate: result.duplicate, job: { id: result.job.id, revisionId: result.job.revisionId, state: result.job.state } };
    }),
    status: wrap([], (req, id) => service.getJob(req, id, req.params?.jobId)),
    images: wrap(['imageRequest'], (req, id, body) => service.validateImage(req, id, body.imageRequest)),
    activate: wrap([], (req, id) => service.activate(req, id)),
    approve: wrap(['revisionId', 'expectedInputRevision'], (req, id, body) => service.approve(req, id, body)),
    publish: wrap(['revisionId', 'expectedInputRevision', 'expectedPublishedRevisionId'], (req, id, body) => service.publish(req, id, body)),
  };
}
