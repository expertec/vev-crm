import express from 'express';
import { admin, db } from '../firebaseAdmin.js';
import {
  claimNextLead,
  getAgentQueueStats,
  getNextAgentWork,
  registerAgentOutcome,
} from '../services/salesQueue/index.js';

function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function readBearer(req) {
  const header = String(req.get('authorization') || '').trim();
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match?.[1] || '';
}

async function resolveAgent(req) {
  const token = readBearer(req);
  if (token) {
    try {
      const decoded = await admin.auth().verifyIdToken(token);
      return {
        uid: decoded.uid,
        name: cleanText(decoded.name || decoded.email || req.body?.agentName || '', 180) || decoded.uid,
        email: cleanText(decoded.email || '', 180),
      };
    } catch {
      // El CRM actual usa varios endpoints sin Bearer; se mantiene fallback por compatibilidad.
    }
  }

  const uid = cleanText(req.body?.agentUid || req.get('x-user-id') || '', 180);
  return {
    uid,
    name: cleanText(req.body?.agentName || req.get('x-user-name') || req.get('x-user-email') || uid, 180),
    email: cleanText(req.get('x-user-email') || '', 180),
  };
}

function sendError(res, error) {
  const status = Number.isInteger(error?.statusCode) ? error.statusCode : 500;
  return res.status(status).json({
    success: false,
    error: error?.message || String(error),
  });
}

export function createAgentQueueRouter({ logger = console } = {}) {
  const router = express.Router();

  router.post('/crm/agent-queue/stats', async (req, res) => {
    try {
      const agent = await resolveAgent(req);
      const stats = await getAgentQueueStats({ db, agentUid: agent.uid });
      return res.json({ success: true, stats });
    } catch (error) {
      logger.error('[agent-queue/stats] Error:', error?.message || error);
      return sendError(res, error);
    }
  });

  router.post('/crm/agent-queue/claim-next', async (req, res) => {
    try {
      const agent = await resolveAgent(req);
      const result = await claimNextLead({ db, agentUid: agent.uid, agentName: agent.name });
      return res.json({ success: true, ...result });
    } catch (error) {
      logger.error('[agent-queue/claim-next] Error:', error?.message || error);
      return sendError(res, error);
    }
  });

  router.post('/crm/agent-queue/next', async (req, res) => {
    try {
      const agent = await resolveAgent(req);
      const result = await getNextAgentWork({ db, agentUid: agent.uid, agentName: agent.name });
      return res.json({ success: true, ...result });
    } catch (error) {
      logger.error('[agent-queue/next] Error:', error?.message || error);
      return sendError(res, error);
    }
  });

  router.post('/crm/agent-queue/:leadId/outcome', async (req, res) => {
    try {
      const agent = await resolveAgent(req);
      const result = await registerAgentOutcome({
        db,
        leadId: req.params.leadId,
        agentUid: agent.uid,
        agentName: agent.name,
        outcome: req.body?.outcome,
        notes: req.body?.notes,
        followUpAt: req.body?.followUpAt,
        followUpReason: req.body?.followUpReason,
      });
      return res.json({ success: true, ...result });
    } catch (error) {
      logger.error('[agent-queue/outcome] Error:', error?.message || error);
      return sendError(res, error);
    }
  });

  return router;
}
