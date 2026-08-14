import express from 'express';
import { admin, db } from '../firebaseAdmin.js';
import {
  assignLeadToAgent,
  claimNextLead,
  getAgentQueueStats,
  getNextAgentWork,
  listSalesAgents,
  registerAgentOutcome,
  unassignLead,
  upsertSalesAgent,
} from '../services/salesQueue/index.js';

function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function readBearer(req) {
  const header = String(req.get('authorization') || '').trim();
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match?.[1] || '';
}

function normalizeRole(value = '') {
  const role = String(value || '').trim();
  const lowered = role.toLowerCase();
  if (lowered === 'superadmin' || lowered === 'super_admin') return 'superAdmin';
  if (lowered === 'salesagent' || lowered === 'sales_agent') return 'salesAgent';
  if (lowered === 'admin') return 'admin';
  return role;
}

async function loadUserAccess(uid = '') {
  const safeUid = cleanText(uid, 180);
  if (!safeUid) return {};
  const snap = await db.collection('users').doc(safeUid).get().catch(() => null);
  if (!snap?.exists) return {};
  const data = snap.data() || {};
  return {
    role: normalizeRole(data.role || ''),
    permissions: data.permissions && typeof data.permissions === 'object' ? data.permissions : {},
    active: data.active !== false,
    displayName: cleanText(data.displayName || data.name || '', 180),
    email: cleanText(data.email || '', 180),
  };
}

async function resolveAgent(req) {
  const token = readBearer(req);
  if (token) {
    try {
      const decoded = await admin.auth().verifyIdToken(token);
      const access = await loadUserAccess(decoded.uid);
      return {
        uid: decoded.uid,
        name: cleanText(access.displayName || decoded.name || decoded.email || req.body?.agentName || '', 180) || decoded.uid,
        email: cleanText(access.email || decoded.email || '', 180),
        role: access.role || '',
        permissions: access.permissions || {},
        active: access.active !== false,
      };
    } catch {
      // El CRM actual usa varios endpoints sin Bearer; se mantiene fallback por compatibilidad.
    }
  }

  const uid = cleanText(req.body?.agentUid || req.get('x-user-id') || '', 180);
  const access = await loadUserAccess(uid);
  return {
    uid,
    name: cleanText(access.displayName || req.body?.agentName || req.get('x-user-name') || req.get('x-user-email') || uid, 180),
    email: cleanText(access.email || req.get('x-user-email') || '', 180),
    role: access.role || '',
    permissions: access.permissions || {},
    active: access.active !== false,
  };
}

function canManageSalesAgents(agent = {}) {
  return normalizeRole(agent.role) === 'superAdmin' || agent.permissions?.canManageAgents === true;
}

function canAssignLeads(agent = {}) {
  const role = normalizeRole(agent.role);
  if (!role) return true;
  return role === 'superAdmin'
    || role === 'admin'
    || agent.permissions?.canAssignLeads === true;
}

function canClaimGeneralLeads(agent = {}) {
  const role = normalizeRole(agent.role);
  if (!role) return true;
  if (role !== 'salesAgent') return true;
  return agent.permissions?.canAssignLeads === true || agent.permissions?.canViewAllLeads === true;
}

function assertForbidden(message = 'No tienes permiso para esta acción.') {
  const error = new Error(message);
  error.statusCode = 403;
  throw error;
}

async function resolveAuthUidForSalesAgent({
  uid = '',
  email = '',
  password = '',
  name = '',
} = {}) {
  const safeUid = cleanText(uid, 180);
  const safeEmail = cleanText(email, 180).toLowerCase();
  const safeName = cleanText(name, 180);
  const safePassword = String(password || '').trim();

  if (safeUid) return safeUid;
  if (!safeEmail) return '';

  try {
    const existing = await admin.auth().getUserByEmail(safeEmail);
    if (safeName && existing.displayName !== safeName) {
      await admin.auth().updateUser(existing.uid, { displayName: safeName }).catch(() => {});
    }
    return existing.uid;
  } catch (error) {
    if (error?.code !== 'auth/user-not-found') throw error;
  }

  const userRecord = await admin.auth().createUser({
    email: safeEmail,
    displayName: safeName || safeEmail,
    ...(safePassword ? { password: safePassword } : {}),
  });
  return userRecord.uid;
}

async function syncSalesAgentUserDoc({
  uid = '',
  name = '',
  email = '',
  phone = '',
  active = true,
  permissions = {},
  updatedBy = '',
} = {}) {
  const safeUid = cleanText(uid, 180);
  if (!safeUid) return;
  await db.collection('users').doc(safeUid).set({
    role: 'salesAgent',
    displayName: cleanText(name, 180),
    name: cleanText(name, 180),
    email: cleanText(email, 180).toLowerCase(),
    phone: cleanText(phone, 40),
    active: active !== false,
    permissions: {
      canViewAllLeads: permissions?.canViewAllLeads === true,
      canAssignLeads: permissions?.canAssignLeads === true,
      canManageAgents: false,
      canUseSharedWhatsapp: permissions?.canUseSharedWhatsapp !== false,
    },
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedBy: cleanText(updatedBy, 180),
  }, { merge: true });
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

  router.get('/crm/sales-agents', async (req, res) => {
    try {
      const agents = await listSalesAgents({
        db,
        includeInactive: String(req.query?.includeInactive || '').toLowerCase() === 'true',
      });
      return res.json({ success: true, agents });
    } catch (error) {
      logger.error('[sales-agents:list] Error:', error?.message || error);
      return sendError(res, error);
    }
  });

  router.post('/crm/sales-agents', async (req, res) => {
    try {
      const actor = await resolveAgent(req);
      if (!canManageSalesAgents(actor)) assertForbidden('Solo superadmin puede registrar agentes.');
      const uid = await resolveAuthUidForSalesAgent({
        uid: req.body?.agentUid || req.body?.uid || req.body?.id,
        email: req.body?.email,
        password: req.body?.password,
        name: req.body?.name || req.body?.displayName,
      });
      const agent = await upsertSalesAgent({
        db,
        agentUid: uid,
        name: req.body?.name || req.body?.displayName,
        email: req.body?.email,
        phone: req.body?.phone,
        role: req.body?.role || 'sales_agent',
        active: req.body?.active !== false,
        permissions: req.body?.permissions || {},
        updatedBy: actor.uid,
      });
      await syncSalesAgentUserDoc({
        uid: agent.uid,
        name: agent.name,
        email: agent.email,
        phone: agent.phone,
        active: agent.active,
        permissions: req.body?.permissions || {},
        updatedBy: actor.uid,
      });
      return res.json({ success: true, agent });
    } catch (error) {
      logger.error('[sales-agents:upsert] Error:', error?.message || error);
      return sendError(res, error);
    }
  });

  router.patch('/crm/sales-agents/:agentUid', async (req, res) => {
    try {
      const actor = await resolveAgent(req);
      if (!canManageSalesAgents(actor)) assertForbidden('Solo superadmin puede editar agentes.');
      const agent = await upsertSalesAgent({
        db,
        agentUid: req.params.agentUid,
        name: req.body?.name || req.body?.displayName,
        email: req.body?.email,
        phone: req.body?.phone,
        role: req.body?.role || 'sales_agent',
        active: req.body?.active !== false,
        permissions: req.body?.permissions || {},
        updatedBy: actor.uid,
      });
      await syncSalesAgentUserDoc({
        uid: agent.uid,
        name: agent.name,
        email: agent.email,
        phone: agent.phone,
        active: agent.active,
        permissions: req.body?.permissions || {},
        updatedBy: actor.uid,
      });
      return res.json({ success: true, agent });
    } catch (error) {
      logger.error('[sales-agents:update] Error:', error?.message || error);
      return sendError(res, error);
    }
  });

  router.post('/crm/leads/:leadId/assign', async (req, res) => {
    try {
      const actor = await resolveAgent(req);
      if (!canAssignLeads(actor)) assertForbidden('No tienes permiso para asignar leads.');
      const targetAgent = cleanText(req.body?.agentUid || req.body?.assignedTo || '', 180);
      if (!targetAgent) {
        const result = await unassignLead({
          db,
          leadId: req.params.leadId,
          assignedBy: actor.uid,
          reason: req.body?.reason || 'manual_unassignment',
        });
        return res.json({ success: true, assigned: false, ...result });
      }

      const result = await assignLeadToAgent({
        db,
        leadId: req.params.leadId,
        agentUid: targetAgent,
        assignedBy: actor.uid,
        assignedByName: actor.name,
        reason: req.body?.reason || 'manual_assignment',
      });
      return res.json({ success: true, assigned: true, ...result });
    } catch (error) {
      logger.error('[lead:assign] Error:', error?.message || error);
      return sendError(res, error);
    }
  });

  router.delete('/crm/leads/:leadId/assign', async (req, res) => {
    try {
      const actor = await resolveAgent(req);
      if (!canAssignLeads(actor)) assertForbidden('No tienes permiso para desasignar leads.');
      const result = await unassignLead({
        db,
        leadId: req.params.leadId,
        assignedBy: actor.uid,
        reason: req.body?.reason || 'manual_unassignment',
      });
      return res.json({ success: true, assigned: false, ...result });
    } catch (error) {
      logger.error('[lead:unassign] Error:', error?.message || error);
      return sendError(res, error);
    }
  });

  router.post('/crm/agent-queue/stats', async (req, res) => {
    try {
      const agent = await resolveAgent(req);
      const stats = await getAgentQueueStats({
        db,
        agentUid: agent.uid,
        includeGeneral: canClaimGeneralLeads(agent),
      });
      return res.json({ success: true, stats });
    } catch (error) {
      logger.error('[agent-queue/stats] Error:', error?.message || error);
      return sendError(res, error);
    }
  });

  router.post('/crm/agent-queue/claim-next', async (req, res) => {
    try {
      const agent = await resolveAgent(req);
      const result = await claimNextLead({
        db,
        agentUid: agent.uid,
        agentName: agent.name,
        allowGeneralClaim: canClaimGeneralLeads(agent),
      });
      return res.json({ success: true, ...result });
    } catch (error) {
      logger.error('[agent-queue/claim-next] Error:', error?.message || error);
      return sendError(res, error);
    }
  });

  router.post('/crm/agent-queue/next', async (req, res) => {
    try {
      const agent = await resolveAgent(req);
      const result = await getNextAgentWork({
        db,
        agentUid: agent.uid,
        agentName: agent.name,
        allowGeneralClaim: canClaimGeneralLeads(agent),
      });
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
