import { admin, db as defaultDb } from '../../firebaseAdmin.js';

const { FieldValue } = admin.firestore;

function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

export async function recordSalesActivity({
  db = defaultDb,
  leadRef = null,
  leadId = '',
  type = '',
  agentId = '',
  metadata = {},
} = {}) {
  const safeType = cleanText(type, 80);
  const safeLeadId = cleanText(leadId, 220);
  const ref = leadRef || (safeLeadId ? db.collection('leads').doc(safeLeadId) : null);
  if (!ref || !safeType) return null;

  const payload = {
    type: safeType,
    agentId: cleanText(agentId, 180) || null,
    createdAt: FieldValue.serverTimestamp(),
    metadata: metadata && typeof metadata === 'object' && !Array.isArray(metadata) ? metadata : {},
  };
  const doc = await ref.collection('salesActivity').add(payload);
  return { id: doc.id, ...payload };
}
