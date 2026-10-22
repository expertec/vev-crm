export function canGenerateAiSample(user, lead = {}) {
  if (!user || user.active === false) return false;
  const role = String(user.role || '').toLowerCase().replace(/_/g, '');
  if (role === 'superadmin' || role === 'admin') return true;
  return role === 'salesagent' && (user.permissions?.canViewAllLeads === true
    || lead.assignedTo === user.uid || lead.salesOwner === user.uid);
}

export async function readAiSampleUser(req, { admin, db }) {
  const match = String(req.headers.authorization || '').match(/^Bearer\s+(.+)$/i);
  if (!match) return null;
  try {
    const decoded = await admin.auth().verifyIdToken(match[1]);
    const snap = await db.collection('users').doc(decoded.uid).get();
    if (!snap.exists) return null;
    return { ...snap.data(), uid: decoded.uid };
  } catch { return null; }
}

export function isAiSampleAvailable(data = {}) {
  return data.sampleGenerator === 'ai-page-v1' && data.schema?.aiPage?.version === 'ai-page-v1' && !data.archivedAt && data.archived !== true;
}
