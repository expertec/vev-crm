import { fail, SiteV3Error } from './contracts.mjs';

const PERMISSIONS = Object.freeze({
  owner: ['create', 'read', 'edit', 'confirm', 'process', 'images', 'approve', 'publish', 'event'],
  editor: ['read', 'edit', 'process', 'images'], viewer: ['read'],
  worker: ['work', 'recover'], billing: ['activate'],
});
export function safeId(id) { fail(typeof id === 'string' && /^[A-Za-z][A-Za-z0-9_-]{0,119}$/.test(id), 'INVALID_ID'); return id; }
export function firebaseIdTokenVerifier(auth) {
  fail(typeof auth?.verifyIdToken === 'function', 'AUTH_NOT_CONFIGURED', 503);
  return token => auth.verifyIdToken(token, true); // signature/audience/expiry + revocation via Admin SDK
}
export function createAuthorizer({ store, verifySession, now = Date.now }) {
  return async (request, negocioId, operation) => {
    safeId(negocioId);
    const header = request?.headers?.get?.('authorization') ?? request?.headers?.authorization;
    const match = typeof header === 'string' && header.match(/^Bearer ([^\s]+)$/i);
    fail(match, 'AUTH_REQUIRED', 401);
    fail(typeof verifySession === 'function', 'AUTH_NOT_CONFIGURED', 503);
    let decoded;
    try { decoded = await verifySession(match[1]); } catch { throw new SiteV3Error('INVALID_SESSION', 401); }
    fail(typeof decoded?.uid === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(decoded.uid) && Number.isFinite(decoded.exp) && decoded.exp * 1000 > now() && decoded.revoked !== true, 'INVALID_SESSION', 401);
    const membership = await store.get(`SiteV3Access/${negocioId}/members/${decoded.uid}`);
    fail(membership?.active === true && Array.isArray(membership.roles) && membership.roles.some(role => PERMISSIONS[role]?.includes(operation)), 'FORBIDDEN', 403);
    return { uid: decoded.uid, negocioId, operation };
  };
}
