// utils/mailboxAuth.js
// Hash de contraseñas de buzón (scrypt) y tokens de sesión (HMAC-SHA256) usando
// solo el módulo `crypto` nativo — sin dependencias externas.
import crypto from 'node:crypto';

export function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const derived = crypto.scryptSync(String(password), salt, 64).toString('hex');
  return `scrypt$${salt}$${derived}`;
}

export function verifyPassword(password, stored) {
  const parts = String(stored || '').split('$');
  if (parts.length !== 3 || parts[0] !== 'scrypt') return false;
  const [, salt, expected] = parts;
  let derived;
  try {
    derived = crypto.scryptSync(String(password), salt, 64).toString('hex');
  } catch {
    return false;
  }
  const a = Buffer.from(expected, 'hex');
  const b = Buffer.from(derived, 'hex');
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function base64url(input) {
  return Buffer.from(input)
    .toString('base64')
    .replace(/=+$/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

function fromBase64url(str) {
  const padded = String(str).replace(/-/g, '+').replace(/_/g, '/');
  return Buffer.from(padded, 'base64').toString('utf8');
}

function hmac(secret, data) {
  return crypto.createHmac('sha256', secret).update(data).digest('base64')
    .replace(/=+$/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

export function signMailboxToken(payload, { secret, expiresInSeconds = 60 * 60 * 12 } = {}) {
  if (!secret) throw new Error('signMailboxToken requiere secret');
  const now = Math.floor(Date.now() / 1000);
  const body = { ...payload, iat: now, exp: now + Number(expiresInSeconds || 0) };
  const data = base64url(JSON.stringify(body));
  const sig = hmac(secret, data);
  return `${data}.${sig}`;
}

export function verifyMailboxToken(token, { secret } = {}) {
  if (!secret) return null;
  const [data, sig] = String(token || '').split('.');
  if (!data || !sig) return null;
  const expected = hmac(secret, data);
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
  let payload;
  try {
    payload = JSON.parse(fromBase64url(data));
  } catch {
    return null;
  }
  if (!payload || typeof payload !== 'object') return null;
  if (payload.exp && Math.floor(Date.now() / 1000) > Number(payload.exp)) return null;
  return payload;
}
