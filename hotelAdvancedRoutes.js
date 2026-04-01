import express from 'express';
import crypto from 'crypto';
import dayjs from 'dayjs';
import { Timestamp } from 'firebase-admin/firestore';
import { db } from './firebaseAdmin.js';
import { normalizarTelefono } from './pinUtils.js';
import { stripe } from './stripeConfig.js';

const APP_KEY_HOTEL = 'hotel_premium';
const CLIENT_REALM = 'cliente_portal';
const CLIENT_ROLE = 'cliente';
const JWT_ALG = 'HS256';
const DEFAULT_TOKEN_TTL_SECONDS = 60 * 60 * 12;

const BASE_HOSTS = new Set([
  'plataforma-nw.vercel.app',
  'negociosweb.mx',
  'www.negociosweb.mx',
  'localhost',
  '127.0.0.1',
  '0.0.0.0',
]);

const ROLE_OWNER = 'owner';
const ROLE_MANAGER = 'manager';
const ROLE_AGENT = 'agent';

function toLowerSafe(value) {
  return String(value || '').trim().toLowerCase();
}

function toNumberSafe(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function normalizeRole(value) {
  const role = toLowerSafe(value);
  if ([ROLE_OWNER, ROLE_MANAGER, ROLE_AGENT].includes(role)) return role;
  return ROLE_AGENT;
}

function normalizeCurrency(value) {
  const candidate = String(value || '').trim().toLowerCase();
  if (!candidate) return 'mxn';
  if (!/^[a-z]{3}$/.test(candidate)) return 'mxn';
  return candidate;
}

function normalizeDomain(value) {
  let domain = String(value || '').trim().toLowerCase();
  if (!domain) return '';
  domain = domain.replace(/^https?:\/\//, '');
  domain = domain.split('/')[0] || '';
  domain = domain.replace(/:\d+$/, '');
  domain = domain.replace(/\.$/, '');
  return domain;
}

function extractRequestHost(req) {
  const host =
    req.get('x-request-host') ||
    req.get('x-forwarded-host') ||
    req.get('host') ||
    '';

  const firstHost = String(host).split(',')[0] || '';
  return normalizeDomain(firstHost);
}

function isBaseHost(hostname) {
  const host = normalizeDomain(hostname);
  if (!host) return true;
  if (BASE_HOSTS.has(host)) return true;
  if (host.endsWith('.localhost')) return true;
  return false;
}

function parseTimestamp(value) {
  if (!value) return null;
  if (typeof value.toDate === 'function') return value.toDate();
  if (value instanceof Date) return value;
  if (value?.seconds) return new Date(value.seconds * 1000);
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function getNegocioExpiryDate(negocioData = {}) {
  return (
    parseTimestamp(negocioData.planRenewalDate) ||
    parseTimestamp(negocioData.planExpiresAt) ||
    parseTimestamp(negocioData.expiresAt) ||
    null
  );
}

function isPremiumActive(negocioData = {}) {
  if (!negocioData || typeof negocioData !== 'object') return false;

  const plan = toLowerSafe(negocioData.plan);
  if (plan !== 'premium') return false;

  if (negocioData.websiteArchived || negocioData.siteTemporarilyDisabled) {
    return false;
  }

  const expiresAt = getNegocioExpiryDate(negocioData);
  if (expiresAt && expiresAt.getTime() <= Date.now()) {
    return false;
  }

  return true;
}

function sanitizeAdvancedApp(input = {}) {
  const data = input && typeof input === 'object' ? input : {};
  return {
    key: String(data.key || '').trim() || null,
    status: String(data.status || '').trim() || null,
    config: data.config && typeof data.config === 'object' ? data.config : {},
    activatedAt: parseTimestamp(data.activatedAt),
    activatedBy: String(data.activatedBy || '').trim() || null,
    deactivatedAt: parseTimestamp(data.deactivatedAt),
    deactivatedBy: String(data.deactivatedBy || '').trim() || null,
  };
}

function isHotelAdvancedAppActive(negocioData = {}) {
  const app = sanitizeAdvancedApp(negocioData.advancedApp || {});
  const status = toLowerSafe(app.status || 'inactive');
  return app.key === APP_KEY_HOTEL && status === 'active';
}

async function readNegocioById(negocioId) {
  const safeNegocioId = String(negocioId || '').trim();
  if (!safeNegocioId) return null;

  const ref = db.collection('Negocios').doc(safeNegocioId);
  const snap = await ref.get();

  if (!snap.exists) return null;

  return {
    id: snap.id,
    ref,
    data: snap.data() || {},
  };
}

async function findNegocioByDomain(hostname) {
  const normalizedHost = normalizeDomain(hostname);
  if (!normalizedHost || isBaseHost(normalizedHost)) return null;

  const candidateHosts = new Set([normalizedHost]);
  if (normalizedHost.startsWith('www.')) {
    candidateHosts.add(normalizedHost.slice(4));
  } else {
    candidateHosts.add(`www.${normalizedHost}`);
  }

  const fields = ['dominio', 'domain', 'customDomain', 'custom_domain'];

  for (const field of fields) {
    for (const candidate of candidateHosts) {
      const snap = await db
        .collection('Negocios')
        .where(field, '==', candidate)
        .limit(1)
        .get();

      if (!snap.empty) {
        const docSnap = snap.docs[0];
        return {
          id: docSnap.id,
          ref: docSnap.ref,
          data: docSnap.data() || {},
        };
      }
    }
  }

  return null;
}

async function findNegocioBySlug(slug) {
  const normalizedSlug = String(slug || '').trim().toLowerCase();
  if (!normalizedSlug) return null;

  const snap = await db
    .collection('Negocios')
    .where('slug', '==', normalizedSlug)
    .limit(1)
    .get();

  if (snap.empty) return null;

  const docSnap = snap.docs[0];
  return {
    id: docSnap.id,
    ref: docSnap.ref,
    data: docSnap.data() || {},
  };
}

function jsonError(res, status, code, message, details) {
  return res.status(status).json({
    success: false,
    error: {
      code,
      message,
      ...(details !== undefined ? { details } : {}),
    },
  });
}

function base64UrlEncode(input) {
  const buffer = Buffer.isBuffer(input) ? input : Buffer.from(String(input));
  return buffer
    .toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

function base64UrlDecode(input) {
  const normalized = String(input || '')
    .replace(/-/g, '+')
    .replace(/_/g, '/');
  const pad = '='.repeat((4 - (normalized.length % 4)) % 4);
  return Buffer.from(normalized + pad, 'base64');
}

function getClientJwtSecret() {
  const candidates = [
    process.env.CLIENT_PORTAL_JWT_SECRET,
    process.env.SESSION_SECRET,
    process.env.JWT_SECRET,
    process.env.APP_JWT_SECRET,
    process.env.INTERNAL_API_SECRET,
    process.env.INTERNAL_API_KEY,
  ];
  const found = candidates.find((value) => String(value || '').trim());
  return String(found || '').trim();
}

function timingSafeEqualString(a, b) {
  const left = Buffer.from(String(a || ''));
  const right = Buffer.from(String(b || ''));
  if (left.length !== right.length) return false;
  return crypto.timingSafeEqual(left, right);
}

function signJwt(payload, { expiresInSeconds = DEFAULT_TOKEN_TTL_SECONDS } = {}) {
  const secret = getClientJwtSecret();
  if (!secret) {
    throw new Error('CLIENT_PORTAL_JWT_SECRET/SESSION_SECRET no configurado.');
  }

  const now = Math.floor(Date.now() / 1000);
  const exp = now + Math.max(60, Number(expiresInSeconds) || DEFAULT_TOKEN_TTL_SECONDS);

  const fullPayload = {
    ...payload,
    iat: now,
    exp,
  };

  const headerEncoded = base64UrlEncode(JSON.stringify({ alg: JWT_ALG, typ: 'JWT' }));
  const payloadEncoded = base64UrlEncode(JSON.stringify(fullPayload));
  const content = `${headerEncoded}.${payloadEncoded}`;
  const signature = crypto.createHmac('sha256', secret).update(content).digest();
  const signatureEncoded = base64UrlEncode(signature);

  return {
    token: `${content}.${signatureEncoded}`,
    exp,
    iat: now,
    expiresInSeconds: exp - now,
  };
}

function verifyJwt(token, { allowExpired = false } = {}) {
  const secret = getClientJwtSecret();
  if (!secret) {
    return { valid: false, reason: 'missing_secret' };
  }

  const parts = String(token || '').trim().split('.');
  if (parts.length !== 3) {
    return { valid: false, reason: 'invalid_format' };
  }

  const [headerEncoded, payloadEncoded, signatureEncoded] = parts;
  const content = `${headerEncoded}.${payloadEncoded}`;
  const expectedSignature = base64UrlEncode(
    crypto.createHmac('sha256', secret).update(content).digest()
  );

  if (!timingSafeEqualString(expectedSignature, signatureEncoded)) {
    return { valid: false, reason: 'bad_signature' };
  }

  let payload;
  try {
    payload = JSON.parse(base64UrlDecode(payloadEncoded).toString('utf8'));
  } catch {
    return { valid: false, reason: 'invalid_payload' };
  }

  const exp = Number(payload?.exp || 0);
  const now = Math.floor(Date.now() / 1000);

  if (!allowExpired && (!Number.isFinite(exp) || exp <= now)) {
    return { valid: false, reason: 'expired', payload };
  }

  return { valid: true, payload };
}

function readBearerToken(req) {
  const authHeader = String(req.get('authorization') || '').trim();
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

function normalizeAudience(aud) {
  if (Array.isArray(aud)) {
    return aud.map((entry) => toLowerSafe(entry)).filter(Boolean);
  }
  const value = toLowerSafe(aud);
  return value ? [value] : [];
}

function normalizeRoles(payload) {
  const role = toLowerSafe(payload?.role);
  const roles = Array.isArray(payload?.roles)
    ? payload.roles.map((entry) => toLowerSafe(entry)).filter(Boolean)
    : [];
  return role ? [role, ...roles] : roles;
}

function hasHotelAgentClaims(payload) {
  if (!payload || typeof payload !== 'object') return false;

  const aud = normalizeAudience(payload.aud);
  const realm = toLowerSafe(payload.realm);
  const roles = normalizeRoles(payload);
  const appKey = String(payload.appKey || '').trim();
  const negocioId = String(payload.negocioId || '').trim();
  const agentId = String(payload.agentId || '').trim();

  const hasRealm = realm === CLIENT_REALM || aud.includes(CLIENT_REALM);
  const hasRole = roles.includes(CLIENT_ROLE);

  return (
    hasRealm &&
    hasRole &&
    appKey === APP_KEY_HOTEL &&
    Boolean(negocioId) &&
    Boolean(agentId)
  );
}

function hashPin(pin, salt = crypto.randomBytes(16).toString('hex')) {
  const normalizedPin = String(pin || '').trim();
  const digest = crypto
    .createHmac('sha256', String(salt || ''))
    .update(normalizedPin)
    .digest('hex');

  return {
    pinHash: digest,
    pinSalt: salt,
  };
}

function comparePin(pin, agentData = {}) {
  const normalizedPin = String(pin || '').trim();
  const storedHash = String(agentData.pinHash || '').trim().toLowerCase();
  const storedSalt = String(agentData.pinSalt || '').trim();

  if (storedHash && storedSalt) {
    const calculated = hashPin(normalizedPin, storedSalt).pinHash.toLowerCase();
    return timingSafeEqualString(calculated, storedHash);
  }

  const legacyPin = String(agentData.pin || '').trim();
  if (!legacyPin) return false;

  return timingSafeEqualString(legacyPin, normalizedPin);
}

async function ensureDefaultOwnerAgentForNegocio({ negocioRecord, actor }) {
  const record = negocioRecord && typeof negocioRecord === 'object' ? negocioRecord : null;
  if (!record?.ref) return;

  const negocioData = record.data || {};
  const phone = normalizarTelefono(
    negocioData.contactWhatsapp ||
      negocioData.leadPhone ||
      negocioData.phone ||
      ''
  );
  const pin = String(negocioData.pin || '').trim();

  if (!phone || !/^\d{4}$/.test(pin)) {
    return;
  }

  const duplicateSnap = await record.ref
    .collection('hotelAgents')
    .where('phone', '==', phone)
    .limit(1)
    .get();

  if (!duplicateSnap.empty) {
    return;
  }

  const { pinHash, pinSalt } = hashPin(pin);
  const now = Timestamp.now();
  const fallbackName = String(negocioData.companyInfo || 'Owner').trim() || 'Owner';
  const ownerName = String(negocioData.ownerName || `Owner ${fallbackName}`).trim();

  await record.ref.collection('hotelAgents').add({
    name: ownerName.slice(0, 120),
    phone,
    role: ROLE_OWNER,
    active: true,
    pinHash,
    pinSalt,
    createdAt: now,
    updatedAt: now,
    createdBy: String(actor || 'super_admin').trim() || 'super_admin',
    source: 'advanced_app_activation',
  });
}

function sanitizeAgent(agentData = {}, agentId = '') {
  return {
    id: String(agentId || '').trim() || null,
    name: String(agentData.name || '').trim() || null,
    phone: String(agentData.phone || '').trim() || null,
    role: normalizeRole(agentData.role || ROLE_AGENT),
    active: agentData.active !== false,
    lastLoginAt: parseTimestamp(agentData.lastLoginAt),
    createdAt: parseTimestamp(agentData.createdAt),
    updatedAt: parseTimestamp(agentData.updatedAt),
  };
}

function sanitizeNegocio(negocioId, negocioData = {}) {
  return {
    negocioId,
    companyInfo: negocioData.companyInfo || 'Mi Negocio',
    slug: negocioData.slug || '',
    plan: negocioData.plan || null,
    templateId: negocioData.templateId || 'info',
    dominio:
      negocioData.dominio ||
      negocioData.domain ||
      negocioData.customDomain ||
      negocioData.custom_domain ||
      '',
    advancedApp: sanitizeAdvancedApp(negocioData.advancedApp || {}),
  };
}

function buildHotelSessionPayload({ negocioId, agentId, agentRole, phone }) {
  return {
    iss: 'vevcrm',
    sub: String(agentId || '').trim(),
    aud: CLIENT_REALM,
    role: CLIENT_ROLE,
    realm: CLIENT_REALM,
    negocioId: String(negocioId || '').trim(),
    appKey: APP_KEY_HOTEL,
    agentId: String(agentId || '').trim(),
    agentRole: normalizeRole(agentRole),
    phone: String(phone || '').trim(),
    sid: crypto.randomUUID(),
  };
}

function getDateKey(value) {
  return dayjs(value).format('YYYY-MM-DD');
}

function parseDateStrict(value) {
  const parsed = dayjs(String(value || '').trim(), 'YYYY-MM-DD', true);
  if (!parsed.isValid()) return null;
  return parsed;
}

function buildInventoryDocId(dateKey, roomTypeId) {
  const safeDate = String(dateKey || '').trim();
  const safeRoomType = String(roomTypeId || '').trim();
  return `${safeDate}_${safeRoomType}`;
}

function listStayDates(checkInKey, checkOutKey) {
  const checkInDate = parseDateStrict(checkInKey);
  const checkOutDate = parseDateStrict(checkOutKey);

  if (!checkInDate || !checkOutDate || !checkOutDate.isAfter(checkInDate)) {
    return [];
  }

  const nights = checkOutDate.diff(checkInDate, 'day');
  const dates = [];

  for (let index = 0; index < nights; index += 1) {
    dates.push(checkInDate.add(index, 'day').format('YYYY-MM-DD'));
  }

  return dates;
}

function calculateNights(checkInKey, checkOutKey) {
  const checkInDate = parseDateStrict(checkInKey);
  const checkOutDate = parseDateStrict(checkOutKey);
  if (!checkInDate || !checkOutDate) return 0;
  const nights = checkOutDate.diff(checkInDate, 'day');
  return nights > 0 ? nights : 0;
}

function computeRolePermissions(role) {
  const normalizedRole = normalizeRole(role);

  return {
    canManageSettings: normalizedRole === ROLE_OWNER,
    canManageAgents: normalizedRole === ROLE_OWNER,
    canManageRoomTypes: normalizedRole === ROLE_OWNER || normalizedRole === ROLE_MANAGER,
    canManageInventory: normalizedRole === ROLE_OWNER || normalizedRole === ROLE_MANAGER,
    canManageReservations:
      normalizedRole === ROLE_OWNER ||
      normalizedRole === ROLE_MANAGER ||
      normalizedRole === ROLE_AGENT,
  };
}

function buildPhoneCandidates(rawPhone) {
  const digits = String(rawPhone || '').replace(/\D/g, '');
  if (!digits) return [];

  const candidates = [];
  const add = (value) => {
    const safe = String(value || '').replace(/\D/g, '');
    if (!safe) return;
    if (!candidates.includes(safe)) candidates.push(safe);
  };

  add(digits);

  if (digits.length === 10) {
    add(`52${digits}`);
    add(`521${digits}`);
  }

  if (digits.startsWith('52') && digits.length === 12) {
    const local10 = digits.slice(2);
    add(local10);
    add(`521${local10}`);
  }

  if (digits.startsWith('521') && digits.length === 13) {
    const local10 = digits.slice(3);
    add(local10);
    add(`52${local10}`);
  }

  if (digits.length > 10) {
    add(digits.slice(-10));
  }

  return candidates.slice(0, 10);
}

async function findAgentsByPhoneCandidates(negocioRef, phoneInput) {
  const candidates = buildPhoneCandidates(phoneInput);
  if (!candidates.length) return [];

  const found = new Map();

  for (const candidate of candidates) {
    const snap = await negocioRef
      .collection('hotelAgents')
      .where('phone', '==', candidate)
      .limit(10)
      .get();

    for (const docSnap of snap.docs) {
      if (!found.has(docSnap.id)) {
        found.set(docSnap.id, docSnap);
      }
    }
  }

  return [...found.values()];
}

function pickCheckoutBaseUrl(req) {
  const origin = String(req.headers.origin || '').trim();
  if (origin) return origin.replace(/\/+$/, '');

  const host = extractRequestHost(req);
  if (!host) return '';

  const proto = String(req.headers['x-forwarded-proto'] || req.protocol || 'https').trim();
  return `${proto}://${host}`;
}

function formatCheckoutPathForHost(hostname) {
  return isBaseHost(hostname) ? '/cliente-login' : '/acceso';
}

function validateCheckoutBody(body = {}) {
  const checkIn = String(body.checkIn || '').trim();
  const checkOut = String(body.checkOut || '').trim();
  const roomTypeId = String(body.roomTypeId || '').trim();

  if (!checkIn || !checkOut || !roomTypeId) {
    return {
      ok: false,
      error: 'checkIn, checkOut y roomTypeId son obligatorios.',
    };
  }

  const nights = calculateNights(checkIn, checkOut);
  if (!nights) {
    return {
      ok: false,
      error: 'Rango de fechas inválido.',
    };
  }

  return {
    ok: true,
    checkIn,
    checkOut,
    roomTypeId,
    nights,
  };
}

function buildRoomTypePayload(input = {}, current = {}) {
  const merged = {
    ...current,
    ...input,
  };

  const name = String(merged.name || '').trim();
  const capacity = Math.max(1, Math.floor(toNumberSafe(merged.capacity, 1)));
  const baseRate = Math.max(0, toNumberSafe(merged.baseRate, 0));
  const unitsTotal = Math.max(1, Math.floor(toNumberSafe(merged.unitsTotal, 1)));

  return {
    name,
    capacity,
    baseRate,
    unitsTotal,
    active: merged.active !== false,
  };
}

async function resolveNegocioForHotelAuth(req) {
  const host = extractRequestHost(req);
  const headerNegocioId = String(req.get('x-negocio-id') || '').trim();
  const headerNegocioSlug = String(req.get('x-negocio-slug') || '').trim().toLowerCase();

  let negocioRecord = null;

  if (host && !isBaseHost(host)) {
    negocioRecord = await findNegocioByDomain(host);

    if (!negocioRecord) {
      return {
        error: {
          status: 401,
          code: 'INVALID_CREDENTIALS',
          message: 'Credenciales inválidas.',
        },
      };
    }

    if (negocioRecord && headerNegocioId && headerNegocioId !== negocioRecord.id) {
      return {
        error: {
          status: 403,
          code: 'FORBIDDEN',
          message: 'El negocio enviado no coincide con el dominio.',
        },
      };
    }

    if (negocioRecord && headerNegocioSlug) {
      const slugRecord = await findNegocioBySlug(headerNegocioSlug);
      if (!slugRecord || slugRecord.id !== negocioRecord.id) {
        return {
          error: {
            status: 403,
            code: 'FORBIDDEN',
            message: 'El slug enviado no coincide con el dominio.',
          },
        };
      }
    }

    return {
      negocioRecord,
      host,
    };
  }

  if (!negocioRecord && headerNegocioId) {
    negocioRecord = await readNegocioById(headerNegocioId);
  }

  if (!negocioRecord && headerNegocioSlug) {
    negocioRecord = await findNegocioBySlug(headerNegocioSlug);
  }

  if (!negocioRecord) {
    return {
      error: {
        status: 401,
        code: 'INVALID_CREDENTIALS',
        message: 'Credenciales inválidas.',
      },
    };
  }

  return {
    negocioRecord,
    host,
  };
}

async function verifyHotelSession(req, res, next) {
  try {
    const token = readBearerToken(req);
    if (!token) {
      return jsonError(res, 401, 'UNAUTHORIZED', 'Falta token Bearer.');
    }

    const verification = verifyJwt(token);
    if (!verification.valid) {
      return jsonError(res, 401, 'UNAUTHORIZED', 'Token inválido o expirado.', {
        reason: verification.reason,
      });
    }

    const claims = verification.payload || {};
    if (!hasHotelAgentClaims(claims)) {
      return jsonError(res, 403, 'FORBIDDEN', 'Token no autorizado para hotel_premium.');
    }

    const negocioId = String(claims.negocioId || '').trim();
    const agentId = String(claims.agentId || '').trim();

    const negocioRecord = await readNegocioById(negocioId);
    if (!negocioRecord) {
      return jsonError(res, 404, 'NEGOCIO_NOT_FOUND', 'Negocio no encontrado.');
    }

    if (!isHotelAdvancedAppActive(negocioRecord.data || {})) {
      return jsonError(res, 403, 'APP_NOT_ACTIVE', 'La app hotel no está activa para este negocio.');
    }

    const host = extractRequestHost(req);
    if (host && !isBaseHost(host)) {
      const domainRecord = await findNegocioByDomain(host);
      if (!domainRecord || domainRecord.id !== negocioId) {
        return jsonError(
          res,
          403,
          'FORBIDDEN',
          'El token no corresponde al negocio del dominio actual.'
        );
      }
    }

    const agentRef = negocioRecord.ref.collection('hotelAgents').doc(agentId);
    const agentSnap = await agentRef.get();
    if (!agentSnap.exists) {
      return jsonError(res, 401, 'UNAUTHORIZED', 'Agente no encontrado.');
    }

    const agentData = agentSnap.data() || {};
    if (agentData.active === false) {
      return jsonError(res, 401, 'UNAUTHORIZED', 'Agente inactivo.');
    }

    const agentRole = normalizeRole(agentData.role || claims.agentRole || ROLE_AGENT);

    req.hotelSession = {
      token,
      claims,
      negocioId,
      negocioRef: negocioRecord.ref,
      negocioData: negocioRecord.data || {},
      agentId,
      agentRole,
      agentRef,
      agentData,
      permissions: computeRolePermissions(agentRole),
    };

    return next();
  } catch (error) {
    console.error('[hotel session] Error:', error);
    return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo validar la sesión.', error?.message || null);
  }
}

function requirePermission(permissionKey) {
  return (req, res, next) => {
    const permissions = req.hotelSession?.permissions || {};
    if (!permissions[permissionKey]) {
      return jsonError(res, 403, 'FORBIDDEN', 'No tienes permisos para esta acción.');
    }
    return next();
  };
}

async function ensureInventoryAvailable({ negocioRef, roomTypeId, stayDates, fallbackUnits }) {
  for (const dateKey of stayDates) {
    const inventoryId = buildInventoryDocId(dateKey, roomTypeId);
    const inventorySnap = await negocioRef
      .collection('hotelInventoryDaily')
      .doc(inventoryId)
      .get();

    const inventoryData = inventorySnap.exists ? inventorySnap.data() || {} : {};
    const unitsAvailable = Math.max(
      0,
      Math.floor(
        toNumberSafe(
          inventoryData.unitsAvailable,
          Math.max(1, Math.floor(toNumberSafe(fallbackUnits, 1)))
        )
      )
    );
    const unitsBooked = Math.max(0, Math.floor(toNumberSafe(inventoryData.unitsBooked, 0)));

    if (unitsBooked + 1 > unitsAvailable) {
      return {
        ok: false,
        date: dateKey,
      };
    }
  }

  return { ok: true };
}

async function markReservationPaymentIssue({ negocioId, reservationId, reason }) {
  try {
    if (!negocioId || !reservationId) return;

    const ref = db
      .collection('Negocios')
      .doc(String(negocioId).trim())
      .collection('hotelReservations')
      .doc(String(reservationId).trim());

    await ref.set(
      {
        status: 'payment_review_required',
        updatedAt: Timestamp.now(),
        payment: {
          status: 'paid_needs_review',
          reviewReason: String(reason || 'inventory_conflict').slice(0, 240),
        },
      },
      { merge: true }
    );
  } catch (error) {
    console.error('[hotel webhook] No se pudo marcar reserva en revisión:', error);
  }
}

async function confirmReservationAfterCheckout(session = {}) {
  const metadata = session.metadata || {};
  const negocioId = String(metadata.negocioId || '').trim();
  const reservationId = String(metadata.reservationId || '').trim();

  if (!negocioId || !reservationId) {
    throw new Error('checkout.session.completed sin metadata de negocio/reserva.');
  }

  const negocioRef = db.collection('Negocios').doc(negocioId);
  const reservationRef = negocioRef.collection('hotelReservations').doc(reservationId);

  await db.runTransaction(async (transaction) => {
    const reservationSnap = await transaction.get(reservationRef);
    if (!reservationSnap.exists) {
      throw new Error('Reserva no encontrada para confirmar pago.');
    }

    const reservationData = reservationSnap.data() || {};
    const currentStatus = toLowerSafe(reservationData.status || '');

    if (['confirmed', 'checked_in', 'checked_out'].includes(currentStatus)) {
      return;
    }

    if (currentStatus === 'cancelled') {
      throw new Error('La reserva ya está cancelada.');
    }

    const roomTypeId = String(reservationData.roomTypeId || '').trim();
    if (!roomTypeId) {
      throw new Error('Reserva sin roomTypeId.');
    }

    const roomTypeRef = negocioRef.collection('hotelRoomTypes').doc(roomTypeId);
    const roomTypeSnap = await transaction.get(roomTypeRef);
    if (!roomTypeSnap.exists) {
      throw new Error('Tipo de habitación no encontrado para la reserva.');
    }

    const roomTypeData = roomTypeSnap.data() || {};
    const unitsTotal = Math.max(1, Math.floor(toNumberSafe(roomTypeData.unitsTotal, 1)));

    const checkIn = String(reservationData.checkIn || '').trim();
    const checkOut = String(reservationData.checkOut || '').trim();
    const stayDates = listStayDates(checkIn, checkOut);

    if (!stayDates.length) {
      throw new Error('Reserva con fechas inválidas.');
    }

    for (const dateKey of stayDates) {
      const inventoryId = buildInventoryDocId(dateKey, roomTypeId);
      const inventoryRef = negocioRef.collection('hotelInventoryDaily').doc(inventoryId);
      const inventorySnap = await transaction.get(inventoryRef);
      const inventoryData = inventorySnap.exists ? inventorySnap.data() || {} : {};

      const unitsAvailable = Math.max(
        0,
        Math.floor(toNumberSafe(inventoryData.unitsAvailable, unitsTotal))
      );
      const unitsBooked = Math.max(0, Math.floor(toNumberSafe(inventoryData.unitsBooked, 0)));

      if (unitsBooked + 1 > unitsAvailable) {
        throw new Error(`No hay disponibilidad para ${dateKey}.`);
      }

      transaction.set(
        inventoryRef,
        {
          date: dateKey,
          roomTypeId,
          unitsAvailable,
          unitsBooked: unitsBooked + 1,
          updatedAt: Timestamp.now(),
          createdAt: inventoryData.createdAt || Timestamp.now(),
        },
        { merge: true }
      );
    }

    const amountTotal = toNumberSafe(session.amount_total, 0);
    const amountReceived = amountTotal > 0 ? amountTotal / 100 : toNumberSafe(reservationData.deposit, 0);

    transaction.update(reservationRef, {
      status: 'confirmed',
      confirmedAt: Timestamp.now(),
      updatedAt: Timestamp.now(),
      payment: {
        status: 'paid',
        provider: 'stripe',
        checkoutSessionId: String(session.id || '').trim() || null,
        paymentIntentId: String(session.payment_intent || '').trim() || null,
        amountReceived,
        currency: normalizeCurrency(session.currency || reservationData.currency || 'mxn'),
        paidAt: Timestamp.now(),
      },
    });
  });
}

export async function hotelStripeWebhook(req, res) {
  let event = null;

  try {
    const signature = req.headers['stripe-signature'];
    const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;

    if (signature && webhookSecret && Buffer.isBuffer(req.body)) {
      event = stripe.webhooks.constructEvent(req.body, signature, webhookSecret);
    } else if (Buffer.isBuffer(req.body)) {
      const parsed = JSON.parse(req.body.toString('utf8'));
      event = parsed && typeof parsed === 'object' ? parsed : null;
    } else if (req.body && typeof req.body === 'object') {
      event = req.body;
    }

    if (!event?.type) {
      return res.status(400).json({ success: false, error: 'Evento de webhook inválido.' });
    }

    if (event.type === 'checkout.session.completed') {
      const session = event.data?.object || {};
      if (String(session.metadata?.hotelFlow || '') === 'hotel_premium_deposit') {
        try {
          await confirmReservationAfterCheckout(session);
        } catch (confirmError) {
          console.error('[hotel webhook] Error confirmando reserva:', confirmError);
          await markReservationPaymentIssue({
            negocioId: session.metadata?.negocioId,
            reservationId: session.metadata?.reservationId,
            reason: confirmError?.message || 'inventory_conflict',
          });
        }
      }
    }

    return res.json({ success: true, received: true, type: event.type });
  } catch (error) {
    console.error('[hotel webhook] Error:', error);
    return res.status(400).json({ success: false, error: error?.message || 'Webhook inválido.' });
  }
}

export function createAdvancedAppsRouter() {
  const router = express.Router();

  router.get('/admin/advanced-apps/:negocioId', async (req, res) => {
    try {
      const negocioId = String(req.params?.negocioId || '').trim();
      if (!negocioId) {
        return jsonError(res, 400, 'VALIDATION_ERROR', 'negocioId es obligatorio.');
      }

      const negocioRecord = await readNegocioById(negocioId);
      if (!negocioRecord) {
        return jsonError(res, 404, 'NEGOCIO_NOT_FOUND', 'Negocio no encontrado.');
      }

      const negocioData = negocioRecord.data || {};
      const advancedApp = sanitizeAdvancedApp(negocioData.advancedApp || {});

      return res.json({
        success: true,
        data: {
          negocioId,
          plan: negocioData.plan || null,
          premiumEligible: isPremiumActive(negocioData),
          advancedApp,
          appActive: isHotelAdvancedAppActive(negocioData),
        },
      });
    } catch (error) {
      console.error('[advanced apps] Error consultando app:', error);
      return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo consultar la app avanzada.', error?.message || null);
    }
  });

  router.post('/admin/advanced-apps/activate', async (req, res) => {
    try {
      const negocioId = String(req.body?.negocioId || '').trim();
      const appKey = String(req.body?.appKey || APP_KEY_HOTEL).trim();
      const actor = String(req.body?.actor || 'super_admin').trim();
      const config = req.body?.config && typeof req.body.config === 'object' ? req.body.config : {};

      if (!negocioId) {
        return jsonError(res, 400, 'VALIDATION_ERROR', 'negocioId es obligatorio.');
      }

      if (appKey !== APP_KEY_HOTEL) {
        return jsonError(res, 400, 'UNSUPPORTED_APP', 'Solo se soporta hotel_premium en esta versión.');
      }

      const negocioRecord = await readNegocioById(negocioId);
      if (!negocioRecord) {
        return jsonError(res, 404, 'NEGOCIO_NOT_FOUND', 'Negocio no encontrado.');
      }

      const negocioData = negocioRecord.data || {};
      if (!isPremiumActive(negocioData)) {
        return jsonError(
          res,
          403,
          'PREMIUM_REQUIRED',
          'Solo negocios premium activos pueden activar hotel_premium.'
        );
      }

      const currentAdvancedApp = sanitizeAdvancedApp(negocioData.advancedApp || {});
      const currentStatus = toLowerSafe(currentAdvancedApp.status || 'inactive');
      const currentKey = String(currentAdvancedApp.key || '').trim();

      if (currentStatus === 'active' && currentKey && currentKey !== appKey) {
        return jsonError(
          res,
          409,
          'APP_ALREADY_ACTIVE',
          'Ya existe otra app avanzada activa. Desactívala antes de cambiar.'
        );
      }

      const advancedApp = {
        key: appKey,
        status: 'active',
        config,
        activatedAt: Timestamp.now(),
        activatedBy: actor || 'super_admin',
        deactivatedAt: null,
        deactivatedBy: null,
      };

      await negocioRecord.ref.set(
        {
          advancedApp,
          panelWebEnabled: true,
          updatedAt: Timestamp.now(),
        },
        { merge: true }
      );

      await ensureDefaultOwnerAgentForNegocio({
        negocioRecord,
        actor,
      });

      return res.json({
        success: true,
        message: 'App avanzada activada correctamente.',
        data: {
          negocioId,
          advancedApp: sanitizeAdvancedApp(advancedApp),
        },
      });
    } catch (error) {
      console.error('[advanced apps] Error activando app:', error);
      return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo activar la app avanzada.', error?.message || null);
    }
  });

  router.post('/admin/advanced-apps/deactivate', async (req, res) => {
    try {
      const negocioId = String(req.body?.negocioId || '').trim();
      const actor = String(req.body?.actor || 'super_admin').trim();

      if (!negocioId) {
        return jsonError(res, 400, 'VALIDATION_ERROR', 'negocioId es obligatorio.');
      }

      const negocioRecord = await readNegocioById(negocioId);
      if (!negocioRecord) {
        return jsonError(res, 404, 'NEGOCIO_NOT_FOUND', 'Negocio no encontrado.');
      }

      const current = sanitizeAdvancedApp(negocioRecord.data?.advancedApp || {});
      const next = {
        key: current.key || APP_KEY_HOTEL,
        status: 'inactive',
        config: current.config || {},
        activatedAt: current.activatedAt ? Timestamp.fromDate(current.activatedAt) : null,
        activatedBy: current.activatedBy || null,
        deactivatedAt: Timestamp.now(),
        deactivatedBy: actor || 'super_admin',
      };

      await negocioRecord.ref.set(
        {
          advancedApp: next,
          updatedAt: Timestamp.now(),
        },
        { merge: true }
      );

      return res.json({
        success: true,
        message: 'App avanzada desactivada.',
        data: {
          negocioId,
          advancedApp: sanitizeAdvancedApp(next),
        },
      });
    } catch (error) {
      console.error('[advanced apps] Error desactivando app:', error);
      return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo desactivar la app avanzada.', error?.message || null);
    }
  });

  return router;
}

export function createHotelAppRouter() {
  const router = express.Router();

  router.get('/public/tenant/resolve', async (req, res) => {
    try {
      const explicitDomain = normalizeDomain(req.query?.domain || '');
      const requestDomain = extractRequestHost(req);
      const domain = explicitDomain || requestDomain;

      if (!domain) {
        return jsonError(res, 400, 'VALIDATION_ERROR', 'No se pudo resolver el dominio del request.');
      }

      const negocioRecord = await findNegocioByDomain(domain);
      if (!negocioRecord) {
        return jsonError(res, 404, 'TENANT_NOT_FOUND', 'No encontramos un negocio para ese dominio.');
      }

      const negocioData = negocioRecord.data || {};
      return res.json({
        success: true,
        data: {
          ...sanitizeNegocio(negocioRecord.id, negocioData),
          premiumEligible: isPremiumActive(negocioData),
          appActive: isHotelAdvancedAppActive(negocioData),
        },
      });
    } catch (error) {
      console.error('[hotel tenant resolve] Error:', error);
      return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo resolver el tenant.', error?.message || null);
    }
  });

  router.get('/public/tenant/by-slug/:slug', async (req, res) => {
    try {
      const slug = String(req.params?.slug || '').trim().toLowerCase();
      if (!slug) {
        return jsonError(res, 400, 'VALIDATION_ERROR', 'slug es obligatorio.');
      }

      const negocioRecord = await findNegocioBySlug(slug);
      if (!negocioRecord) {
        return jsonError(res, 404, 'TENANT_NOT_FOUND', 'No encontramos un negocio para ese slug.');
      }

      const negocioData = negocioRecord.data || {};
      return res.json({
        success: true,
        data: {
          ...sanitizeNegocio(negocioRecord.id, negocioData),
          premiumEligible: isPremiumActive(negocioData),
          appActive: isHotelAdvancedAppActive(negocioData),
        },
      });
    } catch (error) {
      console.error('[hotel tenant by-slug] Error:', error);
      return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo resolver el tenant por slug.', error?.message || null);
    }
  });

  router.get('/public/tenant/by-negocio/:negocioId', async (req, res) => {
    try {
      const negocioId = String(req.params?.negocioId || '').trim();
      if (!negocioId) {
        return jsonError(res, 400, 'VALIDATION_ERROR', 'negocioId es obligatorio.');
      }

      const negocioRecord = await readNegocioById(negocioId);
      if (!negocioRecord) {
        return jsonError(res, 404, 'TENANT_NOT_FOUND', 'No encontramos un negocio para ese ID.');
      }

      const negocioData = negocioRecord.data || {};
      return res.json({
        success: true,
        data: {
          ...sanitizeNegocio(negocioRecord.id, negocioData),
          premiumEligible: isPremiumActive(negocioData),
          appActive: isHotelAdvancedAppActive(negocioData),
        },
      });
    } catch (error) {
      console.error('[hotel tenant by-negocio] Error:', error);
      return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo resolver el tenant por negocio.', error?.message || null);
    }
  });

  router.post('/cliente/hotel/auth/login', async (req, res) => {
    try {
      const phoneDigits = normalizarTelefono(req.body?.phone || '');
      const pin = String(req.body?.pin || '').trim();

      if (!phoneDigits || !pin) {
        return jsonError(res, 400, 'VALIDATION_ERROR', 'phone y pin son obligatorios.');
      }

      if (!/^\d{4}$/.test(pin)) {
        return jsonError(res, 400, 'VALIDATION_ERROR', 'El PIN debe tener 4 dígitos.');
      }

      const resolved = await resolveNegocioForHotelAuth(req);
      if (resolved.error) {
        return jsonError(
          res,
          resolved.error.status,
          resolved.error.code,
          resolved.error.message
        );
      }

      const negocioRecord = resolved.negocioRecord;
      const negocioData = negocioRecord.data || {};

      if (!isHotelAdvancedAppActive(negocioData)) {
        return jsonError(
          res,
          403,
          'APP_NOT_ACTIVE',
          'La app hotel no está activa para este negocio.'
        );
      }

      await ensureDefaultOwnerAgentForNegocio({
        negocioRecord,
        actor: 'system_login_bootstrap',
      });

      const agentDocs = await findAgentsByPhoneCandidates(negocioRecord.ref, phoneDigits);
      if (!agentDocs.length) {
        return jsonError(res, 401, 'INVALID_CREDENTIALS', 'Credenciales inválidas.');
      }

      let matchedAgent = null;

      for (const docSnap of agentDocs) {
        const agentData = docSnap.data() || {};
        if (agentData.active === false) continue;

        if (comparePin(pin, agentData)) {
          matchedAgent = {
            id: docSnap.id,
            ref: docSnap.ref,
            data: agentData,
          };
          break;
        }
      }

      if (!matchedAgent) {
        return jsonError(res, 401, 'INVALID_CREDENTIALS', 'Credenciales inválidas.');
      }

      const agentRole = normalizeRole(matchedAgent.data.role || ROLE_AGENT);

      let session;
      try {
        session = signJwt(
          buildHotelSessionPayload({
            negocioId: negocioRecord.id,
            agentId: matchedAgent.id,
            agentRole,
            phone: phoneDigits,
          }),
          { expiresInSeconds: DEFAULT_TOKEN_TTL_SECONDS }
        );
      } catch (signError) {
        return jsonError(
          res,
          500,
          'SESSION_CONFIG_ERROR',
          'No se pudo iniciar sesión por configuración de token en el servidor.',
          signError?.message || null
        );
      }

      await matchedAgent.ref.set(
        {
          lastLoginAt: Timestamp.now(),
          updatedAt: Timestamp.now(),
        },
        { merge: true }
      );

      return res.json({
        success: true,
        data: {
          token: session.token,
          sessionToken: session.token,
          expiresInSeconds: session.expiresInSeconds,
          negocio: sanitizeNegocio(negocioRecord.id, negocioData),
          advancedApp: sanitizeAdvancedApp(negocioData.advancedApp || {}),
          agent: sanitizeAgent(matchedAgent.data, matchedAgent.id),
        },
      });
    } catch (error) {
      console.error('[hotel auth login] Error:', error);
      return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo completar el login.', error?.message || null);
    }
  });

  router.get('/cliente/hotel/auth/me', verifyHotelSession, async (req, res) => {
    const session = req.hotelSession;

    return res.json({
      success: true,
      data: {
        negocio: sanitizeNegocio(session.negocioId, session.negocioData),
        advancedApp: sanitizeAdvancedApp(session.negocioData.advancedApp || {}),
        agent: sanitizeAgent(session.agentData || {}, session.agentId),
        role: session.agentRole,
      },
    });
  });

  router.post('/cliente/hotel/auth/logout', verifyHotelSession, async (_req, res) => {
    return res.json({
      success: true,
      message: 'Sesión cerrada.',
    });
  });

  router.get(
    '/cliente/hotel/room-types',
    verifyHotelSession,
    requirePermission('canManageReservations'),
    async (req, res) => {
      try {
        const snap = await req.hotelSession.negocioRef
          .collection('hotelRoomTypes')
          .get();

        const items = snap.docs
          .map((docSnap) => {
            const data = docSnap.data() || {};
            return {
              id: docSnap.id,
              name: data.name || '',
              capacity: toNumberSafe(data.capacity, 1),
              baseRate: toNumberSafe(data.baseRate, 0),
              unitsTotal: toNumberSafe(data.unitsTotal, 1),
              active: data.active !== false,
              createdAt: parseTimestamp(data.createdAt),
              updatedAt: parseTimestamp(data.updatedAt),
            };
          })
          .sort((a, b) => String(a.name).localeCompare(String(b.name), 'es'));

        return res.json({ success: true, data: items });
      } catch (error) {
        console.error('[hotel room-types] Error listando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudieron listar los tipos de habitación.');
      }
    }
  );

  router.post(
    '/cliente/hotel/room-types',
    verifyHotelSession,
    requirePermission('canManageRoomTypes'),
    async (req, res) => {
      try {
        const payload = buildRoomTypePayload(req.body || {});
        if (!payload.name) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'name es obligatorio.');
        }

        const now = Timestamp.now();
        const ref = await req.hotelSession.negocioRef.collection('hotelRoomTypes').add({
          ...payload,
          createdAt: now,
          updatedAt: now,
          createdBy: req.hotelSession.agentId,
        });

        return res.status(201).json({
          success: true,
          data: {
            id: ref.id,
            ...payload,
          },
        });
      } catch (error) {
        console.error('[hotel room-types] Error creando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo crear el tipo de habitación.');
      }
    }
  );

  router.patch(
    '/cliente/hotel/room-types/:roomTypeId',
    verifyHotelSession,
    requirePermission('canManageRoomTypes'),
    async (req, res) => {
      try {
        const roomTypeId = String(req.params?.roomTypeId || '').trim();
        if (!roomTypeId) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'roomTypeId es obligatorio.');
        }

        const ref = req.hotelSession.negocioRef.collection('hotelRoomTypes').doc(roomTypeId);
        const snap = await ref.get();
        if (!snap.exists) {
          return jsonError(res, 404, 'ROOM_TYPE_NOT_FOUND', 'Tipo de habitación no encontrado.');
        }

        const current = snap.data() || {};
        const payload = buildRoomTypePayload(req.body || {}, current);

        if (!payload.name) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'name es obligatorio.');
        }

        await ref.set(
          {
            ...payload,
            updatedAt: Timestamp.now(),
            updatedBy: req.hotelSession.agentId,
          },
          { merge: true }
        );

        return res.json({
          success: true,
          data: {
            id: roomTypeId,
            ...payload,
          },
        });
      } catch (error) {
        console.error('[hotel room-types] Error actualizando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo actualizar el tipo de habitación.');
      }
    }
  );

  router.get(
    '/cliente/hotel/inventory',
    verifyHotelSession,
    requirePermission('canManageReservations'),
    async (req, res) => {
      try {
        const dateFrom = String(req.query?.dateFrom || '').trim();
        const dateTo = String(req.query?.dateTo || '').trim();
        const roomTypeId = String(req.query?.roomTypeId || '').trim();

        const snap = await req.hotelSession.negocioRef
          .collection('hotelInventoryDaily')
          .get();

        const items = snap.docs
          .map((docSnap) => {
            const data = docSnap.data() || {};
            return {
              id: docSnap.id,
              date: String(data.date || ''),
              roomTypeId: String(data.roomTypeId || ''),
              unitsAvailable: toNumberSafe(data.unitsAvailable, 0),
              unitsBooked: toNumberSafe(data.unitsBooked, 0),
              updatedAt: parseTimestamp(data.updatedAt),
              createdAt: parseTimestamp(data.createdAt),
            };
          })
          .filter((item) => {
            if (roomTypeId && item.roomTypeId !== roomTypeId) return false;
            if (dateFrom && item.date < dateFrom) return false;
            if (dateTo && item.date > dateTo) return false;
            return true;
          })
          .sort((a, b) => {
            if (a.date === b.date) return a.roomTypeId.localeCompare(b.roomTypeId, 'es');
            return a.date.localeCompare(b.date, 'es');
          });

        return res.json({ success: true, data: items });
      } catch (error) {
        console.error('[hotel inventory] Error listando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo listar el inventario.');
      }
    }
  );

  router.post(
    '/cliente/hotel/inventory',
    verifyHotelSession,
    requirePermission('canManageInventory'),
    async (req, res) => {
      try {
        const date = String(req.body?.date || '').trim();
        const roomTypeId = String(req.body?.roomTypeId || '').trim();
        const unitsAvailable = Math.max(0, Math.floor(toNumberSafe(req.body?.unitsAvailable, NaN)));

        if (!date || !roomTypeId || Number.isNaN(unitsAvailable)) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'date, roomTypeId y unitsAvailable son obligatorios.');
        }

        const parsedDate = parseDateStrict(date);
        if (!parsedDate) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'date debe tener formato YYYY-MM-DD.');
        }

        const inventoryId = buildInventoryDocId(parsedDate.format('YYYY-MM-DD'), roomTypeId);
        const ref = req.hotelSession.negocioRef.collection('hotelInventoryDaily').doc(inventoryId);

        const existingSnap = await ref.get();
        const existingData = existingSnap.exists ? existingSnap.data() || {} : {};
        const unitsBooked = Math.max(0, Math.floor(toNumberSafe(existingData.unitsBooked, 0)));

        if (unitsBooked > unitsAvailable) {
          return jsonError(
            res,
            409,
            'INVALID_INVENTORY',
            'unitsAvailable no puede ser menor que unitsBooked actual.'
          );
        }

        await ref.set(
          {
            date: parsedDate.format('YYYY-MM-DD'),
            roomTypeId,
            unitsAvailable,
            unitsBooked,
            updatedAt: Timestamp.now(),
            updatedBy: req.hotelSession.agentId,
            createdAt: existingData.createdAt || Timestamp.now(),
          },
          { merge: true }
        );

        return res.json({
          success: true,
          data: {
            id: inventoryId,
            date: parsedDate.format('YYYY-MM-DD'),
            roomTypeId,
            unitsAvailable,
            unitsBooked,
          },
        });
      } catch (error) {
        console.error('[hotel inventory] Error guardando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo guardar inventario.');
      }
    }
  );

  router.patch(
    '/cliente/hotel/inventory/:inventoryId',
    verifyHotelSession,
    requirePermission('canManageInventory'),
    async (req, res) => {
      try {
        const inventoryId = String(req.params?.inventoryId || '').trim();
        if (!inventoryId) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'inventoryId es obligatorio.');
        }

        const ref = req.hotelSession.negocioRef.collection('hotelInventoryDaily').doc(inventoryId);
        const snap = await ref.get();
        if (!snap.exists) {
          return jsonError(res, 404, 'INVENTORY_NOT_FOUND', 'Inventario no encontrado.');
        }

        const current = snap.data() || {};
        const nextUnitsAvailable = req.body?.unitsAvailable;
        const unitsAvailable = Number.isFinite(Number(nextUnitsAvailable))
          ? Math.max(0, Math.floor(Number(nextUnitsAvailable)))
          : Math.max(0, Math.floor(toNumberSafe(current.unitsAvailable, 0)));
        const unitsBooked = Math.max(0, Math.floor(toNumberSafe(current.unitsBooked, 0)));

        if (unitsBooked > unitsAvailable) {
          return jsonError(
            res,
            409,
            'INVALID_INVENTORY',
            'unitsAvailable no puede ser menor que unitsBooked actual.'
          );
        }

        await ref.set(
          {
            unitsAvailable,
            updatedAt: Timestamp.now(),
            updatedBy: req.hotelSession.agentId,
          },
          { merge: true }
        );

        return res.json({
          success: true,
          data: {
            id: inventoryId,
            date: current.date || null,
            roomTypeId: current.roomTypeId || null,
            unitsAvailable,
            unitsBooked,
          },
        });
      } catch (error) {
        console.error('[hotel inventory] Error actualizando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo actualizar inventario.');
      }
    }
  );

  router.get(
    '/cliente/hotel/reservations',
    verifyHotelSession,
    requirePermission('canManageReservations'),
    async (req, res) => {
      try {
        const statusFilter = toLowerSafe(req.query?.status || '');

        const snap = await req.hotelSession.negocioRef
          .collection('hotelReservations')
          .get();

        const items = snap.docs
          .map((docSnap) => {
            const data = docSnap.data() || {};
            return {
              id: docSnap.id,
              roomTypeId: String(data.roomTypeId || ''),
              checkIn: String(data.checkIn || ''),
              checkOut: String(data.checkOut || ''),
              nights: toNumberSafe(data.nights, 0),
              status: String(data.status || ''),
              total: toNumberSafe(data.total, 0),
              deposit: toNumberSafe(data.deposit, 0),
              currency: normalizeCurrency(data.currency || 'mxn'),
              guestName: String(data.guestName || ''),
              guestEmail: String(data.guestEmail || ''),
              guestPhone: String(data.guestPhone || ''),
              payment: data.payment || {},
              refundStatus: String(data.refundStatus || ''),
              cancelReason: String(data.cancelReason || ''),
              createdAt: parseTimestamp(data.createdAt),
              updatedAt: parseTimestamp(data.updatedAt),
              confirmedAt: parseTimestamp(data.confirmedAt),
            };
          })
          .filter((item) => {
            if (!statusFilter) return true;
            return toLowerSafe(item.status) === statusFilter;
          })
          .sort((a, b) => {
            const aTime = a.createdAt ? a.createdAt.getTime() : 0;
            const bTime = b.createdAt ? b.createdAt.getTime() : 0;
            return bTime - aTime;
          });

        return res.json({ success: true, data: items });
      } catch (error) {
        console.error('[hotel reservations] Error listando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudieron listar las reservas.');
      }
    }
  );

  router.patch(
    '/cliente/hotel/reservations/:reservationId',
    verifyHotelSession,
    requirePermission('canManageReservations'),
    async (req, res) => {
      try {
        const reservationId = String(req.params?.reservationId || '').trim();
        if (!reservationId) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'reservationId es obligatorio.');
        }

        const ref = req.hotelSession.negocioRef.collection('hotelReservations').doc(reservationId);
        const snap = await ref.get();
        if (!snap.exists) {
          return jsonError(res, 404, 'RESERVATION_NOT_FOUND', 'Reserva no encontrada.');
        }

        const updates = {};
        const role = req.hotelSession.agentRole;

        const editableByAgent = new Set(['status', 'notes']);
        const editableByManager = new Set(['status', 'notes', 'guestName', 'guestEmail', 'guestPhone']);
        const editableByOwner = new Set([
          'status',
          'notes',
          'guestName',
          'guestEmail',
          'guestPhone',
          'total',
          'deposit',
          'currency',
          'roomTypeId',
          'checkIn',
          'checkOut',
        ]);

        let allowed = editableByAgent;
        if (role === ROLE_MANAGER) allowed = editableByManager;
        if (role === ROLE_OWNER) allowed = editableByOwner;

        for (const [key, value] of Object.entries(req.body || {})) {
          if (!allowed.has(key)) continue;
          updates[key] = value;
        }

        if (!Object.keys(updates).length) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'No se recibieron campos permitidos para actualizar.');
        }

        if (Object.prototype.hasOwnProperty.call(updates, 'checkIn')) {
          const safeCheckIn = String(updates.checkIn || '').trim();
          if (safeCheckIn && !parseDateStrict(safeCheckIn)) {
            return jsonError(res, 400, 'VALIDATION_ERROR', 'checkIn inválido (YYYY-MM-DD).');
          }
          updates.checkIn = safeCheckIn;
        }

        if (Object.prototype.hasOwnProperty.call(updates, 'checkOut')) {
          const safeCheckOut = String(updates.checkOut || '').trim();
          if (safeCheckOut && !parseDateStrict(safeCheckOut)) {
            return jsonError(res, 400, 'VALIDATION_ERROR', 'checkOut inválido (YYYY-MM-DD).');
          }
          updates.checkOut = safeCheckOut;
        }

        if (Object.prototype.hasOwnProperty.call(updates, 'currency')) {
          updates.currency = normalizeCurrency(updates.currency);
        }

        if (Object.prototype.hasOwnProperty.call(updates, 'total')) {
          updates.total = Math.max(0, toNumberSafe(updates.total, 0));
        }

        if (Object.prototype.hasOwnProperty.call(updates, 'deposit')) {
          updates.deposit = Math.max(0, toNumberSafe(updates.deposit, 0));
        }

        updates.updatedAt = Timestamp.now();
        updates.updatedBy = req.hotelSession.agentId;

        await ref.set(updates, { merge: true });

        const updatedSnap = await ref.get();
        const updatedData = updatedSnap.data() || {};

        return res.json({
          success: true,
          data: {
            id: updatedSnap.id,
            ...updatedData,
          },
        });
      } catch (error) {
        console.error('[hotel reservations] Error actualizando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo actualizar la reserva.');
      }
    }
  );

  router.post(
    '/cliente/hotel/reservations/:reservationId/cancel',
    verifyHotelSession,
    requirePermission('canManageReservations'),
    async (req, res) => {
      try {
        const reservationId = String(req.params?.reservationId || '').trim();
        if (!reservationId) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'reservationId es obligatorio.');
        }

        const cancelReason = String(req.body?.reason || '').trim();
        const ref = req.hotelSession.negocioRef.collection('hotelReservations').doc(reservationId);
        const snap = await ref.get();
        if (!snap.exists) {
          return jsonError(res, 404, 'RESERVATION_NOT_FOUND', 'Reserva no encontrada.');
        }

        await ref.set(
          {
            status: 'cancelled',
            cancelReason,
            cancelledAt: Timestamp.now(),
            cancelledBy: req.hotelSession.agentId,
            refundStatus: 'manual_pending',
            updatedAt: Timestamp.now(),
          },
          { merge: true }
        );

        return res.json({
          success: true,
          message: 'Reserva cancelada. Reembolso pendiente de gestión manual.',
          data: {
            reservationId,
            refundStatus: 'manual_pending',
          },
        });
      } catch (error) {
        console.error('[hotel reservations] Error cancelando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo cancelar la reserva.');
      }
    }
  );

  router.get(
    '/cliente/hotel/agents',
    verifyHotelSession,
    requirePermission('canManageReservations'),
    async (req, res) => {
      try {
        const role = req.hotelSession.agentRole;
        if (role === ROLE_AGENT) {
          return jsonError(res, 403, 'FORBIDDEN', 'No tienes permisos para consultar agentes.');
        }

        const snap = await req.hotelSession.negocioRef
          .collection('hotelAgents')
          .get();

        const items = snap.docs
          .map((docSnap) => sanitizeAgent(docSnap.data() || {}, docSnap.id))
          .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'es'));

        return res.json({ success: true, data: items });
      } catch (error) {
        console.error('[hotel agents] Error listando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudieron listar agentes.');
      }
    }
  );

  router.post(
    '/cliente/hotel/agents',
    verifyHotelSession,
    requirePermission('canManageAgents'),
    async (req, res) => {
      try {
        const name = String(req.body?.name || '').trim();
        const phone = normalizarTelefono(req.body?.phone || '');
        const pin = String(req.body?.pin || '').trim();
        const role = normalizeRole(req.body?.role || ROLE_AGENT);
        const active = req.body?.active !== false;

        if (!name || !phone || !/^\d{4}$/.test(pin)) {
          return jsonError(
            res,
            400,
            'VALIDATION_ERROR',
            'name, phone y pin (4 dígitos) son obligatorios.'
          );
        }

        const duplicateSnap = await req.hotelSession.negocioRef
          .collection('hotelAgents')
          .where('phone', '==', phone)
          .limit(1)
          .get();

        if (!duplicateSnap.empty) {
          return jsonError(res, 409, 'AGENT_EXISTS', 'Ya existe un agente con ese teléfono.');
        }

        const { pinHash, pinSalt } = hashPin(pin);

        const now = Timestamp.now();
        const ref = await req.hotelSession.negocioRef.collection('hotelAgents').add({
          name,
          phone,
          role,
          active,
          pinHash,
          pinSalt,
          createdAt: now,
          updatedAt: now,
          createdBy: req.hotelSession.agentId,
        });

        return res.status(201).json({
          success: true,
          data: {
            id: ref.id,
            name,
            phone,
            role,
            active,
          },
        });
      } catch (error) {
        console.error('[hotel agents] Error creando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo crear el agente.');
      }
    }
  );

  router.patch(
    '/cliente/hotel/agents/:agentId',
    verifyHotelSession,
    requirePermission('canManageAgents'),
    async (req, res) => {
      try {
        const agentId = String(req.params?.agentId || '').trim();
        if (!agentId) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'agentId es obligatorio.');
        }

        const ref = req.hotelSession.negocioRef.collection('hotelAgents').doc(agentId);
        const snap = await ref.get();
        if (!snap.exists) {
          return jsonError(res, 404, 'AGENT_NOT_FOUND', 'Agente no encontrado.');
        }

        const updates = {};

        if (Object.prototype.hasOwnProperty.call(req.body || {}, 'name')) {
          const name = String(req.body.name || '').trim();
          if (!name) {
            return jsonError(res, 400, 'VALIDATION_ERROR', 'name no puede estar vacío.');
          }
          updates.name = name;
        }

        if (Object.prototype.hasOwnProperty.call(req.body || {}, 'phone')) {
          const phone = normalizarTelefono(req.body.phone || '');
          if (!phone) {
            return jsonError(res, 400, 'VALIDATION_ERROR', 'phone inválido.');
          }

          const duplicateSnap = await req.hotelSession.negocioRef
            .collection('hotelAgents')
            .where('phone', '==', phone)
            .limit(5)
            .get();

          const existsOther = duplicateSnap.docs.some((docSnap) => docSnap.id !== agentId);
          if (existsOther) {
            return jsonError(res, 409, 'AGENT_EXISTS', 'Ya existe otro agente con ese teléfono.');
          }

          updates.phone = phone;
        }

        if (Object.prototype.hasOwnProperty.call(req.body || {}, 'role')) {
          updates.role = normalizeRole(req.body.role || ROLE_AGENT);
        }

        if (Object.prototype.hasOwnProperty.call(req.body || {}, 'active')) {
          updates.active = req.body.active !== false;
        }

        if (Object.prototype.hasOwnProperty.call(req.body || {}, 'pin')) {
          const pin = String(req.body.pin || '').trim();
          if (!/^\d{4}$/.test(pin)) {
            return jsonError(res, 400, 'VALIDATION_ERROR', 'pin debe tener 4 dígitos.');
          }
          const { pinHash, pinSalt } = hashPin(pin);
          updates.pinHash = pinHash;
          updates.pinSalt = pinSalt;
        }

        if (!Object.keys(updates).length) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'No se recibieron campos para actualizar.');
        }

        updates.updatedAt = Timestamp.now();
        updates.updatedBy = req.hotelSession.agentId;

        await ref.set(updates, { merge: true });

        const updatedSnap = await ref.get();
        return res.json({
          success: true,
          data: sanitizeAgent(updatedSnap.data() || {}, updatedSnap.id),
        });
      } catch (error) {
        console.error('[hotel agents] Error actualizando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo actualizar el agente.');
      }
    }
  );

  router.delete(
    '/cliente/hotel/agents/:agentId',
    verifyHotelSession,
    requirePermission('canManageAgents'),
    async (req, res) => {
      try {
        const agentId = String(req.params?.agentId || '').trim();
        if (!agentId) {
          return jsonError(res, 400, 'VALIDATION_ERROR', 'agentId es obligatorio.');
        }

        const ref = req.hotelSession.negocioRef.collection('hotelAgents').doc(agentId);
        const snap = await ref.get();
        if (!snap.exists) {
          return jsonError(res, 404, 'AGENT_NOT_FOUND', 'Agente no encontrado.');
        }

        await ref.delete();

        return res.json({
          success: true,
          message: 'Agente eliminado.',
          data: { agentId },
        });
      } catch (error) {
        console.error('[hotel agents] Error eliminando:', error);
        return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo eliminar el agente.');
      }
    }
  );

  router.post('/public/hotel/:slug/reservations/checkout', async (req, res) => {
    try {
      const slug = String(req.params?.slug || '').trim().toLowerCase();
      if (!slug) {
        return jsonError(res, 400, 'VALIDATION_ERROR', 'slug es obligatorio.');
      }

      const validation = validateCheckoutBody(req.body || {});
      if (!validation.ok) {
        return jsonError(res, 400, 'VALIDATION_ERROR', validation.error);
      }

      const negocioRecord = await findNegocioBySlug(slug);
      if (!negocioRecord) {
        return jsonError(res, 404, 'NEGOCIO_NOT_FOUND', 'Negocio no encontrado.');
      }

      const negocioData = negocioRecord.data || {};
      if (!isHotelAdvancedAppActive(negocioData)) {
        return jsonError(res, 403, 'APP_NOT_ACTIVE', 'La app hotel no está activa para este negocio.');
      }

      const { checkIn, checkOut, roomTypeId, nights } = validation;

      const roomTypeRef = negocioRecord.ref.collection('hotelRoomTypes').doc(roomTypeId);
      const roomTypeSnap = await roomTypeRef.get();
      if (!roomTypeSnap.exists) {
        return jsonError(res, 404, 'ROOM_TYPE_NOT_FOUND', 'Tipo de habitación no encontrado.');
      }

      const roomType = roomTypeSnap.data() || {};
      if (roomType.active === false) {
        return jsonError(res, 409, 'ROOM_TYPE_INACTIVE', 'Este tipo de habitación no está disponible.');
      }

      const baseRate = Math.max(0, toNumberSafe(roomType.baseRate, 0));
      const unitsTotal = Math.max(1, Math.floor(toNumberSafe(roomType.unitsTotal, 1)));
      const total = Math.max(0, baseRate * nights);
      const deposit = Math.max(baseRate, 0);

      const stayDates = listStayDates(checkIn, checkOut);
      const availability = await ensureInventoryAvailable({
        negocioRef: negocioRecord.ref,
        roomTypeId,
        stayDates,
        fallbackUnits: unitsTotal,
      });

      if (!availability.ok) {
        return jsonError(
          res,
          409,
          'NO_AVAILABILITY',
          `No hay disponibilidad para la fecha ${availability.date}.`
        );
      }

      const guestName = String(req.body?.guestName || '').trim();
      const guestEmail = String(req.body?.guestEmail || '').trim().toLowerCase();
      const guestPhone = normalizarTelefono(req.body?.guestPhone || '');
      const notes = String(req.body?.notes || '').trim();
      const currency = normalizeCurrency(
        negocioData?.advancedApp?.config?.currency ||
          req.body?.currency ||
          'mxn'
      );

      const now = Timestamp.now();
      const reservationRef = await negocioRecord.ref.collection('hotelReservations').add({
        roomTypeId,
        checkIn,
        checkOut,
        nights,
        total,
        deposit,
        currency,
        status: 'pending_payment',
        guestName,
        guestEmail,
        guestPhone,
        notes,
        payment: {
          status: 'pending',
          provider: 'stripe',
        },
        refundStatus: null,
        createdAt: now,
        updatedAt: now,
        source: 'public_checkout',
      });

      const host = extractRequestHost(req);
      const baseUrl = pickCheckoutBaseUrl(req);
      const loginPath = formatCheckoutPathForHost(host);
      const successUrl = `${baseUrl}${loginPath}?payment=success&reservationId=${encodeURIComponent(
        reservationRef.id
      )}`;
      const cancelUrl = `${baseUrl}${loginPath}?payment=cancelled&reservationId=${encodeURIComponent(
        reservationRef.id
      )}`;

      const amountCents = Math.max(100, Math.round(deposit * 100));

      const stripeSession = await stripe.checkout.sessions.create({
        mode: 'payment',
        payment_method_types: ['card'],
        line_items: [
          {
            price_data: {
              currency,
              product_data: {
                name: `Anticipo reservación - ${negocioData.companyInfo || 'Hotel'}`,
                description: `${roomType.name || 'Habitación'} · ${checkIn} a ${checkOut}`,
              },
              unit_amount: amountCents,
            },
            quantity: 1,
          },
        ],
        success_url: successUrl,
        cancel_url: cancelUrl,
        metadata: {
          hotelFlow: 'hotel_premium_deposit',
          negocioId: negocioRecord.id,
          reservationId: reservationRef.id,
          roomTypeId,
          checkIn,
          checkOut,
        },
        locale: 'es-419',
        ...(guestEmail ? { customer_email: guestEmail } : {}),
      });

      await reservationRef.set(
        {
          payment: {
            status: 'pending',
            provider: 'stripe',
            checkoutSessionId: String(stripeSession.id || ''),
            checkoutUrl: String(stripeSession.url || ''),
          },
          updatedAt: Timestamp.now(),
        },
        { merge: true }
      );

      return res.status(201).json({
        success: true,
        data: {
          reservationId: reservationRef.id,
          checkoutUrl: stripeSession.url,
          sessionId: stripeSession.id,
          amount: deposit,
          currency,
        },
      });
    } catch (error) {
      console.error('[hotel checkout] Error:', error);
      return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo crear el checkout de la reserva.', error?.message || null);
    }
  });

  return router;
}
