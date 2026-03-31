import crypto from 'crypto';
import { admin, db } from './firebaseAdmin.js';

const CLIENT_REALM = 'cliente_portal';
const CLIENT_ROLE = 'cliente';
const JWT_ALG = 'HS256';
const DEFAULT_TOKEN_TTL_SECONDS = 60 * 60 * 12; // 12h
const DEFAULT_UPLOAD_URL_TTL_SECONDS = 10 * 60; // 10m

function toLowerSafe(value) {
  return String(value || '').trim().toLowerCase();
}

function uniqueStrings(values = []) {
  return [...new Set(values.map((entry) => String(entry || '').trim()).filter(Boolean))];
}

function getClientJwtSecret() {
  const candidates = [
    process.env.CLIENT_PORTAL_JWT_SECRET,
    process.env.SESSION_SECRET,
    process.env.INTERNAL_API_KEY,
  ];
  const found = candidates.find((value) => String(value || '').trim());
  return String(found || '').trim();
}

function getClientTokenTtlSeconds() {
  const raw = Number(process.env.CLIENT_PORTAL_TOKEN_TTL_SECONDS || 0);
  if (Number.isFinite(raw) && raw > 0) return Math.floor(raw);
  return DEFAULT_TOKEN_TTL_SECONDS;
}

function getUploadUrlTtlSeconds() {
  const raw = Number(process.env.CLIENT_UPLOAD_URL_TTL_SECONDS || 0);
  if (Number.isFinite(raw) && raw > 0) return Math.floor(raw);
  return DEFAULT_UPLOAD_URL_TTL_SECONDS;
}

function getFirebaseWebApiKey() {
  const fallbackPublicApiKey = 'AIzaSyDRlMNKL27by9k3Fb2iXia9aHLI1WRAoHA';

  const candidates = [
    process.env.FIREBASE_WEB_API_KEY,
    process.env.FIREBASE_API_KEY,
    process.env.NEXT_PUBLIC_FIREBASE_API_KEY,
    process.env.NEXT_PUBLIC_FIREBASE_APIKEY,
    fallbackPublicApiKey,
  ];

  const found = candidates.find((value) => String(value || '').trim());
  return String(found || '').trim();
}

function jsonError(res, status, code, message, details) {
  return res.status(status).json({
    success: false,
    message,
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

function timingSafeEqualString(a, b) {
  const left = Buffer.from(String(a || ''));
  const right = Buffer.from(String(b || ''));
  if (left.length !== right.length) return false;
  return crypto.timingSafeEqual(left, right);
}

function hashSha256(value) {
  return crypto.createHash('sha256').update(String(value || '')).digest('hex');
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

function decodeJwtUnsafe(token) {
  const parts = String(token || '').trim().split('.');
  if (parts.length < 2) return null;

  try {
    const payloadBuffer = base64UrlDecode(parts[1]);
    const payload = JSON.parse(payloadBuffer.toString('utf8'));
    return payload && typeof payload === 'object' ? payload : null;
  } catch {
    return null;
  }
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

function isClientePortalClaims(payload) {
  if (!payload || typeof payload !== 'object') return false;

  const realm = toLowerSafe(payload.realm);
  const aud = normalizeAudience(payload.aud);
  const roles = normalizeRoles(payload);

  const hasRealm = realm === CLIENT_REALM || aud.includes(CLIENT_REALM);
  const hasRole = roles.includes(CLIENT_ROLE);

  return hasRealm && hasRole;
}

function readBearerToken(req) {
  const authHeader = String(req.get('authorization') || '').trim();
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

async function readNegocioApiConfig(negocioId) {
  const safeNegocioId = String(negocioId || '').trim();
  if (!safeNegocioId) return null;

  const collections = ['negociosapi', 'NegociosApi'];

  for (const collectionName of collections) {
    const snap = await db.collection(collectionName).doc(safeNegocioId).get();
    if (snap.exists) {
      return {
        sourceCollection: collectionName,
        negocioId: safeNegocioId,
        data: snap.data() || {},
      };
    }
  }

  return null;
}

function extractApiKeyCandidates(configData = {}) {
  const plain = uniqueStrings([
    configData.apiKey,
    configData.key,
    configData.token,
    configData.secret,
    configData.credentials?.apiKey,
  ]);

  const hashes = uniqueStrings([
    configData.apiKeyHash,
    configData.keyHash,
    configData.sha256,
    configData.hashedKey,
    configData.credentials?.apiKeyHash,
  ]);

  return { plain, hashes };
}

function negocioApiConfigEnabled(configData = {}) {
  if (configData.enabled === false) return false;
  if (configData.active === false) return false;

  const status = toLowerSafe(configData.status);
  if (!status) return true;
  if (['disabled', 'inactive', 'suspended', 'revoked'].includes(status)) {
    return false;
  }

  return true;
}

function negocioApiConfigHasScope(configData = {}, requiredScope = CLIENT_REALM) {
  const scopes = Array.isArray(configData.scopes)
    ? configData.scopes.map((entry) => toLowerSafe(entry)).filter(Boolean)
    : [];

  if (!scopes.length) return true;
  const safeRequiredScope = toLowerSafe(requiredScope);

  return scopes.includes('*') || scopes.includes(safeRequiredScope);
}

function apiKeyMatchesConfig(incomingApiKey, configData = {}) {
  const safeIncomingApiKey = String(incomingApiKey || '').trim();
  if (!safeIncomingApiKey) return false;

  const { plain, hashes } = extractApiKeyCandidates(configData);
  const incomingHash = hashSha256(safeIncomingApiKey);

  if (plain.some((entry) => timingSafeEqualString(entry, safeIncomingApiKey))) {
    return true;
  }

  if (hashes.some((entry) => timingSafeEqualString(toLowerSafe(entry), incomingHash))) {
    return true;
  }

  return false;
}

async function validateTenantHeaders(req, res, { requiredScope = CLIENT_REALM } = {}) {
  const negocioId = String(req.get('x-negocio-id') || '').trim();
  const apiKey = String(req.get('x-negocio-api-key') || '').trim();

  if (!negocioId || !apiKey) {
    return {
      ok: false,
      response: jsonError(
        res,
        401,
        'MISSING_TENANT_HEADERS',
        'Faltan headers x-negocio-id o x-negocio-api-key.'
      ),
    };
  }

  const configRecord = await readNegocioApiConfig(negocioId);
  if (!configRecord) {
    return {
      ok: false,
      response: jsonError(res, 403, 'TENANT_NOT_ENABLED', 'El negocio no está habilitado en negociosapi.'),
    };
  }

  const configData = configRecord.data || {};

  if (!negocioApiConfigEnabled(configData)) {
    return {
      ok: false,
      response: jsonError(res, 403, 'TENANT_DISABLED', 'La API del negocio está deshabilitada.'),
    };
  }

  if (!apiKeyMatchesConfig(apiKey, configData)) {
    return {
      ok: false,
      response: jsonError(res, 401, 'INVALID_API_KEY', 'API key de negocio inválida.'),
    };
  }

  if (!negocioApiConfigHasScope(configData, requiredScope)) {
    return {
      ok: false,
      response: jsonError(
        res,
        403,
        'INSUFFICIENT_SCOPE',
        `El negocio no tiene scope ${requiredScope}.`
      ),
    };
  }

  return {
    ok: true,
    tenant: {
      negocioId,
      config: configData,
      sourceCollection: configRecord.sourceCollection,
    },
  };
}

async function firebaseSignInWithEmailPassword(email, password) {
  const apiKey = getFirebaseWebApiKey();
  if (!apiKey) {
    throw new Error('Falta FIREBASE_WEB_API_KEY/FIREBASE_API_KEY.');
  }

  const endpoint = `https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=${encodeURIComponent(apiKey)}`;

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      email,
      password,
      returnSecureToken: true,
    }),
  });

  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  if (!response.ok) {
    const errorCode = String(payload?.error?.message || '').trim();
    if (['INVALID_LOGIN_CREDENTIALS', 'EMAIL_NOT_FOUND', 'INVALID_PASSWORD', 'USER_DISABLED'].includes(errorCode)) {
      return {
        ok: false,
        status: 401,
        error: 'Credenciales inválidas.',
        details: errorCode,
      };
    }

    return {
      ok: false,
      status: 502,
      error: 'No se pudo validar usuario en Firebase Auth.',
      details: errorCode || payload || null,
    };
  }

  return {
    ok: true,
    uid: String(payload?.localId || '').trim(),
    email: String(payload?.email || email || '').trim().toLowerCase(),
    idToken: String(payload?.idToken || '').trim(),
  };
}

async function readNegocioById(negocioId) {
  const docRef = db.collection('Negocios').doc(String(negocioId || '').trim());
  const snap = await docRef.get();
  if (!snap.exists) return null;
  return {
    id: snap.id,
    ref: docRef,
    data: snap.data() || {},
  };
}

function isUserAuthorizedForNegocio(negocioData = {}, { uid = '', email = '' } = {}) {
  const safeUid = String(uid || '').trim();
  const safeEmail = toLowerSafe(email);

  const ownerUid = String(negocioData.ownerUID || '').trim();
  if (ownerUid && safeUid && ownerUid !== safeUid) {
    return false;
  }

  const ownerEmail = toLowerSafe(negocioData.ownerEmail);
  const contactEmail = toLowerSafe(negocioData.contactEmail);
  const candidateEmails = [ownerEmail, contactEmail];

  if (Array.isArray(negocioData.panelEmails)) {
    candidateEmails.push(...negocioData.panelEmails.map((entry) => toLowerSafe(entry)));
  }
  if (Array.isArray(negocioData.allowedPanelEmails)) {
    candidateEmails.push(...negocioData.allowedPanelEmails.map((entry) => toLowerSafe(entry)));
  }
  if (Array.isArray(negocioData.adminEmails)) {
    candidateEmails.push(...negocioData.adminEmails.map((entry) => toLowerSafe(entry)));
  }

  const emailSet = new Set(candidateEmails.filter(Boolean));

  if (emailSet.size && safeEmail && !emailSet.has(safeEmail)) {
    return false;
  }

  return true;
}

function evaluateNegocioAccess(negocioData = {}) {
  let hasAccess = false;
  let accessReason = '';
  const details = {};

  const panelExplicitEnabled =
    negocioData.panelWebEnabled === true
    || negocioData.features?.panelWeb === true
    || negocioData.specialPlanEnabled === true;

  if (panelExplicitEnabled) {
    hasAccess = true;
    accessReason = 'panel_feature';
  }

  if (!hasAccess && negocioData.trialActive) {
    const now = Date.now();
    const trialEnd = negocioData.trialEndDate?.toMillis?.() || 0;
    if (trialEnd > now) {
      hasAccess = true;
      accessReason = 'trial';
      details.trialEndsAt = new Date(trialEnd).toISOString();
    }
  }

  if (!hasAccess && String(negocioData.subscriptionStatus || '').toLowerCase() === 'active') {
    hasAccess = true;
    accessReason = 'subscription';
    details.subscriptionStatus = 'active';
  }

  if (!hasAccess) {
    const plan = String(negocioData.plan || '').toLowerCase();
    const paidPlans = ['basic', 'basico', 'pro', 'premium', 'enterprise', 'especial'];

    if (paidPlans.includes(plan)) {
      const renewalDateMillis =
        negocioData.planRenewalDate?.toMillis?.()
        || negocioData.planExpiresAt?.toMillis?.()
        || negocioData.expiresAt?.toMillis?.()
        || 0;

      if (renewalDateMillis > Date.now()) {
        hasAccess = true;
        accessReason = 'manual_plan';
        details.expiresAt = new Date(renewalDateMillis).toISOString();
      }
    }
  }

  if (negocioData.websiteArchived || negocioData.siteTemporarilyDisabled) {
    hasAccess = false;
    accessReason = '';
  }

  if (hasAccess) {
    return {
      allowed: true,
      reason: accessReason || 'active',
      details,
    };
  }

  const subscriptionStatus = String(negocioData.subscriptionStatus || '').toLowerCase();
  if (subscriptionStatus === 'past_due') {
    return {
      allowed: false,
      status: 403,
      error: 'Tu suscripción tiene pago pendiente. Actualiza tu método de pago.',
      details: { needsPayment: true },
    };
  }

  if (negocioData.trialUsed && !negocioData.subscriptionId) {
    return {
      allowed: false,
      status: 403,
      error: 'Tu período de prueba expiró. Suscríbete para continuar.',
      details: { trialExpired: true, canSubscribe: true },
    };
  }

  return {
    allowed: false,
    status: 403,
    error: 'Tu plan ha expirado. Contacta al administrador para renovar.',
    details: { canSubscribe: true },
  };
}

function buildClienteSessionPayload({ negocioId, uid, email }) {
  return {
    iss: 'vevcrm',
    sub: String(uid || '').trim(),
    aud: CLIENT_REALM,
    role: CLIENT_ROLE,
    realm: CLIENT_REALM,
    negocioId: String(negocioId || '').trim(),
    email: toLowerSafe(email),
    sid: crypto.randomUUID(),
  };
}

function sanitizeNegocioResponse(negocioId, negocioData = {}) {
  return {
    negocioId,
    companyInfo: negocioData.companyInfo || 'Mi Negocio',
    slug: negocioData.slug || '',
    plan: negocioData.plan || null,
    templateId: negocioData.templateId || 'info',
    logoURL: negocioData.logoURL || '',
    contactEmail: negocioData.contactEmail || '',
    contactWhatsapp: negocioData.contactWhatsapp || '',
    subscriptionStatus: negocioData.subscriptionStatus || null,
    hasStripeSubscription: Boolean(negocioData.subscriptionId),
    expiresAt:
      negocioData.planRenewalDate?.toDate?.()
      || negocioData.planExpiresAt?.toDate?.()
      || negocioData.expiresAt?.toDate?.()
      || null,
    panelWebEnabled:
      negocioData.panelWebEnabled === true
      || negocioData.features?.panelWeb === true
      || negocioData.specialPlanEnabled === true,
  };
}

function verifyClienteBearerToken(req, res, expectedNegocioId) {
  const token = readBearerToken(req);
  if (!token) {
    return {
      ok: false,
      response: jsonError(res, 401, 'UNAUTHORIZED', 'Falta token Bearer.'),
    };
  }

  const verification = verifyJwt(token);
  if (!verification.valid) {
    return {
      ok: false,
      response: jsonError(res, 401, 'UNAUTHORIZED', 'Token inválido o expirado.', {
        reason: verification.reason,
      }),
    };
  }

  const payload = verification.payload || {};
  if (!isClientePortalClaims(payload)) {
    return {
      ok: false,
      response: jsonError(res, 403, 'FORBIDDEN', 'Token no autorizado para cliente_portal.'),
    };
  }

  const tokenNegocioId = String(payload.negocioId || '').trim();
  if (expectedNegocioId && tokenNegocioId && tokenNegocioId !== expectedNegocioId) {
    return {
      ok: false,
      response: jsonError(res, 403, 'FORBIDDEN', 'El token no pertenece al negocio solicitado.'),
    };
  }

  return {
    ok: true,
    token,
    payload,
  };
}

function sanitizePathSegment(input, fallback = 'item') {
  const cleaned = String(input || '')
    .trim()
    .replace(/[^a-zA-Z0-9._-]/g, '_')
    .replace(/_+/g, '_');
  return cleaned || fallback;
}

function sanitizeFolderPath(input) {
  const safe = String(input || 'uploads')
    .trim()
    .replace(/\\/g, '/')
    .replace(/\.{2,}/g, '.')
    .replace(/[^a-zA-Z0-9/_-]/g, '_')
    .replace(/\/+/g, '/')
    .replace(/^\/+|\/+$/g, '');

  return safe || 'uploads';
}

function buildStoragePublicUrl(bucketName, objectPath) {
  const encoded = String(objectPath || '')
    .split('/')
    .map((segment) => encodeURIComponent(segment))
    .join('/');

  return `https://storage.googleapis.com/${encodeURIComponent(bucketName)}/${encoded}`;
}

export async function loginClientePortalAuth(req, res) {
  try {
    const tenantCheck = await validateTenantHeaders(req, res, { requiredScope: CLIENT_REALM });
    if (!tenantCheck.ok) return tenantCheck.response;

    const { negocioId } = tenantCheck.tenant;
    const email = toLowerSafe(req.body?.email);
    const password = String(req.body?.password || '').trim();

    if (!email || !password) {
      return jsonError(res, 400, 'VALIDATION_ERROR', 'email y password son obligatorios.');
    }

    const signInResult = await firebaseSignInWithEmailPassword(email, password);
    if (!signInResult.ok) {
      return jsonError(res, signInResult.status, 'INVALID_CREDENTIALS', signInResult.error, signInResult.details);
    }

    const negocioRecord = await readNegocioById(negocioId);
    if (!negocioRecord) {
      return jsonError(res, 404, 'NEGOCIO_NOT_FOUND', 'Negocio no encontrado.');
    }

    const negocioData = negocioRecord.data || {};
    if (!isUserAuthorizedForNegocio(negocioData, { uid: signInResult.uid, email: signInResult.email })) {
      return jsonError(
        res,
        403,
        'FORBIDDEN',
        'El usuario no tiene acceso a este negocio.'
      );
    }

    const access = evaluateNegocioAccess(negocioData);
    if (!access.allowed) {
      return jsonError(res, access.status || 403, 'ACCESS_DENIED', access.error, access.details);
    }

    const session = signJwt(buildClienteSessionPayload({
      negocioId,
      uid: signInResult.uid,
      email: signInResult.email,
    }), {
      expiresInSeconds: getClientTokenTtlSeconds(),
    });

    await negocioRecord.ref.update({
      lastLoginAt: new Date(),
      lastLoginIP: req.ip || req.headers['x-forwarded-for'] || 'unknown',
      lastLoginEmail: signInResult.email,
    });

    return res.json({
      success: true,
      data: {
        ...sanitizeNegocioResponse(negocioId, negocioData),
        uid: signInResult.uid,
        email: signInResult.email,
        accessType: access.reason,
        accessDetails: access.details,
        token: session.token,
        sessionToken: session.token,
        expiresInSeconds: session.expiresInSeconds,
      },
    });
  } catch (error) {
    console.error('[cliente auth email login] Error:', error);
    return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo completar el login.', error?.message || null);
  }
}

export async function meClientePortalAuth(req, res) {
  try {
    const tenantCheck = await validateTenantHeaders(req, res, { requiredScope: CLIENT_REALM });
    if (!tenantCheck.ok) return tenantCheck.response;

    const { negocioId } = tenantCheck.tenant;
    const tokenCheck = verifyClienteBearerToken(req, res, negocioId);
    if (!tokenCheck.ok) return tokenCheck.response;

    const negocioRecord = await readNegocioById(negocioId);
    if (!negocioRecord) {
      return jsonError(res, 404, 'NEGOCIO_NOT_FOUND', 'Negocio no encontrado.');
    }

    const negocioData = negocioRecord.data || {};
    const access = evaluateNegocioAccess(negocioData);
    if (!access.allowed) {
      return jsonError(res, access.status || 403, 'ACCESS_DENIED', access.error, access.details);
    }

    return res.json({
      success: true,
      data: {
        ...sanitizeNegocioResponse(negocioId, negocioData),
        uid: tokenCheck.payload.sub,
        email: tokenCheck.payload.email,
        accessType: access.reason,
        accessDetails: access.details,
      },
    });
  } catch (error) {
    console.error('[cliente auth me] Error:', error);
    return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo validar sesión.', error?.message || null);
  }
}

export async function refreshClientePortalAuth(req, res) {
  try {
    const tenantCheck = await validateTenantHeaders(req, res, { requiredScope: CLIENT_REALM });
    if (!tenantCheck.ok) return tenantCheck.response;

    const { negocioId } = tenantCheck.tenant;
    const tokenCheck = verifyClienteBearerToken(req, res, negocioId);
    if (!tokenCheck.ok) return tokenCheck.response;

    const negocioRecord = await readNegocioById(negocioId);
    if (!negocioRecord) {
      return jsonError(res, 404, 'NEGOCIO_NOT_FOUND', 'Negocio no encontrado.');
    }

    const negocioData = negocioRecord.data || {};
    const access = evaluateNegocioAccess(negocioData);
    if (!access.allowed) {
      return jsonError(res, access.status || 403, 'ACCESS_DENIED', access.error, access.details);
    }

    const session = signJwt(buildClienteSessionPayload({
      negocioId,
      uid: tokenCheck.payload.sub,
      email: tokenCheck.payload.email,
    }), {
      expiresInSeconds: getClientTokenTtlSeconds(),
    });

    return res.json({
      success: true,
      data: {
        sessionToken: session.token,
        token: session.token,
        expiresInSeconds: session.expiresInSeconds,
      },
    });
  } catch (error) {
    console.error('[cliente auth refresh] Error:', error);
    return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo refrescar sesión.', error?.message || null);
  }
}

export async function logoutClientePortalAuth(req, res) {
  try {
    const tenantCheck = await validateTenantHeaders(req, res, { requiredScope: CLIENT_REALM });
    if (!tenantCheck.ok) return tenantCheck.response;

    const { negocioId } = tenantCheck.tenant;
    const bearer = readBearerToken(req);

    if (bearer) {
      const validation = verifyJwt(bearer, { allowExpired: true });
      if (validation.valid && validation.payload?.negocioId && validation.payload.negocioId !== negocioId) {
        return jsonError(res, 403, 'FORBIDDEN', 'El token no pertenece al negocio actual.');
      }
      if (validation.valid && !isClientePortalClaims(validation.payload)) {
        return jsonError(res, 403, 'FORBIDDEN', 'Token no autorizado para cliente_portal.');
      }
    }

    return res.json({
      success: true,
      message: 'Sesión cerrada.',
    });
  } catch (error) {
    console.error('[cliente auth logout] Error:', error);
    return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo cerrar sesión.', error?.message || null);
  }
}

export async function getClienteNegocioById(req, res) {
  try {
    const tenantCheck = await validateTenantHeaders(req, res, { requiredScope: CLIENT_REALM });
    if (!tenantCheck.ok) return tenantCheck.response;

    const requestedNegocioId = String(req.params?.negocioId || '').trim();
    const headerNegocioId = tenantCheck.tenant.negocioId;

    if (!requestedNegocioId || requestedNegocioId !== headerNegocioId) {
      return jsonError(res, 403, 'FORBIDDEN', 'No puedes consultar otro negocio.');
    }

    const tokenCheck = verifyClienteBearerToken(req, res, requestedNegocioId);
    if (!tokenCheck.ok) return tokenCheck.response;

    const negocioRecord = await readNegocioById(requestedNegocioId);
    if (!negocioRecord) {
      return jsonError(res, 404, 'NEGOCIO_NOT_FOUND', 'Negocio no encontrado.');
    }

    return res.json({
      success: true,
      data: sanitizeNegocioResponse(requestedNegocioId, negocioRecord.data || {}),
    });
  } catch (error) {
    console.error('[cliente negocios by id] Error:', error);
    return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo consultar el negocio.', error?.message || null);
  }
}

export async function createClienteStorageUploadUrl(req, res) {
  try {
    const tenantCheck = await validateTenantHeaders(req, res, { requiredScope: CLIENT_REALM });
    if (!tenantCheck.ok) return tenantCheck.response;

    const { negocioId } = tenantCheck.tenant;
    const tokenCheck = verifyClienteBearerToken(req, res, negocioId);
    if (!tokenCheck.ok) return tokenCheck.response;

    const fileName = sanitizePathSegment(req.body?.fileName, 'file.bin');
    const mimeType = String(req.body?.mimeType || '').trim().toLowerCase();
    const fileSize = Number(req.body?.fileSize || 0);
    const folder = sanitizeFolderPath(req.body?.folder || 'uploads');

    if (!mimeType || !fileSize || fileSize <= 0) {
      return jsonError(res, 400, 'VALIDATION_ERROR', 'mimeType y fileSize son obligatorios.');
    }

    const maxFileSize = Number(process.env.CLIENT_UPLOAD_MAX_BYTES || 25 * 1024 * 1024);
    if (fileSize > maxFileSize) {
      return jsonError(res, 413, 'FILE_TOO_LARGE', `El archivo excede ${maxFileSize} bytes.`);
    }

    const objectPath = `negocios/${sanitizePathSegment(negocioId, 'negocio')}/${folder}/${Date.now()}_${fileName}`;
    const bucket = admin.storage().bucket();
    const [uploadUrl] = await bucket.file(objectPath).getSignedUrl({
      version: 'v4',
      action: 'write',
      expires: Date.now() + getUploadUrlTtlSeconds() * 1000,
      contentType: mimeType,
    });

    const publicUrl = buildStoragePublicUrl(bucket.name, objectPath);

    return res.json({
      success: true,
      data: {
        uploadUrl,
        method: 'PUT',
        filePath: objectPath,
        bucket: bucket.name,
        mimeType,
        publicUrl,
        expiresInSeconds: getUploadUrlTtlSeconds(),
      },
    });
  } catch (error) {
    console.error('[cliente storage upload-url] Error:', error);
    return jsonError(res, 500, 'INTERNAL_ERROR', 'No se pudo generar URL firmada.', error?.message || null);
  }
}

export function rejectClienteTokenOnAdminRoutes(req, res, next) {
  try {
    const token = readBearerToken(req);
    if (!token) return next();

    const verified = verifyJwt(token, { allowExpired: true });
    const payload = verified.valid ? verified.payload : decodeJwtUnsafe(token);

    if (isClientePortalClaims(payload)) {
      return jsonError(
        res,
        403,
        'FORBIDDEN',
        'Tokens de cliente_portal no pueden acceder a rutas admin.'
      );
    }

    return next();
  } catch (error) {
    console.error('[admin guard] Error:', error);
    return next();
  }
}
