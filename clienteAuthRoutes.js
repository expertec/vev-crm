// clienteAuthRoutes.js - Sistema de autenticación para clientes con soporte de suscripciones

import crypto from 'crypto';
import { admin, db } from './firebaseAdmin.js';
import { normalizarTelefono } from './pinUtils.js';

const CLIENT_REALM = 'cliente_portal';
const CLIENT_ROLE = 'cliente';
const JWT_ALG = 'HS256';
const DEFAULT_TOKEN_TTL_SECONDS = 60 * 60 * 12; // 12h

function toLowerSafe(value) {
  return String(value || '').trim().toLowerCase();
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
  if (found) return String(found).trim();

  if (toLowerSafe(process.env.NODE_ENV || 'development') !== 'production') {
    return String(
      process.env.CLIENT_PORTAL_DEV_JWT_SECRET ||
      process.env.DEV_SESSION_SECRET ||
      'dev_local_cliente_portal_secret_change_me'
    ).trim();
  }

  return '';
}

function getClientTokenTtlSeconds() {
  const raw = Number(process.env.CLIENT_PORTAL_TOKEN_TTL_SECONDS || 0);
  if (Number.isFinite(raw) && raw > 0) return Math.floor(raw);
  return DEFAULT_TOKEN_TTL_SECONDS;
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
    expiresInSeconds: exp - now,
    exp,
    iat: now,
  };
}

function verifyJwt(token) {
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
  if (!Number.isFinite(exp) || exp <= now) {
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

  return (
    realm === CLIENT_REALM ||
    aud.includes(CLIENT_REALM) ||
    roles.includes(CLIENT_ROLE)
  );
}

function buildClienteSessionPayload({ negocioId, phone, accountPhone, accessType }) {
  return {
    iss: 'vevcrm',
    sub: String(negocioId || '').trim(),
    aud: CLIENT_REALM,
    role: CLIENT_ROLE,
    realm: CLIENT_REALM,
    negocioId: String(negocioId || '').trim(),
    phone: String(phone || '').trim(),
    // Teléfono de la "cuenta" (multinegocio). Permite listar/cambiar entre los
    // negocios del mismo WhatsApp sin re-autenticar. Por defecto = phone.
    accountPhone: String(accountPhone || phone || '').trim(),
    accessType: String(accessType || '').trim() || 'manual',
    sid: crypto.randomUUID(),
  };
}

function decodeLegacyToken(token) {
  try {
    const decoded = Buffer.from(String(token || ''), 'base64').toString('utf-8');
    const payload = JSON.parse(decoded);
    return payload && typeof payload === 'object' ? payload : null;
  } catch {
    return null;
  }
}

// ============================================================================
// Cuenta multinegocio
// ----------------------------------------------------------------------------
// Una "cuenta" = un teléfono (WhatsApp). Un mismo cliente puede tener varios
// negocios con el mismo número, cada uno con su propio plan. El PIN es a nivel
// de cuenta: un solo teléfono+PIN entra a TODOS sus negocios.
//
// La sesión (JWT) sigue siendo POR-NEGOCIO (el negocio activo va en el token),
// para no tocar el panel ni el BFF. Cambiar de negocio = emitir un token nuevo
// para otro negocio de la cuenta (ver switchNegocio).
// ============================================================================

const CUENTAS_COLLECTION = 'CuentasCliente';
const ADD_NEGOCIO_SCOPE = 'add_negocio';
const ADD_NEGOCIO_TOKEN_TTL_SECONDS = 60 * 60 * 24; // 24h para completar el brief
const PLANES_ACTIVOS = ['basic', 'basico', 'pro', 'premium', 'ventas'];

function cuentaRef(phoneDigits) {
  return db.collection(CUENTAS_COLLECTION).doc(String(phoneDigits || '').trim());
}

function uniqueStrings(values = []) {
  return [...new Set(values.map((v) => String(v || '').trim()).filter(Boolean))];
}

// Evalúa el acceso de un negocio (trial / suscripción / plan manual). Misma
// lógica que usaba loginCliente, extraída para reutilizarla en la lista de
// negocios de la cuenta y en el switch.
function evaluateLegacyAccess(negocioData = {}) {
  if (negocioData.websiteArchived) {
    return { allowed: false, reason: '', expiresAt: null };
  }

  if (negocioData.trialActive) {
    const trialEnd = negocioData.trialEndDate?.toMillis?.() || 0;
    if (Date.now() < trialEnd) {
      return { allowed: true, reason: 'trial', expiresAt: new Date(trialEnd).toISOString() };
    }
  }

  if (String(negocioData.subscriptionStatus || '').toLowerCase() === 'active') {
    const next = negocioData.subscriptionCurrentPeriodEnd?.toDate?.() || null;
    return { allowed: true, reason: 'subscription', expiresAt: next ? next.toISOString() : null };
  }

  const plan = String(negocioData.plan || '').toLowerCase();
  if (plan && PLANES_ACTIVOS.includes(plan)) {
    const renewal =
      negocioData.planRenewalDate?.toMillis?.() ||
      negocioData.planExpiresAt?.toMillis?.() ||
      negocioData.expiresAt?.toMillis?.() ||
      0;
    if (renewal > Date.now()) {
      return { allowed: true, reason: 'manual', expiresAt: new Date(renewal).toISOString() };
    }
  }

  return { allowed: false, reason: '', expiresAt: null };
}

// Resumen ligero de un negocio para el selector de cuenta.
function buildNegocioSummary(negocioId, negocioData = {}) {
  const access = evaluateLegacyAccess(negocioData);
  return {
    negocioId,
    companyInfo: negocioData.companyInfo || 'Mi Negocio',
    slug: negocioData.slug || '',
    logoURL: negocioData.logoURL || '',
    plan: negocioData.plan || null,
    templateId: negocioData.templateId || 'info',
    addons: negocioData.addons || {},
    hasAccess: access.allowed,
    accessType: access.reason || '',
    expiresAt: access.expiresAt || null,
    status: negocioData.status || '',
  };
}

// Payload completo del negocio activo (mismos campos que devolvía el login),
// reutilizado por login y por switchNegocio para no divergir.
function buildActiveNegocioData(negocioId, negocioData, access) {
  return {
    negocioId,
    companyInfo: negocioData.companyInfo || 'Mi Negocio',
    slug: negocioData.slug || '',
    plan: negocioData.plan,
    addons: negocioData.addons || {},
    templateId: negocioData.templateId || 'info',
    logoURL: negocioData.logoURL || '',
    contactEmail: negocioData.contactEmail || '',
    contactWhatsapp: negocioData.contactWhatsapp || '',
    dominio:
      negocioData.dominio ||
      negocioData.domain ||
      negocioData.customDomain ||
      negocioData.custom_domain ||
      '',
    advancedApp: negocioData.advancedApp || null,
    advancedAppActive:
      String(negocioData?.advancedApp?.key || '').toLowerCase() === 'hotel_premium' &&
      String(negocioData?.advancedApp?.status || '').toLowerCase() === 'active',
    subscriptionType: access.reason,
    subscriptionStatus: negocioData.subscriptionStatus,
    hasStripeSubscription: !!negocioData.subscriptionId,
    expiresAt:
      negocioData.planRenewalDate?.toDate?.() ||
      negocioData.planExpiresAt?.toDate?.() ||
      negocioData.expiresAt?.toDate?.() ||
      null,
    planRenewalDate: negocioData.planRenewalDate?.toDate?.() || null,
    planExpiresAt: negocioData.planExpiresAt?.toDate?.() || null,
  };
}

// Reúne todos los negocios de un teléfono (por leadPhone y por accountPhone,
// que marcamos en los negocios creados desde una cuenta).
async function collectNegociosByPhone(phoneDigits) {
  const map = new Map();
  const [byLead, byAccount] = await Promise.all([
    db.collection('Negocios').where('leadPhone', '==', phoneDigits).get(),
    db.collection('Negocios').where('accountPhone', '==', phoneDigits).get(),
  ]);
  byLead.forEach((d) => map.set(d.id, d));
  byAccount.forEach((d) => map.set(d.id, d));
  return [...map.values()];
}

// Carga la cuenta del teléfono; si no existe, la aprovisiona desde los negocios
// que ya comparten ese WhatsApp (retrocompatible con clientes de un solo
// negocio). El PIN de la cuenta se toma del negocio principal.
// Devuelve { phone, pin, negocioIds, primaryNegocioId, _negocios } o null.
async function loadOrProvisionCuenta(phoneDigits) {
  const phone = String(phoneDigits || '').trim();
  if (!phone) return null;

  const ref = cuentaRef(phone);
  const [snap, negocios] = await Promise.all([ref.get(), collectNegociosByPhone(phone)]);

  const stored = snap.exists ? (snap.data() || {}) : {};
  const negocioIds = uniqueStrings([...(stored.negocioIds || []), ...negocios.map((d) => d.id)]);

  if (!negocioIds.length) return null;

  let pin = String(stored.pin || '').trim();
  if (!/^\d{4}$/.test(pin)) {
    const primaryDoc =
      negocios.find((d) => d.id === stored.primaryNegocioId) ||
      negocios.find((d) => /^\d{4}$/.test(String(d.data().pin || '').trim())) ||
      negocios[0];
    pin = String(primaryDoc?.data()?.pin || '').trim();
  }

  const primaryNegocioId = String(stored.primaryNegocioId || '').trim() || negocioIds[0] || '';

  // Solo escribimos si algo cambió (esta función se llama en cada verificarSesion,
  // así que evitamos amplificación de escrituras).
  const sameIds =
    Array.isArray(stored.negocioIds) &&
    stored.negocioIds.length === negocioIds.length &&
    negocioIds.every((id) => stored.negocioIds.includes(id));
  const needsWrite =
    !snap.exists ||
    String(stored.pin || '') !== pin ||
    String(stored.primaryNegocioId || '') !== primaryNegocioId ||
    !sameIds;

  const merged = { phone, pin, negocioIds, primaryNegocioId };

  if (needsWrite) {
    await ref.set(
      {
        ...merged,
        updatedAt: new Date(),
        ...(snap.exists ? {} : { createdAt: new Date() }),
      },
      { merge: true }
    );
  }

  return { ...merged, _negocios: negocios };
}

// Elige el negocio activo por defecto: el principal si tiene acceso, si no el
// primero con acceso, si no el principal (bloqueado).
function pickActiveNegocioId(summaries, primaryNegocioId) {
  const allowed = summaries.filter((n) => n.hasAccess);
  const primaryAllowed = allowed.find((n) => n.negocioId === primaryNegocioId);
  if (primaryAllowed) return primaryAllowed.negocioId;
  if (allowed.length) return allowed[0].negocioId;
  return primaryNegocioId || (summaries[0] && summaries[0].negocioId) || '';
}

// Lee y valida el Bearer del cliente (JWT nuevo o token legado) y devuelve la
// cuenta (teléfono) + negocio del token, o responde 401 y devuelve null.
function requireClienteBearer(req, res) {
  const authHeader = String(req.headers?.authorization || '').trim();
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  const token = match ? match[1].trim() : '';

  if (!token) {
    res.status(401).json({ success: false, error: 'No autorizado' });
    return null;
  }

  const verification = verifyJwt(token);
  const payload = verification.valid ? (verification.payload || {}) : decodeLegacyToken(token);
  if (!payload) {
    res.status(401).json({ success: false, error: 'Sesión inválida' });
    return null;
  }

  const phone = normalizarTelefono(payload.accountPhone || payload.phone || '');
  if (!phone) {
    res.status(401).json({ success: false, error: 'Sesión sin cuenta asociada' });
    return null;
  }

  return { phone, negocioId: String(payload.negocioId || payload.sub || '').trim(), payload };
}

// Token de un solo propósito para dar de alta un negocio nuevo desde una cuenta
// autenticada. Lo consume /api/web/after-form para saltar el dedup por teléfono.
export function createAddNegocioToken(phoneDigits) {
  return signJwt(
    {
      iss: 'vevcrm',
      realm: CLIENT_REALM,
      role: CLIENT_ROLE,
      scope: ADD_NEGOCIO_SCOPE,
      accountPhone: normalizarTelefono(phoneDigits),
      sid: crypto.randomUUID(),
    },
    { expiresInSeconds: ADD_NEGOCIO_TOKEN_TTL_SECONDS }
  ).token;
}

export function verifyAddNegocioToken(token) {
  const verification = verifyJwt(token);
  if (!verification.valid) return { valid: false };
  const payload = verification.payload || {};
  if (String(payload.scope || '') !== ADD_NEGOCIO_SCOPE) return { valid: false };
  const phone = normalizarTelefono(payload.accountPhone || '');
  if (!phone) return { valid: false };
  return { valid: true, phone };
}

// Liga un negocio recién creado a la cuenta (array de negocioIds) y devuelve el
// PIN de la cuenta para asignárselo al negocio nuevo (así entra con el mismo
// teléfono+PIN). Usado por /api/web/after-form en el alta desde cuenta.
export async function linkNegocioToCuenta(phoneDigits, negocioId) {
  const phone = normalizarTelefono(phoneDigits);
  const id = String(negocioId || '').trim();
  if (!phone || !id) return '';

  const ref = cuentaRef(phone);
  await ref.set(
    {
      phone,
      negocioIds: admin.firestore.FieldValue.arrayUnion(id),
      updatedAt: new Date(),
    },
    { merge: true }
  );

  const snap = await ref.get();
  return String(snap.data()?.pin || '').trim();
}

/**
 * POST /api/cliente/login
 * 
 * Autentica a un cliente usando teléfono + PIN
 * Ahora soporta suscripciones de Stripe y trials
 */
export async function loginCliente(req, res) {
  try {
    const { phone, pin } = req.body;

    // Validaciones básicas
    if (!phone || !pin) {
      return res.status(400).json({
        success: false,
        error: 'Teléfono y PIN son requeridos'
      });
    }

    // Normalizar teléfono (solo dígitos)
    const phoneDigits = normalizarTelefono(phone);

    // Validar formato de PIN (4 dígitos)
    const pinStr = String(pin).trim();
    if (!/^\d{4}$/.test(pinStr)) {
      return res.status(400).json({
        success: false,
        error: 'El PIN debe tener 4 dígitos'
      });
    }

    console.log(`🔐 Intento de login - Teléfono: ${phoneDigits}, PIN: ${pinStr}`);

    // Cargar la cuenta (multinegocio) del teléfono. Agrupa todos los negocios
    // que comparten ese WhatsApp; el PIN es a nivel de cuenta.
    const cuenta = await loadOrProvisionCuenta(phoneDigits);
    if (!cuenta) {
      console.log(`❌ No se encontró negocio con teléfono: ${phoneDigits}`);
      return res.status(401).json({
        success: false,
        error: 'Teléfono o PIN incorrectos'
      });
    }

    if (!/^\d{4}$/.test(String(cuenta.pin || ''))) {
      console.log(`⚠️ Cuenta ${phoneDigits} sin PIN asignado`);
      return res.status(401).json({
        success: false,
        error: 'No tienes acceso al panel. Contacta al administrador.'
      });
    }

    if (String(cuenta.pin).trim() !== pinStr) {
      console.log(`❌ PIN incorrecto para cuenta ${phoneDigits}`);
      return res.status(401).json({
        success: false,
        error: 'Teléfono o PIN incorrectos'
      });
    }

    // Lista de negocios de la cuenta + elección del negocio activo por defecto.
    const negocios = cuenta._negocios.map((d) => buildNegocioSummary(d.id, d.data()));
    const activeId = pickActiveNegocioId(negocios, cuenta.primaryNegocioId);
    const negocioDoc = cuenta._negocios.find((d) => d.id === activeId) || cuenta._negocios[0];
    if (!negocioDoc) {
      return res.status(401).json({
        success: false,
        error: 'Teléfono o PIN incorrectos'
      });
    }
    const negocioData = negocioDoc.data();
    const negocioId = negocioDoc.id;
    const access = evaluateLegacyAccess(negocioData);

    const accountInfo = {
      phone: phoneDigits,
      primaryNegocioId: cuenta.primaryNegocioId,
      negocioIds: cuenta.negocioIds,
    };

    // Si el negocio activo no tiene acceso significa que NINGÚN negocio de la
    // cuenta está activo (pickActive habría elegido uno con acceso si existiera).
    // Conservamos el mismo gating que antes, pero devolvemos la lista de negocios
    // para que el panel pueda ofrecer "Activar plan".
    if (!access.allowed) {
      if (negocioData.subscriptionStatus === 'past_due') {
        return res.status(403).json({
          success: false,
          error: 'Tu suscripción tiene un pago pendiente. Por favor actualiza tu método de pago.',
          needsPayment: true,
          subscriptionId: negocioData.subscriptionId,
          negocios,
          account: accountInfo,
        });
      }

      if (negocioData.trialUsed && !negocioData.subscriptionId) {
        return res.status(403).json({
          success: false,
          error: 'Tu período de prueba ha expirado. Suscríbete para continuar.',
          trialExpired: true,
          canSubscribe: true,
          negocios,
          account: accountInfo,
        });
      }

      return res.status(403).json({
        success: false,
        error: 'Tu plan ha expirado. Contacta al administrador para renovar.',
        canSubscribe: true,
        negocios,
        account: accountInfo,
      });
    }

    // ✅ Login exitoso
    console.log(`✅ Login exitoso - Cuenta: ${phoneDigits} - Negocio activo: ${negocioId} (${negocioData.companyInfo}) - ${negocios.length} negocio(s)`);

    // Generar token compatible con el portal cliente y mantener el token legado
    const tokenData = {
      negocioId,
      phone: phoneDigits,
      timestamp: Date.now(),
      accessType: access.reason,
    };
    const legacyToken = Buffer.from(JSON.stringify(tokenData)).toString('base64');
    const session = signJwt(
      buildClienteSessionPayload({
        negocioId,
        phone: phoneDigits,
        accountPhone: phoneDigits,
        accessType: access.reason,
      }),
      {
        expiresInSeconds: getClientTokenTtlSeconds(),
      }
    );

    // Actualizar última fecha de acceso
    await negocioDoc.ref.update({
      lastLoginAt: new Date(),
      lastLoginIP: req.ip || req.headers['x-forwarded-for'] || 'unknown'
    });

    // Responder con datos del negocio activo + lista de la cuenta.
    return res.json({
      success: true,
      message: 'Login exitoso',
      data: {
        ...buildActiveNegocioData(negocioId, negocioData, access),
        ...(access.reason === 'trial' && access.expiresAt ? { trialEndsAt: access.expiresAt } : {}),
        token: session.token,
        sessionToken: session.token,
        legacyToken,
        legacySessionToken: legacyToken,
        tokenFormat: 'jwt',
        // Multinegocio: lista para el selector + datos de la cuenta.
        negocios,
        account: accountInfo,
      }
    });

  } catch (error) {
    console.error('❌ Error en login de cliente:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
}

/**
 * POST /api/cliente/verificar-sesion
 * 
 * Verifica si un token de sesión es válido
 * Actualizado para soportar suscripciones
 */
export async function verificarSesion(req, res) {
  try {
    const { token } = req.body;

    if (!token) {
      return res.status(400).json({
        success: false,
        error: 'Token requerido'
      });
    }

    let tokenData = null;
    let negocioId = '';
    let timestamp = 0;

    const jwtVerification = verifyJwt(token);
    if (jwtVerification.valid && isClientePortalClaims(jwtVerification.payload)) {
      tokenData = jwtVerification.payload || {};
      negocioId = String(tokenData.negocioId || tokenData.sub || '').trim();
      timestamp = Number(tokenData.iat || 0) * 1000;
    } else {
      tokenData = decodeLegacyToken(token);
      if (!tokenData) {
        return res.status(401).json({
          success: false,
          error: 'Token inválido'
        });
      }

      negocioId = String(tokenData.negocioId || '').trim();
      timestamp = Number(tokenData.timestamp || 0);
    }

    // Verificar que el token no tenga más de 30 días
    const thirtyDaysAgo = Date.now() - (30 * 24 * 60 * 60 * 1000);
    if (timestamp < thirtyDaysAgo) {
      return res.status(401).json({
        success: false,
        error: 'Sesión expirada'
      });
    }

    // Obtener datos actualizados del negocio
    const negocioDoc = await db.collection('Negocios').doc(negocioId).get();

    if (!negocioDoc.exists) {
      return res.status(404).json({
        success: false,
        error: 'Negocio no encontrado'
      });
    }

    const negocioData = negocioDoc.data();

    // Verificar acceso actual (misma lógica que login)
    let hasAccess = false;
    let subscriptionInfo = {};

    // Trial activo
    if (negocioData.trialActive) {
      const now = Date.now();
      const trialEnd = negocioData.trialEndDate?.toMillis() || 0;
      if (now < trialEnd) {
        hasAccess = true;
        subscriptionInfo.trialEndsAt = new Date(trialEnd).toISOString();
      }
    }

    // Suscripción Stripe activa
    if (!hasAccess && negocioData.subscriptionStatus === 'active') {
      hasAccess = true;
      subscriptionInfo.subscriptionActive = true;
      subscriptionInfo.nextPayment = negocioData.subscriptionCurrentPeriodEnd?.toDate();
    }

    // Plan manual / pago único activo
    if (!hasAccess) {
      const plan = negocioData.plan;
      const planesActivos = ['basic', 'basico', 'pro', 'premium', 'ventas'];
      if (plan && planesActivos.includes(String(plan).toLowerCase())) {
        const renewalDate =
          negocioData.planRenewalDate?.toMillis() ||
          negocioData.planExpiresAt?.toMillis() ||
          negocioData.expiresAt?.toMillis() ||
          0;
        if (renewalDate > Date.now()) {
          hasAccess = true;
          subscriptionInfo.manualPlan = true;
          subscriptionInfo.expiresAt = new Date(renewalDate).toISOString();
        }
      }
    }

    // Verificar si está archivado
    if (negocioData.websiteArchived) {
      hasAccess = false;
    }

    if (!hasAccess) {
      return res.status(403).json({
        success: false,
        error: 'Tu plan ha expirado',
        needsRenewal: true
      });
    }

    // Multinegocio: adjunta la lista de negocios de la cuenta (por teléfono del
    // token) para que el selector del panel sobreviva a recargas de página.
    let negocios = [];
    let account = null;
    const accountPhone = normalizarTelefono(tokenData.accountPhone || tokenData.phone || '');
    if (accountPhone) {
      try {
        const cuenta = await loadOrProvisionCuenta(accountPhone);
        if (cuenta) {
          negocios = cuenta._negocios.map((d) => buildNegocioSummary(d.id, d.data()));
          account = {
            phone: accountPhone,
            primaryNegocioId: cuenta.primaryNegocioId,
            negocioIds: cuenta.negocioIds,
          };
        }
      } catch (accErr) {
        console.warn('[verificarSesion] no se pudo cargar la cuenta:', accErr?.message || accErr);
      }
    }

    // ✅ Sesión válida
    return res.json({
      success: true,
      data: {
        negocioId,
        negocios,
        account,
        companyInfo: negocioData.companyInfo || 'Mi Negocio',
        slug: negocioData.slug || '',
        plan: negocioData.plan,
        // Add-ons activables desde SuperAdmin (ej. { marketing: true }).
        // El panel del cliente lo usa para mostrar/ocultar módulos como Marketing.
        addons: negocioData.addons || {},
        templateId: negocioData.templateId || 'info',
        logoURL: negocioData.logoURL || '',
        contactEmail: negocioData.contactEmail || '',
        contactWhatsapp: negocioData.contactWhatsapp || '',
        dominio:
          negocioData.dominio ||
          negocioData.domain ||
          negocioData.customDomain ||
          negocioData.custom_domain ||
          '',
        advancedApp: negocioData.advancedApp || null,
        advancedAppActive:
          String(negocioData?.advancedApp?.key || '').toLowerCase() === 'hotel_premium' &&
          String(negocioData?.advancedApp?.status || '').toLowerCase() === 'active',
        subscriptionStatus: negocioData.subscriptionStatus,
        hasStripeSubscription: !!negocioData.subscriptionId,
        expiresAt:
          negocioData.planRenewalDate?.toDate?.() ||
          negocioData.planExpiresAt?.toDate?.() ||
          negocioData.expiresAt?.toDate?.() ||
          null,
        planRenewalDate: negocioData.planRenewalDate?.toDate?.() || null,
        planExpiresAt: negocioData.planExpiresAt?.toDate?.() || null,
        ...subscriptionInfo
      }
    });

  } catch (error) {
    console.error('❌ Error verificando sesión:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor'
    });
  }
}

/**
 * POST /api/cliente/logout
 * 
 * Cierra sesión del cliente
 */
export async function logoutCliente(req, res) {
  try {
    // En este sistema simple, el logout es manejado por el frontend
    // eliminando el token del localStorage
    
    return res.json({
      success: true,
      message: 'Sesión cerrada exitosamente'
    });
  } catch (error) {
    console.error('❌ Error en logout:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor'
    });
  }
}

/**
 * GET /api/cliente/account/negocios
 * Lista los negocios de la cuenta autenticada (para refrescar el selector).
 */
export async function listCuentaNegocios(req, res) {
  try {
    const auth = requireClienteBearer(req, res);
    if (!auth) return undefined;

    const cuenta = await loadOrProvisionCuenta(auth.phone);
    if (!cuenta) {
      return res.json({ success: true, data: { negocios: [], account: null } });
    }

    const negocios = cuenta._negocios.map((d) => buildNegocioSummary(d.id, d.data()));
    return res.json({
      success: true,
      data: {
        negocios,
        account: {
          phone: auth.phone,
          primaryNegocioId: cuenta.primaryNegocioId,
          negocioIds: cuenta.negocioIds,
        },
      },
    });
  } catch (error) {
    console.error('❌ Error listando negocios de la cuenta:', error);
    return res.status(500).json({ success: false, error: 'Error interno del servidor' });
  }
}

/**
 * POST /api/cliente/account/switch  { negocioId }
 * Cambia el negocio activo: verifica que pertenezca a la cuenta y con acceso,
 * y emite un token nuevo apuntando a ese negocio.
 */
export async function switchNegocio(req, res) {
  try {
    const auth = requireClienteBearer(req, res);
    if (!auth) return undefined;

    const targetId = String(req.body?.negocioId || '').trim();
    if (!targetId) {
      return res.status(400).json({ success: false, error: 'Falta negocioId' });
    }

    const cuenta = await loadOrProvisionCuenta(auth.phone);
    if (!cuenta || !cuenta.negocioIds.includes(targetId)) {
      return res.status(403).json({ success: false, error: 'Ese negocio no pertenece a tu cuenta' });
    }

    let negocioDoc = cuenta._negocios.find((d) => d.id === targetId);
    if (!negocioDoc) {
      negocioDoc = await db.collection('Negocios').doc(targetId).get();
      if (!negocioDoc.exists) {
        return res.status(404).json({ success: false, error: 'Negocio no encontrado' });
      }
    }

    const negocioData = negocioDoc.data();

    // Defensa extra: el negocio debe compartir el teléfono de la cuenta.
    const negocioPhone = normalizarTelefono(negocioData.leadPhone || negocioData.accountPhone || '');
    if (negocioPhone && negocioPhone !== auth.phone) {
      return res.status(403).json({ success: false, error: 'Ese negocio no pertenece a tu cuenta' });
    }

    const access = evaluateLegacyAccess(negocioData);
    if (!access.allowed) {
      // Negocio sin plan activo (p. ej. recién agregado): el panel debe abrir el
      // flujo de pago en lugar de cambiar de negocio.
      return res.status(402).json({
        success: false,
        needsPayment: true,
        negocioId: targetId,
        error: 'Este negocio no tiene un plan activo. Actívalo para acceder.',
      });
    }

    const session = signJwt(
      buildClienteSessionPayload({
        negocioId: targetId,
        phone: auth.phone,
        accountPhone: auth.phone,
        accessType: access.reason,
      }),
      { expiresInSeconds: getClientTokenTtlSeconds() }
    );
    const legacyToken = Buffer.from(
      JSON.stringify({ negocioId: targetId, phone: auth.phone, timestamp: Date.now(), accessType: access.reason })
    ).toString('base64');

    await negocioDoc.ref.update({ lastLoginAt: new Date() }).catch(() => {});

    const negocios = cuenta._negocios.map((d) => buildNegocioSummary(d.id, d.data()));
    return res.json({
      success: true,
      data: {
        ...buildActiveNegocioData(targetId, negocioData, access),
        ...(access.reason === 'trial' && access.expiresAt ? { trialEndsAt: access.expiresAt } : {}),
        token: session.token,
        sessionToken: session.token,
        legacyToken,
        legacySessionToken: legacyToken,
        tokenFormat: 'jwt',
        negocios,
        account: {
          phone: auth.phone,
          primaryNegocioId: cuenta.primaryNegocioId,
          negocioIds: cuenta.negocioIds,
        },
      },
    });
  } catch (error) {
    console.error('❌ Error cambiando de negocio:', error);
    return res.status(500).json({ success: false, error: 'Error interno del servidor' });
  }
}

/**
 * POST /api/cliente/account/create-web-link
 * Genera un enlace tokenizado para que el cliente cree una web nueva ligada a
 * su mismo número. El brief lo consume y da de alta el negocio en la cuenta.
 */
export async function createNegocioLink(req, res) {
  try {
    const auth = requireClienteBearer(req, res);
    if (!auth) return undefined;

    const token = createAddNegocioToken(auth.phone);
    const base = String(
      process.env.CLIENT_WEB_BRIEF_URL ||
      process.env.WEB_FORM_URL ||
      'https://negociosweb.mx/webgratis-v2'
    ).trim().replace(/\/+$/, '');
    const sep = base.includes('?') ? '&' : '?';
    const url = `${base}${sep}add=${encodeURIComponent(token)}&wa=${encodeURIComponent(auth.phone)}`;

    return res.json({
      success: true,
      data: { url, token, expiresInSeconds: ADD_NEGOCIO_TOKEN_TTL_SECONDS },
    });
  } catch (error) {
    console.error('❌ Error generando enlace de alta:', error);
    return res.status(500).json({ success: false, error: 'Error interno del servidor' });
  }
}
