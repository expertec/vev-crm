// clienteAuthRoutes.js - Sistema de autenticación para clientes con soporte de suscripciones

import crypto from 'crypto';
import { db } from './firebaseAdmin.js';
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

function buildClienteSessionPayload({ negocioId, phone, accessType }) {
  return {
    iss: 'vevcrm',
    sub: String(negocioId || '').trim(),
    aud: CLIENT_REALM,
    role: CLIENT_ROLE,
    realm: CLIENT_REALM,
    negocioId: String(negocioId || '').trim(),
    phone: String(phone || '').trim(),
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

    // Buscar negocio por teléfono
    const negociosSnap = await db.collection('Negocios')
      .where('leadPhone', '==', phoneDigits)
      .limit(1)
      .get();

    if (negociosSnap.empty) {
      console.log(`❌ No se encontró negocio con teléfono: ${phoneDigits}`);
      return res.status(401).json({
        success: false,
        error: 'Teléfono o PIN incorrectos'
      });
    }

    const negocioDoc = negociosSnap.docs[0];
    const negocioData = negocioDoc.data();
    const negocioId = negocioDoc.id;

    // Verificar que tenga un PIN asignado
    if (!negocioData.pin) {
      console.log(`⚠️ Negocio ${negocioId} no tiene PIN asignado`);
      return res.status(401).json({
        success: false,
        error: 'No tienes acceso al panel. Contacta al administrador.'
      });
    }

    // Verificar PIN
    if (String(negocioData.pin).trim() !== pinStr) {
      console.log(`❌ PIN incorrecto para negocio ${negocioId}`);
      return res.status(401).json({
        success: false,
        error: 'Teléfono o PIN incorrectos'
      });
    }

    // NUEVA LÓGICA: Verificar acceso según tipo de plan
    let hasAccess = false;
    let accessReason = '';
    let subscriptionInfo = {};

    // 1. Verificar trial activo
    if (negocioData.trialActive) {
      const now = Date.now();
      const trialEnd = negocioData.trialEndDate?.toMillis() || 0;
      
      if (now < trialEnd) {
        hasAccess = true;
        accessReason = 'trial';
        subscriptionInfo.trialEndsAt = new Date(trialEnd).toISOString();
        console.log(`✅ Acceso por trial activo hasta ${subscriptionInfo.trialEndsAt}`);
      } else {
        // Trial expirado, actualizar en BD
        await negocioDoc.ref.update({
          trialActive: false,
          websiteArchived: true,
          archivedReason: 'trial_expired'
        });
      }
    }

    // 2. Verificar suscripción de Stripe
    if (!hasAccess && negocioData.subscriptionStatus === 'active') {
      hasAccess = true;
      accessReason = 'subscription';
      subscriptionInfo.subscriptionType = 'stripe';
      subscriptionInfo.nextPayment = negocioData.subscriptionCurrentPeriodEnd?.toDate();
      console.log(`✅ Acceso por suscripción Stripe activa`);
    }

    // 3. Verificar plan manual (transferencia / pago único)
    if (!hasAccess) {
      const plan = negocioData.plan;
      const planesActivos = ['basic', 'basico', 'pro', 'premium', 'ventas'];
      
      if (plan && planesActivos.includes(String(plan).toLowerCase())) {
        // Verificar fecha de renovación
        const renewalDate =
          negocioData.planRenewalDate?.toMillis() ||
          negocioData.planExpiresAt?.toMillis() ||
          negocioData.expiresAt?.toMillis() ||
          0;
        
        if (renewalDate > Date.now()) {
          hasAccess = true;
          accessReason = 'manual';
          subscriptionInfo.planType = plan;
          subscriptionInfo.expiresAt = new Date(renewalDate).toISOString();
          console.log(`✅ Acceso por plan manual ${plan} hasta ${subscriptionInfo.expiresAt}`);
        }
      }
    }

    // Verificar si el sitio está archivado
    if (negocioData.websiteArchived) {
      hasAccess = false;
      console.log(`⚠️ Sitio archivado para negocio ${negocioId}`);
    }

    // Manejo de casos especiales
    if (!hasAccess) {
      // Caso: Suscripción con pago pendiente
      if (negocioData.subscriptionStatus === 'past_due') {
        return res.status(403).json({
          success: false,
          error: 'Tu suscripción tiene un pago pendiente. Por favor actualiza tu método de pago.',
          needsPayment: true,
          subscriptionId: negocioData.subscriptionId
        });
      }

      // Caso: Trial expirado
      if (negocioData.trialUsed && !negocioData.subscriptionId) {
        return res.status(403).json({
          success: false,
          error: 'Tu período de prueba ha expirado. Suscríbete para continuar.',
          trialExpired: true,
          canSubscribe: true
        });
      }

      // Caso general: Sin acceso
      return res.status(403).json({
        success: false,
        error: 'Tu plan ha expirado. Contacta al administrador para renovar.',
        canSubscribe: true
      });
    }

    // ✅ Login exitoso
    console.log(`✅ Login exitoso - Negocio: ${negocioId} (${negocioData.companyInfo}) - Acceso por: ${accessReason}`);

    // Generar token compatible con el portal cliente y mantener el token legado
    const tokenData = {
      negocioId,
      phone: phoneDigits,
      timestamp: Date.now(),
      accessType: accessReason
    };
    const legacyToken = Buffer.from(JSON.stringify(tokenData)).toString('base64');
    const session = signJwt(
      buildClienteSessionPayload({
        negocioId,
        phone: phoneDigits,
        accessType: accessReason,
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

    // Responder con datos del negocio
    return res.json({
      success: true,
      message: 'Login exitoso',
      data: {
        negocioId,
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
        // Información de suscripción
        subscriptionType: accessReason,
        subscriptionStatus: negocioData.subscriptionStatus,
        hasStripeSubscription: !!negocioData.subscriptionId,
        expiresAt:
          negocioData.planRenewalDate?.toDate?.() ||
          negocioData.planExpiresAt?.toDate?.() ||
          negocioData.expiresAt?.toDate?.() ||
          null,
        planRenewalDate: negocioData.planRenewalDate?.toDate?.() || null,
        planExpiresAt: negocioData.planExpiresAt?.toDate?.() || null,
        ...subscriptionInfo,
        token: session.token,
        sessionToken: session.token,
        legacyToken,
        legacySessionToken: legacyToken,
        tokenFormat: 'jwt'
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

    // ✅ Sesión válida
    return res.json({
      success: true,
      data: {
        negocioId,
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
