// clienteAuthRoutes.js - Sistema de autenticación para clientes con soporte de suscripciones

import { db } from './firebaseAdmin.js';
import { normalizarTelefono } from './pinUtils.js';

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
      const planesActivos = ['basic', 'basico', 'pro', 'premium'];
      
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

    // Generar token de sesión
    const tokenData = {
      negocioId,
      phone: phoneDigits,
      timestamp: Date.now(),
      accessType: accessReason
    };
    const token = Buffer.from(JSON.stringify(tokenData)).toString('base64');

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
        token
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

    // Decodificar token
    let tokenData;
    try {
      const decoded = Buffer.from(token, 'base64').toString('utf-8');
      tokenData = JSON.parse(decoded);
    } catch {
      return res.status(401).json({
        success: false,
        error: 'Token inválido'
      });
    }

    const { negocioId, timestamp } = tokenData;

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
      const planesActivos = ['basic', 'basico', 'pro', 'premium'];
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
