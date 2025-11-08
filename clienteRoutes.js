// clienteAuthRoutes.js - Sistema de autenticación para clientes

import { db } from './firebaseAdmin.js';
import { normalizarTelefono } from './pinUtils.js';

/**
 * POST /api/cliente/login
 * 
 * Autentica a un cliente usando teléfono + PIN
 * 
 * Body:
 * {
 *   phone: string,      // Teléfono del cliente
 *   pin: string         // PIN de 4 dígitos
 * }
 * 
 * Response:
 * {
 *   success: true,
 *   data: {
 *     negocioId: string,
 *     companyInfo: string,
 *     slug: string,
 *     plan: string,
 *     templateId: string,
 *     token: string      // Token de sesión (JWT simplificado)
 *   }
 * }
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

    // Verificar que tenga un plan activo (no free)
    const plan = negocioData.plan;
    const planesActivos = ['basic', 'pro', 'premium'];
    
    if (!plan || !planesActivos.includes(String(plan).toLowerCase())) {
      console.log(`⚠️ Negocio ${negocioId} no tiene plan activo`);
      return res.status(403).json({
        success: false,
        error: 'Tu plan ha expirado. Contacta al administrador para renovar.'
      });
    }

    // ✅ Login exitoso
    console.log(`✅ Login exitoso - Negocio: ${negocioId} (${negocioData.companyInfo})`);

    // Generar token de sesión (simplificado - base64 de negocioId + timestamp)
    const tokenData = {
      negocioId,
      phone: phoneDigits,
      timestamp: Date.now()
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
        planRenewalDate: negocioData.planRenewalDate?.toDate?.()?.toISOString() || null,
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
 * 
 * Body:
 * {
 *   token: string
 * }
 * 
 * Response:
 * {
 *   success: true,
 *   data: {
 *     negocioId: string,
 *     companyInfo: string,
 *     ...
 *   }
 * }
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

    // Verificar que siga teniendo plan activo
    const plan = negocioData.plan;
    const planesActivos = ['basic', 'pro', 'premium'];
    
    if (!plan || !planesActivos.includes(String(plan).toLowerCase())) {
      return res.status(403).json({
        success: false,
        error: 'Tu plan ha expirado'
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
        planRenewalDate: negocioData.planRenewalDate?.toDate?.()?.toISOString() || null
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
 * Cierra sesión del cliente (opcional, el frontend puede solo borrar el token)
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