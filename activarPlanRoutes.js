// activarPlanRoutes.js - Endpoint para activar planes y generar PINs

import { db } from './firebaseAdmin.js';
import { Timestamp } from 'firebase-admin/firestore';
import { generarPIN, generarMensajeCredenciales, normalizarTelefono } from './pinUtils.js';
import { enviarMensaje } from './scheduler.js';
import dayjs from 'dayjs';

/**
 * POST /api/activar-plan
 * 
 * Activa un plan de pago para un negocio:
 * - Genera un PIN de 4 dígitos
 * - Actualiza fechas del plan en Firestore
 * - Envía credenciales por WhatsApp
 * 
 * Body:
 * {
 *   negocioId: string,
 *   plan: 'basic' | 'pro' | 'premium',
 *   email: string (opcional)
 * }
 */
export async function activarPlan(req, res) {
  try {
    const { negocioId, plan, email } = req.body;

    // Validaciones
    if (!negocioId) {
      return res.status(400).json({ 
        success: false, 
        error: 'El campo negocioId es requerido' 
      });
    }

    if (!plan || !['basic', 'pro', 'premium'].includes(plan)) {
      return res.status(400).json({ 
        success: false, 
        error: 'Plan inválido. Debe ser: basic, pro o premium' 
      });
    }

    // Obtener datos del negocio
    const negocioRef = db.collection('Negocios').doc(negocioId);
    const negocioDoc = await negocioRef.get();

    if (!negocioDoc.exists) {
      return res.status(404).json({ 
        success: false, 
        error: 'Negocio no encontrado' 
      });
    }

    const negocioData = negocioDoc.data();

    // Generar PIN único
    const pin = generarPIN();

    // Calcular fechas del plan
    let planDurationDays = 30;
    if (plan === 'premium') planDurationDays = 365;
    if (plan === 'basic') planDurationDays = 365;

    const startDate = new Date();
    const renewalDate = dayjs(startDate).add(planDurationDays, 'day').toDate();

    // Preparar datos a actualizar
    const updateData = {
      plan,
      pin,
      pinCreatedAt: Timestamp.now(),
      planStartDate: Timestamp.fromDate(startDate),
      planRenewalDate: Timestamp.fromDate(renewalDate),
      planDurationDays,
      planActivatedAt: Timestamp.now(),
      updatedAt: Timestamp.now()
    };

    // Si se proporciona email, actualizarlo
    if (email && email.trim()) {
      updateData.contactEmail = email.trim();
    }

    // Actualizar en Firestore
    await negocioRef.update(updateData);

    console.log(`✅ Plan ${plan} activado para negocio ${negocioId} con PIN: ${pin}`);

    // Preparar datos para el mensaje de WhatsApp
    const phoneRaw = negocioData.leadPhone || negocioData.contactWhatsapp || '';
    const companyName = negocioData.companyInfo || 'Tu negocio';
    
    // URL del panel de cliente (ajusta según tu dominio)
    const loginUrl = process.env.CLIENT_PANEL_URL || 'https://negociosweb.mx/cliente-login';

    // Generar mensaje con credenciales
    const mensaje = generarMensajeCredenciales({
      companyName,
      pin,
      phone: phoneRaw,
      plan,
      loginUrl
    });

    // Enviar por WhatsApp
    if (phoneRaw) {
      try {
        await enviarMensaje(
          { 
            telefono: normalizarTelefono(phoneRaw),
            nombre: companyName 
          },
          { 
            type: 'texto', 
            contenido: mensaje 
          }
        );
        console.log(`📤 Credenciales enviadas por WhatsApp a: ${phoneRaw}`);
      } catch (waError) {
        console.error('⚠️ Error enviando WhatsApp:', waError);
        // No fallar la petición si solo falla el WhatsApp
      }
    } else {
      console.warn('⚠️ No se encontró teléfono para enviar WhatsApp');
    }

    // Respuesta exitosa
    return res.json({
      success: true,
      message: 'Plan activado correctamente',
      data: {
        negocioId,
        pin,
        plan,
        planStartDate: startDate.toISOString(),
        planRenewalDate: renewalDate.toISOString(),
        loginUrl,
        whatsappSent: !!phoneRaw
      }
    });

  } catch (error) {
    console.error('❌ Error activando plan:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
}

/**
 * POST /api/reenviar-pin
 * 
 * Reenvía el PIN por WhatsApp en caso de que el cliente lo haya perdido
 * 
 * Body:
 * {
 *   negocioId: string
 * }
 */
export async function reenviarPIN(req, res) {
  try {
    const { negocioId } = req.body;

    if (!negocioId) {
      return res.status(400).json({ 
        success: false, 
        error: 'El campo negocioId es requerido' 
      });
    }

    // Obtener datos del negocio
    const negocioDoc = await db.collection('Negocios').doc(negocioId).get();

    if (!negocioDoc.exists) {
      return res.status(404).json({ 
        success: false, 
        error: 'Negocio no encontrado' 
      });
    }

    const negocioData = negocioDoc.data();

    // Verificar que tenga PIN
    if (!negocioData.pin) {
      return res.status(400).json({ 
        success: false, 
        error: 'Este negocio no tiene un PIN asignado. Debe activar un plan primero.' 
      });
    }

    const phoneRaw = negocioData.leadPhone || negocioData.contactWhatsapp || '';
    
    if (!phoneRaw) {
      return res.status(400).json({ 
        success: false, 
        error: 'No se encontró número de teléfono para enviar el PIN' 
      });
    }

    const companyName = negocioData.companyInfo || 'Tu negocio';
    const loginUrl = process.env.CLIENT_PANEL_URL || 'https://negociosweb.mx/cliente-login';

    // Mensaje simple de reenvío de PIN
    const mensaje = `🔐 *Recuperación de PIN - ${companyName}*

Tu PIN de acceso es: *${negocioData.pin}*

📱 Teléfono: ${normalizarTelefono(phoneRaw)}

🌐 Accede a tu panel aquí:
${loginUrl}

💡 Guarda tu PIN en un lugar seguro.`;

    // Enviar por WhatsApp
    await enviarMensaje(
      { 
        telefono: normalizarTelefono(phoneRaw),
        nombre: companyName 
      },
      { 
        type: 'texto', 
        contenido: mensaje 
      }
    );

    console.log(`📤 PIN reenviado por WhatsApp a: ${phoneRaw}`);

    return res.json({
      success: true,
      message: 'PIN reenviado exitosamente por WhatsApp'
    });

  } catch (error) {
    console.error('❌ Error reenviando PIN:', error);
    return res.status(500).json({
      success: false,
      error: 'Error interno del servidor',
      details: error.message
    });
  }
}