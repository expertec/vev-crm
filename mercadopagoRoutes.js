// mercadopagoRoutes.js - Rutas de Mercado Pago Checkout Pro
import express from 'express';
import { Timestamp } from 'firebase-admin/firestore';
import { preferenceClient, paymentClient, PLANES, MP_CONFIG } from './mercadopagoConfig.js';
import { db } from './firebaseAdmin.js';

const router = express.Router();

/**
 * GET /api/mp/planes
 * Retorna la lista de planes disponibles
 */
router.get('/planes', (req, res) => {
  try {
    const planesArray = Object.values(PLANES).map(plan => ({
      id: plan.id,
      nombre: plan.nombre,
      precio: plan.precio,
      currency: plan.currency,
      descripcion: plan.descripcion,
      features: plan.features
    }));

    return res.json({
      success: true,
      planes: planesArray
    });
  } catch (error) {
    console.error('[MP] Error obteniendo planes:', error);
    return res.status(500).json({
      success: false,
      error: 'Error al obtener planes'
    });
  }
});

/**
 * POST /api/mp/create-preference
 * Crea una preferencia de pago en Mercado Pago
 * Body: { planId, phone, email?, negocioId? }
 */
router.post('/create-preference', async (req, res) => {
  try {
    const { planId, phone, email, negocioId } = req.body;

    // Validaciones
    if (!planId || !phone) {
      return res.status(400).json({
        success: false,
        error: 'Faltan campos requeridos: planId y phone son obligatorios'
      });
    }

    const plan = PLANES[planId];
    if (!plan) {
      return res.status(400).json({
        success: false,
        error: `Plan no válido: ${planId}. Planes disponibles: basico, pro`
      });
    }

    // Buscar negocioId si no se proporcionó
    let finalNegocioId = negocioId;
    if (!finalNegocioId) {
      const phoneDigits = String(phone).replace(/\D/g, '');
      const negocioSnap = await db.collection('Negocios')
        .where('leadPhone', '==', phoneDigits)
        .limit(1)
        .get();

      if (!negocioSnap.empty) {
        finalNegocioId = negocioSnap.docs[0].id;
      }
    }

    if (!finalNegocioId) {
      return res.status(404).json({
        success: false,
        error: 'No se encontró un negocio asociado a este teléfono'
      });
    }

    // Crear external_reference con formato: negocioId:phone:planId
    const externalReference = `${finalNegocioId}:${phone}:${planId}`;

    // Configurar preferencia
    const preferenceData = {
      items: [
        {
          id: plan.id,
          title: plan.nombre,
          description: plan.descripcion,
          quantity: 1,
          currency_id: 'MXN',
          unit_price: plan.precio
        }
      ],
      payer: {
        phone: {
          number: phone
        },
        ...(email && { email })
      },
      back_urls: {
        success: `${MP_CONFIG.frontendUrl}/pago?status=success`,
        failure: `${MP_CONFIG.frontendUrl}/pago?status=failure`,
        pending: `${MP_CONFIG.frontendUrl}/pago?status=pending`
      },
      auto_return: 'approved',
      external_reference: externalReference,
      statement_descriptor: 'NegociosWeb',
      ...(MP_CONFIG.webhookUrl && { notification_url: MP_CONFIG.webhookUrl })
    };

    // Crear preferencia en Mercado Pago
    const preference = await preferenceClient.create({ body: preferenceData });

    console.log(`[MP] Preferencia creada: ${preference.id} para negocio ${finalNegocioId}`);

    // Guardar registro del intento de pago
    await db.collection('pagos_mp').add({
      preferenceId: preference.id,
      negocioId: finalNegocioId,
      phone,
      email: email || null,
      planId,
      planNombre: plan.nombre,
      monto: plan.precio,
      currency: 'MXN',
      status: 'pending',
      externalReference,
      createdAt: Timestamp.now()
    });

    return res.json({
      success: true,
      preferenceId: preference.id,
      initPoint: preference.init_point,
      sandboxInitPoint: preference.sandbox_init_point
    });

  } catch (error) {
    console.error('[MP] Error creando preferencia:', error);
    return res.status(500).json({
      success: false,
      error: error.message || 'Error al crear preferencia de pago'
    });
  }
});

/**
 * POST /api/mp/webhook
 * Recibe notificaciones de Mercado Pago (IPN)
 */
router.post('/webhook', async (req, res) => {
  try {
    const { type, data } = req.body;

    console.log(`[MP Webhook] Recibido: type=${type}, data=`, data);

    // Siempre responder 200 para evitar reintentos
    if (type !== 'payment') {
      return res.status(200).json({ received: true });
    }

    const paymentId = data?.id;
    if (!paymentId) {
      console.warn('[MP Webhook] No se recibió payment ID');
      return res.status(200).json({ received: true });
    }

    // Obtener información del pago
    const payment = await paymentClient.get({ id: paymentId });

    console.log(`[MP Webhook] Payment ${paymentId}: status=${payment.status}`);

    // Solo procesar pagos aprobados
    if (payment.status !== 'approved') {
      // Actualizar registro si existe
      const pagoSnap = await db.collection('pagos_mp')
        .where('externalReference', '==', payment.external_reference)
        .limit(1)
        .get();

      if (!pagoSnap.empty) {
        await pagoSnap.docs[0].ref.update({
          paymentId: String(paymentId),
          status: payment.status,
          statusDetail: payment.status_detail || null,
          updatedAt: Timestamp.now()
        });
      }

      return res.status(200).json({ received: true, processed: false });
    }

    // Parsear external_reference: negocioId:phone:planId
    const externalRef = payment.external_reference || '';
    const [negocioId, phone, planId] = externalRef.split(':');

    if (!negocioId || !planId) {
      console.error('[MP Webhook] external_reference inválido:', externalRef);
      return res.status(200).json({ received: true, error: 'invalid_reference' });
    }

    // Calcular fecha de expiración (30 días desde ahora)
    const expiresAt = new Date();
    expiresAt.setDate(expiresAt.getDate() + 30);

    // Actualizar el plan del negocio
    const negocioRef = db.collection('Negocios').doc(negocioId);
    const negocioSnap = await negocioRef.get();

    if (!negocioSnap.exists) {
      console.error('[MP Webhook] Negocio no encontrado:', negocioId);
      return res.status(200).json({ received: true, error: 'negocio_not_found' });
    }

    const plan = PLANES[planId];

    await negocioRef.update({
      plan: planId,
      planNombre: plan?.nombre || planId,
      planActivatedAt: Timestamp.now(),
      planExpiresAt: Timestamp.fromDate(expiresAt),
      planRenewalDate: Timestamp.fromDate(expiresAt),
      trialActive: false,
      websiteArchived: false,
      lastPaymentId: String(paymentId),
      lastPaymentAmount: payment.transaction_amount,
      lastPaymentDate: Timestamp.now(),
      updatedAt: Timestamp.now()
    });

    console.log(`[MP Webhook] Plan actualizado: negocio=${negocioId}, plan=${planId}, expira=${expiresAt.toISOString()}`);

    // Actualizar registro de pago
    const pagoSnap = await db.collection('pagos_mp')
      .where('externalReference', '==', externalRef)
      .limit(1)
      .get();

    if (!pagoSnap.empty) {
      await pagoSnap.docs[0].ref.update({
        paymentId: String(paymentId),
        status: 'approved',
        statusDetail: payment.status_detail || null,
        transactionAmount: payment.transaction_amount,
        processedAt: Timestamp.now(),
        updatedAt: Timestamp.now()
      });
    } else {
      // Crear registro si no existe
      await db.collection('pagos_mp').add({
        paymentId: String(paymentId),
        negocioId,
        phone,
        planId,
        planNombre: plan?.nombre || planId,
        monto: payment.transaction_amount,
        currency: 'MXN',
        status: 'approved',
        statusDetail: payment.status_detail || null,
        externalReference: externalRef,
        processedAt: Timestamp.now(),
        createdAt: Timestamp.now()
      });
    }

    return res.status(200).json({
      received: true,
      processed: true,
      negocioId,
      planId
    });

  } catch (error) {
    console.error('[MP Webhook] Error:', error);
    // Siempre responder 200 para evitar reintentos infinitos
    return res.status(200).json({
      received: true,
      error: error.message
    });
  }
});

/**
 * GET /api/mp/payment/:id
 * Consulta el estado de un pago específico
 */
router.get('/payment/:id', async (req, res) => {
  try {
    const { id } = req.params;

    if (!id) {
      return res.status(400).json({
        success: false,
        error: 'Se requiere el ID del pago'
      });
    }

    const payment = await paymentClient.get({ id });

    return res.json({
      success: true,
      payment: {
        id: payment.id,
        status: payment.status,
        statusDetail: payment.status_detail,
        externalReference: payment.external_reference,
        transactionAmount: payment.transaction_amount,
        currencyId: payment.currency_id,
        paymentMethodId: payment.payment_method_id,
        paymentTypeId: payment.payment_type_id,
        dateCreated: payment.date_created,
        dateApproved: payment.date_approved
      }
    });

  } catch (error) {
    console.error('[MP] Error consultando pago:', error);
    return res.status(500).json({
      success: false,
      error: error.message || 'Error al consultar el pago'
    });
  }
});

export default router;
