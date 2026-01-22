// stripeOneTimeRoutes.js - Pagos únicos con Stripe Checkout
import express from 'express';
import { db } from './firebaseAdmin.js';
import { stripe, STRIPE_CONFIG } from './stripeConfig.js';
import { Timestamp } from 'firebase-admin/firestore';
import { normalizarTelefono, generarPIN } from './pinUtils.js';
import { enviarMensaje } from './scheduler.js';

const router = express.Router();

// Planes disponibles para pago único
const PLANES = {
  basico: {
    id: 'basico',
    nombre: 'Plan Básico',
    precio: 397,
    precioCentavos: 39700,
    currency: 'mxn',
    descripcion: 'Página web profesional con todas las funciones básicas',
    duracionDias: 30,
    features: [
      'Página web profesional',
      'Dominio personalizado',
      'Certificado SSL',
      'Soporte por WhatsApp',
      'Actualizaciones mensuales'
    ]
  },
  pro: {
    id: 'pro',
    nombre: 'Plan Pro',
    precio: 997,
    precioCentavos: 99700,
    currency: 'mxn',
    descripcion: 'Página web premium con funciones avanzadas y prioridad',
    duracionDias: 30,
    features: [
      'Todo lo del Plan Básico',
      'Diseño premium personalizado',
      'SEO avanzado',
      'Integraciones especiales',
      'Soporte prioritario 24/7',
      'Analíticas avanzadas'
    ]
  }
};

/**
 * GET /api/stripe-onetime/planes
 * Retorna la lista de planes disponibles
 */
router.get('/planes', (req, res) => {
  const planesArray = Object.values(PLANES).map(plan => ({
    id: plan.id,
    nombre: plan.nombre,
    precio: plan.precio,
    currency: plan.currency,
    descripcion: plan.descripcion,
    duracionDias: plan.duracionDias,
    features: plan.features
  }));

  return res.json({
    success: true,
    planes: planesArray
  });
});

/**
 * POST /api/stripe-onetime/create-checkout
 * Crea una sesión de checkout para pago único
 * Body: { planId, phone, email?, negocioId? }
 */
router.post('/create-checkout', async (req, res) => {
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

    // Normalizar teléfono
    const phoneDigits = normalizarTelefono(phone);
    if (!phoneDigits || phoneDigits.length < 10) {
      return res.status(400).json({
        success: false,
        error: 'Teléfono inválido'
      });
    }

    // Buscar negocio
    let negocioRef;
    let negocioData;

    if (negocioId) {
      negocioRef = db.collection('Negocios').doc(negocioId);
      const doc = await negocioRef.get();
      if (!doc.exists) {
        return res.status(404).json({
          success: false,
          error: 'Negocio no encontrado'
        });
      }
      negocioData = doc.data();
    } else {
      // Buscar por teléfono
      const negociosSnap = await db
        .collection('Negocios')
        .where('leadPhone', '==', phoneDigits)
        .limit(1)
        .get();

      if (!negociosSnap.empty) {
        negocioRef = negociosSnap.docs[0].ref;
        negocioData = negociosSnap.docs[0].data();
      } else {
        return res.status(404).json({
          success: false,
          error: 'No se encontró un negocio asociado a este teléfono'
        });
      }
    }

    // Crear o recuperar customer de Stripe
    let stripeCustomerId = negocioData.stripeCustomerId;

    if (!stripeCustomerId) {
      const customer = await stripe.customers.create({
        phone: phoneDigits,
        email: email || negocioData.contactEmail || undefined,
        metadata: {
          negocioId: negocioRef.id,
          phone: phoneDigits,
        },
      });

      stripeCustomerId = customer.id;

      await negocioRef.update({
        stripeCustomerId,
        updatedAt: Timestamp.now(),
      });
    }

    // URLs de retorno
    const baseUrl = process.env.CLIENT_URL || 'https://negociosweb.mx';

    // Crear sesión de checkout para PAGO ÚNICO
    const session = await stripe.checkout.sessions.create({
      customer: stripeCustomerId,
      payment_method_types: ['card'],
      line_items: [
        {
          price_data: {
            currency: plan.currency,
            product_data: {
              name: plan.nombre,
              description: plan.descripcion,
            },
            unit_amount: plan.precioCentavos,
          },
          quantity: 1,
        },
      ],
      mode: 'payment', // Pago único, NO suscripción
      success_url: `${baseUrl}/pago?status=success&session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${baseUrl}/pago?status=canceled`,
      metadata: {
        negocioId: negocioRef.id,
        phone: phoneDigits,
        planId: plan.id,
        planNombre: plan.nombre,
        duracionDias: String(plan.duracionDias),
        paymentType: 'one_time', // Identificador para el webhook
      },
      locale: 'es-419',
    });

    console.log(`✅ Sesión de pago único creada: ${session.id} para negocio ${negocioRef.id}`);

    // Guardar registro del intento de pago
    await db.collection('pagos_stripe').add({
      sessionId: session.id,
      negocioId: negocioRef.id,
      phone: phoneDigits,
      email: email || null,
      planId: plan.id,
      planNombre: plan.nombre,
      monto: plan.precio,
      montoCentavos: plan.precioCentavos,
      currency: plan.currency,
      status: 'pending',
      paymentType: 'one_time',
      createdAt: Timestamp.now()
    });

    return res.json({
      success: true,
      checkoutUrl: session.url,
      sessionId: session.id,
      negocioId: negocioRef.id
    });

  } catch (error) {
    console.error('❌ Error creando sesión de checkout:', error);
    return res.status(500).json({
      success: false,
      error: 'Error al crear sesión de pago',
      details: error.message
    });
  }
});

/**
 * POST /api/stripe-onetime/webhook
 * Webhook para pagos únicos - se debe configurar por separado del de suscripciones
 * O manejar en el webhook existente verificando metadata.paymentType
 */
router.post('/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;

  try {
    event = stripe.webhooks.constructEvent(
      req.body,
      sig,
      STRIPE_CONFIG.webhookSecret
    );
  } catch (err) {
    console.error('❌ Webhook signature verification failed:', err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  console.log(`📨 Webhook pago único recibido: ${event.type}`);

  try {
    if (event.type === 'checkout.session.completed') {
      const session = event.data.object;

      // Solo procesar pagos únicos
      if (session.mode === 'payment' && session.metadata?.paymentType === 'one_time') {
        await handleOneTimePaymentCompleted(session);
      }
    }

    res.json({ received: true });
  } catch (error) {
    console.error('❌ Error procesando webhook:', error);
    res.status(500).json({ error: 'Error procesando webhook' });
  }
});

/**
 * Handler para pago único completado
 */
async function handleOneTimePaymentCompleted(session) {
  const { metadata, payment_intent, amount_total } = session;
  const { negocioId, phone, planId, planNombre, duracionDias } = metadata || {};

  console.log(`✅ Pago único completado para negocio ${negocioId}`);

  if (!negocioId) {
    console.error('❌ No hay negocioId en metadata del checkout');
    return;
  }

  const negocioRef = db.collection('Negocios').doc(negocioId);
  const negocioSnap = await negocioRef.get();

  if (!negocioSnap.exists) {
    console.error(`❌ Negocio ${negocioId} no existe en Firestore`);
    return;
  }

  const negocioData = negocioSnap.data() || {};

  // Calcular fecha de expiración
  const dias = parseInt(duracionDias) || 30;
  const expiresAt = new Date();
  expiresAt.setDate(expiresAt.getDate() + dias);

  // PIN final
  const finalPin = negocioData.pin || generarPIN();

  // Actualizar negocio
  await negocioRef.update({
    plan: planId,
    planNombre: planNombre,
    planActivatedAt: Timestamp.now(),
    planExpiresAt: Timestamp.fromDate(expiresAt),
    planRenewalDate: Timestamp.fromDate(expiresAt),
    trialActive: false,
    websiteArchived: false,
    pin: finalPin,
    lastPaymentId: payment_intent,
    lastPaymentAmount: amount_total / 100,
    lastPaymentDate: Timestamp.now(),
    paymentMethod: 'stripe_onetime',
    updatedAt: Timestamp.now()
  });

  console.log(`✅ Plan actualizado: negocio=${negocioId}, plan=${planId}, expira=${expiresAt.toISOString()}`);

  // Actualizar registro de pago
  const pagoSnap = await db.collection('pagos_stripe')
    .where('sessionId', '==', session.id)
    .limit(1)
    .get();

  if (!pagoSnap.empty) {
    await pagoSnap.docs[0].ref.update({
      paymentIntentId: payment_intent,
      status: 'completed',
      processedAt: Timestamp.now(),
      updatedAt: Timestamp.now()
    });
  }

  // Enviar confirmación por WhatsApp
  const finalPhone = phone || negocioData.leadPhone;
  if (finalPhone) {
    const loginUrl = process.env.CLIENT_PANEL_URL || 'https://negociosweb.mx/cliente-login';

    const mensaje = `🎉 ¡Pago recibido exitosamente!

✅ Plan: ${planNombre}
💰 Monto: $${amount_total / 100} MXN
📅 Válido hasta: ${expiresAt.toLocaleDateString('es-MX')}

🔐 Tu PIN de acceso: ${finalPin}

🌐 Ingresa a tu panel:
${loginUrl}

¡Gracias por tu confianza! 🚀`;

    try {
      await enviarMensaje(
        { telefono: finalPhone, nombre: negocioData.companyInfo || 'Cliente' },
        { type: 'texto', contenido: mensaje }
      );
      console.log(`📤 Confirmación enviada por WhatsApp a ${finalPhone}`);
    } catch (waErr) {
      console.error('❌ Error enviando WhatsApp:', waErr);
    }
  }

  // Registrar en historial
  await db.collection('PaymentHistory').add({
    negocioId,
    event: 'one_time_payment_completed',
    sessionId: session.id,
    paymentIntentId: payment_intent,
    planId,
    amount: amount_total / 100,
    currency: 'mxn',
    expiresAt: Timestamp.fromDate(expiresAt),
    timestamp: Timestamp.now()
  });
}

/**
 * GET /api/stripe-onetime/session/:sessionId
 * Consulta el estado de una sesión de checkout
 */
router.get('/session/:sessionId', async (req, res) => {
  try {
    const { sessionId } = req.params;

    const session = await stripe.checkout.sessions.retrieve(sessionId);

    return res.json({
      success: true,
      session: {
        id: session.id,
        status: session.status,
        paymentStatus: session.payment_status,
        amountTotal: session.amount_total / 100,
        currency: session.currency,
        customerEmail: session.customer_details?.email,
        metadata: session.metadata
      }
    });

  } catch (error) {
    console.error('❌ Error consultando sesión:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

export default router;
