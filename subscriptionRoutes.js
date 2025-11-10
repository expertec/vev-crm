// subscriptionRoutes.js - Sistema completo de suscripciones con Stripe
import { db } from './firebaseAdmin.js';
import { stripe, STRIPE_CONFIG, SUBSCRIPTION_STATUS } from './stripeConfig.js';
import { Timestamp } from 'firebase-admin/firestore';
import { normalizarTelefono, generarPIN } from './pinUtils.js';
import { enviarMensaje } from './scheduler.js';
import dayjs from 'dayjs';

/**
 * POST /api/subscription/create-checkout
 * Crea una sesión de checkout de Stripe para nueva suscripción
 */
export async function createCheckoutSession(req, res) {
  try {
    const { phone, pin, email, negocioId } = req.body;

    // 1) Validar teléfono
    const phoneDigits = normalizarTelefono(phone);
    if (!phoneDigits || phoneDigits.length < 10) {
      return res.status(400).json({
        success: false,
        error: 'Teléfono inválido',
      });
    }

    // 2) Buscar o crear negocio
    let negocioRef;
    let negocioData;
    let isNewNegocio = false;

    if (negocioId) {
      // Negocio por ID
      negocioRef = db.collection('Negocios').doc(negocioId);
      const doc = await negocioRef.get();
      if (!doc.exists) {
        return res.status(404).json({
          success: false,
          error: 'Negocio no encontrado',
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
        // Crear nuevo negocio base
        isNewNegocio = true;
        const nuevoPin = pin || generarPIN();

        const newNegocioData = {
          leadPhone: phoneDigits,
          contactWhatsapp: phoneDigits,
          contactEmail: email || '',
          pin: nuevoPin,
          plan: 'pending', // se ajusta a basic cuando se confirma el pago
          createdAt: Timestamp.now(),
          updatedAt: Timestamp.now(),
          subscriptionType: 'pending_stripe',
          trialUsed: false,
          status: 'Sin procesar', // para que genere el schema
          websiteArchived: false,
        };

        const docRef = await db.collection('Negocios').add(newNegocioData);
        negocioRef = docRef;
        negocioData = newNegocioData;
      }
    }

    // 3) Determinar PIN final (no romper PIN existente)
    let finalPin;
    if (negocioData && negocioData.pin) {
      finalPin = String(negocioData.pin).trim();
    } else if (pin) {
      finalPin = String(pin).trim();
    } else {
      finalPin = generarPIN();
    }

    // Si es negocio existente, actualizamos contacto/email sin pisar PIN existente
    if (!isNewNegocio) {
      const updateData = {
        contactWhatsapp: negocioData.contactWhatsapp || phoneDigits,
        contactEmail: email || negocioData.contactEmail || '',
        updatedAt: Timestamp.now(),
      };
      // Solo escribimos pin si antes no tenía
      if (!negocioData.pin) {
        updateData.pin = finalPin;
      }
      await negocioRef.update(updateData);
      negocioData = { ...negocioData, ...updateData };
    }

    // 4) Crear o recuperar customer de Stripe
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

      const updateData = {
        stripeCustomerId,
        updatedAt: Timestamp.now(),
      };
      if (!negocioData.pin) {
        updateData.pin = finalPin;
      }

      await negocioRef.update(updateData);
      negocioData = { ...negocioData, ...updateData };
    }

    // 5) URLs de retorno
    const baseClientUrl = process.env.CLIENT_URL || 'https://negociosweb.mx';
    const panelUrl =
      process.env.CLIENT_PANEL_URL || `${baseClientUrl}/cliente-login`;
    const suscripcionUrl = `${baseClientUrl}/suscripcion`;

    // 6) Crear sesión de checkout de Stripe
    const session = await stripe.checkout.sessions.create({
      customer: stripeCustomerId,
      payment_method_types: ['card'],
      line_items: [
        {
          price: STRIPE_CONFIG.priceId,
          quantity: 1,
        },
      ],
      mode: 'subscription',
      // Sin session_id en la URL para evitar problemas con ModSecurity
      success_url: `${panelUrl}?success=true`,
      cancel_url: `${suscripcionUrl}?canceled=true`,
      metadata: {
        negocioId: negocioRef.id,
        phone: phoneDigits,
        pin: finalPin,
      },
      subscription_data: {
        metadata: {
          negocioId: negocioRef.id,
          phone: phoneDigits,
        },
      },
      // Locale válido (antes: es-MX -> error)
      locale: 'es-419',
    });

    console.log(`✅ Sesión de checkout creada para negocio ${negocioRef.id}`);

    return res.json({
      success: true,
      checkoutUrl: session.url,
      sessionId: session.id,
      negocioId: negocioRef.id,
    });
  } catch (error) {
    console.error('❌ Error creando sesión de checkout:', error);
    return res.status(500).json({
      success: false,
      error: 'Error al crear sesión de pago',
      details: error.message,
    });
  }
}


/**
 * POST /api/subscription/webhook
 * Webhook de Stripe para manejar eventos de suscripción
 * 
 * ⚠️ IMPORTANTE: Este endpoint DEBE recibir el body RAW
 * Se configura en server.js con: bodyParser.raw({ type: 'application/json' })
 */
export async function stripeWebhook(req, res) {
  const sig = req.headers['stripe-signature'];
  let event;

  try {
    // ✅ req.body aquí es un Buffer (raw body), no un objeto JSON
    event = stripe.webhooks.constructEvent(
      req.body,
      sig,
      STRIPE_CONFIG.webhookSecret
    );
  } catch (err) {
    console.error('❌ Webhook signature verification failed:', err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  console.log(`📨 Webhook recibido: ${event.type}`);

  try {
    switch (event.type) {
  case 'checkout.session.completed':
    await handleCheckoutCompleted(event.data.object);
    break;

  case 'customer.subscription.created':
  case 'customer.subscription.updated':
    await handleSubscriptionUpdate(event.data.object);
    break;

  case 'customer.subscription.deleted':
    await handleSubscriptionCanceled(event.data.object);
    break;

  case 'invoice.payment_failed':
    await handlePaymentFailed(event.data.object);
    break;

  case 'invoice.payment_succeeded':
  case 'invoice.paid': // 👈 agregar esto
    await handlePaymentSucceeded(event.data.object);
    break;

  default:
    console.log(`Evento no manejado: ${event.type}`);
}


    res.json({ received: true });
  } catch (error) {
    console.error('❌ Error procesando webhook:', error);
    res.status(500).json({ error: 'Error procesando webhook' });
  }
}

// Handlers de eventos de Stripe
async function handleCheckoutCompleted(session) {
  const { subscription, metadata, customer } = session;
  const { negocioId, phone, pin } = metadata || {};

  console.log(`✅ Checkout completado para negocio ${negocioId}`);

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

  // PIN y teléfono finales
  const finalPin =
    pin ||
    negocioData.pin ||
    generarPIN();

  const finalPhone =
    phone ||
    negocioData.leadPhone ||
    negocioData.phone ||
    null;

  let sub = null;

  // Intentar recuperar la suscripción de Stripe si viene el id
  if (subscription) {
    try {
      sub = await stripe.subscriptions.retrieve(subscription);
    } catch (err) {
      console.error(
        `❌ No se pudo obtener la suscripción ${subscription} desde Stripe:`,
        err.message
      );
    }
  }

  const updateData = {
    pin: finalPin,
    websiteArchived: false,
    trialActive: false,
    updatedAt: Timestamp.now(),
  };

  if (sub) {
    updateData.subscriptionId = sub.id;
    updateData.subscriptionStatus = sub.status;
    updateData.paymentMethod = 'stripe';
    updateData.plan = updateData.plan || 'basic';

    const periodEndSeconds = parseInt(sub.current_period_end);

    if (periodEndSeconds && !isNaN(periodEndSeconds)) {
      const periodEndDate = new Date(periodEndSeconds * 1000);

      updateData.subscriptionCurrentPeriodEnd =
        Timestamp.fromDate(periodEndDate);

      // Solo setear si no existen (para no pisar updates posteriores)
      if (!negocioData.planStartDate) {
        updateData.planStartDate = Timestamp.now();
      }
      if (!negocioData.planActivatedAt) {
        updateData.planActivatedAt = Timestamp.now();
      }
      updateData.planRenewalDate =
        Timestamp.fromDate(periodEndDate);
    } else {
      console.warn(
        `⚠️ current_period_end vacío o inválido para sub ${sub.id} (status: ${sub.status}). ` +
          `No rompemos el webhook; se completará con customer.subscription.updated / invoice.paid.`
      );
    }
  } else {
    console.warn(
      `⚠️ No se pudo recuperar la suscripción asociada al checkout.session ${session.id}. ` +
        `Esperaremos a los siguientes eventos de Stripe para completar los datos.`
    );
  }

  // Guardar cambios en el negocio
  await negocioRef.update(updateData);

  console.log(
    `✅ Datos de suscripción inicial guardados para negocio ${negocioId} - PIN: ${finalPin}`
  );

  // Enviar acceso por WhatsApp (si tenemos número)
  if (finalPhone) {
    const companyName =
      negocioData.companyInfo ||
      negocioData.companyName ||
      'Tu Negocio';
    const loginUrl =
      process.env.CLIENT_PANEL_URL ||
      `${process.env.CLIENT_URL || 'https://negociosweb.mx'}/cliente-login`;

    const mensaje = `🎉 ¡Suscripción activada!

✅ Hemos recibido tu registro en el sistema.
💳 Plan: Mensual
📱 Teléfono: ${finalPhone}
🔐 PIN de acceso: ${finalPin}

🌐 Ingresa a tu panel:
${loginUrl}

Si tu banco aún está procesando el cobro, Stripe confirmará automáticamente y tu suscripción quedará en estado activo.

Cualquier duda, respóndeme por aquí 🚀`;

    try {
      await enviarMensaje(
        { telefono: finalPhone, nombre: companyName },
        { type: 'texto', contenido: mensaje }
      );
      console.log(`📤 Credenciales enviadas por WhatsApp a ${finalPhone}`);
    } catch (waErr) {
      console.error('❌ Error enviando WhatsApp:', waErr);
    }
  }

  // Registrar en historial
  await db.collection('SubscriptionHistory').add({
    negocioId,
    event: 'checkout_completed',
    subscriptionId: (sub && sub.id) || subscription || null,
    status: sub ? sub.status : null,
    timestamp: Timestamp.now(),
  });
}


async function handleSubscriptionUpdate(subscription) {
  const { metadata } = subscription;
  const { negocioId } = metadata || {};

  if (!negocioId) {
    console.error('❌ No hay negocioId en metadata de suscripción');
    return;
  }

  console.log(`🔄 Actualizando suscripción para negocio ${negocioId}`);

  const negocioRef = db.collection('Negocios').doc(negocioId);

  // Validar y convertir timestamp
  const periodEndSeconds = parseInt(subscription.current_period_end);
  if (!periodEndSeconds || isNaN(periodEndSeconds)) {
    console.error('❌ Invalid subscription period end timestamp');
    return;
  }
  
  // ✅ CORRECCIÓN: Usar fromDate en lugar de fromMillis
  const periodEndDate = new Date(periodEndSeconds * 1000);

  await negocioRef.update({
    subscriptionStatus: subscription.status,
    subscriptionCurrentPeriodEnd: Timestamp.fromDate(periodEndDate),
    planRenewalDate: Timestamp.fromDate(periodEndDate),
    websiteArchived: subscription.status !== 'active',
    updatedAt: Timestamp.now(),
  });

  // Registrar en historial
  await db.collection('SubscriptionHistory').add({
    negocioId,
    event: 'subscription_updated',
    subscriptionId: subscription.id,
    status: subscription.status,
    timestamp: Timestamp.now(),
  });
}

async function handleSubscriptionCanceled(subscription) {
  const { metadata } = subscription;
  const { negocioId } = metadata || {};

  if (!negocioId) return;

  console.log(`❌ Suscripción cancelada para negocio ${negocioId}`);

  const negocioRef = db.collection('Negocios').doc(negocioId);

  await negocioRef.update({
    subscriptionStatus: 'canceled',
    plan: 'canceled',
    websiteArchived: true,
    archivedReason: 'subscription_canceled',
    canceledAt: Timestamp.now(),
    updatedAt: Timestamp.now(),
  });

  // Registrar en historial
  await db.collection('SubscriptionHistory').add({
    negocioId,
    event: 'subscription_canceled',
    subscriptionId: subscription.id,
    timestamp: Timestamp.now(),
  });
}

async function handlePaymentFailed(invoice) {
  const subscription = invoice.subscription;
  
  if (!subscription) return;

  const sub = await stripe.subscriptions.retrieve(subscription);
  const { negocioId } = sub.metadata || {};

  if (!negocioId) return;

  console.log(`⚠️ Pago fallido para negocio ${negocioId}`);

  const negocioRef = db.collection('Negocios').doc(negocioId);

  await negocioRef.update({
    subscriptionStatus: 'past_due',
    plan: 'past_due',
    websiteArchived: true,
    archivedReason: 'payment_failed',
    lastPaymentFailed: Timestamp.now(),
    updatedAt: Timestamp.now(),
  });

  // Registrar en historial
  await db.collection('SubscriptionHistory').add({
    negocioId,
    event: 'payment_failed',
    invoiceId: invoice.id,
    amount: invoice.amount_due,
    timestamp: Timestamp.now(),
  });
}

async function handlePaymentSucceeded(invoice) {
  const subscription = invoice.subscription;
  
  if (!subscription) return;

  const sub = await stripe.subscriptions.retrieve(subscription);
  const { negocioId } = sub.metadata || {};

  if (!negocioId) return;

  console.log(`✅ Pago exitoso para negocio ${negocioId}`);

  const negocioRef = db.collection('Negocios').doc(negocioId);

  // ✅ CORRECCIÓN: Convertir timestamp correctamente
  const periodEndDate = new Date(sub.current_period_end * 1000);

  await negocioRef.update({
    subscriptionStatus: 'active',
    plan: 'basic',
    websiteArchived: false,
    subscriptionCurrentPeriodEnd: Timestamp.fromDate(periodEndDate),
    planRenewalDate: Timestamp.fromDate(periodEndDate),
    lastPaymentSucceeded: Timestamp.now(),
    updatedAt: Timestamp.now(),
  });

  // Registrar en historial
  await db.collection('SubscriptionHistory').add({
    negocioId,
    event: 'payment_succeeded',
    invoiceId: invoice.id,
    amount: invoice.amount_paid,
    timestamp: Timestamp.now(),
  });
}

/**
 * POST /api/subscription/cancel
 * Cancela una suscripción
 */
export async function cancelSubscription(req, res) {
  try {
    const { negocioId } = req.body;

    const negocioDoc = await db
      .collection('Negocios')
      .doc(negocioId)
      .get();

    if (!negocioDoc.exists) {
      return res.status(404).json({
        success: false,
        error: 'Negocio no encontrado',
      });
    }

    const { subscriptionId } = negocioDoc.data();

    if (!subscriptionId) {
      return res.status(400).json({
        success: false,
        error: 'No hay suscripción activa',
      });
    }

    // Cancelar suscripción en Stripe
    const subscription = await stripe.subscriptions.cancel(subscriptionId);

    console.log(`✅ Suscripción cancelada: ${subscriptionId}`);

    return res.json({
      success: true,
      message: 'Suscripción cancelada',
      endsAt: new Date(subscription.current_period_end * 1000).toISOString(),
    });
  } catch (error) {
    console.error('❌ Error cancelando suscripción:', error);
    return res.status(500).json({
      success: false,
      error: 'Error al cancelar suscripción',
      details: error.message,
    });
  }
}

/**
 * POST /api/subscription/portal
 * Crea una sesión del portal de Stripe para gestión de suscripción
 */
export async function createPortalSession(req, res) {
  try {
    const { negocioId } = req.body;

    const negocioDoc = await db
      .collection('Negocios')
      .doc(negocioId)
      .get();
    if (!negocioDoc.exists) {
      return res.status(404).json({
        success: false,
        error: 'Negocio no encontrado',
      });
    }

    const { stripeCustomerId } = negocioDoc.data();
    if (!stripeCustomerId) {
      return res.status(400).json({
        success: false,
        error: 'No hay suscripción activa',
      });
    }

    // Crear sesión del portal
    const session =
      await stripe.billingPortal.sessions.create({
        customer: stripeCustomerId,
        return_url:
          process.env.CLIENT_PANEL_URL ||
          `${process.env.CLIENT_URL || 'https://negociosweb.mx'}/cliente-login`,
      });

    return res.json({
      success: true,
      url: session.url,
    });
  } catch (error) {
    console.error(
      '❌ Error creando sesión del portal:',
      error
    );
    return res.status(500).json({
      success: false,
      error: 'Error al crear sesión del portal',
    });
  }
}

/**
 * POST /api/subscription/trial
 * Activa período de prueba de 24 horas (sin Stripe)
 */
export async function activateTrial(req, res) {
  try {
    const { phone, pin, email, companyInfo } = req.body;

    const phoneDigits = normalizarTelefono(phone);
    const finalPin = pin || generarPIN();

    // Buscar si ya existe
    const negociosSnap = await db
      .collection('Negocios')
      .where('leadPhone', '==', phoneDigits)
      .limit(1)
      .get();

    let negocioRef;
    let negocioData;

    if (!negociosSnap.empty) {
      negocioRef = negociosSnap.docs[0].ref;
      negocioData = negociosSnap.docs[0].data();

      // Verificar si ya usó trial
      if (negocioData.trialUsed) {
        return res.status(400).json({
          success: false,
          error:
            'Ya utilizaste tu período de prueba gratuito',
        });
      }
    } else {
      // Crear nuevo negocio con trial
      const newData = {
        leadPhone: phoneDigits,
        contactWhatsapp: phoneDigits,
        contactEmail: email || '',
        companyInfo: companyInfo || 'Mi Negocio',
        pin: finalPin,
        plan: 'trial',
        trialActive: true,
        trialStartDate: Timestamp.now(),
        trialEndDate: Timestamp.fromMillis(
          Date.now() + 24 * 60 * 60 * 1000
        ),
        trialUsed: true,
        createdAt: Timestamp.now(),
        updatedAt: Timestamp.now(),
        status: 'Sin procesar',
        websiteArchived: false,
      };

      const docRef = await db
        .collection('Negocios')
        .add(newData);
      negocioRef = docRef;
      negocioData = newData;
    }

    // Activar trial
    await negocioRef.update({
      plan: 'trial',
      trialActive: true,
      trialStartDate: Timestamp.now(),
      trialEndDate: Timestamp.fromMillis(
        Date.now() + 24 * 60 * 60 * 1000
      ),
      trialUsed: true,
      pin: finalPin,
      websiteArchived: false,
      updatedAt: Timestamp.now(),
    });

    // Enviar credenciales
    await enviarMensaje(
      {
        telefono: phoneDigits,
        nombre: companyInfo || 'Cliente',
      },
      {
        type: 'texto',
        contenido:
          `🎁 ¡Prueba Gratuita Activada!\n\n` +
          `⏰ Válida por 24 horas\n` +
          `📱 Teléfono: ${phoneDigits}\n` +
          `🔐 PIN: ${finalPin}\n\n` +
          `🌐 Accede a tu panel:\n` +
          `${
            process.env.CLIENT_PANEL_URL ||
            'https://negociosweb.mx/cliente-login'
          }\n\n` +
          `¡Aprovecha para personalizar tu sitio!`,
      }
    );

    return res.json({
      success: true,
      message: 'Prueba gratuita activada',
      data: {
        negocioId: negocioRef.id,
        pin: finalPin,
        expiresAt: new Date(
          Date.now() + 24 * 60 * 60 * 1000
        ).toISOString(),
      },
    });
  } catch (error) {
    console.error(
      '❌ Error activando trial:',
      error
    );
    return res.status(500).json({
      success: false,
      error: 'Error al activar prueba gratuita',
    });
  }
}

/**
 * GET /api/subscription/status/:negocioId
 * Obtiene el estado de la suscripción
 */
export async function getSubscriptionStatus(req, res) {
  try {
    const { negocioId } = req.params;

    const negocioDoc = await db
      .collection('Negocios')
      .doc(negocioId)
      .get();
    if (!negocioDoc.exists) {
      return res.status(404).json({
        success: false,
        error: 'Negocio no encontrado',
      });
    }

    const data = negocioDoc.data();

    let subscriptionInfo = {
      hasSubscription: !!data.subscriptionId,
      status: data.subscriptionStatus || 'none',
      plan: data.plan,
      isActive: false,
      canAccess: false,
      message: '',
    };

    // Trial
    if (data.trialActive) {
      const now = Date.now();
      const trialEnd =
        data.trialEndDate?.toMillis() || 0;

      if (now < trialEnd) {
        subscriptionInfo.isActive = true;
        subscriptionInfo.canAccess = true;
        subscriptionInfo.message =
          'Período de prueba activo';
        subscriptionInfo.trialEndsAt = new Date(
          trialEnd
        ).toISOString();
      } else {
        subscriptionInfo.message =
          'Período de prueba expirado';
      }
    }

    // Suscripción Stripe
    if (data.subscriptionStatus === 'active') {
      subscriptionInfo.isActive = true;
      subscriptionInfo.canAccess = true;
      subscriptionInfo.message =
        'Suscripción activa';
      subscriptionInfo.nextPayment =
        data.subscriptionCurrentPeriodEnd?.toDate();
    } else if (
      data.subscriptionStatus === 'past_due'
    ) {
      subscriptionInfo.message =
        'Pago pendiente - Sitio suspendido';
      subscriptionInfo.needsPaymentUpdate = true;
    } else if (
      data.subscriptionStatus === 'canceled'
    ) {
      subscriptionInfo.message =
        'Suscripción cancelada';
    }

    // Plan manual (transferencia)
    if (
      !subscriptionInfo.canAccess &&
      ['basic', 'pro', 'premium'].includes(
        String(data.plan || '').toLowerCase()
      )
    ) {
      const renewalDate =
        data.planRenewalDate?.toMillis() || 0;
      if (Date.now() < renewalDate) {
        subscriptionInfo.isActive = true;
        subscriptionInfo.canAccess = true;
        subscriptionInfo.message =
          'Plan activo (pago manual)';
        subscriptionInfo.expiresAt = new Date(
          renewalDate
        ).toISOString();
      }
    }

    return res.json({
      success: true,
      subscription: subscriptionInfo,
    });
  } catch (error) {
    console.error(
      'Error obteniendo estado de suscripción:',
      error
    );
    return res.status(500).json({
      success: false,
      error:
        'Error al obtener estado de suscripción',
    });
  }
}

// Exportar todas las funciones
export default {
  createCheckoutSession,
  stripeWebhook,
  cancelSubscription,
  createPortalSession,
  activateTrial,
  getSubscriptionStatus,
};