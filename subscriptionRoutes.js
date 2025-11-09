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

    // Validaciones
    const phoneDigits = normalizarTelefono(phone);
    if (!phoneDigits || phoneDigits.length < 10) {
      return res.status(400).json({
        success: false,
        error: 'Teléfono inválido',
      });
    }

    // Buscar o crear negocio
    let negocioRef;
    let negocioData;
    let isNewNegocio = false;

    if (negocioId) {
      // Negocio existente por ID
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

        // PIN para nuevo negocio
        const nuevoPin = pin || generarPIN();

        const newNegocioData = {
          leadPhone: phoneDigits,
          contactWhatsapp: phoneDigits,
          contactEmail: email || '',
          pin: nuevoPin,
          plan: 'trial', // se actualiza al confirmar pago
          createdAt: Timestamp.now(),
          updatedAt: Timestamp.now(),
          subscriptionType: 'pending_stripe',
          trialUsed: false,
          status: 'Sin procesar', // para generación de schema
        };

        const docRef = await db.collection('Negocios').add(newNegocioData);
        negocioRef = docRef;
        negocioData = newNegocioData;
      }
    }

    // Determinar PIN final:
    // - Si el negocio ya tiene PIN, lo respetamos.
    // - Si se envía un PIN manual, lo usamos solo si no había uno.
    // - Si no hay ninguno, generamos uno nuevo.
    let finalPin;
    if (negocioData && negocioData.pin) {
      finalPin = String(negocioData.pin).trim();
    } else if (pin) {
      finalPin = String(pin).trim();
    } else {
      finalPin = generarPIN();
    }

    // Si es negocio existente (por phone o negocioId) y no es nuevo,
    // podemos actualizar contacto/email sin tocar PIN existente.
    if (!isNewNegocio) {
      await negocioRef.update({
        contactWhatsapp: negocioData.contactWhatsapp || phoneDigits,
        contactEmail: email || negocioData.contactEmail || '',
        updatedAt: Timestamp.now(),
        // No sobrescribimos pin si ya existía
      });
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

      // Guardar customerId en Firebase
      // Solo escribimos pin si el negocio aún no tenía uno
      const updateData = {
        stripeCustomerId: stripeCustomerId,
        updatedAt: Timestamp.now(),
      };
      if (!negocioData.pin) {
        updateData.pin = finalPin;
      }

      await negocioRef.update(updateData);

      // Refrescar negocioData en memoria
      negocioData = {
        ...negocioData,
        stripeCustomerId,
        pin: negocioData.pin || finalPin,
      };
    }

    // URLs de retorno
    const baseClientUrl = process.env.CLIENT_URL || 'https://negociosweb.mx';
    const panelUrl =
      process.env.CLIENT_PANEL_URL || `${baseClientUrl}/cliente-login`;
    const suscripcionUrl = `${baseClientUrl}/suscripcion`;

    // Crear sesión de checkout
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
      success_url: `${panelUrl}?success=true&session_id={CHECKOUT_SESSION_ID}`,
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
      // Locale válido para Stripe (antes: es-MX -> error)
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
 */
export async function stripeWebhook(req, res) {
  const sig = req.headers['stripe-signature'];
  let event;

  try {
    // Verificar firma del webhook
    event = stripe.webhooks.constructEvent(
      req.rawBody || req.body, // Necesitas configurar rawBody en Express
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
  const { subscription, metadata } = session;
  const { negocioId, phone, pin } = metadata || {};

  console.log(`✅ Checkout completado para negocio ${negocioId}`);

  const negocioRef = db.collection('Negocios').doc(negocioId);

  // Obtener detalles de la suscripción
  const sub = await stripe.subscriptions.retrieve(subscription);

  const finalPin = pin || generarPIN();

  await negocioRef.update({
    plan: 'basic', // Plan mensual
    subscriptionId: subscription,
    subscriptionStatus: sub.status,
    subscriptionCurrentPeriodEnd: Timestamp.fromMillis(
      sub.current_period_end * 1000
    ),
    subscriptionStartDate: Timestamp.now(),
    planActivatedAt: Timestamp.now(),
    paymentMethod: 'stripe',
    pin: finalPin,
    websiteArchived: false,
    updatedAt: Timestamp.now(),
  });

  // Enviar credenciales por WhatsApp
  const mensaje = `🎉 ¡Suscripción Activada!

✅ Tu pago ha sido confirmado
💳 Plan: Mensual $99 MXN
📱 Teléfono: ${phone}
🔐 PIN: ${finalPin}

🌐 Accede a tu panel:
${process.env.CLIENT_PANEL_URL || 'https://negociosweb.mx/cliente-login'}

Tu suscripción se renovará automáticamente cada mes.
Para cancelar o actualizar tu método de pago, entra a tu panel.

¡Gracias por confiar en nosotros! 🚀`;

  await enviarMensaje(
    { telefono: phone, nombre: 'Cliente' },
    { type: 'texto', contenido: mensaje }
  );

  // Registrar en historial
  await db.collection('SubscriptionHistory').add({
    negocioId,
    event: 'subscription_created',
    subscriptionId: subscription,
    amount: 99,
    currency: 'mxn',
    timestamp: Timestamp.now(),
  });
}

async function handleSubscriptionUpdate(subscription) {
  const { id, customer, status, current_period_end, cancel_at_period_end } =
    subscription;

  // Buscar negocio por customerId
  const negociosSnap = await db
    .collection('Negocios')
    .where('stripeCustomerId', '==', customer)
    .limit(1)
    .get();

  if (negociosSnap.empty) {
    console.warn(`No se encontró negocio para customer ${customer}`);
    return;
  }

  const negocioRef = negociosSnap.docs[0].ref;

  // Determinar si el sitio debe archivarse
  const shouldArchive =
    status !== SUBSCRIPTION_STATUS.ACTIVE &&
    status !== SUBSCRIPTION_STATUS.TRIALING;

  await negocioRef.update({
    subscriptionId: id,
    subscriptionStatus: status,
    subscriptionCurrentPeriodEnd: Timestamp.fromMillis(
      current_period_end * 1000
    ),
    subscriptionCancelAtPeriodEnd: cancel_at_period_end,
    plan:
      status === SUBSCRIPTION_STATUS.ACTIVE ? 'basic' : 'suspended',
    websiteArchived: shouldArchive,
    updatedAt: Timestamp.now(),
  });

  console.log(`📝 Suscripción actualizada: ${id} - Estado: ${status}`);
}

async function handleSubscriptionCanceled(subscription) {
  const { id, customer } = subscription;

  // Buscar negocio
  const negociosSnap = await db
    .collection('Negocios')
    .where('stripeCustomerId', '==', customer)
    .limit(1)
    .get();

  if (negociosSnap.empty) return;

  const negocioRef = negociosSnap.docs[0].ref;
  const negocioData = negociosSnap.docs[0].data();

  // Archivar sitio inmediatamente
  await negocioRef.update({
    subscriptionStatus: 'canceled',
    subscriptionCanceledAt: Timestamp.now(),
    plan: 'canceled',
    websiteArchived: true,
    archivedReason: 'subscription_canceled',
    updatedAt: Timestamp.now(),
  });

  // Mover a colección de archivados
  if (negocioData.schema) {
    await db.collection('ArchivedSites').doc(negocioRef.id).set({
      ...negocioData,
      archivedAt: Timestamp.now(),
      archivedReason: 'subscription_canceled',
    });
  }

  // Notificar por WhatsApp
  if (negocioData.leadPhone) {
    await enviarMensaje(
      {
        telefono: negocioData.leadPhone,
        nombre: negocioData.companyInfo || 'Cliente',
      },
      {
        type: 'texto',
        contenido:
          `Tu suscripción ha sido cancelada y tu sitio web ha sido archivado.\n\n` +
          `Puedes reactivarlo en cualquier momento volviendo a suscribirte.\n\n` +
          `Gracias por haber confiado en nosotros.`,
      }
    );
  }

  console.log(`❌ Suscripción cancelada y sitio archivado: ${id}`);
}

async function handlePaymentFailed(invoice) {
  const { customer } = invoice;

  const negociosSnap = await db
    .collection('Negocios')
    .where('stripeCustomerId', '==', customer)
    .limit(1)
    .get();

  if (negociosSnap.empty) return;

  const negocioRef = negociosSnap.docs[0].ref;
  const negocioData = negociosSnap.docs[0].data();

  // Marcar como pago fallido y archivar
  await negocioRef.update({
    subscriptionStatus: 'past_due',
    plan: 'suspended',
    websiteArchived: true,
    archivedReason: 'payment_failed',
    lastPaymentFailed: Timestamp.now(),
    updatedAt: Timestamp.now(),
  });

  // Notificar por WhatsApp
  if (negocioData.leadPhone) {
    await enviarMensaje(
      {
        telefono: negocioData.leadPhone,
        nombre: negocioData.companyInfo || 'Cliente',
      },
      {
        type: 'texto',
        contenido:
          `⚠️ No pudimos procesar tu pago mensual.\n\n` +
          `Tu sitio web ha sido suspendido temporalmente.\n\n` +
          `Por favor actualiza tu método de pago en:\n` +
          `${process.env.CLIENT_PANEL_URL || 'https://negociosweb.mx/cliente-login'}\n\n` +
          `Si necesitas ayuda, contáctanos.`,
      }
    );
  }

  console.log(`💳❌ Pago fallido para negocio ${negocioRef.id}`);
}

async function handlePaymentSucceeded(invoice) {
  const { customer } = invoice;

  const negociosSnap = await db
    .collection('Negocios')
    .where('stripeCustomerId', '==', customer)
    .limit(1)
    .get();

  if (negociosSnap.empty) return;

  const negocioRef = negociosSnap.docs[0].ref;
  const negocioData = negociosSnap.docs[0].data();

  // Reactivar si estaba suspendido por pago fallido
  if (
    negocioData.websiteArchived &&
    negocioData.archivedReason === 'payment_failed'
  ) {
    await negocioRef.update({
      subscriptionStatus: 'active',
      plan: 'basic',
      websiteArchived: false,
      archivedReason: null,
      lastPaymentSuccess: Timestamp.now(),
      updatedAt: Timestamp.now(),
    });

    // Borrar copia archivada si existía
    const archivedDoc = await db
      .collection('ArchivedSites')
      .doc(negocioRef.id)
      .get();
    if (archivedDoc.exists) {
      await archivedDoc.ref.delete();
    }

    console.log(
      `✅ Sitio reactivado por pago exitoso: ${negocioRef.id}`
    );
  }

  // Registrar pago exitoso
  await db.collection('SubscriptionHistory').add({
    negocioId: negocioRef.id,
    event: 'payment_succeeded',
    amount: 99,
    currency: 'mxn',
    timestamp: Timestamp.now(),
  });
}

/**
 * POST /api/subscription/cancel
 * Cancela una suscripción activa
 */
export async function cancelSubscription(req, res) {
  try {
    const { negocioId, phone, pin } = req.body;

    const phoneDigits = normalizarTelefono(phone);

    const negocioRef = db.collection('Negocios').doc(negocioId);
    const negocioDoc = await negocioRef.get();

    if (!negocioDoc.exists) {
      return res.status(404).json({
        success: false,
        error: 'Negocio no encontrado',
      });
    }

    const negocioData = negocioDoc.data();

    // Verificar PIN y teléfono
    if (
      String(negocioData.pin).trim() !== String(pin).trim() ||
      negocioData.leadPhone !== phoneDigits
    ) {
      return res.status(401).json({
        success: false,
        error: 'Credenciales inválidas',
      });
    }

    if (!negocioData.subscriptionId) {
      return res.status(400).json({
        success: false,
        error: 'No hay suscripción activa',
      });
    }

    // Cancelar en Stripe
    await stripe.subscriptions.cancel(negocioData.subscriptionId);

    console.log(
      `🚫 Suscripción cancelada: ${negocioData.subscriptionId}`
    );

    return res.json({
      success: true,
      message: 'Suscripción cancelada exitosamente',
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
