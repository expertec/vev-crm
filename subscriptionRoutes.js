// subscriptionRoutes.js - Sistema completo de suscripciones con Stripe
import { db } from './firebaseAdmin.js';
import { stripe, STRIPE_CONFIG, SUBSCRIPTION_STATUS } from './stripeConfig.js';
import { Timestamp } from 'firebase-admin/firestore';
import { normalizarTelefono, generarPIN } from './pinUtils.js';
import { enviarMensaje } from './scheduler.js';
import dayjs from 'dayjs';

// Helpers para URLs base (soporta local y prod sin tocar código)
const getClientUrl = (req) =>
  process.env.CLIENT_URL ||
  req.headers.origin ||
  `http://${req.headers.host || 'localhost:3000'}`;

const getApiBaseUrl = (req) => {
  if (process.env.API_BASE_URL) return process.env.API_BASE_URL;
  const host = req.headers['x-forwarded-host'] || req.headers.host || 'localhost:3001';
  const proto = req.headers['x-forwarded-proto'] || req.protocol || 'http';
  return `${proto}://${host}`;
};

// Evita bloquear el webhook en operaciones no críticas (ej. WhatsApp)
const sendWhatsAppBackground = (lead, payload, context = 'WhatsApp') => {
  Promise.resolve()
    .then(() => enviarMensaje(lead, payload))
    .then(() => console.log(`📤 ${context}`))
    .catch((error) => {
      console.error(`❌ Error en envío de WhatsApp (${context}):`, error);
    });
};

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

    // 5) URLs de retorno (local friendly)
    const baseClientUrl = getClientUrl(req);
    const apiBaseUrl = getApiBaseUrl(req);
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
      // Pasamos por backend para limpiar session_id y evitar ModSecurity
      success_url: `${apiBaseUrl}/api/subscription/redirect-success`,
      cancel_url: `${apiBaseUrl}/api/subscription/redirect-cancel`,
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

// Redirecciones para limpiar session_id y evitar ModSecurity
export function subscriptionRedirectSuccess(req, res) {
  const clientPanelUrl =
    process.env.CLIENT_PANEL_URL ||
    `${getClientUrl(req)}/cliente-login`;
  return res.redirect(`${clientPanelUrl}?success=true`);
}

export function subscriptionRedirectCancel(req, res) {
  const clientBase = getClientUrl(req);
  return res.redirect(`${clientBase}/suscripcion?canceled=true`);
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
    if (!STRIPE_CONFIG.webhookSecret) {
      console.warn('⚠️ STRIPE_WEBHOOK_SECRET no configurado, se omite validación de firma (solo recomendado en local/test)');
      event = JSON.parse(req.body.toString()); // Stripe CLI envía JSON
    } else {
      event = stripe.webhooks.constructEvent(
        req.body,
        sig,
        STRIPE_CONFIG.webhookSecret
      );
    }
  } catch (err) {
    console.error('❌ Webhook signature verification failed:', err.message);
    // Fallback opcional para entorno de prueba en Render
    if (process.env.ALLOW_WEBHOOK_FALLBACK === 'true') {
      try {
        event = JSON.parse(req.body.toString());
        console.warn('⚠️ Fallback de webhook SIN verificación de firma aplicado (solo prueba)');
      } catch (parseErr) {
        return res.status(400).send(`Webhook Error: ${err.message}`);
      }
    } else {
      return res.status(400).send(`Webhook Error: ${err.message}`);
    }
  }

  console.log(`📨 Webhook recibido: ${event.type}`);

  try {
    switch (event.type) {
  case 'checkout.session.completed':
    const session = event.data.object;
    // Detectar si es pago único o suscripción
    if (session.mode === 'payment' && session.metadata?.paymentType === 'one_time') {
      // Para pagos con tarjeta, payment_status es 'paid'
      // Para OXXO, payment_status es 'unpaid' hasta que paguen
      if (session.payment_status === 'paid') {
        await handleOneTimePaymentCompleted(session);
      } else {
        console.log(`⏳ Pago pendiente (OXXO): ${session.id} - esperando confirmación`);
        // Guardar como pendiente
        await handleOxxoPending(session);
      }
    } else {
      await handleCheckoutCompleted(session);
    }
    break;

  case 'checkout.session.async_payment_succeeded':
    // Este evento se dispara cuando un pago OXXO se completa
    const asyncSession = event.data.object;
    if (asyncSession.metadata?.paymentType === 'one_time') {
      console.log(`✅ Pago OXXO confirmado: ${asyncSession.id}`);
      await handleOneTimePaymentCompleted(asyncSession);
    }
    break;

  case 'checkout.session.async_payment_failed':
    // Pago OXXO falló (expiró sin pagar)
    const failedSession = event.data.object;
    if (failedSession.metadata?.paymentType === 'one_time') {
      console.log(`❌ Pago OXXO expirado/fallido: ${failedSession.id}`);
      await handleOxxoFailed(failedSession);
    }
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

    sendWhatsAppBackground(
      { telefono: finalPhone, nombre: companyName },
      { type: 'texto', contenido: mensaje },
      `Credenciales enviadas por WhatsApp a ${finalPhone}`
    );
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
    const requestedPin = String(pin || '').trim();
    const requestedPinValid = /^\d{4}$/.test(requestedPin);
    const panelURL =
      process.env.CLIENT_PANEL_URL ||
      'https://negociosweb.mx/cliente-login';

    // Buscar si ya existe
    const negociosSnap = await db
      .collection('Negocios')
      .where('leadPhone', '==', phoneDigits)
      .limit(1)
      .get();

    let negocioRef;
    let negocioData;
    let archivedNegocioData = null;
    let archivedNegocioId = '';

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
      const archivedSnap = await db
        .collection('ArchivoNegocios')
        .where('leadPhone', '==', phoneDigits)
        .limit(1)
        .get();
      if (!archivedSnap.empty) {
        archivedNegocioData = archivedSnap.docs[0].data() || {};
        archivedNegocioId = archivedSnap.docs[0].id;
      }

      const archivedPin = String(
        archivedNegocioData?.pin || ''
      ).trim();
      const reusedPin = /^\d{4}$/.test(archivedPin)
        ? archivedPin
        : '';
      const finalPin = requestedPinValid
        ? requestedPin
        : reusedPin || generarPIN();

      // Crear nuevo negocio con trial
      const newData = {
        leadPhone: phoneDigits,
        contactWhatsapp: phoneDigits,
        contactEmail:
          email || archivedNegocioData?.contactEmail || '',
        companyInfo:
          companyInfo ||
          archivedNegocioData?.companyInfo ||
          'Mi Negocio',
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
        archivedSourceId: archivedNegocioId || null,
        reactivatedFromArchive: !!archivedNegocioData,
      };

      const docRef = await db
        .collection('Negocios')
        .add(newData);
      negocioRef = docRef;
      negocioData = newData;

      if (archivedNegocioId) {
        await db
          .collection('ArchivoNegocios')
          .doc(archivedNegocioId)
          .set(
            {
              reactivatedAt: Timestamp.now(),
              reactivatedNegocioId: docRef.id,
            },
            { merge: true }
          )
          .catch(() => {});
      }
    }

    const existingPin = String(negocioData?.pin || '').trim();
    const finalPin = requestedPinValid
      ? requestedPin
      : /^\d{4}$/.test(existingPin)
      ? existingPin
      : generarPIN();

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
          `✅ ¡Listo! Tu muestra gratuita está en camino.\n\n` +
          `Te comparto los datos de acceso:\n\n` +
          `📱 Tel: ${phoneDigits} 🔐 PIN: ${finalPin} 🌐 Panel: ${panelURL}\n\n` +
          `⏰ Activa por 24 horas.`,
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
      ['basic', 'basico', 'pro', 'premium'].includes(
        String(data.plan || '').toLowerCase()
      )
    ) {
      const renewalDate =
        data.planRenewalDate?.toMillis() ||
        data.planExpiresAt?.toMillis() ||
        data.expiresAt?.toMillis() ||
        0;
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

/**
 * Handler para pago OXXO pendiente
 */
async function handleOxxoPending(session) {
  const { metadata } = session;
  const { negocioId, phone, planId, planNombre } = metadata || {};

  if (!negocioId) return;

  // Actualizar registro de pago como pendiente
  const pagoSnap = await db.collection('pagos_stripe')
    .where('sessionId', '==', session.id)
    .limit(1)
    .get();

  if (!pagoSnap.empty) {
    await pagoSnap.docs[0].ref.update({
      status: 'pending_oxxo',
      paymentMethod: 'oxxo',
      updatedAt: Timestamp.now()
    });
  }

  // Enviar instrucciones por WhatsApp
  const negocioRef = db.collection('Negocios').doc(negocioId);
  const negocioSnap = await negocioRef.get();
  const negocioData = negocioSnap.exists ? negocioSnap.data() : {};

  const finalPhone = phone || negocioData.leadPhone;
  if (finalPhone) {
    const mensaje = `📋 ¡Casi listo! Tu pago en OXXO está pendiente.

💳 Plan: ${planNombre}
📍 Ve a cualquier OXXO y realiza el pago

⏰ Tienes hasta 3 días para completar el pago antes de que expire.

Una vez que pagues, recibirás tu confirmación automáticamente por WhatsApp.

¡Gracias por tu preferencia! 🙌`;

    sendWhatsAppBackground(
      { telefono: finalPhone, nombre: negocioData.companyInfo || 'Cliente' },
      { type: 'texto', contenido: mensaje },
      `OXXO pending enviado a ${finalPhone}`
    );
  }
}

/**
 * Handler para pago OXXO fallido/expirado
 */
async function handleOxxoFailed(session) {
  const { metadata } = session;
  const { negocioId, phone } = metadata || {};

  if (!negocioId) return;

  // Actualizar registro de pago como fallido
  const pagoSnap = await db.collection('pagos_stripe')
    .where('sessionId', '==', session.id)
    .limit(1)
    .get();

  if (!pagoSnap.empty) {
    await pagoSnap.docs[0].ref.update({
      status: 'expired',
      updatedAt: Timestamp.now()
    });
  }

  // Notificar por WhatsApp
  const negocioRef = db.collection('Negocios').doc(negocioId);
  const negocioSnap = await negocioRef.get();
  const negocioData = negocioSnap.exists ? negocioSnap.data() : {};

  const finalPhone = phone || negocioData.leadPhone;
  if (finalPhone) {
    const mensaje = `⚠️ Tu pago en OXXO ha expirado.

Si aún deseas activar tu plan, puedes generar un nuevo pago desde nuestra página.

¿Necesitas ayuda? Responde a este mensaje. 🙋‍♂️`;

    sendWhatsAppBackground(
      { telefono: finalPhone, nombre: negocioData.companyInfo || 'Cliente' },
      { type: 'texto', contenido: mensaje },
      `OXXO expired enviado a ${finalPhone}`
    );
  }
}

/**
 * Handler para pago único completado (Stripe Checkout mode: payment)
 */
async function handleOneTimePaymentCompleted(session) {
  const { metadata, payment_intent, amount_total } = session;
  const { negocioId, phone, planId, planNombre, duracionDias } = metadata || {};
  const safePlanId = planId || 'basico';
  const safePlanNombre = planNombre || 'Plan Anual';
  const safeAmountTotal = Number.isFinite(amount_total) ? amount_total : 0;
  const safePaymentIntent = payment_intent || null;

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
    plan: safePlanId,
    planNombre: safePlanNombre,
    subscriptionStatus: 'active', // Para que el panel lo considere activo
    planStartDate: negocioData.planStartDate || Timestamp.now(),
    planActivatedAt: Timestamp.now(),
    planExpiresAt: Timestamp.fromDate(expiresAt),
    planRenewalDate: Timestamp.fromDate(expiresAt),
    expiresAt: Timestamp.fromDate(expiresAt), // nombre alterno que usa el panel
    trialActive: false,
    websiteArchived: false,
    pin: finalPin,
    lastPaymentId: safePaymentIntent,
    lastPaymentAmount: safeAmountTotal / 100,
    lastPaymentDate: Timestamp.now(),
    paymentMethod: 'stripe_onetime',
    updatedAt: Timestamp.now()
  });

  console.log(`✅ Plan actualizado: negocio=${negocioId}, plan=${safePlanId}, expira=${expiresAt.toISOString()}`);

  // Actualizar registro de pago si existe
  const pagoSnap = await db.collection('pagos_stripe')
    .where('sessionId', '==', session.id)
    .limit(1)
    .get();

  if (!pagoSnap.empty) {
    await pagoSnap.docs[0].ref.update({
      paymentIntentId: safePaymentIntent,
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

✅ Plan: ${safePlanNombre}
💰 Monto: $${safeAmountTotal / 100} MXN
📅 Válido hasta: ${expiresAt.toLocaleDateString('es-MX')}

🔐 Tu PIN de acceso: ${finalPin}

🌐 Ingresa a tu panel:
${loginUrl}

¡Gracias por tu confianza! 🚀`;

    sendWhatsAppBackground(
      { telefono: finalPhone, nombre: negocioData.companyInfo || 'Cliente' },
      { type: 'texto', contenido: mensaje },
      `Confirmación enviada por WhatsApp a ${finalPhone}`
    );
  }

  // Registrar en historial
  await db.collection('PaymentHistory').add({
    negocioId,
    event: 'one_time_payment_completed',
    sessionId: session.id,
    paymentIntentId: safePaymentIntent,
    planId: safePlanId,
    amount: safeAmountTotal / 100,
    currency: 'mxn',
    expiresAt: Timestamp.fromDate(expiresAt),
    timestamp: Timestamp.now()
  });
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
