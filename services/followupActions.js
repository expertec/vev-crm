// services/followupActions.js
//
// Acciones de seguimiento "de un clic" para el vendedor: botones que generan
// (y envian, desde el endpoint) un mensaje listo y personalizado para revivir
// la conversacion con un cliente. NO es automatizacion total: el humano decide
// cuando y a quien; la herramienta arma el mensaje correcto.
//
import {
  buildSampleLink,
  buildSampleFormLink,
  resolveSampleSlug,
  hasLeadCompletedForm,
} from './leadReactivationService.js';
import { db } from '../firebaseAdmin.js';
import { stripe } from '../stripeConfig.js';
import { Timestamp } from 'firebase-admin/firestore';

function cleanText(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function firstName(value = '') {
  const raw = cleanText(value);
  return raw ? raw.split(' ')[0] : '';
}

function renderTemplate(template = '', vars = {}) {
  let text = String(template || '')
    .replace(/\{\{nombre\}\}/g, vars.nombre || '')
    .replace(/\{\{link\}\}/g, vars.link || '');
  text = text.replace(/^Hola\s*,/i, 'Hola,');
  text = text.replace(/^\s*,\s*/, '');
  text = cleanText(text.replace(/\s([,.!?;:])/g, '$1'));
  if (text) text = text.charAt(0).toUpperCase() + text.slice(1);
  return text;
}

function pickRandom(list = []) {
  if (!Array.isArray(list) || list.length === 0) return '';
  return list[Math.floor(Math.random() * list.length)];
}

// Datos de transferencia bancaria desde PAYMENT_BANK_DETAILS.
// Permite separar líneas con "|" o con "\n" literal en la variable de entorno.
function getBankDetails() {
  const raw = String(process.env.PAYMENT_BANK_DETAILS || '').trim();
  if (!raw) return '';
  return raw.replace(/\\n/g, '\n').replace(/\s*\|\s*/g, '\n').trim();
}

const PAYMENT_REFERENCE_PLANS = {
  basico: {
    id: 'basico',
    name: 'Plan Basico Negocios Web',
    description: 'Pagina web profesional con funciones basicas',
    amountCents: 39700,
    currency: 'mxn',
    durationDays: 365,
  },
  pro: {
    id: 'pro',
    name: 'Plan Pro Negocios Web',
    description: 'Pagina web premium con funciones avanzadas',
    amountCents: 99700,
    currency: 'mxn',
    durationDays: 365,
  },
};

function cleanDigits(value = '') {
  return String(value || '').replace(/\D/g, '');
}

function cleanEmail(value = '') {
  const email = cleanText(value).toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : '';
}

function getClientUrl(context = {}) {
  const req = context.req || {};
  return (
    process.env.CLIENT_URL ||
    req.headers?.origin ||
    (req.headers?.host ? `${req.protocol || 'http'}://${req.headers.host}` : 'https://negociosweb.mx')
  );
}

function centsFromEnv() {
  const cents = Number.parseInt(String(process.env.STRIPE_PAYMENT_REFERENCE_AMOUNT_CENTS || '').trim(), 10);
  if (Number.isFinite(cents) && cents > 0) return cents;

  const amount = Number.parseFloat(
    String(process.env.STRIPE_PAYMENT_REFERENCE_AMOUNT || process.env.PAYMENT_REFERENCE_AMOUNT || '')
      .replace(/,/g, '')
      .trim()
  );
  if (Number.isFinite(amount) && amount > 0) return Math.round(amount * 100);

  return null;
}

function centsFromInput(value = '') {
  const raw = String(value ?? '').replace(/,/g, '').trim();
  if (!raw) return null;
  const amount = Number.parseFloat(raw);
  if (!Number.isFinite(amount) || amount <= 0) {
    const error = new Error('Define un monto valido mayor a 0 para generar la referencia de Stripe.');
    error.code = 'INVALID_PAYMENT_AMOUNT';
    throw error;
  }
  return Math.round(amount * 100);
}

function normalizeCatalogContextItem(value = null) {
  if (!value || typeof value !== 'object') return null;
  return {
    id: cleanText(value.id || ''),
    source: cleanText(value.source || ''),
    sourceId: cleanText(value.sourceId || ''),
    name: cleanText(value.name || value.title || ''),
    description: cleanText(value.description || ''),
    category: cleanText(value.category || ''),
    imageUrl: String(value.imageUrl || value.image || '').trim(),
  };
}

function getPaymentReferenceConfig(context = {}) {
  const planId = cleanText(process.env.STRIPE_PAYMENT_REFERENCE_PLAN_ID || 'basico').toLowerCase();
  const basePlan = PAYMENT_REFERENCE_PLANS[planId] || PAYMENT_REFERENCE_PLANS.basico;
  const catalogItem = normalizeCatalogContextItem(context.paymentCatalogItem);
  const amountCents = centsFromInput(context.paymentAmount) || centsFromEnv() || basePlan.amountCents;
  const currency = cleanText(process.env.STRIPE_PAYMENT_REFERENCE_CURRENCY || basePlan.currency || 'mxn').toLowerCase();
  const customConcept = cleanText(context.paymentConcept || catalogItem?.name || '');
  const name = cleanText(customConcept || process.env.STRIPE_PAYMENT_REFERENCE_PRODUCT_NAME || basePlan.name);
  const description = cleanText(catalogItem?.description || customConcept || process.env.STRIPE_PAYMENT_REFERENCE_DESCRIPTION || basePlan.description);
  const durationDays = Number.parseInt(
    String(process.env.STRIPE_PAYMENT_REFERENCE_DURATION_DAYS || basePlan.durationDays || 365),
    10
  );

  return {
    planId: basePlan.id,
    name,
    description,
    amountCents,
    amount: amountCents / 100,
    currency,
    customConcept,
    catalogItem,
    durationDays: Number.isFinite(durationDays) && durationDays > 0 ? durationDays : 365,
  };
}

function timestampToMillis(value) {
  if (!value) return 0;
  if (typeof value.toMillis === 'function') return value.toMillis();
  if (typeof value.toDate === 'function') return value.toDate().getTime();
  if (value instanceof Date) return value.getTime();
  const asNumber = Number(value);
  return Number.isFinite(asNumber) ? asNumber : 0;
}

function reusableStripeReference(lead = {}, config = {}) {
  const ref = lead?.stripePaymentReference || {};
  const expiresAtMs = timestampToMillis(ref.expiresAt);
  const isUsable =
    String(ref.checkoutUrl || '').trim() &&
    String(ref.status || '').startsWith('pending') &&
    Number(ref.amountCents || 0) === config.amountCents &&
    String(ref.currency || '').toLowerCase() === config.currency &&
    cleanText(ref.productName || '') === config.name &&
    expiresAtMs > Date.now() + 15 * 60 * 1000;

  return isUsable ? ref : null;
}

async function getOrCreateStripeCustomer(lead = {}) {
  const leadId = cleanText(lead.id || '');
  const phone = cleanDigits(lead.telefono || lead.phone || lead.leadPhone || '');
  const name = cleanText(lead.nombre || lead.name || '');
  const email = cleanEmail(lead.email || lead.contactEmail || lead.customerEmail || '');
  let customerId = cleanText(lead.stripeCustomerId || '');

  if (customerId) {
    try {
      await stripe.customers.retrieve(customerId);
      return customerId;
    } catch {
      customerId = '';
    }
  }

  const customer = await stripe.customers.create({
    ...(name ? { name } : {}),
    ...(email ? { email } : {}),
    ...(phone ? { phone } : {}),
    metadata: {
      ...(leadId ? { leadId } : {}),
      ...(phone ? { phone } : {}),
      source: 'crm_followup_payment_reference',
    },
  });

  if (leadId) {
    await db.collection('leads').doc(leadId).set({
      stripeCustomerId: customer.id,
      updatedAt: Timestamp.now(),
    }, { merge: true });
  }

  return customer.id;
}

async function createStripePaymentReference(lead = {}, context = {}) {
  const leadId = cleanText(lead.id || '');
  if (!leadId) {
    const error = new Error('Este lead no tiene ID para asociar la referencia de pago.');
    error.code = 'NO_LINK_AVAILABLE';
    throw error;
  }

  const config = getPaymentReferenceConfig(context);
  const reusable = reusableStripeReference(lead, config);
  if (reusable) {
    return {
      ...reusable,
      planId: reusable.planId || config.planId,
      productName: reusable.productName || config.name,
      amount: Number(reusable.amount || config.amount),
    };
  }

  const customerId = await getOrCreateStripeCustomer(lead);
  const clientUrl = getClientUrl(context).replace(/\/+$/, '');
  const phone = cleanDigits(lead.telefono || lead.phone || lead.leadPhone || '');
  const negocioId = cleanText(lead.negocioId || lead.businessId || lead.negocio?.id || '');
  const productData = {
    name: config.name,
    description: config.description,
  };
  if (config.catalogItem?.imageUrl) {
    productData.images = [config.catalogItem.imageUrl];
  }

  const session = await stripe.checkout.sessions.create({
    customer: customerId,
    payment_method_types: ['customer_balance'],
    payment_method_options: {
      customer_balance: {
        funding_type: 'bank_transfer',
        bank_transfer: {
          type: 'mx_bank_transfer',
          requested_address_types: ['spei'],
        },
      },
    },
    line_items: [
      {
        price_data: {
          currency: config.currency,
          product_data: productData,
          unit_amount: config.amountCents,
        },
        quantity: 1,
      },
    ],
    mode: 'payment',
    success_url: `${clientUrl}/pago?status=success`,
    cancel_url: `${clientUrl}/pago?status=canceled`,
    metadata: {
      paymentType: 'lead_bank_transfer',
      leadId,
      ...(phone ? { phone } : {}),
      ...(negocioId ? { negocioId } : {}),
      planId: config.planId,
      planNombre: config.name,
      ...(config.customConcept ? { concepto: config.customConcept } : {}),
      ...(config.catalogItem?.id ? { catalogItemId: config.catalogItem.id } : {}),
      ...(config.catalogItem?.source ? { catalogSource: config.catalogItem.source } : {}),
      ...(config.catalogItem?.sourceId ? { catalogSourceId: config.catalogItem.sourceId } : {}),
      duracionDias: String(config.durationDays),
    },
    locale: 'es-419',
  });

  const expiresAt = session.expires_at
    ? Timestamp.fromDate(new Date(Number(session.expires_at) * 1000))
    : null;
  const reference = {
    provider: 'stripe',
    paymentMethod: 'mx_bank_transfer',
    sessionId: session.id,
    checkoutUrl: session.url,
    customerId,
    planId: config.planId,
    productName: config.name,
    catalogItem: config.catalogItem || null,
    amount: config.amount,
    amountCents: config.amountCents,
    currency: config.currency,
    status: 'pending_bank_transfer',
    ...(expiresAt ? { expiresAt } : {}),
    createdAt: Timestamp.now(),
    updatedAt: Timestamp.now(),
  };

  await db.collection('leads').doc(leadId).set({
    stripeCustomerId: customerId,
    stripePaymentReference: reference,
    updatedAt: Timestamp.now(),
  }, { merge: true });

  await db.collection('pagos_stripe').add({
    sessionId: session.id,
    leadId,
    ...(negocioId ? { negocioId } : {}),
    phone: phone || null,
    customerId,
    planId: config.planId,
    planNombre: config.name,
    catalogItem: config.catalogItem || null,
    monto: config.amount,
    montoCentavos: config.amountCents,
    currency: config.currency,
    status: 'pending_bank_transfer',
    paymentMethod: 'stripe_bank_transfer',
    paymentType: 'lead_bank_transfer',
    checkoutUrl: session.url,
    ...(expiresAt ? { expiresAt } : {}),
    createdAt: Timestamp.now(),
  });

  return reference;
}

function buildBankPaymentMessage(lead = {}, details = '') {
  const nombre = firstName(lead?.nombre || '');
  const saludo = nombre ? `Hola ${nombre}, ` : 'Hola, ';
  return `${saludo}con gusto. Estos son los datos para tu pago por transferencia:\n\n${details}\n\nEn cuanto la hagas, mandame el comprobante por aqui y activo tu pagina. Gracias!`;
}

function buildStripePaymentMessage(lead = {}, reference = {}) {
  const nombre = firstName(lead?.nombre || '');
  const saludo = nombre ? `Hola ${nombre}, ` : 'Hola, ';
  const amount = Number(reference.amount || 0).toLocaleString('es-MX', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  });
  const currency = cleanText(reference.currency || 'mxn').toUpperCase();
  const productName = cleanText(reference.productName || 'Negocios Web');

  return `${saludo}para que tu pago quede registrado de forma segura por Negocios Web, te comparto este enlace de Stripe:\n\n${reference.checkoutUrl}\n\nElige transferencia bancaria SPEI. Stripe te mostrara la CLABE y referencia unica para completar el pago por ${productName} (${amount} ${currency}). En cuanto se confirme, queda registrado automaticamente.`;
}

// Catalogo de acciones. requiresLink: 'sample_or_form' (dinamico) | 'form' | null.
const ACTIONS = [
  {
    key: 'recordar_muestra',
    label: 'Recordar muestra',
    emoji: '🌐',
    description: 'Reenvia su muestra si ya la tiene; si no, lo invita a llenar el formulario.',
    requiresLink: 'sample_or_form',
    sampleVariants: [
      'Hola {{nombre}}, te reenvio tu muestra de pagina para que la veas con calma. Si te gusta, hoy mismo la dejamos lista: {{link}}',
      '{{nombre}}, echale un ojo otra vez a tu muestra y me dices si la dejamos lista: {{link}}',
      'Hola {{nombre}}, aqui esta de nuevo el enlace de tu muestra, cualquier ajuste lo vemos juntos: {{link}}',
    ],
    formVariants: [
      'Hola {{nombre}}, te habia mandado el formulario para hacerte tu muestra de pagina GRATIS y vi que aun no lo llenas. Son 2 minutos y con eso te la armo: {{link}}',
      '{{nombre}}, para hacerte tu muestra gratis solo necesito que llenes este formulario corto, asi la dejo a la medida de tu negocio: {{link}}',
      'Hola {{nombre}}, aun puedo hacerte tu muestra de pagina sin costo. Llena aqui tus datos y yo me encargo de lo demas: {{link}}',
    ],
  },
  {
    key: 'recordar_prueba',
    label: 'Recordar prueba gratis',
    emoji: '🧪',
    description: 'Recuerda al cliente que tiene una prueba/demo gratis disponible.',
    requiresLink: null,
    variants: [
      'Hola {{nombre}}, te recuerdo que tu prueba gratis sigue disponible. Te la dejo lista para que veas como te funcionaria, sin compromiso.',
      '{{nombre}}, no quiero que pierdas tu prueba gratis. Te la estoy preparando para que la veas funcionando hoy mismo.',
      'Hola {{nombre}}, sigues a tiempo de tu prueba sin costo. Te la dejo lista y te paso el acceso por aqui.',
    ],
  },
  {
    key: 'llenar_formulario',
    label: 'Pedir llenar formulario',
    emoji: '📝',
    description: 'Le pide que complete el formulario para generar su muestra gratis.',
    requiresLink: 'form',
    variants: [
      'Hola {{nombre}}, para armarte tu muestra de pagina GRATIS solo necesito que llenes este formulario corto (2 min): {{link}}',
      '{{nombre}}, en cuanto llenes este formulario te preparo tu muestra sin costo y te la mando por aqui: {{link}}',
      'Hola {{nombre}}, dejame hacerte tu muestra gratis. Llena tus datos aqui y yo me encargo del resto: {{link}}',
    ],
  },
  {
    key: 'seguir_interesado',
    label: '¿Sigue interesado?',
    emoji: '👋',
    description: 'Mensaje suave para retomar contacto sin presionar.',
    requiresLink: null,
    variants: [
      'Hola {{nombre}}, paso a saludarte y saber si aun te interesa avanzar con tu proyecto. Si quieres, te apoyo por aqui.',
      '{{nombre}}, sigo al pendiente contigo. Te late que retomemos lo que habiamos platicado?',
      'Hola {{nombre}}, no quiero dejarte colgado. Sigues interesado? Cualquier duda la resolvemos rapido.',
    ],
  },
  {
    key: 'enviar_oferta',
    label: 'Oferta del mes',
    emoji: '🎯',
    description: 'Empuja con una oferta/condicion especial por avanzar ahora.',
    requiresLink: null,
    variants: [
      'Hola {{nombre}}, este mes tengo una condicion especial para dejar tu proyecto listo. Si arrancamos esta semana la aprovechas. Te aparto el lugar.',
      '{{nombre}}, por iniciar ahora te dejo un mejor precio. Te paso la propuesta para que la veas y la dejamos lista.',
      'Hola {{nombre}}, tengo una promo activa para arrancar este mes y me acorde de ti. Te la dejo lista esta semana con el mejor precio.',
    ],
  },
  {
    key: 'enviar_datos_pago',
    label: 'Enviar referencia Stripe',
    emoji: '💳',
    description: 'Genera un enlace de Stripe con transferencia SPEI y referencia unica para el cliente.',
    requiresLink: 'bank',
    requiresPaymentAmount: true,
    // El mensaje se arma aparte porque puede crear/reutilizar una sesion de Stripe.
    variants: [],
  },
  {
    key: 'hablar_encargado',
    label: 'Pedir hablar con encargado',
    emoji: '🙋',
    description: 'Util cuando contesta un bot o recepcion: pide llegar a la persona que decide.',
    requiresLink: null,
    variants: [
      'Hola, buen dia. Me gustaria platicar con la persona encargada de la pagina web o la publicidad del negocio. Tengo una propuesta concreta que les puede servir. Con quien tengo el gusto?',
      'Que tal, me podrian comunicar con el dueno o el encargado de marketing? Es sobre una propuesta para atraer mas clientes al negocio. Gracias.',
      'Hola, disculpa, esto lo atiende un asistente? Me interesa hablar directo con la persona que toma las decisiones del negocio para mostrarle una propuesta. Como puedo contactarla?',
    ],
  },
  {
    key: 'agendar_llamada',
    label: 'Proponer llamada',
    emoji: '📞',
    description: 'Propone una llamada corta para destrabar la venta.',
    requiresLink: null,
    variants: [
      'Hola {{nombre}}, te parece si hacemos una llamada corta para resolver tus dudas y ver como avanzamos? Que horario te acomoda?',
      '{{nombre}}, en 5 minutos por llamada te explico todo mas claro. Te marco hoy o prefieres manana?',
      'Hola {{nombre}}, agendamos una llamadita rapida? Asi te resuelvo y avanzamos sin tanta vuelta.',
    ],
  },
  {
    key: 'ultima_llamada',
    label: 'Último intento (cierre)',
    emoji: '🔚',
    description: 'Cierre suave que muchas veces provoca respuesta.',
    requiresLink: null,
    variants: [
      'Hola {{nombre}}, no quiero llenarte de mensajes, asi que cierro el seguimiento por ahora. Si mas adelante quieres retomarlo, aqui estoy para apoyarte.',
      '{{nombre}}, te dejo de escribir por ahora para no molestar. Cuando quieras avanzar, me mandas un mensaje y seguimos donde lo dejamos.',
      'Hola {{nombre}}, entiendo que a veces no es el momento. Te dejo tranquilo, y si lo retomas mas adelante con gusto te ayudo.',
    ],
  },
];

const ACTION_MAP = new Map(ACTIONS.map((a) => [a.key, a]));

// Catalogo completo de textos de los botones (para revision de copy / BI).
export function getFollowupMessageCatalog() {
  return ACTIONS.map((a) => ({
    key: a.key,
    label: a.label,
    description: a.description,
    variants: Array.isArray(a.variants) ? a.variants : [],
    sampleVariants: Array.isArray(a.sampleVariants) ? a.sampleVariants : [],
    formVariants: Array.isArray(a.formVariants) ? a.formVariants : [],
  }));
}

// Metadata para pintar los botones en el frontend (sin textos internos).
export function listFollowupActions() {
  return ACTIONS.map((a) => ({
    key: a.key,
    label: a.label,
    emoji: a.emoji,
    description: a.description,
    requiresLink: a.requiresLink || null,
    requiresPaymentAmount: a.requiresPaymentAmount === true,
  }));
}

/**
 * Construye el mensaje de una accion para un lead.
 * @returns {{ actionKey, label, message, linkType }} o lanza si la accion no existe.
 */
export async function buildFollowupMessage(actionKey = '', lead = {}, context = {}) {
  const action = ACTION_MAP.get(String(actionKey || '').trim());
  if (!action) {
    const error = new Error(`Accion de seguimiento desconocida: ${actionKey}`);
    error.code = 'UNKNOWN_ACTION';
    throw error;
  }

  const nombre = firstName(lead?.nombre || '');
  let linkType = null;
  let link = '';
  let variants = action.variants || [];

  // Referencia de pago por Stripe: crea una sesion con customer_balance + mx_bank_transfer.
  if (action.requiresLink === 'bank') {
    if (process.env.STRIPE_PAYMENT_REFERENCE_DISABLED !== 'true') {
      const reference = await createStripePaymentReference(lead, context);
      return {
        actionKey: action.key,
        label: action.label,
        linkType: 'stripe_bank_transfer',
        message: buildStripePaymentMessage(lead, reference).slice(0, 900),
      };
    }

    const details = getBankDetails();
    if (!details || process.env.ALLOW_MANUAL_PAYMENT_DETAILS_FALLBACK !== 'true') {
      const error = new Error('No se pudo generar una referencia de Stripe. Revisa STRIPE_SECRET_KEY y habilita transferencias bancarias en Stripe.');
      error.code = 'NO_LINK_AVAILABLE';
      throw error;
    }

    return {
      actionKey: action.key,
      label: action.label,
      linkType: 'bank',
      message: buildBankPaymentMessage(lead, details).slice(0, 900),
    };
  }

  if (action.requiresLink === 'sample_or_form') {
    const slug = resolveSampleSlug(lead);
    if (slug) {
      link = buildSampleLink(lead);
      linkType = 'sample';
      variants = action.sampleVariants;
    } else {
      link = buildSampleFormLink(lead);
      linkType = 'form';
      variants = action.formVariants;
    }
  } else if (action.requiresLink === 'form') {
    link = buildSampleFormLink(lead);
    linkType = 'form';
  }

  // Si la accion necesita link y el lead no tiene telefono/slug, avisamos.
  if (action.requiresLink && !link) {
    const error = new Error('Este lead no tiene telefono valido ni muestra para generar el enlace.');
    error.code = 'NO_LINK_AVAILABLE';
    throw error;
  }

  const message = renderTemplate(pickRandom(variants), { nombre, link });
  return {
    actionKey: action.key,
    label: action.label,
    linkType,
    message: message.slice(0, 600),
  };
}
