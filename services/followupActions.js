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
      'Hola {{nombre}}, te recuerdo que tu prueba gratis sigue disponible. Es la forma mas facil de que veas como te funcionaria sin compromiso. La activamos?',
      '{{nombre}}, no quiero que pierdas tu prueba gratis. En unos minutos te la dejo lista para que la veas funcionando. Te late?',
      'Hola {{nombre}}, sigues a tiempo de aprovechar tu prueba sin costo. Asi pruebas antes de decidir. Quieres que te la prepare?',
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
      'Hola {{nombre}}, este mes tengo una condicion especial para dejar tu proyecto listo. Si lo retomamos esta semana lo aprovechas. Te paso los detalles?',
      '{{nombre}}, por iniciar ahora te puedo dar un mejor precio. Te interesa que te mande la propuesta?',
      'Hola {{nombre}}, tengo una promo activa para arrancar este mes y me acorde de ti. Quieres que te cuente como queda?',
    ],
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
  }));
}

/**
 * Construye el mensaje de una accion para un lead.
 * @returns {{ actionKey, label, message, linkType }} o lanza si la accion no existe.
 */
export function buildFollowupMessage(actionKey = '', lead = {}) {
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
