import {
  SALES_BRAIN_REPLY_PROMPT_VERSION,
  normalizeNextBestAction,
} from './catalog.js';
import { Configuration, OpenAIApi } from 'openai';

const AI_MODEL = String(process.env.SALES_BRAIN_REPLY_MODEL || process.env.SALES_BRAIN_AI_MODEL || 'gpt-4o-mini').trim() || 'gpt-4o-mini';
const AI_DISABLED = String(process.env.SALES_BRAIN_REPLY_AI || process.env.SALES_BRAIN_AI || '').trim().toLowerCase() === 'off';
const MAX_REPLY_CHARS = Math.max(220, Number(process.env.SALES_BRAIN_MAX_REPLY_CHARS || 650));
const PLAN_REDES_PRICE_LABEL = String(process.env.SALES_BRAIN_PLAN_REDES_PRICE_LABEL || process.env.PLAN_REDES_PRICE_LABEL || '').trim();
const PLAN_REDES_OFFER_LABEL = String(process.env.SALES_BRAIN_PLAN_REDES_OFFER_LABEL || process.env.PLAN_REDES_OFFER_LABEL || '').trim();

let cachedOpenAi = null;
let openAiUnavailable = false;

function cleanText(value = '', max = MAX_REPLY_CHARS) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function firstName(value = '') {
  const raw = cleanText(value, 120);
  return raw ? raw.split(' ')[0] : '';
}

function labelBusiness(value = '') {
  const raw = cleanText(value, 120);
  const map = {
    travel_agency: 'viajes',
    restaurant: 'tu restaurante',
    barbershop: 'tu barberia',
    clinic: 'tu consultorio',
    real_estate: 'tu inmobiliaria',
    store: 'tu tienda',
    service_business: 'tu negocio de servicios',
  };
  return map[raw] || raw || 'tu negocio';
}

function goalText(value = '') {
  const raw = cleanText(value, 120);
  const map = {
    more_customers: 'conseguir mas clientes',
    sell_more: 'vender mas',
    brand_awareness: 'darle mas presencia a la marca',
    look_professional: 'verse mas profesional',
    advertising: 'hacer que la publicidad funcione mejor',
    website: 'mejorar su presencia web',
  };
  return map[raw] || raw || 'atraer mas clientes';
}

function factValue(memory = {}, key = '') {
  const entry = memory?.facts?.[key];
  if (!entry || typeof entry !== 'object') return null;
  return entry.value ?? null;
}

function contextValue({
  qualification = {},
  salesContext = {},
  conversationMemory = {},
  key = '',
  qualifiedKey = '',
} = {}) {
  const qualified = qualifiedKey ? qualification?.[qualifiedKey]?.value : null;
  return qualified
    || salesContext?.[key]
    || factValue(conversationMemory, key)
    || null;
}

function compactList(value = '') {
  return cleanText(value, 260)
    .split(/[,;]|\sy\s/gi)
    .map((item) => cleanText(item, 80))
    .filter(Boolean)
    .slice(0, 5);
}

function personalizedIdeaText({
  prefix = '',
  businessLabel = 'tu negocio',
  goalLabel = 'atraer mas clientes',
  targetAudience = '',
  productsServices = '',
  painOrNeed = '',
} = {}) {
  const services = compactList(productsServices);
  const audience = cleanText(targetAudience, 160);
  const pain = cleanText(painOrNeed, 160);

  if (services.length >= 2) {
    const serviceText = services.length === 2
      ? services.join(' y ')
      : `${services.slice(0, -1).join(', ')} y ${services[services.length - 1]}`;
    const audienceText = audience ? ` para ${audience}` : '';
    return cleanText(`${prefix}con esos servicios yo no anunciaria ${businessLabel} de forma general. Probaria campañas centradas en necesidades concretas${audienceText}, por ejemplo ${serviceText}, y llevaria esas consultas directo a WhatsApp para resolver dudas o cotizar.`);
  }

  if (String(businessLabel).includes('viajes')) {
    return cleanText(`${prefix}en tu caso no enfocaria las redes solo en publicar destinos. Probaria campañas alrededor de viajes concretos u ofertas atractivas para generar consultas y llevar a las personas directamente a WhatsApp para cotizar.`);
  }

  if (services.length === 1) {
    const audienceText = audience ? ` para ${audience}` : '';
    return cleanText(`${prefix}yo no lo anunciaria como algo general. Probaria una campaña enfocada en ${services[0]}${audienceText}, con un mensaje muy directo y llevando las consultas a WhatsApp.`);
  }

  if (pain) {
    return cleanText(`${prefix}con lo que me cuentas, probaria una campaña alrededor de esa necesidad especifica: ${pain}. La idea seria que la persona entienda rapido el problema que resuelves y te escriba a WhatsApp para pedir informacion.`);
  }

  return cleanText(`${prefix}con lo que sabemos, probaria una campaña enfocada en ${goalLabel}, usando un mensaje concreto de ${businessLabel} y llevando las consultas a WhatsApp. Si me confirmas el servicio principal, la idea se puede aterrizar mas.`);
}

async function getOpenAi() {
  if (AI_DISABLED || openAiUnavailable) return null;
  if (cachedOpenAi) return cachedOpenAi;
  if (!process.env.OPENAI_API_KEY) {
    openAiUnavailable = true;
    return null;
  }
  try {
    cachedOpenAi = new OpenAIApi(new Configuration({ apiKey: process.env.OPENAI_API_KEY }));
    return cachedOpenAi;
  } catch (error) {
    console.warn('[SalesBrain] reply:openai_unavailable', error?.message || error);
    openAiUnavailable = true;
    return null;
  }
}

function fallbackReply({
  action,
  conversationObjective = '',
  productStrategy = 'unknown',
  lead = {},
  analysis = {},
  conversationMemory = {},
  qualification = {},
} = {}) {
  const nombre = firstName(lead?.nombre || '');
  const prefix = nombre ? `${nombre}, ` : '';
  const salesContext = lead?.salesContext && typeof lead.salesContext === 'object' ? lead.salesContext : {};
  const businessType = qualification?.business?.value || analysis?.businessType || salesContext.businessType || conversationMemory?.facts?.businessType?.value || 'tu negocio';
  const primaryGoal = qualification?.primaryGoal?.value || salesContext.primaryGoal || analysis?.primaryNeed || conversationMemory?.facts?.primaryNeed?.value || '';
  const targetAudience = contextValue({ qualification, salesContext, conversationMemory, key: 'targetAudience', qualifiedKey: 'targetAudience' });
  const productsServices = contextValue({ qualification, salesContext, conversationMemory, key: 'productsServices', qualifiedKey: 'productsServices' });
  const painOrNeed = qualification?.painOrNeed?.value || salesContext.painPoint || factValue(conversationMemory, 'painPoint') || '';
  const businessLabel = labelBusiness(businessType);
  const goalLabel = goalText(primaryGoal);
  const isRedes = productStrategy === 'redes_sociales';

  if (isRedes && conversationObjective === 'DISCOVER_GOAL') {
    return cleanText(`${prefix}perfecto. Buscas principalmente conseguir personas nuevas que te pidan informacion de ${businessLabel}, o darle mas movimiento y presencia a tus redes?`);
  }
  if (isRedes && conversationObjective === 'DEMONSTRATE_UNDERSTANDING') {
    return cleanText(`${prefix}entonces el enfoque no seria publicar por publicar. La idea seria usar tus redes para generar consultas de personas interesadas en ${businessLabel} y llevarlas a WhatsApp para pedir informacion o cotizacion.`);
  }
  if (isRedes && conversationObjective === 'DISCOVER_CURRENT_SITUATION') {
    return cleanText(`${prefix}hoy esos clientes te llegan mas por recomendacion, por tus redes actuales o ya estas pagando publicidad en Facebook/Instagram?`);
  }
  if (isRedes && (conversationObjective === 'DELIVER_PERSONALIZED_IDEA' || action === 'DELIVER_PERSONALIZED_IDEA' || conversationObjective === 'CREATE_PERSONALIZED_IDEA')) {
    return personalizedIdeaText({
      prefix,
      businessLabel,
      goalLabel,
      targetAudience,
      productsServices,
      painOrNeed,
    });
  }
  if (isRedes && conversationObjective === 'SHOW_RELEVANT_PROOF') {
    return cleanText(`${prefix}despues de esa idea, lo mejor seria que veas ejemplos o portafolio real antes de decidir. No te invento casos aqui; te puedo mostrar referencias disponibles para que aterrices como se veria aplicado a ${businessLabel}.`);
  }
  if (isRedes && conversationObjective === 'EXPLAIN_OFFER') {
    const offer = PLAN_REDES_OFFER_LABEL || 'PlanRedes';
    return cleanText(`${prefix}${offer} se debe entender como el plan para convertir tus redes en un canal de consultas, no solo en publicaciones. La idea es aterrizar mensaje, contenido y siguiente paso para que la gente te escriba por WhatsApp.`);
  }
  if (isRedes && (conversationObjective === 'EXPLAIN_METHOD' || action === 'EXPLAIN_OFFER')) {
    return cleanText(`${prefix}trabajamos primero entendiendo que vendes y que quieres lograr; despues armamos contenido y anuncios alrededor de una accion clara: que la persona te escriba, pregunte o cotice.`);
  }
  if (isRedes && (conversationObjective === 'PRESENT_PRICE' || action === 'PRESENT_PRICE')) {
    if (PLAN_REDES_PRICE_LABEL) {
      return cleanText(`${prefix}el PlanRedes lo estamos manejando en ${PLAN_REDES_PRICE_LABEL}. Si este enfoque te hace sentido, te explico como podemos empezar.`);
    }
    return cleanText(`${prefix}aqui ya conviene revisar el precio vigente del PlanRedes y explicartelo claro antes de avanzar. Si este enfoque te hace sentido, te paso la opcion actual para empezar.`);
  }
  if (isRedes && conversationObjective === 'PRESENT_OFFER') {
    return cleanText(`${prefix}con lo que me cuentas, lo logico seria empezar con un plan enfocado en ${goalLabel}, no solo en subir posts. Si quieres, te explico la opcion inicial para arrancarlo sin hacerlo pesado.`);
  }
  if (isRedes && conversationObjective === 'TEST_PURCHASE_INTENT') {
    return cleanText(`${prefix}si este enfoque te hace sentido, el siguiente paso seria aterrizarlo a tu negocio y ver si conviene arrancarlo este mes. Te gustaria revisarlo?`);
  }

  const templates = {
    ASK_BUSINESS_TYPE: `${prefix}claro. A que se dedica tu negocio o que vendes principalmente?`,
    ASK_PRIMARY_GOAL: `${prefix}buscas principalmente conseguir nuevos clientes, vender mas a los que ya te conocen o darle mas presencia a tu negocio?`,
    ASK_CURRENT_SITUATION: `${prefix}hoy de donde te llegan clientes: recomendaciones, redes, Google o anuncios? Con eso te digo donde conviene atacar primero.`,
    DEMONSTRATE_UNDERSTANDING: `${prefix}entonces el punto no es hacer publicidad por hacerla, sino usarla para ${goalLabel} de forma mas clara y medible.`,
    DELIVER_MICRO_VALUE: `${prefix}lo importante es que el primer mensaje no se vea generico: debe decir rapido que haces, para quien es y cual es el siguiente paso para contactarte.`,
    DELIVER_PERSONALIZED_IDEA: personalizedIdeaText({ prefix, businessLabel, goalLabel, targetAudience, productsServices, painOrNeed }),
    CREATE_PERSONALIZED_IDEA: `${prefix}yo empezaria con una idea simple para ${businessLabel}: enfocar el mensaje en ${goalLabel} y llevar las consultas a WhatsApp con una oferta o motivo claro para escribir.`,
    SHOW_RELEVANT_PROOF: `${prefix}te recomendaria ver ejemplos reales antes de decidir. Sin inventar casos: el siguiente paso seria mostrarte referencias disponibles y aterrizarlas a ${businessLabel}.`,
    EXPLAIN_SERVICE: `${prefix}la idea es convertir tu presencia digital en una herramienta de venta: que la gente entienda rapido que haces, confie y te escriba por WhatsApp con menos friccion.`,
    EXPLAIN_OFFER: `${prefix}${PLAN_REDES_OFFER_LABEL || 'PlanRedes'} se enfoca en que tus redes ayuden a generar consultas, no solo en publicar por publicar.`,
    EXPLAIN_METHOD: `${prefix}primero aterrizamos negocio, objetivo y situacion actual. Con eso definimos que mensaje conviene mostrar y cual debe ser el siguiente paso del cliente.`,
    PRESENT_PRICE: PLAN_REDES_PRICE_LABEL
      ? `${prefix}el PlanRedes lo estamos manejando en ${PLAN_REDES_PRICE_LABEL}. Si este enfoque te hace sentido, te explico como podemos empezar.`
      : `${prefix}aqui ya conviene revisar el precio vigente del PlanRedes y explicartelo claro antes de avanzar.`,
    PRESENT_OFFER: `${prefix}con lo que me dices, lo mas util seria enfocarlo a captar prospectos y generar confianza rapido. Te puedo manejar una opcion inicial y una mas completa; dime si quieres arrancar con algo ligero o con una presencia mas fuerte.`,
    TEST_PURCHASE_INTENT: `${prefix}si este enfoque te hace sentido, podemos aterrizarlo en una opcion concreta para tu negocio. Te gustaria avanzar a revisar eso?`,
    SEND_EXAMPLES: `${prefix}si buscas ejemplos, te mando referencias parecidas a ${businessType}. Fijate sobre todo en claridad, confianza y llamada a WhatsApp; eso es lo que hace que el cliente avance.`,
    SEND_RELEVANT_CASE: `${prefix}te muestro un caso parecido para que aterrices el resultado. La meta no es solo que se vea bonito: es que el prospecto entienda, confie y pregunte.`,
    SEND_TESTIMONIAL: `${prefix}te comparto una referencia para que veas como trabajamos y que tipo de resultado buscamos antes de que tomes una decision.`,
    HANDLE_PRICE_OBJECTION: `${prefix}si el presupuesto importa, conviene empezar por lo que mas impacto tiene: una presencia clara, prueba de confianza y contacto directo. Asi no pagas por adornos que no venden.`,
    HANDLE_TRUST_OBJECTION: `${prefix}si lo que buscas es confianza, lo correcto es mostrar proceso, entregables y ejemplos antes de avanzar. Asi sabes que vas a recibir y evitas sorpresas.`,
    HANDLE_TIME_OBJECTION: `${prefix}lo hacemos simple: te paso una opcion concreta y tu decides si lo retomamos hoy o lo dejamos agendado. No necesitas revisar mil cosas para avanzar.`,
    SEND_FORM: `${prefix}para aterrizarlo a tu negocio, llena este formulario corto. Con eso te preparo una muestra con mejor enfoque comercial, no algo generico.`,
    SEND_PAYMENT_LINK: `${prefix}si quieres avanzar, te paso los datos de pago y dejamos iniciado el proyecto hoy.`,
    START_CLOSING: `${prefix}${primaryGoal === 'more_customers' ? 'lo enfocamos a generar mas prospectos. ' : ''}Para arrancar bien, confirmame el nombre del negocio y el servicio principal que quieres impulsar; con eso te paso el siguiente paso.`,
    START_FOLLOWUP: `${prefix}lo dejo ubicado y te doy seguimiento con una propuesta concreta para que no quede en el aire.`,
    HANDOFF_HUMAN: `${prefix}esto conviene revisarlo directo para darte una respuesta precisa. Te atiendo por aqui y lo cerramos bien.`,
    WAIT: '',
  };

  return cleanText(templates[action] || '');
}

export async function generateReply({
  action = 'WAIT',
  conversationObjective = 'WAIT',
  productStrategy = 'unknown',
  qualification = {},
  selectedAsset = null,
  lead = {},
  analysis = {},
  salesState = {},
  conversationMemory = {},
  acquisitionContext = {},
} = {}) {
  const safeAction = normalizeNextBestAction(action);
  if (safeAction === 'WAIT') {
    return { message: '', model: 'none', replyGenerationStatus: 'empty', replyPromptVersion: SALES_BRAIN_REPLY_PROMPT_VERSION };
  }

  const fallback = fallbackReply({
    action: safeAction,
    conversationObjective,
    productStrategy,
    lead,
    analysis,
    salesState,
    conversationMemory,
    qualification,
  });
  const templateOnlyObjectives = new Set([
    'DISCOVER_BUSINESS',
    'DISCOVER_GOAL',
    'DISCOVER_CURRENT_SITUATION',
    'DEMONSTRATE_UNDERSTANDING',
    'DELIVER_MICRO_VALUE',
    'CREATE_PERSONALIZED_IDEA',
    'DELIVER_PERSONALIZED_IDEA',
    'SHOW_RELEVANT_PROOF',
    'EXPLAIN_OFFER',
    'PRESENT_PRICE',
    'TEST_PURCHASE_INTENT',
  ]);
  if (productStrategy === 'redes_sociales' && templateOnlyObjectives.has(conversationObjective)) {
    return {
      message: fallback,
      model: 'template',
      replyGenerationStatus: fallback ? 'template' : 'failed',
      replyPromptVersion: SALES_BRAIN_REPLY_PROMPT_VERSION,
    };
  }

  const openai = await getOpenAi();
  if (!openai) {
    return { message: fallback, model: 'template', replyGenerationStatus: fallback ? 'fallback' : 'failed', replyPromptVersion: SALES_BRAIN_REPLY_PROMPT_VERSION };
  }

  const system = [
    'Eres closer consultivo de ventas por WhatsApp en Mexico para servicios digitales.',
    'NO decidas la estrategia. conversationObjective y action ya estan decididos y no puedes cambiarlos.',
    'Objetivo principal: precalificar y madurar al lead antes de pasarlo a ventas.',
    'Escribe natural, corto, conversacional y especifico al negocio.',
    'Una idea principal por mensaje. Normalmente una sola pregunta como maximo.',
    'No vendas demasiado pronto si el objetivo es descubrir informacion.',
    'Evita frases como "nuestro plan esta disenado para", "estrategias especificas", "interacciones efectivas", "para poder ayudarte mejor".',
    'Prefiere preguntas faciles de contestar.',
    'No inventes links, precios, descuentos, clientes, estadisticas, testimonios, resultados ni casos.',
    'Si selectedAsset es null, no prometas enviar ejemplos/casos concretos.',
  ].join('\n');

  const user = JSON.stringify({
    conversationObjective,
    action: safeAction,
    productStrategy,
    qualification,
    selectedAsset,
    leadName: firstName(lead?.nombre || ''),
    analysis,
    salesState,
    conversationMemory,
    acquisitionContext,
    salesContext: lead?.salesContext || {},
    manualContext: lead?.salesBrainManualContext || '',
    visualContext: lead?.salesBrainVisualContext || '',
    maxChars: MAX_REPLY_CHARS,
  });

  try {
    const response = await openai.createChatCompletion({
      model: AI_MODEL,
      temperature: 0.45,
      max_tokens: 220,
      messages: [
        { role: 'system', content: system },
        { role: 'user', content: user },
      ],
    });
    const message = cleanText(response?.data?.choices?.[0]?.message?.content || fallback);
    return {
      message: message || fallback,
      model: AI_MODEL,
      replyGenerationStatus: message ? 'ok' : (fallback ? 'fallback' : 'failed'),
      usage: response?.data?.usage || null,
      replyPromptVersion: SALES_BRAIN_REPLY_PROMPT_VERSION,
    };
  } catch (error) {
    console.warn('[SalesBrain] reply:error', error?.response?.data?.error?.message || error?.message || error);
    return { message: fallback, model: 'template', replyGenerationStatus: fallback ? 'fallback' : 'failed', replyPromptVersion: SALES_BRAIN_REPLY_PROMPT_VERSION };
  }
}
