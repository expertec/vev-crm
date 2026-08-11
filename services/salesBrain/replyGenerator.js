import {
  SALES_BRAIN_REPLY_PROMPT_VERSION,
  normalizeNextBestAction,
} from './catalog.js';
import { Configuration, OpenAIApi } from 'openai';

const AI_MODEL = String(process.env.SALES_BRAIN_REPLY_MODEL || process.env.SALES_BRAIN_AI_MODEL || 'gpt-4o-mini').trim() || 'gpt-4o-mini';
const AI_DISABLED = String(process.env.SALES_BRAIN_REPLY_AI || process.env.SALES_BRAIN_AI || '').trim().toLowerCase() === 'off';
const MAX_REPLY_CHARS = Math.max(220, Number(process.env.SALES_BRAIN_MAX_REPLY_CHARS || 650));

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
  if (isRedes && conversationObjective === 'CREATE_PERSONALIZED_IDEA') {
    return cleanText(`${prefix}yo probaria primero una campaña simple enfocada en ${goalLabel}: una oferta o tema concreto de ${businessLabel}, publicaciones que expliquen facil el beneficio y mensajes directos a WhatsApp para cotizar.`);
  }
  if (isRedes && conversationObjective === 'EXPLAIN_METHOD') {
    return cleanText(`${prefix}trabajamos primero entendiendo que vendes y que quieres lograr; despues armamos contenido y anuncios alrededor de una accion clara: que la persona te escriba, pregunte o cotice.`);
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
    CREATE_PERSONALIZED_IDEA: `${prefix}yo empezaria con una idea simple para ${businessLabel}: enfocar el mensaje en ${goalLabel} y llevar las consultas a WhatsApp con una oferta o motivo claro para escribir.`,
    EXPLAIN_SERVICE: `${prefix}la idea es convertir tu presencia digital en una herramienta de venta: que la gente entienda rapido que haces, confie y te escriba por WhatsApp con menos friccion.`,
    EXPLAIN_METHOD: `${prefix}primero aterrizamos negocio, objetivo y situacion actual. Con eso definimos que mensaje conviene mostrar y cual debe ser el siguiente paso del cliente.`,
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
