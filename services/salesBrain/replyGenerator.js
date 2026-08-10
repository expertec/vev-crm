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

function fallbackReply({ action, lead = {}, analysis = {}, conversationMemory = {} } = {}) {
  const nombre = firstName(lead?.nombre || '');
  const prefix = nombre ? `${nombre}, ` : '';
  const salesContext = lead?.salesContext && typeof lead.salesContext === 'object' ? lead.salesContext : {};
  const businessType = analysis?.businessType || salesContext.businessType || conversationMemory?.facts?.businessType?.value || 'tu negocio';
  const primaryGoal = salesContext.primaryGoal || analysis?.primaryNeed || conversationMemory?.facts?.primaryNeed?.value || '';

  const templates = {
    ASK_BUSINESS_TYPE: `${prefix}para ubicar bien la estrategia, dime de que giro es tu negocio y que vendes principalmente.`,
    ASK_PRIMARY_GOAL: `${prefix}para recomendarte algo concreto: buscas mas clientes, mas confianza al presentarte o vender mas por WhatsApp?`,
    ASK_CURRENT_SITUATION: `${prefix}hoy de donde te llegan clientes: recomendaciones, redes, Google o anuncios? Con eso te digo donde conviene atacar primero.`,
    EXPLAIN_SERVICE: `${prefix}la idea es convertir tu presencia digital en una herramienta de venta: que la gente entienda rapido que haces, confie y te escriba por WhatsApp con menos friccion.`,
    PRESENT_OFFER: `${prefix}con lo que me dices, lo mas util seria enfocarlo a captar prospectos y generar confianza rapido. Te puedo manejar una opcion inicial y una mas completa; dime si quieres arrancar con algo ligero o con una presencia mas fuerte.`,
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

  const fallback = fallbackReply({ action: safeAction, lead, analysis, salesState, conversationMemory });
  const openai = await getOpenAi();
  if (!openai) {
    return { message: fallback, model: 'template', replyGenerationStatus: fallback ? 'fallback' : 'failed', replyPromptVersion: SALES_BRAIN_REPLY_PROMPT_VERSION };
  }

  const system = [
    'Eres closer consultivo de ventas por WhatsApp en Mexico para servicios digitales.',
    'NO decidas la estrategia. La accion ya esta decidida y no puedes cambiarla.',
    'Escribe con tono seguro, concreto y orientado a avance comercial.',
    'Evita frases complacientes o blandas como "que bueno", "me encanta", "espero tu respuesta", "para poder ayudarte mejor".',
    'Usa neuromarketing practico: claridad, confianza, beneficio tangible, reduccion de riesgo y siguiente paso simple.',
    'Maximo 1 o 2 parrafos cortos. Haz una sola pregunta de avance cuando aplique.',
    'No uses lenguaje tecnico ni bloques largos. No inventes links, precios, descuentos ni casos especificos.',
  ].join('\n');

  const user = JSON.stringify({
    action: safeAction,
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
      replyPromptVersion: SALES_BRAIN_REPLY_PROMPT_VERSION,
    };
  } catch (error) {
    console.warn('[SalesBrain] reply:error', error?.response?.data?.error?.message || error?.message || error);
    return { message: fallback, model: 'template', replyGenerationStatus: fallback ? 'fallback' : 'failed', replyPromptVersion: SALES_BRAIN_REPLY_PROMPT_VERSION };
  }
}
