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
    ASK_BUSINESS_TYPE: `${prefix}para orientarte mejor, que tipo de negocio tienes?`,
    ASK_PRIMARY_GOAL: `${prefix}que te gustaria mejorar mas en este momento: recibir mas clientes, vender mas o que mas personas conozcan tu negocio?`,
    ASK_CURRENT_SITUATION: `${prefix}hoy como estas consiguiendo clientes y que te gustaria mejorar?`,
    EXPLAIN_SERVICE: `${prefix}te explico rapido: la idea es darte una pagina clara para que tus clientes entiendan que ofreces, vean confianza y puedan contactarte facil por WhatsApp.`,
    PRESENT_OFFER: `${prefix}te puedo pasar las opciones. Para recomendarte bien, primero dime si buscas algo sencillo para presentarte o una pagina mas completa para captar clientes.`,
    SEND_EXAMPLES: `${prefix}claro. Te puedo mandar ejemplos similares para que veas el estilo y como quedaria aplicado a ${businessType}.`,
    SEND_RELEVANT_CASE: `${prefix}entiendo. Antes de hablarte de contratar, prefiero mostrarte un caso parecido para que veas como trabajamos y que puedes esperar.`,
    SEND_TESTIMONIAL: `${prefix}te comparto una referencia para que veas la experiencia de otros clientes antes de decidir.`,
    HANDLE_PRICE_OBJECTION: `${prefix}lo entiendo. Para cuidar tu presupuesto, podemos empezar con lo esencial y dejar listo lo que realmente te ayude a conseguir clientes.`,
    HANDLE_TRUST_OBJECTION: `${prefix}te entiendo. Si ya tuviste una mala experiencia, lo mejor es avanzar con claridad: primero te muestro ejemplos y te explico exactamente que se entrega.`,
    HANDLE_TIME_OBJECTION: `${prefix}sin problema. Para hacerlo facil, puedo resumirte las opciones y cuando tengas un momento retomamos con la que mejor te convenga.`,
    SEND_FORM: `${prefix}para prepararte una muestra aterrizada a tu negocio, te puedo pasar un formulario corto. Toma unos minutos y con eso la armamos mejor.`,
    SEND_PAYMENT_LINK: `${prefix}si ya quieres avanzar, te paso los datos de pago y dejamos iniciado tu proyecto.`,
    START_CLOSING: `${prefix}perfecto. ${primaryGoal === 'more_customers' ? 'Lo enfocamos en ayudarte a recibir mas clientes. ' : ''}Para arrancar, confirmame el nombre de tu negocio y te paso el siguiente paso para dejarlo iniciado.`,
    START_FOLLOWUP: `${prefix}queda pendiente. Te doy seguimiento por aqui para que no se nos pase.`,
    HANDOFF_HUMAN: `${prefix}prefiero revisarlo personalmente para darte una respuesta correcta. Te contacto por aqui en breve.`,
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
    'Eres redactor comercial para WhatsApp en Mexico.',
    'NO decidas la estrategia. La accion ya esta decidida y no puedes cambiarla.',
    'Escribe una respuesta breve, natural y conversacional.',
    'Maximo 1 a 3 parrafos cortos, una pregunta principal cuando aplique.',
    'No uses lenguaje tecnico ni bloques largos. No inventes links, precios ni casos especificos.',
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
