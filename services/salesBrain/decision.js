import { getFactValue } from './memory.js';
import { normalizeNextBestAction } from './catalog.js';

function cleanText(value = '') {
  return String(value || '').trim();
}

function lower(value = '') {
  return cleanText(value).toLowerCase();
}

function hasTag(lead = {}, tag = '') {
  const target = lower(tag);
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas.map(lower) : [];
  return tags.includes(target);
}

function isArchived(lead = {}) {
  return lead?.isArchived === true || lead?.archived === true || Boolean(lead?.archivedAt);
}

function hasReachableTarget(lead = {}) {
  const jid = cleanText(lead?.resolvedJid || lead?.jid || '');
  if (/@s\.whatsapp\.net$/i.test(jid)) return true;
  const phone = cleanText(lead?.telefono || '').replace(/\D/g, '');
  return phone.length >= 10;
}

export function buildSalesBrainGuardrails({ lead = {}, analysis = {} } = {}) {
  const reasons = [];
  const status = lower(lead?.estado || '');
  if (isArchived(lead)) reasons.push('archived');
  if (lead?.stopSequences === true || hasTag(lead, 'StopSequences') || hasTag(lead, 'DetenerSecuencia')) reasons.push('stop_sequences');
  if (status === 'compro' || status === 'cliente' || hasTag(lead, 'Compro')) reasons.push('bought');
  if (analysis?.intent === 'no_interest' || analysis?.signals?.includes?.('stop_requested')) reasons.push('stop_requested');
  if (analysis?.automated) reasons.push('automated_reply');
  if (!hasReachableTarget(lead)) reasons.push('invalid_destination');
  if (lead?.salesBrainHumanControl === true || lead?.humanControl === true) reasons.push('human_control');
  return {
    blocked: reasons.some((item) => !['automated_reply'].includes(item)),
    reasons,
  };
}

export function decideNextAction({
  lead = {},
  analysis = {},
  salesState = {},
  conversationMemory = {},
  guardrails = null,
} = {}) {
  const signals = new Set(Array.isArray(analysis?.signals) ? analysis.signals : []);
  const guards = guardrails || buildSalesBrainGuardrails({ lead, analysis });
  const salesContext = lead?.salesContext && typeof lead.salesContext === 'object' ? lead.salesContext : {};
  const businessType = analysis?.businessType || salesState?.businessType || salesContext.businessType || getFactValue(conversationMemory, 'businessType');
  const primaryNeed = analysis?.primaryNeed || salesState?.primaryNeed || salesContext.primaryGoal || getFactValue(conversationMemory, 'primaryNeed');
  const previousExperience = salesContext.previousExperience || getFactValue(conversationMemory, 'previousExperience');

  const result = (action, reason, extra = {}) => ({
    nextBestAction: normalizeNextBestAction(action),
    reason,
    humanRequired: Boolean(extra.humanRequired),
    automationPaused: Boolean(extra.automationPaused),
    guardrailReasons: guards.reasons || [],
  });

  // PRIORIDAD 1: seguridad / stop.
  if (guards.reasons.includes('bought')) return result('WAIT', 'El lead ya esta marcado como comprado.', { automationPaused: true });
  if (guards.reasons.includes('stop_sequences') || guards.reasons.includes('stop_requested')) {
    return result('WAIT', 'El lead pidio detener mensajes o tiene stop activo.', { automationPaused: true });
  }
  if (guards.reasons.includes('archived')) return result('WAIT', 'El lead esta archivado.', { automationPaused: true });
  if (guards.reasons.includes('invalid_destination')) return result('WAIT', 'No hay destino WhatsApp confiable.', { automationPaused: true });
  if (guards.reasons.includes('human_control')) return result('HANDOFF_HUMAN', 'La conversacion esta marcada bajo control humano.', { humanRequired: true, automationPaused: true });
  if (guards.reasons.includes('automated_reply')) {
    return result('HANDOFF_HUMAN', 'La respuesta parece automatica; conviene contactar por otro canal.', { humanRequired: true, automationPaused: true });
  }

  // PRIORIDAD 2: cierre.
  if (analysis?.intent === 'ready_to_buy' || analysis?.intent === 'asks_how_to_start' || signals.has('ready_to_buy') || signals.has('asks_how_to_start')) {
    return result('START_CLOSING', 'El lead muestra intencion de avanzar o empezar.', { humanRequired: true });
  }

  // PRIORIDAD 3: intencion explicita.
  if (analysis?.intent === 'wants_price' || signals.has('asked_price')) {
    return result('PRESENT_OFFER', 'El lead pidio precio o presupuesto.');
  }
  if (analysis?.intent === 'wants_examples' || signals.has('asked_examples')) {
    return result('SEND_EXAMPLES', 'El lead pidio ejemplos o muestra.');
  }

  // PRIORIDAD 4: objeciones.
  if (analysis?.objection === 'trust' || analysis?.objection === 'bad_previous_experience' || signals.has('trust_objection') || signals.has('previous_bad_agency_experience')) {
    return result('HANDLE_TRUST_OBJECTION', 'Hay objecion de confianza o mala experiencia previa.');
  }
  if (previousExperience === 'bad_experience' || previousExperience === 'no_results') {
    return result('HANDLE_TRUST_OBJECTION', 'Tuvo una mala experiencia previa o no vio resultados.');
  }
  if (analysis?.objection === 'price' || signals.has('price_objection')) {
    return result('HANDLE_PRICE_OBJECTION', 'Hay objecion de precio.');
  }
  if (analysis?.objection === 'time' || signals.has('time_objection')) {
    return result('HANDLE_TIME_OBJECTION', 'Hay objecion de tiempo.');
  }

  // PRIORIDAD 5: descubrimiento.
  if (!businessType) return result('ASK_BUSINESS_TYPE', 'Aun no conocemos el tipo de negocio.');
  if (!primaryNeed) return result('ASK_PRIMARY_GOAL', 'Aun no conocemos el objetivo principal.');

  // PRIORIDAD 6: educacion.
  if (analysis?.intent === 'wants_information' || analysis?.intent === 'question' || analysis?.salesStage === 'education') {
    return result('EXPLAIN_SERVICE', 'El lead necesita entender la solucion.');
  }

  // PRIORIDAD 7: esperar.
  return result('WAIT', 'No hay una accion comercial clara despues del analisis.');
}

export function buildNextSalesState({ previous = {}, analysis = {}, score = 0, decision = {} } = {}) {
  return {
    intent: analysis?.intent || previous?.intent || null,
    businessType: analysis?.businessType || previous?.businessType || null,
    primaryNeed: analysis?.primaryNeed || previous?.primaryNeed || null,
    salesStage: analysis?.salesStage || previous?.salesStage || 'new',
    awareness: analysis?.awareness || previous?.awareness || null,
    objection: analysis?.objection || previous?.objection || null,
    sentiment: analysis?.sentiment || previous?.sentiment || null,
    interestLevel: analysis?.interestLevel || previous?.interestLevel || null,
    leadScore: Number(score || 0),
    lastAction: previous?.lastAction || null,
    humanRequired: Boolean(decision?.humanRequired),
    automationPaused: Boolean(decision?.automationPaused),
    updatedAt: new Date(),
  };
}
