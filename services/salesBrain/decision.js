import { getFactValue } from './memory.js';
import {
  normalizeActionRisk,
  normalizeConversationObjective,
  normalizeNextBestAction,
} from './catalog.js';
import { assetRequiredForObjective, selectApprovedAsset } from './assets.js';
import { buildQualificationSnapshot } from './qualification.js';

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
  acquisitionContext = {},
} = {}) {
  const signals = new Set(Array.isArray(analysis?.signals) ? analysis.signals : []);
  const guards = guardrails || buildSalesBrainGuardrails({ lead, analysis });
  const salesContext = lead?.salesContext && typeof lead.salesContext === 'object' ? lead.salesContext : {};
  const businessType = analysis?.businessType || salesState?.businessType || salesContext.businessType || getFactValue(conversationMemory, 'businessType') || lead?.giro || lead?.negocio;
  const primaryNeed = analysis?.primaryNeed || salesState?.primaryNeed || salesContext.primaryGoal || getFactValue(conversationMemory, 'primaryGoal') || getFactValue(conversationMemory, 'primaryNeed');
  const currentSituation = salesContext.currentSituation
    || getFactValue(conversationMemory, 'currentSituation')
    || salesContext.customerAcquisition
    || getFactValue(conversationMemory, 'customerAcquisition')
    || salesContext.runsAds
    || getFactValue(conversationMemory, 'runsAds')
    || getFactValue(conversationMemory, 'currentlyAdvertising');
  const previousExperience = salesContext.previousExperience || getFactValue(conversationMemory, 'previousExperience');
  const qualification = buildQualificationSnapshot({
    lead,
    analysis,
    salesState,
    conversationMemory,
    acquisitionContext,
  });

  const result = (objective, action, reason, extra = {}) => {
    const conversationObjective = normalizeConversationObjective(objective);
    const nextBestAction = normalizeNextBestAction(action);
    const selectedAsset = extra.selectedAsset !== undefined
      ? extra.selectedAsset
      : selectApprovedAsset({
        productStrategy: qualification.productStrategy,
        conversationObjective,
        businessType: qualification.business.value || businessType || '',
      });
    return {
      conversationObjective,
      nextBestAction,
      reason,
      productStrategy: qualification.productStrategy,
      qualification,
      readyForSales: Boolean(extra.readyForSales ?? qualification.readyForSales),
      humanRequired: Boolean(extra.humanRequired ?? qualification.humanRequired),
      automationPaused: Boolean(extra.automationPaused),
      actionRisk: normalizeActionRisk(extra.actionRisk || (extra.humanRequired ? 'handoff' : 'safe_automation')),
      selectedAsset,
      assetRequired: assetRequiredForObjective(conversationObjective),
      guardrailReasons: guards.reasons || [],
    };
  };

  // PRIORIDAD 1: seguridad / stop.
  if (guards.reasons.includes('bought')) return result('WAIT', 'WAIT', 'El lead ya esta marcado como comprado.', { automationPaused: true });
  if (guards.reasons.includes('stop_sequences') || guards.reasons.includes('stop_requested')) {
    return result('WAIT', 'WAIT', 'El lead pidio detener mensajes o tiene stop activo.', { automationPaused: true });
  }
  if (guards.reasons.includes('archived')) return result('WAIT', 'WAIT', 'El lead esta archivado.', { automationPaused: true });
  if (guards.reasons.includes('invalid_destination')) return result('WAIT', 'WAIT', 'No hay destino WhatsApp confiable.', { automationPaused: true });
  if (guards.reasons.includes('human_control')) return result('HANDOFF_SALES', 'HANDOFF_HUMAN', 'La conversacion esta marcada bajo control humano.', { humanRequired: true, automationPaused: true, actionRisk: 'handoff' });
  if (guards.reasons.includes('automated_reply')) {
    return result('HANDOFF_SALES', 'HANDOFF_HUMAN', 'La respuesta parece automatica; conviene contactar por otro canal.', { humanRequired: true, automationPaused: true, actionRisk: 'handoff' });
  }

  // PRIORIDAD 2: cierre.
  if (analysis?.intent === 'ready_to_buy' || analysis?.intent === 'asks_how_to_start' || signals.has('ready_to_buy') || signals.has('asks_how_to_start') || signals.has('asked_payment_method')) {
    return result('QUALIFY_FOR_SALES', 'START_CLOSING', 'El lead muestra intencion fuerte de avanzar o pagar.', {
      humanRequired: true,
      readyForSales: true,
      actionRisk: 'handoff',
    });
  }

  // PRIORIDAD 3: intencion explicita.
  if ((analysis?.intent === 'wants_price' || signals.has('asked_price')) && businessType && primaryNeed) {
    return result('PRESENT_OFFER', 'PRESENT_OFFER', 'El lead pidio precio y ya tenemos negocio y objetivo.', {
      actionRisk: qualification.readyForSales ? 'handoff' : 'restricted',
    });
  }
  if (analysis?.intent === 'wants_examples' || signals.has('asked_examples')) {
    const selectedAsset = selectApprovedAsset({
      productStrategy: qualification.productStrategy,
      conversationObjective: 'SHOW_RELEVANT_PROOF',
      businessType: qualification.business.value || businessType || '',
    });
    if (!selectedAsset) {
      return result('DELIVER_MICRO_VALUE', 'DELIVER_MICRO_VALUE', 'El lead pidio ejemplos, pero no hay asset aprobado; se entrega valor sin inventar casos.', {
        selectedAsset: null,
      });
    }
    return result('SHOW_RELEVANT_PROOF', 'SEND_EXAMPLES', 'El lead pidio ejemplos o muestra.', { selectedAsset });
  }

  // PRIORIDAD 4: objeciones.
  if (analysis?.objection === 'trust' || analysis?.objection === 'bad_previous_experience' || signals.has('trust_objection') || signals.has('previous_bad_agency_experience')) {
    return result('HANDLE_OBJECTION', 'HANDLE_TRUST_OBJECTION', 'Hay objecion de confianza o mala experiencia previa.', { actionRisk: 'restricted' });
  }
  if (previousExperience === 'bad_experience' || previousExperience === 'no_results') {
    return result('HANDLE_OBJECTION', 'HANDLE_TRUST_OBJECTION', 'Tuvo una mala experiencia previa o no vio resultados.', { actionRisk: 'restricted' });
  }
  if (analysis?.objection === 'price' || signals.has('price_objection')) {
    return result('HANDLE_OBJECTION', 'HANDLE_PRICE_OBJECTION', 'Hay objecion de precio.', { actionRisk: 'restricted' });
  }
  if (analysis?.objection === 'time' || signals.has('time_objection')) {
    return result('HANDLE_OBJECTION', 'HANDLE_TIME_OBJECTION', 'Hay objecion de tiempo.');
  }

  // PRIORIDAD 5: descubrimiento.
  if (!businessType) return result('DISCOVER_BUSINESS', 'ASK_BUSINESS_TYPE', 'Aun no conocemos el tipo de negocio.');
  if (!primaryNeed) return result('DISCOVER_GOAL', 'ASK_PRIMARY_GOAL', 'Aun no conocemos el objetivo principal.');

  // PRIORIDAD 6: balance descubrimiento / valor.
  if (!qualification.delivered.understanding) {
    return result('DEMONSTRATE_UNDERSTANDING', 'DEMONSTRATE_UNDERSTANDING', 'Ya conocemos negocio y objetivo; toca demostrar entendimiento antes de preguntar mas.');
  }
  if (!currentSituation) {
    return result('DISCOVER_CURRENT_SITUATION', 'ASK_CURRENT_SITUATION', 'Falta entender como consigue clientes o que esta intentando hoy.');
  }
  if (qualification.productStrategy === 'redes_sociales' && !qualification.delivered.personalizedIdea) {
    return result('CREATE_PERSONALIZED_IDEA', 'CREATE_PERSONALIZED_IDEA', 'Ya hay contexto suficiente para dar una idea personalizada de bajo riesgo.');
  }

  // PRIORIDAD 7: educacion.
  if (analysis?.intent === 'wants_information' || analysis?.intent === 'question' || analysis?.salesStage === 'education') {
    return result('EXPLAIN_METHOD', 'EXPLAIN_METHOD', 'El lead necesita entender como trabajamos.', { actionRisk: 'restricted' });
  }
  if (!qualification.delivered.methodExplained) {
    return result('EXPLAIN_METHOD', 'EXPLAIN_METHOD', 'Conviene explicar el metodo antes de presentar oferta.', { actionRisk: 'restricted' });
  }
  if (!qualification.delivered.offer) {
    return result('PRESENT_OFFER', 'PRESENT_OFFER', 'El lead ya esta maduro para conocer la oferta.', { actionRisk: 'restricted' });
  }
  if (!qualification.readyForSales) {
    return result('TEST_PURCHASE_INTENT', 'TEST_PURCHASE_INTENT', 'Ya recibio contexto suficiente; toca validar si quiere avanzar.');
  }

  // PRIORIDAD 8: esperar.
  return result('WAIT', 'WAIT', 'No hay una accion comercial clara despues del analisis.');
}

export function buildNextSalesState({ previous = {}, analysis = {}, score = 0, decision = {} } = {}) {
  const qualification = decision?.qualification && typeof decision.qualification === 'object'
    ? {
      ...decision.qualification,
      qualificationStatus: decision.readyForSales ? 'ready_for_sales' : decision.qualification.qualificationStatus,
      readyForSales: Boolean(decision.readyForSales),
      humanRequired: Boolean(decision.humanRequired),
      lastConversationObjective: decision.conversationObjective || decision.qualification.lastConversationObjective || null,
      lastAction: decision.nextBestAction || previous?.lastAction || null,
    }
    : previous?.qualification || null;

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
    productStrategy: decision?.productStrategy || previous?.productStrategy || qualification?.productStrategy || null,
    conversationObjective: decision?.conversationObjective || previous?.conversationObjective || null,
    lastAction: decision?.nextBestAction || previous?.lastAction || null,
    qualification,
    readyForSales: Boolean(decision?.readyForSales),
    humanRequired: Boolean(decision?.humanRequired),
    automationPaused: Boolean(decision?.automationPaused),
    updatedAt: new Date(),
  };
}
