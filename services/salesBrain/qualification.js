import { getFactValue } from './memory.js';
import {
  normalizeProductStrategy,
  normalizeQualificationStatus,
} from './catalog.js';
import { commercialProgressFromLead } from './commercialProgress.js';

function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function normalizeForMatch(value = '') {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();
}

function objectOr(value, fallback = {}) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : fallback;
}

function firstValue(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && value !== '') return value;
  }
  return null;
}

function hasSignal(analysis = {}, signal = '') {
  return Array.isArray(analysis?.signals) && analysis.signals.includes(signal);
}

function hasAnySignal(analysis = {}, signals = []) {
  return signals.some((signal) => hasSignal(analysis, signal));
}

function leadTagsText(lead = {}) {
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas : [];
  const sequences = Array.isArray(lead?.secuenciasActivas) ? lead.secuenciasActivas : [];
  return [
    ...tags,
    ...sequences.map((item) => item?.trigger || ''),
    lead?.lastMetaSequenceTrigger,
    lead?.sequenceTrigger,
    lead?.trigger,
    lead?.campaign,
    lead?.metaCampaignName,
    lead?.metaCampaignId,
    lead?.lastMetaAttribution?.trigger,
    lead?.lastMetaAttribution?.campaignName,
  ].filter(Boolean).join(' ');
}

export function resolveProductStrategy({ lead = {}, acquisitionContext = {} } = {}) {
  const raw = normalizeForMatch([
    leadTagsText(lead),
    acquisitionContext?.campaign,
    acquisitionContext?.ad,
    acquisitionContext?.source,
    lead?.salesContext?.interestedService,
    lead?.conversationMemory?.facts?.interestedService?.value,
  ].filter(Boolean).join(' '));

  if (/(planredes|redes sociales|redessociales|manejo de redes|meta ads|facebook ads|instagram)/.test(raw)) {
    return 'redes_sociales';
  }
  if (/(webpromo|leadweb|leadwhatsapp|paginaweb|pagina web|sitio web|nuevoleadweb|web)/.test(raw)) {
    return 'web';
  }
  return 'unknown';
}

function normalizeDelivered(previous = {}) {
  const safe = objectOr(previous);
  return {
    understanding: Boolean(safe.understanding || safe.solutionUnderstood),
    microValue: Boolean(safe.microValue),
    proof: Boolean(safe.proof || safe.proofDelivered),
    personalizedIdea: Boolean(safe.personalizedIdea || safe.personalizedValueDelivered),
    methodExplained: Boolean(safe.methodExplained || safe.solutionUnderstood),
    offer: Boolean(safe.offer || safe.offerPresented),
  };
}

function factOrContext({ salesContext = {}, memory = {}, key = '' } = {}) {
  return firstValue(salesContext[key], getFactValue(memory, key));
}

export function buildQualificationSnapshot({
  lead = {},
  analysis = {},
  salesState = {},
  conversationMemory = {},
  acquisitionContext = {},
} = {}) {
  const salesContext = objectOr(lead?.salesContext);
  const previousQualification = objectOr(salesState?.qualification);
  const commercialProgress = commercialProgressFromLead(lead, salesState);
  const delivered = {
    ...normalizeDelivered(previousQualification.delivered),
    understanding: commercialProgress.understandingDemonstrated,
    proof: commercialProgress.proofDelivered,
    personalizedIdea: commercialProgress.personalizedIdeaDelivered,
    methodExplained: commercialProgress.offerExplained,
    offer: commercialProgress.pricePresented,
  };
  const productStrategy = normalizeProductStrategy(
    previousQualification.productStrategy
      || salesState.productStrategy
      || resolveProductStrategy({ lead, acquisitionContext })
  );

  const business = firstValue(
    analysis?.businessType,
    salesState?.businessType,
    salesContext.businessType,
    getFactValue(conversationMemory, 'businessType'),
    lead?.giro,
    lead?.negocio
  );
  const primaryGoal = firstValue(
    analysis?.primaryNeed,
    salesState?.primaryNeed,
    salesContext.primaryGoal,
    getFactValue(conversationMemory, 'primaryGoal'),
    getFactValue(conversationMemory, 'primaryNeed')
  );
  const currentSituation = firstValue(
    salesContext.currentSituation,
    getFactValue(conversationMemory, 'currentSituation'),
    salesContext.customerAcquisition,
    getFactValue(conversationMemory, 'customerAcquisition'),
    salesContext.runsAds,
    getFactValue(conversationMemory, 'runsAds'),
    getFactValue(conversationMemory, 'currentlyAdvertising')
  );
  const targetAudience = factOrContext({ salesContext, memory: conversationMemory, key: 'targetAudience' });
  const productsServices = factOrContext({ salesContext, memory: conversationMemory, key: 'productsServices' });
  const mainOffer = factOrContext({ salesContext, memory: conversationMemory, key: 'mainOffer' });
  const painOrNeed = firstValue(
    salesContext.painPoint,
    getFactValue(conversationMemory, 'painPoint'),
    salesContext.previousExperience === 'bad_experience' || salesContext.previousExperience === 'no_results' ? salesContext.previousExperience : null,
    getFactValue(conversationMemory, 'previousBadExperience') ? 'bad_experience' : null
  );

  const commercialIntentDetected = analysis?.intent === 'wants_price'
    || analysis?.intent === 'wants_examples'
    || analysis?.intent === 'wants_information'
    || analysis?.intent === 'question'
    || hasAnySignal(analysis, ['asked_price', 'asked_examples', 'commercial_question', 'asked_payment_method']);
  const purchaseIntentDetected = analysis?.intent === 'ready_to_buy'
    || analysis?.intent === 'asks_how_to_start'
    || hasAnySignal(analysis, ['ready_to_buy', 'wants_to_buy', 'asks_how_to_start', 'asked_payment_method']);
  const priceIntentDetected = analysis?.intent === 'wants_price' || hasSignal(analysis, 'asked_price');

  const missingFacts = [];
  if (!business) missingFacts.push('business');
  if (!primaryGoal) missingFacts.push('primaryGoal');
  if (!currentSituation) missingFacts.push('currentSituation');

  const businessKnown = Boolean(business);
  const goalKnown = Boolean(primaryGoal);
  const currentSituationKnown = Boolean(currentSituation);
  const enoughContext = businessKnown && goalKnown;
  const personalizedIdeaSignals = [
    primaryGoal,
    targetAudience,
    productsServices,
    mainOffer,
    painOrNeed,
    currentSituation,
  ].filter((item) => item !== undefined && item !== null && item !== '').length;
  const hasContextForPersonalizedIdea = businessKnown && personalizedIdeaSignals >= 1;
  const valueDelivered = delivered.understanding
    || delivered.microValue
    || delivered.proof
    || delivered.personalizedIdea
    || delivered.methodExplained
    || delivered.offer;
  const readyForSales = enoughContext
    && (
      purchaseIntentDetected
      || (priceIntentDetected && valueDelivered)
      || (commercialIntentDetected && delivered.offer)
    );

  let status = 'discovering';
  if (readyForSales) status = 'ready_for_sales';
  else if (enoughContext && valueDelivered) status = 'qualified';
  else if (businessKnown || goalKnown || commercialIntentDetected) status = 'nurturing';

  return {
    productStrategy,
    business: { known: businessKnown, value: cleanText(business || '', 160) || null },
    primaryGoal: { known: goalKnown, value: cleanText(primaryGoal || '', 160) || null },
    currentSituation: { known: currentSituationKnown, value: currentSituation ?? null },
    targetAudience: { known: Boolean(targetAudience), value: targetAudience ?? null },
    productsServices: { known: Boolean(productsServices), value: productsServices ?? null },
    mainOffer: { known: Boolean(mainOffer), value: mainOffer ?? null },
    painOrNeed: { known: Boolean(painOrNeed), value: painOrNeed ?? null },
    delivered,
    commercialProgress,
    hasContextForPersonalizedIdea,
    commercialIntentDetected,
    purchaseIntentDetected,
    priceIntentDetected,
    readyForSales,
    humanRequired: readyForSales && purchaseIntentDetected,
    qualificationStatus: normalizeQualificationStatus(status),
    missingFacts,
    lastConversationObjective: previousQualification.lastConversationObjective || salesState.conversationObjective || null,
    lastAction: salesState.lastAction || null,
  };
}

export function deliveredPatchForObjective(conversationObjective = '') {
  const objective = cleanText(conversationObjective, 80);
  if (objective === 'DEMONSTRATE_UNDERSTANDING') return 'understanding';
  if (objective === 'DELIVER_MICRO_VALUE') return 'microValue';
  if (objective === 'SHOW_RELEVANT_PROOF') return 'proof';
  if (objective === 'DELIVER_PERSONALIZED_IDEA') return 'personalizedIdea';
  if (objective === 'CREATE_PERSONALIZED_IDEA') return 'personalizedIdea';
  if (objective === 'EXPLAIN_OFFER') return 'methodExplained';
  if (objective === 'EXPLAIN_METHOD') return 'methodExplained';
  if (objective === 'PRESENT_PRICE') return 'offer';
  if (objective === 'PRESENT_OFFER') return 'offer';
  return '';
}
