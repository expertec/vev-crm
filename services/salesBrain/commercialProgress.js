function objectOr(value, fallback = {}) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : fallback;
}

function cleanText(value = '', max = 240) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

export function normalizeCommercialProgress(value = {}) {
  const safe = objectOr(value);
  return {
    understandingDemonstrated: Boolean(safe.understandingDemonstrated),
    personalizedIdeaDelivered: Boolean(safe.personalizedIdeaDelivered),
    proofDelivered: Boolean(safe.proofDelivered),
    offerExplained: Boolean(safe.offerExplained),
    pricePresented: Boolean(safe.pricePresented),
    offerReaction: safe.offerReaction ?? null,
    updatedAt: safe.updatedAt || null,
  };
}

export function commercialProgressFromLead(lead = {}, salesState = {}) {
  const direct = normalizeCommercialProgress(lead?.commercialProgress || salesState?.commercialProgress);
  const delivered = objectOr(salesState?.qualification?.delivered);
  return {
    ...direct,
    understandingDemonstrated: direct.understandingDemonstrated || Boolean(delivered.understanding),
    personalizedIdeaDelivered: direct.personalizedIdeaDelivered || Boolean(delivered.personalizedIdea),
    proofDelivered: direct.proofDelivered || Boolean(delivered.proof),
    offerExplained: direct.offerExplained || Boolean(delivered.methodExplained),
    pricePresented: direct.pricePresented || Boolean(delivered.offer),
  };
}

export function progressFieldForDeliveredAction(action = '') {
  const safe = cleanText(action, 80);
  if (safe === 'DEMONSTRATE_UNDERSTANDING') return 'understandingDemonstrated';
  if (safe === 'DELIVER_PERSONALIZED_IDEA' || safe === 'CREATE_PERSONALIZED_IDEA') return 'personalizedIdeaDelivered';
  if (safe === 'SHOW_RELEVANT_PROOF' || safe === 'SEND_EXAMPLES' || safe === 'SEND_RELEVANT_CASE' || safe === 'SEND_TESTIMONIAL') return 'proofDelivered';
  if (safe === 'EXPLAIN_OFFER' || safe === 'EXPLAIN_METHOD') return 'offerExplained';
  if (safe === 'PRESENT_PRICE' || safe === 'PRESENT_OFFER') return 'pricePresented';
  return '';
}

export function qualificationDeliveredFieldForProgress(progressField = '') {
  const safe = cleanText(progressField, 80);
  if (safe === 'understandingDemonstrated') return 'understanding';
  if (safe === 'personalizedIdeaDelivered') return 'personalizedIdea';
  if (safe === 'proofDelivered') return 'proof';
  if (safe === 'offerExplained') return 'methodExplained';
  if (safe === 'pricePresented') return 'offer';
  return '';
}
