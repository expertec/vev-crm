export const SALES_BRAIN_AGENT_VERSION = 'sales-brain-mvp-v1';
export const SALES_BRAIN_ANALYSIS_VERSION = 'v1';
export const SALES_BRAIN_DECISION_VERSION = 'v1';
export const SALES_BRAIN_REPLY_PROMPT_VERSION = 'v1';

export const SALES_BRAIN_MODES = Object.freeze({
  OFF: 'off',
  COPILOT: 'copilot',
});

export const INTEREST_LEVELS = Object.freeze([
  'hot',
  'warm',
  'cold',
  'lost',
]);

export const INTENTS = Object.freeze([
  'wants_information',
  'wants_price',
  'wants_examples',
  'ready_to_buy',
  'asks_how_to_start',
  'needs_time',
  'not_now',
  'no_interest',
  'question',
  'other',
]);

export const SALES_STAGES = Object.freeze([
  'new',
  'discovery',
  'education',
  'evaluation',
  'closing',
  'won',
  'lost',
]);

export const AWARENESS_LEVELS = Object.freeze([
  'unaware',
  'problem_aware',
  'solution_aware',
  'product_aware',
  'most_aware',
  'unknown',
]);

export const OBJECTIONS = Object.freeze([
  'none',
  'price',
  'trust',
  'time',
  'bad_previous_experience',
  'needs_approval',
  'not_ready',
  'other',
]);

export const SENTIMENTS = Object.freeze([
  'positive',
  'neutral',
  'skeptical',
  'negative',
  'confused',
]);

export const NEXT_BEST_ACTIONS = Object.freeze([
  'ASK_BUSINESS_TYPE',
  'ASK_PRIMARY_GOAL',
  'ASK_CURRENT_SITUATION',
  'EXPLAIN_SERVICE',
  'PRESENT_OFFER',
  'SEND_EXAMPLES',
  'SEND_RELEVANT_CASE',
  'SEND_TESTIMONIAL',
  'HANDLE_PRICE_OBJECTION',
  'HANDLE_TRUST_OBJECTION',
  'HANDLE_TIME_OBJECTION',
  'SEND_FORM',
  'SEND_PAYMENT_LINK',
  'START_CLOSING',
  'START_FOLLOWUP',
  'HANDOFF_HUMAN',
  'WAIT',
]);

export const SIGNALS = Object.freeze([
  'meta_ad',
  'answered',
  'business_identified',
  'primary_need_identified',
  'asked_price',
  'asked_examples',
  'asks_how_to_start',
  'ready_to_buy',
  'wants_to_buy',
  'no_interest',
  'stop_requested',
  'previous_bad_agency_experience',
  'trust_objection',
  'price_objection',
  'time_objection',
  'form_needed',
  'sample_seen',
  'examples_sent',
  'automated_reply',
]);

export const FACT_KEYS = Object.freeze([
  'businessType',
  'city',
  'currentlyAdvertising',
  'previousAgency',
  'previousBadExperience',
  'interestedService',
  'sawPrice',
  'receivedExamples',
  'receivedSample',
  'primaryNeed',
]);

export function enumOr(value, allowed, fallback) {
  const safe = String(value || '').trim();
  return allowed.includes(safe) ? safe : fallback;
}

export function uniqueAllowed(values = [], allowed = []) {
  const allowedSet = new Set(allowed);
  return Array.from(new Set(
    (Array.isArray(values) ? values : [])
      .map((item) => String(item || '').trim())
      .filter((item) => allowedSet.has(item))
  ));
}

export function normalizeNextBestAction(value = '') {
  return enumOr(value, NEXT_BEST_ACTIONS, 'WAIT');
}

export function normalizeInputMessageId(value = '') {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 180)
    .replace(/[^\w.-]/g, '_');
}

export function eventIdForInputMessage(inputMessageId = '') {
  const safe = normalizeInputMessageId(inputMessageId);
  return safe ? `inbound_${safe}` : '';
}
