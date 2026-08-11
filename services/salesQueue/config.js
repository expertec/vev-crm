export const QUEUE_STATUSES = Object.freeze({
  AUTOMATION: 'automation',
  WAITING: 'waiting',
  CLAIMED: 'claimed',
  FOLLOWUP: 'followup',
  DORMANT: 'dormant',
  CLOSED: 'closed',
});

export const ROUTING_STATUSES = Object.freeze({
  AUTOMATION: 'automation',
  READY_FOR_AGENT: 'ready_for_agent',
  FOLLOWUP: 'followup',
  DORMANT: 'dormant',
  CLOSED: 'closed',
});

export const OUTCOME_TYPES = Object.freeze([
  'interested',
  'followup',
  'sale',
  'not_interested',
  'no_response',
]);

export const QUEUE_PRIORITY_WEIGHTS = Object.freeze({
  salesScoreMultiplier: 0.75,
  unansweredMessage: 18,
  waitingFirstHour: 10,
  waitingAdditionalHour: 4,
  waitingMax: 26,
  overdueFollowUp: 24,
  buyingIntent: 18,
  paymentQuestion: 22,
  priceQuestion: 14,
  commercialQuestion: 10,
  urgency: 12,
  engagement: 8,
  objectionPenalty: 8,
  inactivityPenaltyPerDay: 3,
  inactivityPenaltyMax: 24,
  claimedPenalty: 12,
});

export const ROUTING_REASONS = Object.freeze({
  READY_FOR_SALES: 'ready_for_sales',
  READY_TO_BUY: 'ready_to_buy',
  ASKED_PAYMENT_METHOD: 'asked_payment_method',
  ASKED_PRICE: 'asked_price',
  COMMERCIAL_QUESTION: 'commercial_question',
  HIGH_INTEREST: 'high_interest',
  FOLLOWUP_OVERDUE: 'followup_overdue',
  LOW_INTEREST: 'low_interest',
  NO_SIGNAL: 'no_signal',
  CLOSED: 'closed',
  DORMANT: 'dormant',
});
