import {
  SALES_BRAIN_AGENT_VERSION,
  SALES_BRAIN_ANALYSIS_VERSION,
  SALES_BRAIN_DECISION_VERSION,
  SALES_BRAIN_REPLY_PROMPT_VERSION,
} from './catalog.js';

function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

export function buildSalesBrainEventPayload({
  type = 'inbound_analysis',
  inputMessageId = '',
  analysis = {},
  previousSalesState = null,
  newSalesState = null,
  previousConversationMemory = null,
  newConversationMemory = null,
  scoreBreakdown = {},
  leadScore = 0,
  nextBestAction = 'WAIT',
  reason = '',
  suggestedReply = '',
  replyGenerationStatus = '',
  model = '',
  replyModel = '',
  createdAt = new Date(),
} = {}) {
  const safeReplyStatus = cleanText(replyGenerationStatus || (suggestedReply ? 'ok' : 'empty'), 40);
  return {
    type,
    inputMessageId: cleanText(inputMessageId, 180),
    analysis,
    previousSalesState,
    newSalesState,
    previousConversationMemory,
    newConversationMemory,
    scoreBreakdown,
    leadScore: Number(leadScore || 0),
    nextBestAction,
    reason: cleanText(reason, 500),
    suggestedReply: cleanText(suggestedReply, 1200),
    replyGenerationStatus: safeReplyStatus,
    sellerDecision: null,
    finalReply: '',
    status: safeReplyStatus === 'failed' ? 'failed' : (suggestedReply ? 'pending' : 'no_action'),
    model: cleanText(model || analysis?.model || '', 120),
    replyModel: cleanText(replyModel, 120),
    agentVersion: SALES_BRAIN_AGENT_VERSION,
    analysisVersion: SALES_BRAIN_ANALYSIS_VERSION,
    decisionVersion: SALES_BRAIN_DECISION_VERSION,
    replyPromptVersion: SALES_BRAIN_REPLY_PROMPT_VERSION,
    createdAt,
    updatedAt: createdAt,
  };
}
