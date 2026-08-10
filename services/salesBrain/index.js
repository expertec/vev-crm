import { admin, db as defaultDb } from '../../firebaseAdmin.js';
import { analyzeConversation } from './analyzeConversation.js';
import { mergeConversationMemory } from './memory.js';
import { calculateLeadScore } from './leadScore.js';
import {
  buildNextSalesState,
  buildSalesBrainGuardrails,
  decideNextAction,
} from './decision.js';
import { generateReply } from './replyGenerator.js';
import {
  buildSalesBrainEventPayload,
  eventIdForInputMessage,
  expireCurrentSuggestion,
  findSalesBrainEventByInput,
} from './events.js';
import { SALES_BRAIN_MODES } from './catalog.js';
import { buildSalesContextPatch } from './salesContext.js';
import {
  buildCommercialSignals,
  calculateQueuePriority,
} from '../salesQueue/priority.js';
import {
  decideRouting,
} from '../salesQueue/routing.js';

const { FieldValue } = admin.firestore;

const LOCK_TTL_MS = Math.max(30_000, Number(process.env.SALES_BRAIN_LOCK_TTL_MS || 90_000));
const LOCK_WAIT_MS = Math.max(0, Number(process.env.SALES_BRAIN_LOCK_WAIT_MS || 12_000));
const LOCK_POLL_MS = Math.max(100, Number(process.env.SALES_BRAIN_LOCK_POLL_MS || 350));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function toMillis(value) {
  if (!value) return 0;
  if (value instanceof Date) return value.getTime() || 0;
  if (typeof value?.toMillis === 'function') return value.toMillis() || 0;
  if (typeof value?.toDate === 'function') return value.toDate()?.getTime?.() || 0;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

export function resolveSalesBrainMode(lead = {}) {
  const mode = cleanText(lead?.salesBrainMode || SALES_BRAIN_MODES.OFF, 40).toLowerCase();
  return mode === SALES_BRAIN_MODES.COPILOT ? SALES_BRAIN_MODES.COPILOT : SALES_BRAIN_MODES.OFF;
}

export function buildAcquisitionContext(lead = {}) {
  return {
    campaign: cleanText(lead?.metaCampaignName || lead?.metaCampaignId || lead?.campaign || '', 180),
    adset: cleanText(lead?.metaAdSetId || lead?.lastMetaAttribution?.adSetId || '', 180),
    ad: cleanText(lead?.metaAdId || lead?.metaSourceId || lead?.lastMetaAttribution?.adName || '', 180),
    ctwaClid: cleanText(lead?.metaCtwaClid || lead?.lastMetaAttribution?.ctwaClid || '', 180),
    source: cleanText(lead?.source || lead?.lastMetaAttribution?.source || '', 100),
  };
}

async function acquireLeadLock({ db, leadRef, leadId }) {
  const ref = leadRef || db.collection('leads').doc(String(leadId));
  const started = Date.now();

  while (true) {
    const nowMs = Date.now();
    const lockedUntil = new Date(nowMs + LOCK_TTL_MS);
    const acquired = await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists) return false;
      const data = snap.data() || {};
      const currentUntil = toMillis(data?.salesBrainProcessing?.lockedUntil);
      if (currentUntil && currentUntil > nowMs) return false;
      tx.set(ref, {
        salesBrainProcessing: {
          lockedAt: new Date(nowMs),
          lockedUntil,
        },
      }, { merge: true });
      return true;
    });

    if (acquired) return { acquired: true, ref };
    if ((Date.now() - started) >= LOCK_WAIT_MS) {
      console.warn('[SalesBrain] guardrail:lock_active');
      return { acquired: false, ref };
    }
    await sleep(LOCK_POLL_MS);
  }
}

async function releaseLeadLock(leadRef) {
  await leadRef.set(
    {
      salesBrainProcessing: FieldValue.delete(),
    },
    { merge: true }
  ).catch(() => {});
}

function serializeForEvent(value) {
  if (!value || typeof value !== 'object') return value || null;
  return JSON.parse(JSON.stringify(value, (_key, item) => {
    if (item instanceof Date) return item.toISOString();
    if (typeof item?.toDate === 'function') return item.toDate().toISOString();
    return item;
  }));
}

export async function runSalesBrainForInbound({
  db = defaultDb,
  leadRef = null,
  leadId = '',
  leadData = {},
  latestText = '',
  inputMessageId = '',
  recentMessages = [],
  analysis = null,
} = {}) {
  const safeLeadId = cleanText(leadId, 220);
  const safeInputMessageId = cleanText(inputMessageId, 180);
  if (!safeLeadId || !safeInputMessageId || !cleanText(latestText, 1000)) {
    return { ok: false, skipped: true, reason: 'missing_input' };
  }

  const initialLead = leadData && typeof leadData === 'object' ? leadData : {};
  if (resolveSalesBrainMode(initialLead) !== SALES_BRAIN_MODES.COPILOT) {
    return { ok: true, skipped: true, reason: 'mode_off' };
  }

  const targetRef = leadRef || db.collection('leads').doc(safeLeadId);
  const eventId = eventIdForInputMessage(safeInputMessageId);
  if (!eventId) return { ok: false, skipped: true, reason: 'invalid_input_message_id' };

  const lock = await acquireLeadLock({ db, leadRef: targetRef, leadId: safeLeadId });
  if (!lock.acquired) return { ok: false, skipped: true, reason: 'lock_active' };

  try {
    const existing = await findSalesBrainEventByInput({ leadRef: targetRef, inputMessageId: safeInputMessageId });
    if (existing) {
      console.log('[SalesBrain] idempotency:reuse');
      return { ok: true, reused: true, eventId: existing.id, event: existing.data };
    }

    await expireCurrentSuggestion({
      db,
      leadRef: targetRef,
      reason: 'new_inbound_message',
      exceptEventId: eventId,
    });

    const leadSnap = await targetRef.get();
    if (!leadSnap.exists) return { ok: false, skipped: true, reason: 'missing_lead' };
    const currentLead = { id: leadSnap.id, ...(leadSnap.data() || {}) };
    if (resolveSalesBrainMode(currentLead) !== SALES_BRAIN_MODES.COPILOT) {
      return { ok: true, skipped: true, reason: 'mode_off' };
    }

    const acquisitionContext = buildAcquisitionContext(currentLead);
    const finalAnalysis = analysis || await analyzeConversation({
      lead: currentLead,
      recentMessages,
      latestText,
      acquisitionContext,
    });

    const previousSalesState = currentLead.salesState && typeof currentLead.salesState === 'object'
      ? currentLead.salesState
      : {};
    const previousConversationMemory = currentLead.conversationMemory && typeof currentLead.conversationMemory === 'object'
      ? currentLead.conversationMemory
      : {};

    const nextMemory = mergeConversationMemory(previousConversationMemory, finalAnalysis, {
      inputMessageId: safeInputMessageId,
      now: new Date(),
    });
    const score = calculateLeadScore({ lead: currentLead, analysis: finalAnalysis, memory: nextMemory });
    const guardrails = buildSalesBrainGuardrails({ lead: currentLead, analysis: finalAnalysis });
    const decision = decideNextAction({
      lead: currentLead,
      analysis: finalAnalysis,
      salesState: previousSalesState,
      conversationMemory: nextMemory,
      guardrails,
    });
    console.log(`[SalesBrain] decision:${decision.nextBestAction}`);

    const nextSalesState = buildNextSalesState({
      previous: previousSalesState,
      analysis: finalAnalysis,
      score: score.total,
      decision,
    });
    const commercialSignals = buildCommercialSignals({
      lead: { ...currentLead, salesState: nextSalesState },
      analysis: finalAnalysis,
      latestText,
    });
    const routing = decideRouting({
      lead: { ...currentLead, salesState: nextSalesState },
      analysis: finalAnalysis,
      latestText,
      commercialSignals,
    });
    const queuePriority = calculateQueuePriority({
      lead: { ...currentLead, salesState: nextSalesState },
      analysis: finalAnalysis,
      latestText,
      commercialSignals,
    });
    const pendingQuestion = currentLead.sequenceQuestionPending && typeof currentLead.sequenceQuestionPending === 'object'
      ? currentLead.sequenceQuestionPending
      : null;
    const salesContextPatch = buildSalesContextPatch({
      previousSalesContext: currentLead.salesContext,
      previousSalesContextRaw: currentLead.salesContextRaw,
      previousConfidence: currentLead.salesContextConfidence,
      analysis: finalAnalysis,
      memory: nextMemory,
      latestText,
      saveTo: pendingQuestion?.saveTo || '',
    });

    let reply;
    try {
      reply = await generateReply({
        action: decision.nextBestAction,
        lead: currentLead,
        analysis: finalAnalysis,
        salesState: nextSalesState,
        conversationMemory: nextMemory,
        acquisitionContext,
      });
    } catch (replyError) {
      console.warn('[SalesBrain] reply:fatal', replyError?.message || replyError);
      reply = {
        message: '',
        model: 'none',
        replyGenerationStatus: 'failed',
      };
    }

    const eventPayload = buildSalesBrainEventPayload({
      inputMessageId: safeInputMessageId,
      analysis: serializeForEvent(finalAnalysis),
      previousSalesState: serializeForEvent(previousSalesState),
      newSalesState: serializeForEvent(nextSalesState),
      previousConversationMemory: serializeForEvent(previousConversationMemory),
      newConversationMemory: serializeForEvent(nextMemory),
      scoreBreakdown: score.breakdown,
      leadScore: score.total,
      nextBestAction: decision.nextBestAction,
      reason: decision.reason,
      routing: {
        status: routing.status,
        priority: queuePriority.priority,
        reason: routing.reason,
      },
      commercialSignals,
      suggestedReply: reply.message,
      replyGenerationStatus: reply.replyGenerationStatus,
      model: finalAnalysis.model,
      replyModel: reply.model,
      createdAt: new Date(),
    });

    const eventRef = targetRef.collection('salesBrainEvents').doc(eventId);
    const batch = db.batch();
    batch.set(eventRef, eventPayload, { merge: false });
    batch.set(
      targetRef,
      {
        salesState: nextSalesState,
        conversationMemory: nextMemory,
        commercialSignals,
        salesContext: salesContextPatch.salesContext,
        salesContextRaw: salesContextPatch.salesContextRaw,
        salesContextConfidence: salesContextPatch.salesContextConfidence,
        salesScoreState: {
          appliedSignals: score.appliedSignals,
          updatedAt: FieldValue.serverTimestamp(),
        },
        salesBrainCurrent: {
          eventId,
          nextBestAction: decision.nextBestAction,
          status: eventPayload.status,
          routing: {
            status: routing.status,
            priority: queuePriority.priority,
            reason: routing.reason,
            updatedAt: FieldValue.serverTimestamp(),
          },
          replyGenerationStatus: eventPayload.replyGenerationStatus,
          updatedAt: FieldValue.serverTimestamp(),
        },
      },
      { merge: true }
    );
    await batch.commit();

    console.log('[SalesBrain] state:updated');
    if (reply.message) console.log('[SalesBrain] suggestion:created');

    return {
      ok: true,
      eventId,
      analysis: finalAnalysis,
      salesState: nextSalesState,
      conversationMemory: nextMemory,
      scoreBreakdown: score.breakdown,
      newlyAppliedScoreSignals: score.newlyApplied,
      leadScore: score.total,
      nextBestAction: decision.nextBestAction,
      suggestedReply: reply.message,
      salesBrainEvent: eventPayload,
    };
  } catch (error) {
    console.warn('[SalesBrain] error', error?.message || error);
    return { ok: false, reason: 'error', error: String(error?.message || error) };
  } finally {
    await releaseLeadLock(targetRef);
  }
}

export {
  expireCurrentSuggestion,
  recordSellerFeedback,
} from './events.js';

export {
  analyzeConversation,
} from './analyzeConversation.js';

export {
  buildNewInboundLeadSalesBrainDefaults,
  getDefaultSalesBrainMode,
} from './defaultMode.js';

export {
  calculateLeadScore,
} from './leadScore.js';

export {
  decideNextAction,
  buildSalesBrainGuardrails,
} from './decision.js';
