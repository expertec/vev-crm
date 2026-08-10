import { admin, db as defaultDb } from '../../firebaseAdmin.js';
import { eventIdForInputMessage, normalizeInputMessageId } from './catalog.js';
import { buildSalesBrainEventPayload } from './eventPayload.js';

const { FieldValue } = admin.firestore;

function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

export { eventIdForInputMessage, normalizeInputMessageId };

export async function findSalesBrainEventByInput({ leadRef, inputMessageId }) {
  const eventId = eventIdForInputMessage(inputMessageId);
  if (!leadRef || !eventId) return null;
  const snap = await leadRef.collection('salesBrainEvents').doc(eventId).get();
  if (!snap.exists) return null;
  return { id: snap.id, ref: snap.ref, data: snap.data() || {} };
}

export async function expireCurrentSuggestion({
  db = defaultDb,
  leadRef,
  leadId,
  reason = 'context_changed',
  exceptEventId = '',
} = {}) {
  const ref = leadRef || (leadId ? db.collection('leads').doc(String(leadId)) : null);
  if (!ref) return { expired: false, reason: 'missing_lead' };

  const snap = await ref.get();
  if (!snap.exists) return { expired: false, reason: 'missing_lead' };
  const lead = snap.data() || {};
  const current = lead.salesBrainCurrent && typeof lead.salesBrainCurrent === 'object'
    ? lead.salesBrainCurrent
    : null;
  const eventId = cleanText(current?.eventId || '', 180);
  if (!eventId || eventId === exceptEventId || current?.status !== 'pending') {
    return { expired: false, reason: 'no_pending' };
  }

  const eventRef = ref.collection('salesBrainEvents').doc(eventId);
  const eventSnap = await eventRef.get();
  if (!eventSnap.exists) {
    await ref.set(
      {
        salesBrainCurrent: {
          ...current,
          status: 'expired',
          expiredReason: 'missing_event',
          updatedAt: FieldValue.serverTimestamp(),
        },
      },
      { merge: true }
    );
    console.warn('[SalesBrain] suggestion:expired_missing_event');
    return { expired: true, eventId, reason: 'missing_event' };
  }

  const batch = db.batch();
  batch.set(
    eventRef,
    {
      sellerDecision: 'expired',
      status: 'expired',
      expiredReason: cleanText(reason, 120),
      expiredAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
  batch.set(
    ref,
    {
      salesBrainCurrent: {
        ...current,
        status: 'expired',
        expiredReason: cleanText(reason, 120),
        updatedAt: FieldValue.serverTimestamp(),
      },
    },
    { merge: true }
  );
  await batch.commit();

  console.log('[SalesBrain] suggestion:expired');
  return { expired: true, eventId };
}

export { buildSalesBrainEventPayload };

export async function recordSellerFeedback({
  db = defaultDb,
  leadId = '',
  eventId = '',
  decision = '',
  finalReply = '',
  suggestedReply = '',
  actor = '',
} = {}) {
  const safeLeadId = cleanText(leadId, 220);
  const safeEventId = cleanText(eventId, 220);
  const safeDecision = cleanText(decision, 40);
  if (!safeLeadId || !safeEventId) throw new Error('Faltan leadId o eventId.');
  if (!['accepted', 'edited', 'spoken', 'rejected', 'expired', 'superseded'].includes(safeDecision)) {
    throw new Error('Decision de vendedor invalida.');
  }

  const leadRef = db.collection('leads').doc(safeLeadId);
  const eventRef = leadRef.collection('salesBrainEvents').doc(safeEventId);
  const eventSnap = await eventRef.get();
  if (!eventSnap.exists) throw new Error('Evento Sales Brain no encontrado.');
  const eventData = eventSnap.data() || {};
  const currentDecision = cleanText(eventData?.sellerDecision || eventData?.status || '', 40);
  const finalDecisions = new Set(['accepted', 'edited', 'spoken', 'rejected', 'expired', 'superseded']);
  if (finalDecisions.has(currentDecision)) {
    if (currentDecision === safeDecision) {
      return {
        ok: true,
        idempotent: true,
        leadId: safeLeadId,
        eventId: safeEventId,
        decision: safeDecision,
      };
    }
    throw new Error(`El evento ya fue finalizado como ${currentDecision}.`);
  }

  const payload = {
    sellerDecision: safeDecision,
    status: safeDecision,
    finalReply: cleanText(finalReply, 1200),
    ...(suggestedReply ? { suggestedReply: cleanText(suggestedReply, 1200) } : {}),
    decidedBy: cleanText(actor, 160) || 'crm',
    decidedAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  };

  const leadSnap = await leadRef.get();
  const current = leadSnap.exists ? (leadSnap.data()?.salesBrainCurrent || null) : null;
  const batch = db.batch();
  batch.set(eventRef, payload, { merge: true });
  if (current?.eventId === safeEventId) {
    batch.set(
      leadRef,
      {
        salesBrainCurrent: {
          ...current,
          status: safeDecision,
          updatedAt: FieldValue.serverTimestamp(),
        },
      },
      { merge: true }
    );
  }
  await batch.commit();

  console.log(`[SalesBrain] suggestion:${safeDecision}`);
  return { ok: true, leadId: safeLeadId, eventId: safeEventId, decision: safeDecision };
}
