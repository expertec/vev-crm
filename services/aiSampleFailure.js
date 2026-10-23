// Repair legacy and new failures without retrying OpenAI or contacting the client.
export async function syncAiSampleFailure(db, leadRef, expectedBusinessId = '') {
  return db.runTransaction(async (tx) => {
    const lead = (await tx.get(leadRef)).data() || {};
    const agent = lead.sampleAgent || {};
    const businessId = agent.negocioId;
    if (agent.status !== 'generating' || !businessId || businessId.includes('/') || (expectedBusinessId && businessId !== expectedBusinessId)) return lead;
    const business = (await tx.get(db.collection('Negocios').doc(businessId))).data() || {};
    if (business.leadId !== leadRef.id || business.aiSampleJob?.state !== 'failed') return lead;
    const millis = (value) => value?.toMillis?.() || (value ? new Date(value).getTime() : 0) || 0;
    if (millis(agent.manualRequestedAt) > millis(business.aiSampleJob.finishedAt) && business.aiSampleJob.finishedAt) return lead;
    const updated = { ...agent, status: 'error', enabled: false, error: String(business.aiSampleJob.error || 'La generación se interrumpió. Puedes reintentar la muestra.').slice(0, 500), leaseOwner: '', leaseUntil: new Date(0), updatedAt: new Date() };
    tx.update(leadRef, { sampleAgent: updated });
    return { ...lead, sampleAgent: updated };
  });
}
