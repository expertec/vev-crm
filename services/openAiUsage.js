let writer = null;

export function configureOpenAiUsage(db) {
  writer = (event) => db.collection('aiUsageEvents').add(event);
}

// Operational counters only: never persist prompts, replies, audio or image URLs.
export async function recordOpenAiUsage({ resource, model = '', purpose = '', context = {}, usage = null, requestId = '', status = 200, durationMs = 0, imageCount = 0 }, record = writer) {
  if (!record) return;
  const number = (value) => Math.max(0, Number(value) || 0);
  const input = usage?.input_tokens ?? usage?.prompt_tokens;
  const output = usage?.output_tokens ?? usage?.completion_tokens;
  let timeout;
  try {
    const event = {
      createdAt: new Date(), resource, model, purpose,
      leadId: String(context.leadId || '').slice(0, 220), negocioId: String(context.negocioId || '').slice(0, 220),
      requestId: String(requestId || '').slice(0, 150), status, durationMs: number(durationMs), imageCount: number(imageCount),
      inputTokens: number(input), outputTokens: number(output), totalTokens: number(usage?.total_tokens ?? (number(input) + number(output))),
      cachedInputTokens: number(usage?.input_tokens_details?.cached_tokens ?? usage?.prompt_tokens_details?.cached_tokens),
      usageAvailable: Boolean(usage), inputDetails: usage?.input_tokens_details || {}, outputDetails: usage?.output_tokens_details || {},
    };
    // A slow metrics store must not hold up a WhatsApp response.
    await Promise.race([Promise.resolve().then(() => record(event)), new Promise((resolve) => { timeout = setTimeout(resolve, 1500); })]);
  } catch (error) { console.warn('[ai-usage] No se pudo guardar el contador:', error.code || 'storage_error'); }
  finally { clearTimeout(timeout); }
}
