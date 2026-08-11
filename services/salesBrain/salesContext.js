function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function objectOr(value, fallback = {}) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : fallback;
}

function factEntry(analysis = {}, memory = {}, key = '') {
  const direct = analysis?.facts?.[key];
  if (direct && typeof direct === 'object' && direct.value !== undefined && direct.value !== null && direct.value !== '') return direct;
  const remembered = memory?.facts?.[key];
  if (remembered && typeof remembered === 'object' && remembered.value !== undefined && remembered.value !== null && remembered.value !== '') return remembered;
  return null;
}

function safeFactValue(analysis = {}, memory = {}, key = '') {
  return factEntry(analysis, memory, key)?.value ?? null;
}

function confidenceFor(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(0, Math.min(1, n)) : 0.6;
}

function shouldApplyField(previousValue, previousConfidence, nextEntry) {
  if (!nextEntry) return false;
  if (previousValue === undefined || previousValue === null || previousValue === '') return true;
  const nextConfidence = confidenceFor(nextEntry.confidence);
  return nextEntry.source === 'explicit' && nextConfidence >= confidenceFor(previousConfidence);
}

function setIfBetter({ next, confidence, field, entry }) {
  if (!entry) return;
  if (!shouldApplyField(next[field], confidence[field], entry)) return;
  next[field] = entry.value;
  confidence[field] = confidenceFor(entry.confidence);
}

function rawKeyFromSaveTo(saveTo = '') {
  const safe = String(saveTo || '').trim();
  if (!safe.startsWith('salesContext.')) return '';
  return safe.slice('salesContext.'.length);
}

export function buildSalesContextPatch({
  previousSalesContext = {},
  previousSalesContextRaw = {},
  previousConfidence = {},
  analysis = {},
  memory = {},
  latestText = '',
  saveTo = '',
} = {}) {
  const next = { ...objectOr(previousSalesContext) };
  const raw = { ...objectOr(previousSalesContextRaw) };
  const confidence = { ...objectOr(previousConfidence) };

  setIfBetter({ next, confidence, field: 'businessType', entry: factEntry(analysis, memory, 'businessType') || (analysis.businessType ? { value: analysis.businessType, confidence: 0.8, source: 'explicit' } : null) });
  setIfBetter({ next, confidence, field: 'customerAcquisition', entry: factEntry(analysis, memory, 'customerAcquisition') });
  setIfBetter({ next, confidence, field: 'currentSituation', entry: factEntry(analysis, memory, 'currentSituation') });
  setIfBetter({ next, confidence, field: 'primaryGoal', entry: factEntry(analysis, memory, 'primaryGoal') || factEntry(analysis, memory, 'primaryNeed') || (analysis.primaryNeed ? { value: analysis.primaryNeed, confidence: 0.75, source: 'explicit' } : null) });
  setIfBetter({ next, confidence, field: 'painPoint', entry: factEntry(analysis, memory, 'painPoint') });
  setIfBetter({ next, confidence, field: 'hasWebsite', entry: factEntry(analysis, memory, 'hasWebsite') });
  setIfBetter({ next, confidence, field: 'runsAds', entry: factEntry(analysis, memory, 'runsAds') || factEntry(analysis, memory, 'currentlyAdvertising') });
  setIfBetter({ next, confidence, field: 'previousExperience', entry: factEntry(analysis, memory, 'previousExperience') });
  setIfBetter({ next, confidence, field: 'targetAudience', entry: factEntry(analysis, memory, 'targetAudience') });
  setIfBetter({ next, confidence, field: 'productsServices', entry: factEntry(analysis, memory, 'productsServices') });
  setIfBetter({ next, confidence, field: 'mainOffer', entry: factEntry(analysis, memory, 'mainOffer') });

  const rawKey = rawKeyFromSaveTo(saveTo);
  if (rawKey && cleanText(latestText, 1200)) {
    raw[rawKey] = cleanText(latestText, 1200);
  } else {
    for (const field of ['businessType', 'customerAcquisition', 'primaryGoal', 'previousExperience', 'targetAudience', 'productsServices']) {
      if (next[field] && !raw[field] && cleanText(latestText, 1200)) raw[field] = cleanText(latestText, 1200);
    }
  }

  return {
    salesContext: next,
    salesContextRaw: raw,
    salesContextConfidence: confidence,
  };
}

export function contextFactValue(lead = {}, key = '') {
  const context = objectOr(lead.salesContext);
  const value = context[key];
  if (value === undefined || value === null || value === '') return null;
  return value;
}
