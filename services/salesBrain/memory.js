import { FACT_KEYS } from './catalog.js';

function cleanText(value = '', max = 1200) {
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

function normalizeSource(value = '') {
  return String(value || '').trim() === 'explicit' ? 'explicit' : 'inferred';
}

function normalizeFactEntry(entry, { inputMessageId = '', now = new Date() } = {}) {
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return null;
  if (entry.value === null || entry.value === undefined || entry.value === '') return null;
  return {
    value: entry.value,
    confidence: Math.max(0, Math.min(1, Number(entry.confidence || 0.6))),
    source: normalizeSource(entry.source),
    sourceMessageId: String(entry.sourceMessageId || inputMessageId || '').trim(),
    updatedAt: now,
  };
}

export function shouldReplaceFact(previous = null, incoming = null) {
  if (!incoming) return false;
  if (!previous) return true;

  const prevSource = normalizeSource(previous.source);
  const nextSource = normalizeSource(incoming.source);
  const prevConfidence = Number(previous.confidence || 0);
  const nextConfidence = Number(incoming.confidence || 0);

  if (prevSource === 'explicit' && nextSource !== 'explicit') return false;
  if (prevSource !== 'explicit' && nextSource === 'explicit') return true;
  if (String(previous.value) === String(incoming.value)) return nextConfidence >= prevConfidence;
  if (nextSource === 'explicit' && nextConfidence >= 0.65) return true;
  if (prevSource !== 'explicit' && nextSource !== 'explicit' && nextConfidence >= Math.max(0.8, prevConfidence + 0.15)) return true;
  return false;
}

export function mergeConversationMemory(previous = {}, analysis = {}, {
  inputMessageId = '',
  now = new Date(),
} = {}) {
  const prevFacts = previous?.facts && typeof previous.facts === 'object' ? previous.facts : {};
  const nextFacts = { ...prevFacts };
  const incomingFacts = analysis?.facts && typeof analysis.facts === 'object' ? analysis.facts : {};

  for (const key of FACT_KEYS) {
    const incoming = normalizeFactEntry(incomingFacts[key], { inputMessageId, now });
    if (!incoming) continue;
    if (shouldReplaceFact(nextFacts[key], incoming)) nextFacts[key] = incoming;
  }

  const summaryParts = [
    cleanText(previous?.summary || '', 700),
    cleanText(analysis?.summary || '', 420),
  ].filter(Boolean);
  const summary = Array.from(new Set(summaryParts)).join(' ').slice(0, 1200);

  return {
    summary,
    facts: nextFacts,
    updatedAt: now,
  };
}

export function getFactValue(memory = {}, key = '') {
  const entry = memory?.facts?.[key];
  if (!entry || typeof entry !== 'object') return null;
  return entry.value ?? null;
}

export function hasFreshFact(memory = {}, key = '') {
  const entry = memory?.facts?.[key];
  if (!entry || typeof entry !== 'object') return false;
  if (entry.value === null || entry.value === undefined || entry.value === '') return false;
  return toMillis(entry.updatedAt) > 0 || Boolean(entry.sourceMessageId);
}
