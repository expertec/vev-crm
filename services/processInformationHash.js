import crypto from 'node:crypto';

function isPlainObject(value) {
  return (
    value !== null
    && typeof value === 'object'
    && !Array.isArray(value)
    && Object.prototype.toString.call(value) === '[object Object]'
  );
}

function normalizeForHash(value) {
  if (value === null || value === undefined) return null;
  if (Array.isArray(value)) return value.map((item) => normalizeForHash(item));
  if (value instanceof Date) return value.toISOString();
  if (typeof value?.toDate === 'function') {
    try {
      return value.toDate().toISOString();
    } catch {
      return String(value);
    }
  }
  if (isPlainObject(value)) {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = normalizeForHash(value[key]);
    }
    return out;
  }
  if (typeof value === 'string') return value.trim();
  return value;
}

function extractAdvancedBrief(data = {}) {
  return data?.advancedBrief ?? data?.briefWeb?.advancedBrief ?? '';
}

function extractUpdatedAtMillis(data = {}) {
  const candidate = data?.updatedAt
    || data?.schemaUpdatedAt
    || data?.infoProcessedAt
    || data?.createdAt
    || null;

  if (!candidate) return 0;
  if (typeof candidate?.toMillis === 'function') {
    try {
      return candidate.toMillis();
    } catch {
      return 0;
    }
  }
  if (candidate instanceof Date) return candidate.getTime();
  if (typeof candidate === 'number') return candidate;
  const parsed = Date.parse(String(candidate));
  return Number.isFinite(parsed) ? parsed : 0;
}

export function buildInformationIdempotencyHash({
  negocioId,
  negocioData = {},
}) {
  const payload = {
    negocioId: String(negocioId || '').trim(),
    advancedBrief: normalizeForHash(extractAdvancedBrief(negocioData)),
    updatedAt: extractUpdatedAtMillis(negocioData),
  };

  return crypto
    .createHash('sha256')
    .update(JSON.stringify(payload))
    .digest('hex');
}

