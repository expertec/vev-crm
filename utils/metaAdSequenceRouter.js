const ROUTES_COLLECTION = 'metaAdSequenceRoutes';
const ROUTES_CACHE_TTL_MS = Number(process.env.META_AD_ROUTES_CACHE_TTL_MS) > 0
  ? Number(process.env.META_AD_ROUTES_CACHE_TTL_MS)
  : 60 * 1000;
let routesCache = { at: 0, rules: [] };

function cleanString(value, max = 500) {
  const text = String(value || '').trim();
  if (!text) return '';
  return text.slice(0, max);
}

function lower(value) {
  return cleanString(value).toLowerCase();
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (value === undefined || value === null || value === '') return [];
  return [value];
}

function normalizeRule(raw = {}, id = '') {
  const trigger = cleanString(raw.trigger || raw.sequenceTrigger || raw.sequence || raw.secuencia);
  return {
    id: cleanString(raw.id || id),
    name: cleanString(raw.name || raw.nombre || raw.label),
    trigger,
    active: raw.active !== false && raw.enabled !== false,
    priority: Number(raw.priority || raw.prioridad || 0) || 0,
    adIds: [
      ...asArray(raw.adIds),
      ...asArray(raw.adId),
      ...asArray(raw.metaAdIds),
      ...asArray(raw.metaAdId),
      ...asArray(raw.sourceIds),
      ...asArray(raw.sourceId),
    ].map((value) => cleanString(value)).filter(Boolean),
    adSetIds: [
      ...asArray(raw.adSetIds),
      ...asArray(raw.adSetId),
      ...asArray(raw.adsetIds),
      ...asArray(raw.adsetId),
      ...asArray(raw.metaAdSetIds),
      ...asArray(raw.metaAdSetId),
    ].map((value) => cleanString(value)).filter(Boolean),
    campaignIds: [
      ...asArray(raw.campaignIds),
      ...asArray(raw.campaignId),
      ...asArray(raw.metaCampaignIds),
      ...asArray(raw.metaCampaignId),
      ...asArray(raw.smbClientCampaignId),
      ...asArray(raw.smbServerCampaignId),
    ].map((value) => cleanString(value)).filter(Boolean),
    campaignNames: [
      ...asArray(raw.campaignNames),
      ...asArray(raw.campaignName),
    ].map((value) => lower(value)).filter(Boolean),
    ctwaClids: [
      ...asArray(raw.ctwaClids),
      ...asArray(raw.ctwaClid),
    ].map((value) => cleanString(value)).filter(Boolean),
    sourceUrlIncludes: [
      ...asArray(raw.sourceUrlIncludes),
      ...asArray(raw.urlIncludes),
      ...asArray(raw.sourceUrlContains),
    ].map((value) => lower(value)).filter(Boolean),
    headlineIncludes: [
      ...asArray(raw.headlineIncludes),
      ...asArray(raw.titleIncludes),
      ...asArray(raw.headlineContains),
    ].map((value) => lower(value)).filter(Boolean),
    bodyIncludes: [
      ...asArray(raw.bodyIncludes),
      ...asArray(raw.textIncludes),
      ...asArray(raw.bodyContains),
    ].map((value) => lower(value)).filter(Boolean),
  };
}

function normalizeRoutesFromConfig(config = {}) {
  const raw = config.metaAdSequenceRoutes || config.ctwaSequenceRoutes || config.metaAdRoutes || [];
  if (Array.isArray(raw)) return raw.map((rule, index) => normalizeRule(rule, `config_${index + 1}`));
  if (raw && typeof raw === 'object') {
    return Object.entries(raw).map(([id, rule]) => normalizeRule(rule, id));
  }
  return [];
}

function hasExact(values = [], candidates = []) {
  const normalized = new Set(values.map((value) => cleanString(value)).filter(Boolean));
  return candidates.map((value) => cleanString(value)).filter(Boolean).some((candidate) => normalized.has(candidate));
}

function hasIncludes(needles = [], haystack = '') {
  const text = lower(haystack);
  if (!text) return false;
  return needles.some((needle) => needle && text.includes(needle));
}

const INFERRED_META_ROUTES = [
  {
    trigger: 'PlanRedes',
    needles: [
      'redes sociales',
      'plan de redes',
      'manejo de redes',
      'social media',
      'contenido para redes',
      'publicaciones para redes',
    ],
  },
];

function inferTriggerFromAttribution(attribution = {}) {
  const text = lower([
    attribution.headline,
    attribution.body,
    attribution.campaignName,
    attribution.sourceUrl,
    attribution.adName,
  ].filter(Boolean).join(' '));

  if (!text) return '';

  const route = INFERRED_META_ROUTES.find((item) => (
    item.needles.some((needle) => text.includes(lower(needle)))
  ));

  return route?.trigger || '';
}

function scoreRule(rule, attribution = {}) {
  if (!rule.active || !rule.trigger) return 0;

  let score = 0;
  const adCandidates = [
    attribution.adId,
    attribution.sourceId,
  ];
  const adSetCandidates = [attribution.adSetId, attribution.adsetId];
  const campaignCandidates = [
    attribution.campaignId,
    attribution.smbClientCampaignId,
    attribution.smbServerCampaignId,
  ];

  if (hasExact(rule.adIds, adCandidates)) score = Math.max(score, 1000);
  if (hasExact(rule.adSetIds, adSetCandidates)) score = Math.max(score, 700);
  if (hasExact(rule.campaignIds, campaignCandidates)) score = Math.max(score, 500);
  if (hasExact(rule.ctwaClids, [attribution.ctwaClid])) score = Math.max(score, 450);
  if (hasIncludes(rule.sourceUrlIncludes, attribution.sourceUrl)) score = Math.max(score, 250);
  if (hasIncludes(rule.headlineIncludes, attribution.headline)) score = Math.max(score, 180);
  if (hasIncludes(rule.bodyIncludes, attribution.body)) score = Math.max(score, 120);
  if (rule.campaignNames.includes(lower(attribution.campaignName))) score = Math.max(score, 110);

  return score > 0 ? score + rule.priority : 0;
}

export function resolveMetaAdRouteFromRules({
  attribution = {},
  rules = [],
  fallbackTrigger = '',
} = {}) {
  const normalizedRules = rules.map((rule, index) => normalizeRule(rule, rule?.id || `rule_${index + 1}`));
  const ranked = normalizedRules
    .map((rule) => ({ rule, score: scoreRule(rule, attribution) }))
    .filter((entry) => entry.score > 0)
    .sort((a, b) => b.score - a.score || b.rule.priority - a.rule.priority);

  if (ranked[0]?.rule?.trigger) {
    const { rule, score } = ranked[0];
    return {
      trigger: rule.trigger,
      source: 'meta_ad_route',
      routeId: rule.id || '',
      routeName: rule.name || '',
      matchScore: score,
    };
  }

  const inferredTrigger = inferTriggerFromAttribution(attribution);
  if (inferredTrigger) {
    return {
      trigger: inferredTrigger,
      source: 'meta_ad_inferred',
      routeId: `inferred:${inferredTrigger}`,
      routeName: 'Inferido por metadata de anuncio',
      matchScore: 60,
    };
  }

  return {
    trigger: cleanString(fallbackTrigger),
    source: 'meta_ad_default',
    routeId: '',
    routeName: '',
    matchScore: 0,
  };
}

export function shouldScheduleMetaAdNoContentTrigger({
  metaRoute = null,
  preferMessageTrigger = false,
  messageRule = {},
  allowDefaultFallback = true,
} = {}) {
  const messageSource = String(messageRule?.source || '').toLowerCase();
  if (preferMessageTrigger && ['db', 'hashtag', 'text'].includes(messageSource)) {
    return Boolean(messageRule?.trigger);
  }

  const routeSource = String(metaRoute?.source || '').trim().toLowerCase();
  if (!routeSource || !metaRoute?.trigger) return false;
  if (routeSource !== 'meta_ad_default') return true;

  return Boolean(allowDefaultFallback);
}

export async function resolveMetaAdSequenceRoute({
  db,
  attribution = {},
  config = {},
  fallbackTrigger = '',
} = {}) {
  const configRules = normalizeRoutesFromConfig(config);
  let dbRules = [];

  if (db && typeof db.collection === 'function') {
    try {
      const nowMs = Date.now();
      if (routesCache.at && (nowMs - routesCache.at) < ROUTES_CACHE_TTL_MS) {
        dbRules = routesCache.rules;
      } else {
        const snap = await db.collection(ROUTES_COLLECTION).get();
        dbRules = snap.docs.map((doc) => normalizeRule(doc.data() || {}, doc.id));
        routesCache = { at: nowMs, rules: dbRules };
      }
    } catch (error) {
      console.warn('[metaAdSequenceRoute] No se pudieron leer reglas:', error?.message || error);
    }
  }

  return resolveMetaAdRouteFromRules({
    attribution,
    rules: [...configRules, ...dbRules],
    fallbackTrigger,
  });
}
