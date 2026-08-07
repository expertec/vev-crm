const AD_CONTEXT_TYPES = [
  'extendedTextMessage',
  'imageMessage',
  'videoMessage',
  'documentMessage',
  'buttonsResponseMessage',
  'templateButtonReplyMessage',
];

const AD_INDICATORS = [
  'externalAdReply',
  'quotedAd',
  'utm',
  'smbClientCampaignId',
  'smbServerCampaignId',
  'entryPointConversionSource',
  'entryPointConversionApp',
  'ctwaPayload',
];

const MAX_STRING = 500;

function toObject(value) {
  return value && typeof value === 'object' ? value : null;
}

function cleanString(value, max = MAX_STRING) {
  const text = String(value || '').trim();
  if (!text) return '';
  return text.slice(0, max);
}

function firstNonEmpty(...values) {
  for (const value of values) {
    const clean = cleanString(value);
    if (clean) return clean;
  }
  return '';
}

function parseMaybeJson(value) {
  if (!value) return null;
  if (typeof value === 'object') return value;
  const text = cleanString(value, 5000);
  if (!text) return null;
  try {
    const parsed = JSON.parse(text);
    return toObject(parsed);
  } catch (_error) {
    return null;
  }
}

function mergeClean(target, patch = {}) {
  Object.entries(patch || {}).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    if (typeof value === 'string') {
      const clean = cleanString(value);
      if (clean && !target[key]) target[key] = clean;
      return;
    }
    if (typeof value === 'boolean' || typeof value === 'number') {
      if (target[key] === undefined) target[key] = value;
      return;
    }
    if (value && typeof value === 'object' && !target[key]) {
      target[key] = value;
    }
  });
  return target;
}

function detectIndicatorInContextInfo(contextInfo, basePath) {
  const ci = toObject(contextInfo);
  if (!ci) return null;

  if (ci.externalAdReply) return { indicator: 'externalAdReply', path: `${basePath}.contextInfo.externalAdReply` };
  if (ci.quotedAd) return { indicator: 'quotedAd', path: `${basePath}.contextInfo.quotedAd` };
  if (ci.utm) return { indicator: 'utm', path: `${basePath}.contextInfo.utm` };
  if (ci.smbClientCampaignId) return { indicator: 'smbClientCampaignId', path: `${basePath}.contextInfo.smbClientCampaignId` };
  if (ci.smbServerCampaignId) return { indicator: 'smbServerCampaignId', path: `${basePath}.contextInfo.smbServerCampaignId` };
  if (ci.entryPointConversionSource) return { indicator: 'entryPointConversionSource', path: `${basePath}.contextInfo.entryPointConversionSource` };
  if (ci.entryPointConversionApp) return { indicator: 'entryPointConversionApp', path: `${basePath}.contextInfo.entryPointConversionApp` };
  if (ci.ctwaPayload) return { indicator: 'ctwaPayload', path: `${basePath}.contextInfo.ctwaPayload` };

  return null;
}

function extractUtm(value) {
  const utm = toObject(value) || parseMaybeJson(value);
  if (!utm) {
    const text = cleanString(value);
    return text ? { utmRaw: text } : {};
  }

  return {
    utmSource: firstNonEmpty(utm.source, utm.utm_source),
    utmMedium: firstNonEmpty(utm.medium, utm.utm_medium),
    campaignId: firstNonEmpty(utm.campaign_id, utm.campaignId),
    campaignName: firstNonEmpty(utm.campaign, utm.utm_campaign),
    adSetId: firstNonEmpty(utm.adset_id, utm.adSetId, utm.adsetId),
    adSetName: firstNonEmpty(utm.adset, utm.utm_adset),
    adId: firstNonEmpty(utm.ad_id, utm.adId),
    adName: firstNonEmpty(utm.ad, utm.utm_content, utm.content),
    utmTerm: firstNonEmpty(utm.term, utm.utm_term),
  };
}

function extractReferral(referral) {
  const ref = toObject(referral);
  if (!ref) return {};
  const sourceId = firstNonEmpty(ref.source_id, ref.sourceId, ref.id);
  return {
    sourceId,
    adId: firstNonEmpty(ref.ad_id, ref.adId, sourceId),
    campaignId: firstNonEmpty(ref.campaign_id, ref.campaignId),
    adSetId: firstNonEmpty(ref.adset_id, ref.adSetId, ref.adsetId),
    sourceUrl: firstNonEmpty(ref.source_url, ref.sourceUrl),
    headline: firstNonEmpty(ref.headline, ref.title),
    body: firstNonEmpty(ref.body, ref.description),
    mediaType: firstNonEmpty(ref.media_type, ref.mediaType),
    thumbnailUrl: firstNonEmpty(ref.thumbnail_url, ref.thumbnailUrl, ref.image_url, ref.imageUrl),
    ctwaClid: firstNonEmpty(ref.ctwa_clid, ref.ctwaClid),
  };
}

function extractExternalAdReply(externalAdReply) {
  const ad = toObject(externalAdReply);
  if (!ad) return {};
  return {
    sourceId: firstNonEmpty(ad.sourceId, ad.source_id, ad.id),
    adId: firstNonEmpty(ad.adId, ad.ad_id, ad.sourceId, ad.source_id, ad.id),
    campaignId: firstNonEmpty(ad.campaignId, ad.campaign_id),
    adSetId: firstNonEmpty(ad.adSetId, ad.adsetId, ad.adset_id),
    sourceUrl: firstNonEmpty(ad.sourceUrl, ad.source_url, ad.mediaUrl),
    headline: firstNonEmpty(ad.title, ad.headline),
    body: firstNonEmpty(ad.body, ad.description),
    mediaType: firstNonEmpty(ad.mediaType, ad.media_type),
    thumbnailUrl: firstNonEmpty(ad.thumbnailUrl, ad.thumbnail_url, ad.jpegThumbnail ? '[jpegThumbnail]' : ''),
    ctwaClid: firstNonEmpty(ad.ctwaClid, ad.ctwa_clid),
    renderLargerThumbnail: ad.renderLargerThumbnail === true,
  };
}

function extractCtwaPayload(ctwaPayload) {
  const payload = toObject(ctwaPayload) || parseMaybeJson(ctwaPayload);
  if (!payload) {
    const text = cleanString(ctwaPayload, 1200);
    return text ? { ctwaPayloadRaw: text } : {};
  }

  const sourceId = firstNonEmpty(payload.source_id, payload.sourceId, payload.ad_id, payload.adId, payload.id);
  return {
    sourceId,
    adId: firstNonEmpty(payload.ad_id, payload.adId, sourceId),
    campaignId: firstNonEmpty(payload.campaign_id, payload.campaignId),
    adSetId: firstNonEmpty(payload.adset_id, payload.adSetId, payload.adsetId),
    sourceUrl: firstNonEmpty(payload.source_url, payload.sourceUrl, payload.url),
    headline: firstNonEmpty(payload.headline, payload.title),
    body: firstNonEmpty(payload.body, payload.description),
    mediaType: firstNonEmpty(payload.media_type, payload.mediaType),
    thumbnailUrl: firstNonEmpty(payload.thumbnail_url, payload.thumbnailUrl, payload.image_url, payload.imageUrl),
    ctwaClid: firstNonEmpty(payload.ctwa_clid, payload.ctwaClid, payload.click_id, payload.clickId),
  };
}

function collectAttributionFromNode(node, basePath, attribution) {
  const obj = toObject(node);
  if (!obj) return;

  const recordSignal = (indicator, path) => {
    if (!attribution.indicator) attribution.indicator = indicator;
    if (!attribution.path) attribution.path = path;
    if (!attribution.signals.some((signal) => signal.indicator === indicator && signal.path === path)) {
      attribution.signals.push({ indicator, path });
    }
  };

  if (obj.referral) {
    recordSignal('referral', `${basePath}.referral`);
    mergeClean(attribution, extractReferral(obj.referral));
  }
  if (obj.externalAdReply) {
    recordSignal('externalAdReply', `${basePath}.externalAdReply`);
    mergeClean(attribution, extractExternalAdReply(obj.externalAdReply));
  }
  if (obj.quotedAd) {
    recordSignal('quotedAd', `${basePath}.quotedAd`);
    mergeClean(attribution, extractExternalAdReply(obj.quotedAd));
  }
  if (obj.utm) {
    recordSignal('utm', `${basePath}.utm`);
    mergeClean(attribution, extractUtm(obj.utm));
  }
  if (obj.ctwaPayload) {
    recordSignal('ctwaPayload', `${basePath}.ctwaPayload`);
    mergeClean(attribution, extractCtwaPayload(obj.ctwaPayload));
  }
  if (obj.smbClientCampaignId) {
    recordSignal('smbClientCampaignId', `${basePath}.smbClientCampaignId`);
    mergeClean(attribution, {
      smbClientCampaignId: obj.smbClientCampaignId,
      campaignId: obj.smbClientCampaignId,
    });
  }
  if (obj.smbServerCampaignId) {
    recordSignal('smbServerCampaignId', `${basePath}.smbServerCampaignId`);
    mergeClean(attribution, {
      smbServerCampaignId: obj.smbServerCampaignId,
      campaignId: obj.smbServerCampaignId,
    });
  }
  if (obj.entryPointConversionSource) {
    recordSignal('entryPointConversionSource', `${basePath}.entryPointConversionSource`);
    mergeClean(attribution, { entryPointConversionSource: obj.entryPointConversionSource });
  }
  if (obj.entryPointConversionApp) {
    recordSignal('entryPointConversionApp', `${basePath}.entryPointConversionApp`);
    mergeClean(attribution, { entryPointConversionApp: obj.entryPointConversionApp });
  }

  const ci = toObject(obj.contextInfo);
  if (ci) collectAttributionFromNode(ci, `${basePath}.contextInfo`, attribution);
}

function detectDirectIndicator(node, basePath) {
  const obj = toObject(node);
  if (!obj) return null;

  for (const key of AD_INDICATORS) {
    if (obj[key]) return { indicator: key, path: `${basePath}.${key}` };
  }

  const ciIndicator = detectIndicatorInContextInfo(obj.contextInfo, basePath);
  if (ciIndicator) return ciIndicator;

  for (const type of AD_CONTEXT_TYPES) {
    const ciTypedIndicator = detectIndicatorInContextInfo(obj?.[type]?.contextInfo, `${basePath}.${type}`);
    if (ciTypedIndicator) return ciTypedIndicator;
  }

  return null;
}

function childrenOf(node, basePath) {
  const obj = toObject(node);
  if (!obj) return [];

  const out = [];
  for (const [key, value] of Object.entries(obj)) {
    if (value && typeof value === 'object') out.push({ node: value, path: `${basePath}.${key}` });
  }
  return out;
}

export function detectMetaAdSignal(msg) {
  const attribution = extractMetaAdAttribution(msg);
  return {
    isFromMetaAd: attribution.isFromMetaAd,
    indicator: attribution.indicator,
    path: attribution.path,
  };
}

export function extractMetaAdAttribution(msg) {
  const roots = [];
  const rootMessage = toObject(msg?.message);
  const rootMsg = toObject(msg);
  if (rootMessage) roots.push({ node: rootMessage, path: 'message' });
  if (rootMsg) roots.push({ node: rootMsg, path: 'msg' });
  if (roots.length === 0) {
    return { isFromMetaAd: false, indicator: null, path: null, signals: [] };
  }

  const attribution = {
    isFromMetaAd: false,
    indicator: null,
    path: null,
    source: 'click_to_whatsapp',
    signals: [],
  };
  const stack = [...roots];
  const visited = new Set();

  while (stack.length > 0) {
    const { node, path } = stack.pop();
    const obj = toObject(node);
    if (!obj || visited.has(obj)) continue;
    visited.add(obj);

    collectAttributionFromNode(obj, path, attribution);

    for (const child of childrenOf(obj, path)) stack.push(child);
  }

  attribution.isFromMetaAd = attribution.signals.length > 0;
  if (!attribution.adId && attribution.sourceId) attribution.adId = attribution.sourceId;

  if (!attribution.isFromMetaAd) {
    const legacySignal = rootMessage ? detectDirectIndicator(rootMessage, 'message') : null;
    if (legacySignal) {
      attribution.isFromMetaAd = true;
      attribution.indicator = legacySignal.indicator;
      attribution.path = legacySignal.path;
      attribution.signals.push(legacySignal);
    }
  }

  return attribution;
}

export function isMessageFromMetaAd(msg) {
  return detectMetaAdSignal(msg).isFromMetaAd;
}
