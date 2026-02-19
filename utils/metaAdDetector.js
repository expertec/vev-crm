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

function toObject(value) {
  return value && typeof value === 'object' ? value : null;
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
  const root = toObject(msg?.message);
  if (!root) return { isFromMetaAd: false, indicator: null, path: null };

  const stack = [{ node: root, path: 'message' }];
  const visited = new Set();

  while (stack.length > 0) {
    const { node, path } = stack.pop();
    const obj = toObject(node);
    if (!obj || visited.has(obj)) continue;
    visited.add(obj);

    const signal = detectDirectIndicator(obj, path);
    if (signal) return { isFromMetaAd: true, indicator: signal.indicator, path: signal.path };

    for (const child of childrenOf(obj, path)) stack.push(child);
  }

  return { isFromMetaAd: false, indicator: null, path: null };
}

export function isMessageFromMetaAd(msg) {
  return detectMetaAdSignal(msg).isFromMetaAd;
}

