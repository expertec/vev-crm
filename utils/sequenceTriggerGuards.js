const SOCIAL_SEQUENCE_TRIGGERS = new Set([
  'planredes',
  'planredes990',
]);

const WEB_SEQUENCE_TRIGGERS = new Set([
  'leadweb',
  'leadwhatsapp',
  'leadpaginaweb',
  'nuevolead',
  'nuevoleadweb',
  'webpromo',
  'webenviada',
]);

function triggerKey(value = '') {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '')
    .trim();
}

function triggerFamily(value = '') {
  const key = triggerKey(value);
  if (SOCIAL_SEQUENCE_TRIGGERS.has(key)) return 'social';
  if (WEB_SEQUENCE_TRIGGERS.has(key)) return 'web';
  return '';
}

function normalizeContextText(value = '') {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
}

function collectLeadContextTexts(leadData = {}) {
  return [
    leadData?.salesState?.productStrategy,
    leadData?.salesState?.qualification?.productStrategy,
    leadData?.salesBrainCurrent?.productStrategy,
    leadData?.salesBrainCurrent?.qualification?.productStrategy,
    leadData?.salesContext?.interestedService,
    leadData?.conversationMemory?.facts?.interestedService?.value,
    leadData?.acquisitionContext?.productStrategy,
    leadData?.acquisitionContext?.service,
    leadData?.acquisitionContext?.source,
    leadData?.acquisitionContext?.campaign,
    leadData?.acquisitionContext?.ad,
    leadData?.metaAttribution?.campaignName,
    leadData?.metaAttribution?.adName,
    leadData?.metaAttribution?.headline,
    leadData?.metaAttribution?.body,
    leadData?.lastMetaAttribution?.campaignName,
    leadData?.lastMetaAttribution?.adName,
    leadData?.lastMetaAttribution?.headline,
    leadData?.lastMetaAttribution?.body,
    leadData?.metaCampaignName,
    leadData?.campaign,
  ]
    .map(normalizeContextText)
    .filter(Boolean);
}

function collectLeadTriggerKeys(leadData = {}) {
  const keys = new Set();
  const add = (value) => {
    const key = triggerKey(value);
    if (key) keys.add(key);
  };

  if (Array.isArray(leadData?.secuenciasActivas)) {
    leadData.secuenciasActivas.forEach((seq) => {
      if (seq?.completed === true) return;
      add(seq?.trigger);
    });
  }
  if (Array.isArray(leadData?.sequenceScheduledTriggers)) {
    leadData.sequenceScheduledTriggers.forEach(add);
  }
  if (Array.isArray(leadData?.sequenceDeliveredTriggers)) {
    leadData.sequenceDeliveredTriggers.forEach(add);
  }
  if (Array.isArray(leadData?.etiquetas)) {
    leadData.etiquetas.forEach(add);
  }

  add(leadData?.lastMetaSequenceTrigger);
  add(leadData?.metaAttribution?.trigger);
  add(leadData?.metaAttribution?.route?.trigger);
  add(leadData?.acquisitionContext?.trigger);
  add(leadData?.acquisitionContext?.sequenceTrigger);

  return keys;
}

function hasSocialSalesBrainContext(leadData = {}) {
  const texts = collectLeadContextTexts(leadData);
  return texts.some((text) => (
    text === 'redes_sociales'
    || /\b(planredes|redes sociales|redessociales|manejo de redes|social media|meta ads|facebook ads|instagram)\b/.test(text)
  ));
}

export function shouldBlockSequenceByLeadContext(leadData = {}, nextTrigger = '') {
  const nextFamily = triggerFamily(nextTrigger);
  if (!nextFamily) return { blocked: false, reason: '' };

  const keys = collectLeadTriggerKeys(leadData);
  const hasSocialContext = [...keys].some((key) => SOCIAL_SEQUENCE_TRIGGERS.has(key));
  const hasSocialBrainContext = hasSocialSalesBrainContext(leadData);

  if (nextFamily === 'web' && (hasSocialContext || hasSocialBrainContext)) {
    return { blocked: true, reason: 'social_campaign_sequence_lock' };
  }

  return { blocked: false, reason: '' };
}

export function getSequenceTriggerFamily(trigger = '') {
  return triggerFamily(trigger);
}
