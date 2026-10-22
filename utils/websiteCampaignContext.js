const WEB_TRIGGERS = new Set(['leadwhatsapp', 'nuevoleadweb', 'leadweb', 'webpromo']);
export function isWebsiteCampaignTrigger(trigger) {
  return WEB_TRIGGERS.has(String(trigger || '').trim().toLowerCase());
}
export function hasUnreadWebsiteCampaign(lead = {}) {
  return (lead.etiquetas || []).includes('MensajeNoDesencriptado')
    && isWebsiteCampaignTrigger(lead.metaAttribution?.trigger || lead.lastMetaAttribution?.trigger);
}
