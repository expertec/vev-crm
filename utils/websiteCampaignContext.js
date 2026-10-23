const WEB_TRIGGERS = new Set(['leadwhatsapp', 'nuevoleadweb', 'leadweb', 'webpromo']);
export function isWebsiteCampaignIntro(text) {
  const normalized = String(text || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
  return /^(?:hola )?tengo un negocio y quiero informacion sobre la pagina web de \d+ al ano$/.test(normalized);
}
export function isWebsiteCampaignTrigger(trigger) {
  return WEB_TRIGGERS.has(String(trigger || '').trim().toLowerCase());
}
export function hasUnreadWebsiteCampaign(lead = {}) {
  return (lead.etiquetas || []).includes('MensajeNoDesencriptado')
    && isWebsiteCampaignTrigger(lead.metaAttribution?.trigger || lead.lastMetaAttribution?.trigger);
}
