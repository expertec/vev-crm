function cleanText(value = '', max = 180) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

const APPROVED_ASSETS = Object.freeze({
  web: Object.freeze([]),
  redes_sociales: Object.freeze([]),
});

const OBJECTIVES_THAT_NEED_ASSET = new Set([
  'SHOW_RELEVANT_PROOF',
]);

export function selectApprovedAsset({
  productStrategy = 'unknown',
  conversationObjective = '',
  businessType = '',
} = {}) {
  const productAssets = APPROVED_ASSETS[productStrategy] || [];
  const objective = cleanText(conversationObjective, 80);
  const business = cleanText(businessType, 120).toLowerCase();
  const candidates = productAssets.filter((asset) => {
    if (asset.objective && asset.objective !== objective) return false;
    if (asset.businessType && business && String(asset.businessType).toLowerCase() !== business) return false;
    return true;
  });
  return candidates[0] || null;
}

export function assetRequiredForObjective(conversationObjective = '') {
  return OBJECTIVES_THAT_NEED_ASSET.has(cleanText(conversationObjective, 80));
}
