import { SALES_BRAIN_MODES } from './catalog.js';

export function isSalesBrainEnabled() {
  return !['off', 'false', '0'].includes(String(process.env.SALES_BRAIN_ENABLED || '').trim().toLowerCase());
}

function normalizeDefaultMode(value = '') {
  const safe = String(value || '').trim().toLowerCase();
  if (safe === SALES_BRAIN_MODES.OFF) return SALES_BRAIN_MODES.OFF;
  if (safe === SALES_BRAIN_MODES.COPILOT) return SALES_BRAIN_MODES.COPILOT;
  return SALES_BRAIN_MODES.COPILOT;
}

export function getDefaultSalesBrainMode() {
  if (!isSalesBrainEnabled()) return SALES_BRAIN_MODES.OFF;
  return normalizeDefaultMode(process.env.SALES_BRAIN_DEFAULT_MODE || SALES_BRAIN_MODES.COPILOT);
}

export function buildNewInboundLeadSalesBrainDefaults() {
  const mode = getDefaultSalesBrainMode();
  return {
    salesBrainMode: mode,
    queue: {
      status: 'automation',
      priority: 0,
      reason: null,
      reasonCode: null,
      enteredAt: null,
      claimedAt: null,
      firstAgentActionAt: null,
      outcomeAt: null,
    },
    followUp: {
      status: null,
      nextAt: null,
      reason: null,
    },
    salesContext: {
      businessType: null,
      currentSituation: null,
      primaryGoal: null,
      painPoint: null,
      hasWebsite: null,
      runsAds: null,
      previousExperience: null,
      customerAcquisition: null,
      targetAudience: null,
      productsServices: null,
      mainOffer: null,
    },
    salesContextRaw: {},
    salesContextConfidence: {},
    commercialProgress: {
      understandingDemonstrated: false,
      personalizedIdeaDelivered: false,
      proofDelivered: false,
      offerExplained: false,
      pricePresented: false,
      offerReaction: null,
      updatedAt: null,
    },
  };
}
