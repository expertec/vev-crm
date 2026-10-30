import { SALES_BRAIN_MODES } from './catalog.js';

// Retired: keep existing lead data while disabling all Sales Brain entry points.
export function isSalesBrainEnabled() {
  return false;
}

export function getDefaultSalesBrainMode() {
  return SALES_BRAIN_MODES.OFF;
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
