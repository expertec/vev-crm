import { SALES_BRAIN_MODES } from './catalog.js';

function normalizeDefaultMode(value = '') {
  const safe = String(value || '').trim().toLowerCase();
  if (safe === SALES_BRAIN_MODES.COPILOT) return SALES_BRAIN_MODES.COPILOT;
  return SALES_BRAIN_MODES.COPILOT;
}

export function getDefaultSalesBrainMode() {
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
    },
    salesContextRaw: {},
    salesContextConfidence: {},
  };
}
