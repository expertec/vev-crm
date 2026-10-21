import { fail } from './contracts.mjs';

// Preparatory decision only. No import into SiteTemplate/nwTheme until F1.
// An explicit v3 must never fall through to a legacy normalizer.
export function rendererDecision(doc = {}, { v3BundleAvailable = false } = {}) {
  const raw = doc.schema?.designVersion ?? doc.designVersion;
  if (raw === 'v3') return { kind: v3BundleAvailable ? 'v3' : 'preparing', revisionId: doc.schema?.revisionId ?? null };
  if (raw && !['v1', 'v2'].includes(raw)) return { kind: 'unsupported' };
  return { kind: 'legacy', designVersion: raw === 'v2' ? 'v2' : 'v1' };
}
export function assertLegacyWriteTarget(collection) {
  fail(!['SiteV3Projects', 'SiteV3Jobs', 'PublicSitesV3', 'WebSamples', 'WebSampleEvents', 'BusinessSiteEvents', 'SiteV3Access'].includes(collection), 'LEGACY_WRITER_FORBIDDEN', 409);
}
