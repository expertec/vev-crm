import { assertSectionRegistry, fail, uniqueEntities, validateContract } from './contracts.mjs';

// Intentionally conservative Phase 0 coverage. Other countries remain disabled
// until a country-aware adapter is validated. This does not prove ownership/account status.
export function normalizeWhatsApp({ country, phone } = {}) {
  if (!phone) return { state: 'missing', e164: null, waDigits: null };
  const invalid = { state: 'invalid', e164: null, waDigits: null };
  if (typeof phone !== 'string' || !/^\+?[0-9 ()\-.]+$/.test(phone)) return invalid;
  const digits = phone.replace(/\D/g, '');
  const prefix = country === 'MX' ? '52' : ['US', 'CA'].includes(country) ? '1' : null;
  if (!prefix) return invalid;
  const national = phone.startsWith('+') ? (digits.startsWith(prefix) ? digits.slice(prefix.length) : '') : digits;
  if (!/^[2-9]\d{9}$/.test(national) || /^(\d)\1{9}$/.test(national)) return invalid;
  if (prefix === '1' && !/^[2-9]\d{2}[2-9]\d{6}$/.test(national)) return invalid;
  return { state: 'configured', e164: `+${prefix}${national}`, waDigits: prefix + national };
}
const reference = (set, value, scope) => fail(set.has(value), 'BROKEN_REFERENCE', 422, [`${scope}:${value}`]);
export function validateBrief(brief) {
  validateContract('BusinessBriefV3', brief);
  uniqueEntities(brief.offerings, 'offerings'); uniqueEntities(brief.facts, 'facts');
  const assets = new Set(brief.assetIds);
  for (const o of brief.offerings) for (const m of o.mediaIds) reference(assets, m, 'asset');
  return brief;
}
export function validatePlan(plan, { inputRevision, contentIds, actionIds, assetIds = [], capabilities = {} }) {
  validateContract('CreativePlanV3', plan); assertSectionRegistry(plan.sections);
  fail(plan.expectedInputRevision === inputRevision, 'STALE_INPUT', 409);
  const contents = new Set(contentIds), actions = new Set(actionIds), assets = new Set(assetIds);
  if (plan.primaryOfferRef) reference(contents, plan.primaryOfferRef, 'primaryOffer');
  reference(actions, plan.primaryActionId, 'primaryAction');
  if (plan.secondaryActionId) reference(actions, plan.secondaryActionId, 'secondaryAction');
  fail(plan.heroVariant === plan.sections[0].variant, 'HERO_VARIANT_MISMATCH', 422);
  uniqueEntities(plan.imageRequests.map(x => ({ id: x.requestId })), 'imageRequests');
  for (const s of plan.sections) {
    for (const r of s.contentRefs) reference(contents, r, s.id);
    for (const r of s.actionIds) reference(actions, r, s.id);
    if (s.type === 'hotelBooking') fail(capabilities.hotelCheckout === true, 'CAPABILITY_UNAVAILABLE', 422);
  }
  for (const i of plan.imageRequests) {
    if (i.subjectRef) reference(contents, i.subjectRef, i.requestId);
    if (i.assetIdToReplace) reference(assets, i.assetIdToReplace, i.requestId);
  }
  return plan;
}
export function validateSite(site, { negocioId = site?.negocioId, allowedMediaHosts = [], capabilities = null } = {}) {
  validateContract('SiteSchemaV3', site); assertSectionRegistry(site.sections);
  fail(site.negocioId === negocioId, 'TENANT_MISMATCH', 403);
  const entities = [site.business, ...Object.values(site.content).flat(), ...site.media, ...site.actions, ...site.sections];
  uniqueEntities(entities, 'publicNamespace');
  const content = new Set([site.business.id, ...Object.values(site.content).flat().map(x => x.id)]);
  const typedContent = Object.fromEntries(Object.entries(site.content).map(([key, list]) => [key, new Set(list.map(x => x.id))]));
  const categories = new Set(site.content.categories.map(x => x.id));
  const media = new Set(site.media.map(x => x.id)), actions = new Set(site.actions.map(x => x.id));
  const cap = site.capabilities, wa = site.contact.whatsapp;
  if (capabilities) for (const key of Object.keys(cap)) fail(cap[key] !== true || capabilities[key] === true, 'UNAUTHORIZED_CAPABILITY', 422, [key]);
  if (wa.state === 'configured') {
    fail(wa.e164 === `+${wa.waDigits}`, 'INVALID_CONTACT', 422);
    const country = wa.waDigits.startsWith('52') ? 'MX' : wa.waDigits.startsWith('1') ? 'US' : null;
    fail(normalizeWhatsApp({ country, phone: wa.e164 }).state === 'configured', 'INVALID_CONTACT', 422);
  } else fail(wa.e164 === null && wa.waDigits === null, 'INVALID_CONTACT', 422);
  fail(!cap.whatsappLink || wa.state === 'configured', 'INVALID_CONTACT', 422);
  fail(!cap.cartToWhatsApp || (cap.whatsappLink && cap.catalog), 'INVALID_CAPABILITY_DEPENDENCY', 422);
  for (const o of site.content.offerings) {
    for (const r of o.categoryIds) reference(categories, r, o.id);
    for (const r of o.mediaIds) reference(media, r, o.id);
    for (const r of o.actionIds) reference(actions, r, o.id);
  }
  for (const c of site.content.categories) {
    if (c.mediaId) reference(media, c.mediaId, c.id);
    fail(site.content.offerings.some(o => o.categoryIds.includes(c.id)), 'EMPTY_CATEGORY', 422);
  }
  for (const p of site.content.projects) for (const r of p.mediaIds) reference(media, r, p.id);
  for (const s of site.sections) {
    for (const r of s.contentRefs) reference(content, r, s.id);
    for (const r of s.mediaIds) reference(media, r, s.id);
    for (const r of s.actionIds) reference(actions, r, s.id);
    const kind = ({ categories: 'categories', offerings: 'offerings', projects: 'projects', trust: 'trustItems', testimonials: 'testimonials', faq: 'faqs' })[s.type];
    if (kind && s.enabled) {
      fail(s.contentRefs.length > 0, 'EMPTY_SECTION', 422, [s.id]);
      for (const r of s.contentRefs) reference(typedContent[kind], r, s.id);
    }
    if (s.type === 'gallery' && s.enabled) fail(s.mediaIds.length > 0, 'EMPTY_SECTION', 422, [s.id]);
    if (s.type === 'hotelBooking') fail(cap.hotelCheckout, 'CAPABILITY_UNAVAILABLE', 422);
  }
  const sections = new Set(site.sections.filter(x => x.enabled).map(x => x.id));
  for (const n of site.navigation.items) reference(sections, n.sectionId, 'navigation');
  for (const r of [site.navigation.logoAssetId, site.seo.ogAssetId]) if (r) reference(media, r, 'publicMedia');
  for (const m of site.media) {
    fail(!(m.origin === 'ai_generated' && m.depiction === 'actual'), 'SYNTHETIC_EVIDENCE', 422);
    for (const c of m.crops) fail(c.x + c.width <= 1.000001 && c.y + c.height <= 1.000001, 'INVALID_CROP', 422);
    for (const v of m.variants) {
      let url;
      try { url = new URL(v.url); } catch { fail(false, 'UNAUTHORIZED_MEDIA_HOST', 422); }
      fail(url.protocol === 'https:' && !url.username && !url.password && (!url.port || url.port === '443') && allowedMediaHosts.includes(url.hostname), 'UNAUTHORIZED_MEDIA_HOST', 422);
      fail(Math.abs((v.width / v.height) / (m.width / m.height) - 1) < 0.03, 'INVALID_ASPECT_RATIO', 422);
      fail(v.width <= m.width && v.height <= m.height, 'UPSCALED_MEDIA', 422);
    }
  }
  for (const a of site.actions) {
    fail(a.enabled ? a.disabledReason === null : a.disabledReason !== null, 'INVALID_ACTION_STATE', 422);
    if (a.type === 'whatsapp') {
      fail(a.contactRef === 'contact.whatsapp' && a.messageTemplate !== null, 'INVALID_ACTION_DESTINATION', 422);
      if (a.enabled) fail(cap.whatsappLink && wa.state === 'configured', 'INVALID_CONTACT', 422);
      const variables = [...a.messageTemplate.matchAll(/\{\{(.*?)\}\}/g)].map(x => x[1]);
      fail(variables.every(v => ['business.name', 'product.title', 'service.title', 'cart.summary', 'reference.code'].includes(v)), 'UNSUPPORTED_MESSAGE_VARIABLE', 422);
      fail(!/[{}]/.test(a.messageTemplate.replace(/\{\{[^{}]+\}\}/g, '')), 'INVALID_MESSAGE_TEMPLATE', 422);
    } else if (a.enabled) {
      // Destination payloads for these types are not frozen in the source contract.
      // Keep them closed until their adapters and evidence gates exist.
      fail(false, 'ACTION_ADAPTER_NOT_IMPLEMENTED', 422, [a.type]);
    }
  }
  return site;
}
export function validateImageRequest(request, { negocioId, inputRevision, revisionId, sampleId, contentIds, reservation } = {}) {
  validateContract('ImageRequestV3', request);
  fail(request.negocioId === negocioId && request.sampleId === sampleId, 'TENANT_MISMATCH', 403);
  fail(request.expectedInputRevision === inputRevision && request.targetRevisionId === revisionId, 'STALE_INPUT', 409);
  if (request.subjectRef) reference(new Set(contentIds), request.subjectRef, 'image');
  fail(reservation?.id === request.budgetReservationId && reservation.negocioId === negocioId && reservation.sampleId === sampleId && reservation.requestId === request.requestId && reservation.inputRevision === inputRevision && reservation.revisionId === revisionId && reservation.state === 'reserved', 'BUDGET_NOT_RESERVED', 409);
  fail(Number.isSafeInteger(reservation.estimatedCostMinor) && reservation.estimatedCostMinor > 0, 'COST_NOT_CONFIGURED', 503);
  return request;
}
export function validateCommercialEvent(event, { site, sampleId, audience, variantId, now, serverEvidence = false }) {
  validateContract('CommercialEventV1', event);
  fail(event.negocioId === site.negocioId && event.revisionId === site.revisionId && event.sampleId === sampleId && event.audience === audience && event.variantId === variantId, 'EVENT_CONTEXT_MISMATCH', 403);
  fail(Number.isFinite(Date.parse(event.timestamp)) && Math.abs(Date.parse(event.timestamp) - now) <= 300000, 'INVALID_EVENT_TIME', 422);
  if (['sample_approved', 'payment_confirmed', 'message_sent_confirmed', 'conversation_confirmed'].includes(event.eventName)) fail(serverEvidence === true, 'SERVER_EVIDENCE_REQUIRED', 403);
  if (['whatsapp_cta_clicked', 'whatsapp_navigation_attempted'].includes(event.eventName)) {
    fail(event.actionId && event.ctaLocation && event.pageViewId, 'INCOMPLETE_CLICK_CONTEXT', 422);
    const action = site.actions.find(a => a.id === event.actionId);
    fail(action?.enabled && action.type === 'whatsapp' && site.capabilities.whatsappLink, 'ACTION_UNAVAILABLE', 422);
  }
  if (event.productId || event.serviceId) {
    fail(!(event.productId && event.serviceId), 'AMBIGUOUS_EVENT_SUBJECT', 422);
    const o = site.content.offerings.find(x => x.id === (event.productId || event.serviceId));
    fail(o && o.kind === (event.productId ? 'product' : 'service') && (!event.actionId || o.actionIds.includes(event.actionId)), 'INVALID_EVENT_SUBJECT', 422);
  }
  fail(event.consentState === 'analytics_allowed' || serverEvidence, 'ANALYTICS_NOT_ALLOWED', 403);
  return event;
}
export function cartStorageKey(negocioId, revisionId) {
  fail(/^[A-Za-z][\w-]{0,119}$/.test(negocioId) && /^[A-Za-z][\w-]{0,119}$/.test(revisionId), 'INVALID_ID');
  return `nw:v3:cart:${negocioId}:${revisionId}`;
}
export function buildWhatsAppLink(site, actionId, { productId, serviceId, quantities, referenceCode = '' } = {}) {
  validateSite(site);
  const a = site.actions.find(x => x.id === actionId);
  if (!a?.enabled || a.type !== 'whatsapp' || !site.capabilities.whatsappLink) return null;
  const values = { 'business.name': site.business.name, 'reference.code': referenceCode };
  for (const [id, kind, variable] of [[productId, 'product', 'product.title'], [serviceId, 'service', 'service.title']]) {
    if (id) {
      const o = site.content.offerings.find(x => x.id === id && x.kind === kind && x.actionIds.includes(actionId));
      fail(o, 'INVALID_ACTION_SUBJECT', 422); values[variable] = o.title;
    }
  }
  if (quantities) {
    fail(site.capabilities.cartToWhatsApp && Array.isArray(quantities) && quantities.length > 0 && quantities.length <= 100, 'CART_UNAVAILABLE', 422);
    const seen = new Set();
    values['cart.summary'] = quantities.map(({ offeringId, quantity }) => {
      fail(Number.isSafeInteger(quantity) && quantity > 0 && quantity <= 999 && !seen.has(offeringId), 'INVALID_QUANTITY', 422); seen.add(offeringId);
      const o = site.content.offerings.find(x => x.id === offeringId);
      fail(o, 'INVALID_ACTION_SUBJECT', 422); return `${quantity} × ${o.title}`;
    }).join('\n');
  }
  const message = a.messageTemplate.replace(/\{\{(.*?)\}\}/g, (_, key) => { fail(typeof values[key] === 'string' && values[key].length > 0, 'MISSING_MESSAGE_CONTEXT', 422); return values[key]; });
  fail(message.length <= (quantities ? 3000 : 1000), 'MESSAGE_TOO_LONG', 422);
  return `https://wa.me/${site.contact.whatsapp.waDigits}?text=${encodeURIComponent(message)}`;
}
