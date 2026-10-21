// Portable DTOs. No Firebase, environment loading, provider or framework imports.
export const VERSIONS = Object.freeze({ brief: '1.0.0', plan: '1.0.0', site: '3.0.0', image: '1.0.0', event: '1.0.0', registry: 'phase0.1' });
export const SECTION_REGISTRY = Object.freeze({
  hero: ['product-focus', 'split-portrait', 'editorial-landscape', 'text-first'],
  categories: ['image-tiles', 'compact-list'], offerings: ['catalog-grid', 'service-list', 'menu-list'],
  projects: ['editorial-grid'], process: ['steps'], gallery: ['masonry-lite', 'horizontal-preview'],
  trust: ['verified-facts'], testimonials: ['quotes'], faq: ['accordion'], contact: ['compact'],
  finalCta: ['compact'], hotelBooking: ['integrated'],
});
export const CATEGORIES = ['hardware_store', 'restaurant', 'beauty_salon', 'law_firm', 'dental_clinic', 'hotel', 'repair_shop', 'retail_store', 'construction', 'general'];
const str = (maxLength = 3000, minLength = 0, pattern) => ({ type: 'string', minLength, maxLength, ...(pattern ? { pattern } : {}) });
const en = (...values) => ({ enum: values });
const integer = (minimum = 0, maximum = Number.MAX_SAFE_INTEGER) => ({ type: 'integer', minimum, maximum });
const num = (minimum = 0, maximum = 1) => ({ type: 'number', minimum, maximum });
const bool = { type: 'boolean' };
const lit = value => ({ const: value });
const nullable = schema => ({ anyOf: [schema, { type: 'null' }] });
const array = (items, maxItems = 100, minItems = 0, uniqueItems = false) => ({ type: 'array', items, maxItems, minItems, ...(uniqueItems ? { uniqueItems: true } : {}) });
const obj = (properties, optional = []) => ({ type: 'object', properties, required: Object.keys(properties).filter(k => !optional.includes(k)), additionalProperties: false });
const id = str(120, 1, '^[A-Za-z][A-Za-z0-9_-]*$');
const sectionId = str(64, 1, '^[a-z][a-z0-9-]*$');
const ids = array(id, 100, 0, true);
const goals = en('quote', 'inquiry', 'order_request', 'appointment_request', 'availability_inquiry');
const family = en('info', 'ecommerce', 'booking');
const category = en(...CATEGORIES);
const styles = en('trade-clean', 'warm-local', 'professional-calm', 'editorial-stay');
const density = en('compact', 'balanced', 'rich');
const purpose = en('hero', 'product', 'category', 'context', 'service', 'environment', 'decorative');
const point = obj({ x: num(), y: num() });
const crop = obj({ purpose, x: num(), y: num(), width: num(0.000001), height: num(0.000001) });
const money = obj({ amountMinor: integer(), currency: str(3, 3, '^[A-Z]{3}$') });
const price = obj({ ...money.properties, sourceFactId: id });
const categoryValue = obj({ label: str(120, 1) });
const offeringValue = obj({ kind: en('product', 'service'), title: str(160, 1), description: str(1200), categoryIds: ids });
const whatsappValue = obj({ country: en('MX', 'US', 'CA', 'ES', 'CO', 'AR', 'CL', 'PE'), phone: str(40, 1) });
const hourValue = obj({ days: array(en('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'), 7, 1, true), opens: str(5, 5, '^([01][0-9]|2[0-3]):[0-5][0-9]$'), closes: str(5, 5, '^([01][0-9]|2[0-3]):[0-5][0-9]$') });
const factKey = en('name', 'description', 'offering', 'category', 'price', 'whatsapp', 'email', 'address', 'hours', 'testimonial', 'certification', 'guarantee', 'delivery', 'stock', 'payment', 'project', 'process', 'faq');
const factValue = { anyOf: [str(3000), bool, num(-1e12, 1e12), { type: 'null' }, money, offeringValue, categoryValue, whatsappValue, array(hourValue, 14)] };
const fact = obj({ id, subjectRef: id, key: factKey, value: factValue, origin: en('owner_declared', 'legacy_unverified') });
const briefOffering = obj({ id, ...offeringValue.properties, price: nullable(money), mediaIds: ids });
const brief = obj({
  briefVersion: lit(VERSIONS.brief), expectedInputRevision: integer(),
  business: obj({ name: str(120, 1), description: str(3000), declaredCategory: category, locale: lit('es-MX') }, ['declaredCategory']),
  goal: goals, offerings: array(briefOffering),
  contactDraft: obj({ country: nullable(whatsappValue.properties.country), phone: nullable(str(40)), email: nullable(str(254)), address: nullable(str(500)) }),
  assetIds: array(id, 12, 0, true), facts: array(fact, 300),
  requestedBrand: obj({ logoAssetId: nullable(id), primaryColor: str(7, 7, '^#[0-9A-Fa-f]{6}$'), headingFont: en('Sora', 'Manrope', 'Inter'), bodyFont: en('Sora', 'Manrope', 'Inter') }),
}, ['requestedBrand']);
const planSection = obj({ id: sectionId, type: en(...Object.keys(SECTION_REGISTRY)), variant: str(60, 1), contentRefs: ids, actionIds: array(id, 3, 0, true) });
const imageNeed = obj({ requestId: id, purpose, subjectRef: nullable(id), priority: en('required', 'optional'), assetIdToReplace: nullable(id) });
const plan = obj({
  planVersion: lit(VERSIONS.plan), expectedInputRevision: integer(), businessCategory: category,
  functionalFamily: family, primaryOfferRef: nullable(id), primaryGoal: goals, styleId: styles,
  heroVariant: en(...SECTION_REGISTRY.hero), density, sections: array(planSection, 9, 2),
  omittedSections: array(obj({ type: en(...Object.keys(SECTION_REGISTRY)), reason: en('missing_evidence', 'not_relevant', 'capability_unavailable', 'content_budget', 'user_hidden') }), 16),
  imageRequests: array(imageNeed, 4), primaryActionId: id, secondaryActionId: nullable(id), missingFactKeys: array(str(100, 1), 40, 0, true),
});
const offering = obj({ id, ...offeringValue.properties, price: nullable(price), mediaIds: ids, actionIds: array(id, 3, 0, true) });
const action = obj({ id, type: en('whatsapp', 'anchor', 'phone', 'email', 'hotel_checkout', 'lead_form'), label: str(100, 1), intent: goals, contactRef: nullable(str(120, 1)), messageTemplate: nullable(str(1000, 1)), enabled: bool, disabledReason: nullable(en('missing_contact', 'invalid_contact', 'capability_unavailable')) });
const media = obj({
  id, origin: en('uploaded', 'stock', 'ai_generated'), depiction: en('actual', 'illustrative', 'decorative'), alt: str(300, 1),
  width: integer(1, 20000), height: integer(1, 20000),
  variants: array(obj({ url: str(2048, 1, '^https://'), width: integer(1, 20000), height: integer(1, 20000), mime: en('image/jpeg', 'image/webp', 'image/avif', 'image/png'), bytes: integer(1, 20000000) }), 8, 1),
  focalPoint: point, crops: array(crop, 12),
});
const capabilities = obj({ catalog: bool, whatsappLink: bool, cartToWhatsApp: bool, appointmentRequest: bool, hotelCheckout: bool, leadForm: bool });
const site = obj({
  contractVersion: lit(VERSIONS.site), designVersion: lit('v3'), rendererVersion: lit('3.0.0'), negocioId: id, revisionId: id, revision: integer(1), templateId: family,
  business: obj({ id: lit('business'), name: str(120, 1), category, locale: lit('es-MX'), description: str(3000) }),
  theme: obj({ styleId: styles, density, primaryColor: str(7, 7, '^#[0-9A-Fa-f]{6}$'), surfaceMode: en('light', 'dark'), headingFont: en('Sora', 'Manrope', 'Inter'), bodyFont: en('Sora', 'Manrope', 'Inter') }),
  contact: obj({
    whatsapp: obj({ state: en('configured', 'missing', 'invalid'), e164: nullable(str(16, 9, '^\\+[1-9][0-9]{7,14}$')), waDigits: nullable(str(15, 8, '^[1-9][0-9]{7,14}$')) }),
    email: nullable(str(254, 3)), address: nullable(obj({ text: str(500, 1), sourceFactId: id })),
    hours: array(obj({ ...hourValue.properties, sourceFactId: id }), 14),
    socialLinks: array(obj({ platform: en('facebook', 'instagram', 'tiktok', 'youtube', 'linkedin'), url: str(2048, 1, '^https://'), sourceFactId: id }), 8),
  }), capabilities,
  content: obj({ offerings: array(offering), categories: array(obj({ id, label: str(120, 1), mediaId: nullable(id) }), 30),
    trustItems: array(obj({ id, label: str(500, 1), factId: id, iconId: en('check', 'location', 'shield', 'clock') }), 20),
    testimonials: array(obj({ id, text: str(1200, 1), authorLabel: str(120, 1), evidenceId: id }), 20),
    faqs: array(obj({ id, question: str(200, 1), answer: str(1200, 1), factIds: ids }), 40),
    projects: array(obj({ id, title: str(160, 1), description: str(1200), mediaIds: ids, factIds: ids }), 30),
  }), media: array(media, 12), actions: array(action, 30),
  sections: array(obj({ ...planSection.properties, enabled: bool, mediaIds: array(id, 12, 0, true), copy: obj({ title: str(160), subtitle: str(500), body: str(1200) }, ['title', 'subtitle', 'body']) }), 9, 2),
  navigation: obj({ logoAssetId: nullable(id), items: array(obj({ label: str(60, 1), sectionId }), 9) }),
  seo: obj({ title: str(160, 1), description: str(500), indexing: en('sample_noindex', 'published'), ogAssetId: nullable(id) }),
});
const image = obj({
  imageRequestVersion: lit(VERSIONS.image), requestId: id, negocioId: id, sampleId: id, expectedInputRevision: integer(), targetRevisionId: id,
  purpose, subjectRef: nullable(id), depiction: en('illustrative', 'decorative'),
  brief: obj({ businessCategory: category, subject: str(500, 1), visualObjective: str(500, 1), photographicStyle: str(200, 1), composition: str(500, 1), lighting: str(200, 1), palette: array(str(80, 1), 8), aspectRatio: en('1:1', '4:3', '3:2', '16:9', '3:4'), focalPoint: point, textSafeArea: nullable(en('left', 'right', 'top', 'bottom')), restrictions: array(str(300, 1), 20, 1) }),
  budgetReservationId: id, idempotencyKey: str(200, 1),
});
const event = obj({
  eventVersion: lit(VERSIONS.event), eventId: id, negocioId: id, sampleId: nullable(id), revisionId: id, designVersion: lit('v3'), variantId: nullable(id),
  eventName: en('sample_page_viewed', 'whatsapp_cta_clicked', 'whatsapp_navigation_attempted', 'catalog_explored', 'sample_approved', 'payment_confirmed', 'message_sent_confirmed', 'conversation_confirmed'),
  ctaLocation: nullable(en('hero', 'float', 'mobile_bar', 'product', 'service', 'cart', 'contact')), actionId: nullable(id), productId: nullable(id), serviceId: nullable(id),
  timestamp: str(24, 24, '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$'), audience: en('sample_owner', 'business_visitor'), pageViewId: nullable(id), consentState: en('analytics_allowed', 'essential_only', 'denied'),
});
export const CONTRACTS = Object.freeze(Object.fromEntries(Object.entries({ BusinessBriefV3: brief, CreativePlanV3: plan, SiteSchemaV3: site, ImageRequestV3: image, CommercialEventV1: event }).map(([name, schema]) => [name, { $schema: 'https://json-schema.org/draft/2020-12/schema', $id: `urn:negociosweb:${name}`, ...schema }])));

export class SiteV3Error extends Error {
  constructor(code, status = 400, details = []) { super(code); this.name = 'SiteV3Error'; this.code = code; this.status = status; this.details = details; }
}
export const fail = (condition, code, status = 400, details = []) => { if (!condition) throw new SiteV3Error(code, status, details); };

// Deliberately implements only the keywords used above, not a general JSON Schema engine.
function check(schema, value, path, errors) {
  const bad = reason => errors.push(`${path}: ${reason}`);
  if (schema.anyOf) {
    if (!schema.anyOf.some(s => { const local = []; check(s, value, path, local); return local.length === 0; })) bad('anyOf');
    return;
  }
  if ('const' in schema && value !== schema.const) bad('const');
  if (schema.enum && !schema.enum.includes(value)) bad('enum');
  switch (schema.type) {
    case 'null': if (value !== null) bad('null'); break;
    case 'boolean': if (typeof value !== 'boolean') bad('boolean'); break;
    case 'integer': case 'number':
      if (typeof value !== 'number' || !Number.isFinite(value) || (schema.type === 'integer' && !Number.isSafeInteger(value))) bad(schema.type);
      else if (value < schema.minimum || value > schema.maximum) bad('range');
      break;
    case 'string':
      if (typeof value !== 'string') { bad('string'); break; }
      if (value.length < schema.minLength || value.length > schema.maxLength) bad('length');
      if (schema.pattern && !new RegExp(schema.pattern).test(value)) bad('pattern');
      if (/[<>\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(value) || /(?:javascript|vbscript|data):/i.test(value)) bad('unsafe_text');
      break;
    case 'array':
      if (!Array.isArray(value)) { bad('array'); break; }
      if (value.length < schema.minItems || value.length > schema.maxItems) bad('items');
      if (schema.uniqueItems && new Set(value.map(x => JSON.stringify(x))).size !== value.length) bad('uniqueItems');
      value.forEach((v, i) => check(schema.items, v, `${path}/${i}`, errors));
      break;
    case 'object':
      if (!value || Array.isArray(value) || typeof value !== 'object' || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) { bad('object'); break; }
      for (const key of schema.required) if (!Object.hasOwn(value, key)) errors.push(`${path}/${key}: required`);
      for (const key of Object.keys(value)) {
        if (!Object.hasOwn(schema.properties, key)) errors.push(`${path}/${key}: additionalProperty`);
        else check(schema.properties[key], value[key], `${path}/${key}`, errors);
      }
      break;
  }
}
function bounded(value, depth = 0, seen = new WeakSet()) {
  fail(depth <= 24, 'PAYLOAD_TOO_DEEP');
  if (!value || typeof value !== 'object') return;
  fail(!seen.has(value), 'CYCLIC_PAYLOAD'); seen.add(value);
  fail(Object.keys(value).length <= 1000, 'PAYLOAD_TOO_WIDE');
  for (const item of Object.values(value)) bounded(item, depth + 1, seen);
  seen.delete(value);
}
export function validateContract(name, value) {
  fail(Object.hasOwn(CONTRACTS, name), 'UNKNOWN_CONTRACT');
  bounded(value);
  let encoded;
  try { encoded = JSON.stringify(value); } catch { throw new SiteV3Error('INVALID_JSON'); }
  fail(typeof encoded === 'string', 'INVALID_JSON', 422);
  fail(new TextEncoder().encode(encoded).length <= 250000, 'PAYLOAD_TOO_LARGE', 413);
  const errors = []; check(CONTRACTS[name], value, name, errors);
  fail(errors.length === 0, 'INVALID_CONTRACT', 422, errors.slice(0, 30));
  return value;
}
export function uniqueEntities(entities, scope = 'entities') {
  const values = entities.map(x => x.id);
  fail(new Set(values).size === values.length, 'DUPLICATE_ID', 422, [scope]);
}
export function assertSectionRegistry(sections) {
  uniqueEntities(sections, 'sections');
  for (const s of sections) fail(SECTION_REGISTRY[s.type]?.includes(s.variant), 'UNSUPPORTED_SECTION_VARIANT', 422, [s.id]);
  const active = sections.filter(s => s.enabled !== false);
  fail(active[0]?.type === 'hero' && ['contact', 'finalCta'].includes(active.at(-1)?.type), 'INVALID_SECTION_ORDER', 422);
  fail(new Set(active.map(s => s.type)).size === active.length, 'DUPLICATE_SECTION_TYPE', 422);
}
