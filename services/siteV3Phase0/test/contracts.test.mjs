import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { CONTRACTS, validateContract } from '../contracts.mjs';
import { artifacts } from '../export-contracts.mjs';
import { assertPublishable, compileFactualSite } from '../facts.mjs';
import { buildWhatsAppLink, cartStorageKey, normalizeWhatsApp, validateBrief, validateCommercialEvent, validateImageRequest, validatePlan, validateSite } from '../validation.mjs';
import { rendererDecision } from '../compatibility.mjs';
import { BIZ, candidate, errorCode, fixture, prepared, queued } from './helpers.mjs';

const fixtures = { BusinessBriefV3: 'business-brief', CreativePlanV3: 'creative-plan', SiteSchemaV3: 'site-from-design-document', ImageRequestV3: 'image-request', CommercialEventV1: 'commercial-event' };
for (const [name, file] of Object.entries(fixtures)) {
  test(`${name}: document fixture passes structural validation`, () => validateContract(name, fixture(file)));
  test(`${name}: additional root fields cannot become authority`, () => {
    const data = fixture(file); data.verified = true;
    assert.throws(() => validateContract(name, data), errorCode('INVALID_CONTRACT'));
  });
}
test('exported JSON Schemas and TypeScript types match executable definitions', async () => {
  for (const [name, value] of Object.entries(artifacts())) assert.equal(await readFile(new URL(`../${name}`, import.meta.url), 'utf8'), value);
  assert.equal(Object.keys(CONTRACTS).length, 5);
});
test('versions, nested private fields, executable markup and oversized/deep payloads are rejected', () => {
  for (const change of [b => { b.briefVersion = '2.0.0'; }, b => { b.contactDraft.leadPhone = 'private'; }, b => { b.business.description = '<img onerror=alert(1)>'; }, b => { b.business.name = 'javascript:alert(1)'; }]) {
    const b = fixture('business-brief'); change(b); assert.throws(() => validateBrief(b), errorCode('INVALID_CONTRACT'));
  }
  assert.throws(() => validateBrief({ text: 'x'.repeat(250001) }), errorCode('PAYLOAD_TOO_LARGE'));
  let deep = {}; for (let i = 0; i < 30; i++) deep = { deep };
  assert.throws(() => validateBrief(deep), errorCode('PAYLOAD_TOO_DEEP'));
});
test('duplicate offering IDs and unregistered input assets are rejected', () => {
  const b = fixture('business-brief'); b.offerings.push(structuredClone(b.offerings[0]));
  assert.throws(() => validateBrief(b), errorCode('DUPLICATE_ID'));
  b.offerings.pop(); b.offerings[0].mediaIds = ['unknown'];
  assert.throws(() => validateBrief(b), errorCode('BROKEN_REFERENCE'));
});
const planContext = { inputRevision: 1, contentIds: ['business', 'offer-cemento', 'cat-materiales'], actionIds: ['wa-quote', 'wa-product'] };
test('plan references, section variants and hero order are checked against server registry', () => {
  validatePlan(fixture('creative-plan'), planContext);
  for (const [change, code] of [
    [p => { p.sections[1].contentRefs = ['missing']; }, 'BROKEN_REFERENCE'],
    [p => { p.sections[0].variant = 'free-html'; }, 'UNSUPPORTED_SECTION_VARIANT'],
    [p => p.sections.reverse(), 'INVALID_SECTION_ORDER'],
    [p => { p.expectedInputRevision = 2; }, 'STALE_INPUT'],
    [p => { p.sections[1].id = 'inicio'; }, 'DUPLICATE_ID'],
  ]) { const p = fixture('creative-plan'); change(p); assert.throws(() => validatePlan(p, planContext), errorCode(code)); }
});
test('a plan cannot acquire operational capabilities', () => {
  const p = fixture('creative-plan'); p.sections[1] = { id: 'reserva', type: 'hotelBooking', variant: 'integrated', contentRefs: [], actionIds: [] };
  assert.throws(() => validatePlan(p, planContext), errorCode('CAPABILITY_UNAVAILABLE'));
  p.capabilities = { hotelCheckout: true };
  assert.throws(() => validatePlan(p, planContext), errorCode('INVALID_CONTRACT'));
});
test('site fixture has no working WhatsApp href or private phone fallback', () => {
  const site = fixture('site-from-design-document'); validateSite(site);
  assert.equal(buildWhatsAppLink(site, 'wa-quote'), null);
  assert.equal(normalizeWhatsApp({ country: 'MX', phone: '123' }).state, 'invalid');
  assert.equal(normalizeWhatsApp({ country: 'MX', phone: '+5211111111111' }).state, 'invalid');
  assert.equal(normalizeWhatsApp({ country: 'US', phone: '+1 202 555 0100' }).e164, '+12025550100');
  assert.equal(normalizeWhatsApp({ country: 'US', phone: 'javascript:2025550100' }).state, 'invalid');
  assert.equal(normalizeWhatsApp({ country: 'ES', phone: '+34000000000' }).state, 'invalid');
});
test('site semantics reject missing refs, capability escalation and invalid destination', () => {
  for (const [change, code] of [
    [s => { s.sections[0].contentRefs = ['missing']; }, 'BROKEN_REFERENCE'],
    [s => { s.capabilities.whatsappLink = true; }, 'INVALID_CONTACT'],
    [s => { s.navigation.items[0].sectionId = 'missing'; }, 'BROKEN_REFERENCE'],
    [s => { s.actions[0].messageTemplate = '{{lead.phone}}'; }, 'UNSUPPORTED_MESSAGE_VARIABLE'],
    [s => { s.actions[0].type = 'hotel_checkout'; s.actions[0].enabled = true; s.actions[0].disabledReason = null; }, 'ACTION_ADAPTER_NOT_IMPLEMENTED'],
  ]) { const s = fixture('site-from-design-document'); change(s); assert.throws(() => validateSite(s), errorCode(code)); }
});
function media() { return { id: 'asset-demo', origin: 'uploaded', depiction: 'actual', alt: 'Imagen ficticia de fixture', width: 800, height: 600, variants: [{ url: 'https://assets.example.invalid/demo.jpg', width: 400, height: 300, mime: 'image/jpeg', bytes: 5000 }], focalPoint: { x: 0.5, y: 0.5 }, crops: [] }; }
test('media provenance, CDN allowlist, aspect and crop bounds are enforced without downloads', () => {
  const s = fixture('site-from-design-document'); s.media = [media()];
  assert.throws(() => validateSite(s), errorCode('UNAUTHORIZED_MEDIA_HOST'));
  const context = { allowedMediaHosts: ['assets.example.invalid'] }; validateSite(s, context);
  s.media[0].origin = 'ai_generated'; assert.throws(() => validateSite(s, context), errorCode('SYNTHETIC_EVIDENCE'));
  s.media[0].depiction = 'illustrative'; s.media[0].crops = [{ purpose: 'hero', x: 0.9, y: 0, width: 0.2, height: 1 }];
  assert.throws(() => validateSite(s, context), errorCode('INVALID_CROP'));
});
const imageContext = r => ({ negocioId: BIZ, inputRevision: 1, revisionId: r.targetRevisionId, sampleId: r.sampleId, contentIds: ['cat-materiales'], reservation: { id: r.budgetReservationId, negocioId: BIZ, sampleId: r.sampleId, requestId: r.requestId, inputRevision: 1, revisionId: r.targetRevisionId, state: 'reserved', estimatedCostMinor: 1 } });
test('image contracts require same tenant, revision and cost reservation; no provider is called', () => {
  const r = fixture('image-request'), context = imageContext(r); validateImageRequest(r, context);
  assert.throws(() => validateImageRequest(r, { ...context, negocioId: 'another' }), errorCode('TENANT_MISMATCH'));
  assert.throws(() => validateImageRequest(r, { ...context, inputRevision: 2 }), errorCode('STALE_INPUT'));
  assert.throws(() => validateImageRequest(r, { ...context, reservation: null }), errorCode('BUDGET_NOT_RESERVED'));
  context.reservation.estimatedCostMinor = null;
  assert.throws(() => validateImageRequest(r, context), errorCode('COST_NOT_CONFIGURED'));
});
for (const family of ['info', 'ecommerce', 'booking']) test(`factual compiler preserves ${family} family without fabricating testimonials/prices/hours`, async () => {
  const h = await prepared({ family }), { site } = await candidate(h, await queued(h));
  assert.equal(site.templateId, family); assert.equal(site.content.offerings[0].price, null);
  assert.deepEqual(site.content.testimonials, []); assert.deepEqual(site.content.trustItems, []); assert.deepEqual(site.contact.hours, []);
  assert.equal(site.capabilities.hotelCheckout, false); assert.equal(site.seo.indexing, 'sample_noindex');
});
test('owner confirmation is required; legacy-unverified and another tenant evidence confer no authority', async () => {
  const h = await prepared(), p = await h.store.get(`SiteV3Projects/${BIZ}`);
  const args = { ...p, revisionId: 'rev_demo', revision: 1 };
  assert.throws(() => compileFactualSite({ ...args, facts: [] }), errorCode('BUSINESS_NAME_NOT_CONFIRMED'));
  assert.throws(() => compileFactualSite({ ...args, facts: p.facts.map(f => ({ ...f, negocioId: 'other' })) }), errorCode('BUSINESS_NAME_NOT_CONFIRMED'));
});
for (const [label, mutate] of [
  ['testimonial', s => s.content.testimonials.push({ id: 'fake-review', text: 'Excelente', authorLabel: 'Cliente', evidenceId: 'invented' })],
  ['price', s => { s.content.offerings[0].price = { amountMinor: 1, currency: 'MXN', sourceFactId: 'invented' }; }],
  ['hours', s => { s.contact.hours = [{ days: ['mon'], opens: '09:00', closes: '18:00', sourceFactId: 'invented' }]; }],
  ['certification', s => { s.sections[0].copy.subtitle = 'Certificación internacional'; }],
  ['guarantee', s => { s.business.description = 'Garantía de por vida'; }],
  ['delivery', s => { s.seo.description = 'Entrega nacional mañana'; }],
  ['stock', s => { s.content.offerings[0].description = 'Disponible hoy'; }],
]) test(`publication rejects unconfirmed ${label}, including free copy/SEO surfaces`, async () => {
  const h = await prepared(), { site } = await candidate(h, await queued(h)); mutate(site);
  const p = await h.store.get(`SiteV3Projects/${BIZ}`);
  assert.throws(() => assertPublishable(site, p), errorCode('UNCONFIRMED_PUBLIC_CONTENT'));
});
test('confirmed prices are retained with their private evidence key', async () => {
  const h = await prepared({ price: true }), { site } = await candidate(h, await queued(h));
  assert.deepEqual(site.content.offerings[0].price, { amountMinor: 25000, currency: 'MXN', sourceFactId: 'price-offer-cemento' });
});
test('valid synthetic WhatsApp URLs encode once, bind offering IDs and isolate carts', async () => {
  const h = await prepared({ phone: true }), { site } = await candidate(h, await queued(h));
  const link = buildWhatsAppLink(site, 'wa-product', { productId: 'offer-cemento' });
  assert.equal(new URL(link).hostname, 'wa.me'); assert.equal(new URL(link).pathname, '/12025550100');
  assert.equal(new URL(link).searchParams.get('text'), `Hola, me gustaría consultar: ${site.content.offerings[0].title}.`);
  assert.throws(() => buildWhatsAppLink(site, 'wa-product', { productId: 'unknown' }), errorCode('INVALID_ACTION_SUBJECT'));
  const cart = buildWhatsAppLink(site, 'wa-cart', { quantities: [{ offeringId: 'offer-cemento', quantity: 2 }] });
  assert.ok(new URL(cart).searchParams.get('text').includes('2 × Cemento'));
  assert.throws(() => buildWhatsAppLink(site, 'wa-cart', { quantities: [{ offeringId: 'offer-cemento', quantity: 0 }] }), errorCode('INVALID_QUANTITY'));
  assert.notEqual(cartStorageKey(BIZ, 'rev_a'), cartStorageKey(BIZ, 'rev_b'));
  assert.notEqual(cartStorageKey(BIZ, 'rev_a'), cartStorageKey('another', 'rev_a'));
});
test('event cannot declare a sent message/payment from an ordinary WhatsApp click', async () => {
  const h = await prepared({ phone: true }), { site } = await candidate(h, await queued(h));
  const e = { ...fixture('commercial-event'), revisionId: site.revisionId };
  const context = { site, sampleId: e.sampleId, audience: e.audience, variantId: e.variantId, now: h.now() };
  validateCommercialEvent(e, context);
  e.eventName = 'payment_confirmed'; assert.throws(() => validateCommercialEvent(e, context), errorCode('SERVER_EVIDENCE_REQUIRED'));
  e.eventName = 'whatsapp_app_opened'; assert.throws(() => validateCommercialEvent(e, context), errorCode('INVALID_CONTRACT'));
});
test('renderer preparation keeps v3 out of legacy and does not enable a nonexistent bundle', () => {
  assert.deepEqual(rendererDecision({}), { kind: 'legacy', designVersion: 'v1' });
  assert.equal(rendererDecision({ designVersion: 'v2' }).designVersion, 'v2');
  assert.equal(rendererDecision({ schema: { designVersion: 'v3' } }).kind, 'preparing');
  assert.equal(rendererDecision({ designVersion: 'v4' }).kind, 'unsupported');
});
test('test process forbids outbound network even if an adapter attempts it', () => {
  assert.throws(() => fetch('https://example.invalid'), /NETWORK_DISABLED_DURING_PHASE0_TESTS/);
});
test('a typed section cannot silently consume a different entity or an empty list', () => {
  const s = fixture('site-from-design-document'); s.sections[1].contentRefs = ['offer-cemento'];
  assert.throws(() => validateSite(s), errorCode('BROKEN_REFERENCE'));
  s.sections[1].contentRefs = [];
  assert.throws(() => validateSite(s), errorCode('EMPTY_SECTION'));
});
