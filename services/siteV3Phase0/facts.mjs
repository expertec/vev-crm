import { createHash } from 'node:crypto';
import { fail, uniqueEntities } from './contracts.mjs';
import { normalizeWhatsApp, validateBrief, validateSite } from './validation.mjs';

export function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value !== null && typeof value === 'object') return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}`;
  return JSON.stringify(value);
}
export const digest = value => createHash('sha256').update(canonical(value)).digest('hex');
export function proposeFacts(brief) {
  validateBrief(brief);
  const facts = [
    { id: 'fact-business-name', subjectRef: 'business', key: 'name', value: brief.business.name, origin: 'owner_declared' },
    { id: 'fact-business-description', subjectRef: 'business', key: 'description', value: brief.business.description, origin: 'owner_declared' },
    ...brief.offerings.flatMap(o => [
      { id: `fact-${o.id}`, subjectRef: o.id, key: 'offering', value: { kind: o.kind, title: o.title, description: o.description, categoryIds: o.categoryIds }, origin: 'owner_declared' },
      ...(o.price ? [{ id: `price-${o.id}`, subjectRef: o.id, key: 'price', value: o.price, origin: 'owner_declared' }] : []),
    ]),
  ];
  if (brief.contactDraft.phone && brief.contactDraft.country) facts.push({ id: 'fact-whatsapp', subjectRef: 'business', key: 'whatsapp', value: { country: brief.contactDraft.country, phone: brief.contactDraft.phone }, origin: 'owner_declared' });
  for (const key of ['email', 'address']) if (brief.contactDraft[key]) facts.push({ id: `fact-${key}`, subjectRef: 'business', key, value: brief.contactDraft[key], origin: 'owner_declared' });
  facts.push(...structuredClone(brief.facts));
  uniqueEntities(facts, 'proposedFacts');
  fail(new Set(facts.map(f => `${f.subjectRef}:${f.key}`)).size === facts.length, 'AMBIGUOUS_FACT', 422);
  return facts;
}
function factReader(facts, negocioId) {
  const confirmed = facts.filter(f => f.negocioId === negocioId && ['user_confirmed', 'reviewed_document', 'authorized_integration'].includes(f.source) && f.actorId && Number.isFinite(f.confirmedAt));
  uniqueEntities(confirmed, 'confirmedFacts');
  fail(new Set(confirmed.map(f => `${f.subjectRef}:${f.key}`)).size === confirmed.length, 'AMBIGUOUS_FACT', 422);
  return (subject, key) => confirmed.find(f => f.subjectRef === subject && f.key === key);
}
export const PHASE0_GRANTS = Object.freeze({ catalog: true, whatsappLink: false, cartToWhatsApp: false, appointmentRequest: false, hotelCheckout: false, leadForm: false });

// No AI output is accepted by this compiler. Confirmation lives in the private
// repository, never in a boolean supplied with a browser/model schema.
export function compileFactualSite({ negocioId, revisionId, revision, brief, facts, grants = PHASE0_GRANTS }) {
  validateBrief(brief);
  const fact = factReader(facts, negocioId);
  const nameFact = fact('business', 'name');
  fail(nameFact?.value === brief.business.name, 'BUSINESS_NAME_NOT_CONFIRMED', 422);
  const descriptionFact = fact('business', 'description');
  const description = descriptionFact?.value === brief.business.description ? descriptionFact.value : '';
  const catFacts = facts.filter(f => f.key === 'category' && fact(f.subjectRef, 'category') === f);
  const offerings = brief.offerings.flatMap(o => {
    const confirmed = fact(o.id, 'offering');
    const expected = { kind: o.kind, title: o.title, description: o.description, categoryIds: o.categoryIds };
    if (!confirmed || canonical(confirmed.value) !== canonical(expected)) return [];
    const price = fact(o.id, 'price');
    return [{ ...expected, id: o.id, categoryIds: o.categoryIds.filter(id => catFacts.some(f => f.subjectRef === id)),
      price: price && o.price && canonical(price.value) === canonical(o.price) ? { ...price.value, sourceFactId: price.id } : null,
      mediaIds: [], actionIds: ['wa-product'] }];
  });
  const categories = catFacts.filter(f => offerings.some(o => o.categoryIds.includes(f.subjectRef))).map(f => ({ id: f.subjectRef, label: f.value.label, mediaId: null }));
  const whatsappFact = fact('business', 'whatsapp');
  const whatsapp = normalizeWhatsApp(whatsappFact?.value);
  const waEnabled = grants.whatsappLink === true && whatsapp.state === 'configured';
  const caps = { ...PHASE0_GRANTS, catalog: grants.catalog === true, whatsappLink: waEnabled, cartToWhatsApp: waEnabled && grants.catalog === true && grants.cartToWhatsApp === true };
  const action = (id, label, messageTemplate) => ({ id, type: 'whatsapp', label, intent: brief.goal, contactRef: 'contact.whatsapp', messageTemplate, enabled: waEnabled, disabledReason: waEnabled ? null : whatsapp.state === 'invalid' ? 'invalid_contact' : whatsapp.state === 'missing' ? 'missing_contact' : 'capability_unavailable' });
  const actions = [action('wa-quote', 'Cotizar por WhatsApp', 'Hola, vi su página y me gustaría solicitar información.'), action('wa-product', 'Consultar producto', 'Hola, me gustaría consultar: {{product.title}}.')];
  if (caps.cartToWhatsApp) actions.push(action('wa-cart', 'Cotizar selección', 'Me gustaría consultar estos productos: {{cart.summary}}. ¿Me confirman disponibilidad y precio?'));
  for (const o of offerings) {
    if (o.kind === 'service') {
      if (!actions.some(a => a.id === 'wa-service')) actions.push(action('wa-service', 'Consultar servicio', 'Hola, me interesa el servicio de {{service.title}}. ¿Podrían darme información?'));
      o.actionIds = ['wa-service'];
    }
  }
  const section = (id, type, variant, contentRefs, actionIds, title, body) => ({ id, type, variant, enabled: true, contentRefs, actionIds, mediaIds: [], copy: { title, ...(body ? { body } : {}) } });
  const sections = [section('inicio', 'hero', 'text-first', ['business'], ['wa-quote'], nameFact.value, description)];
  if (categories.length) sections.push(section('categorias', 'categories', 'compact-list', categories.map(x => x.id), [], 'Categorías'));
  if (offerings.length) sections.push(section('ofertas', 'offerings', 'catalog-grid', offerings.map(x => x.id), actions.filter(a => ['wa-product', 'wa-service', 'wa-cart'].includes(a.id)).map(a => a.id), 'Productos y servicios'));
  sections.push(section('contacto', 'contact', 'compact', ['business'], ['wa-quote'], 'Contacto'));
  const email = fact('business', 'email')?.value;
  const address = fact('business', 'address');
  const hours = fact('business', 'hours');
  const site = {
    contractVersion: '3.0.0', designVersion: 'v3', rendererVersion: '3.0.0', negocioId, revisionId, revision,
    templateId: brief.goal === 'appointment_request' || brief.goal === 'availability_inquiry' ? 'booking' : offerings.some(o => o.kind === 'product') ? 'ecommerce' : 'info',
    business: { id: 'business', name: nameFact.value, description, category: brief.business.declaredCategory || 'general', locale: 'es-MX' },
    theme: { styleId: 'trade-clean', density: 'compact', primaryColor: '#B94B19', surfaceMode: 'light', headingFont: 'Sora', bodyFont: 'Manrope' },
    contact: { whatsapp, email: typeof email === 'string' && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : null,
      address: typeof address?.value === 'string' ? { text: address.value, sourceFactId: address.id } : null,
      hours: Array.isArray(hours?.value) ? hours.value.map(h => ({ ...h, sourceFactId: hours.id })) : [], socialLinks: [] },
    capabilities: caps, content: { offerings, categories, trustItems: [], testimonials: [], faqs: [], projects: [] },
    media: [], actions, sections,
    navigation: { logoAssetId: null, items: sections.slice(1).map(s => ({ label: s.copy.title, sectionId: s.id })) },
    seo: { title: nameFact.value, description, indexing: 'sample_noindex', ogAssetId: null },
  };
  return validateSite(site, { negocioId, capabilities: grants });
}
export function assertPublishable(site, context) {
  validateSite(site, { negocioId: context.negocioId, capabilities: context.grants });
  const expected = compileFactualSite({ ...context, revisionId: site.revisionId, revision: site.revision });
  // Exact recompilation intentionally excludes all unreviewed AI copy and data.
  // Later compilers must get a new version/evaluation before expanding this gate.
  fail(canonical(site) === canonical(expected), 'UNCONFIRMED_PUBLIC_CONTENT', 422);
  return site;
}
