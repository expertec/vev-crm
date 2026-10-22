// Offer for the WhatsApp campaign. Keep it separate from the public website catalog.
export const SAMPLE_CAMPAIGN_OFFER = Object.freeze({
  product: 'Página web para negocio',
  price: '$990 MXN al año',
  included: [],
  rules: [
    'Este es el precio de la campaña de WhatsApp; no sustituirlo por el del catálogo general.',
    'Responder primero la duda comercial del cliente y después hacer como máximo una pregunta breve para avanzar.',
    'No prometer primera posición en Google, resultados de ventas, publicidad incluida ni prestaciones que no figuren en included.',
    'La información de nuestra oferta no debe guardarse como si fueran datos del negocio del cliente.',
  ],
});

export function createSampleSalesKnowledgeLoader(db) {
  let cached;
  let expiresAt = 0;
  const load = async () => {
    if (cached && Date.now() < expiresAt) return cached;
    const [configSnap, packageSnap] = await Promise.all([db.collection('config').doc('appConfig').get(), db.collection('sampleAgentPackages').where('enabled', '==', true).limit(20).get()]);
    const settings = configSnap.data() || {};
    const config = settings.sampleAgentWebOffer || {};
    const packages = packageSnap.docs.map((doc) => { const item = doc.data(); return { id: doc.id, name: item.name, price: item.price, included: item.included || [], description: item.description || '', audioScript: item.audioScript || '', hasAudio: Boolean(item.audio), hasImage: Boolean(item.image) }; });
    const primary = packages.find((item) => item.id === settings.sampleAgentWebPackageId) || packages[0];
    cached = {
      ...SAMPLE_CAMPAIGN_OFFER,
      packages,
      defaultPackageId: primary?.id || '',
      price: String(primary?.price || config.price || SAMPLE_CAMPAIGN_OFFER.price).slice(0, 100),
      included: primary ? primary.included : Array.isArray(config.included) ? config.included.slice(0, 12).map((item) => String(item).slice(0, 200)) : [...SAMPLE_CAMPAIGN_OFFER.included],
    };
    expiresAt = Date.now() + 60_000;
    return cached;
  };
  load.invalidate = () => { expiresAt = 0; };
  return load;
}
