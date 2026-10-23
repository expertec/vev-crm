import { normalizeConversationBrief } from './sampleConversationAgent.js';
import { publicImageUrl } from './aiSampleGenerator.js';

export async function cropManualSampleLogo(lead, selection, prepareLogo) {
  const images = [...(lead.sampleAgent?.images || [])];
  const index = selection?.imageIndex, crop = selection?.crop;
  if (!Number.isInteger(index) || !images[index] || !Array.isArray(crop) || crop.length !== 4 || !crop.every(Number.isFinite)) throw Object.assign(Error('Selecciona una imagen y un área de recorte válidas.'), { status: 400 });
  const [x, y, width, height] = crop;
  if (x < 0 || y < 0 || width <= 0 || height <= 0 || x + width > 1.001 || y + height > 1.001 || width * height > 0.92) throw Object.assign(Error('Delimita el logo dentro de la imagen.'), { status: 400 });
  if (!prepareLogo) throw Object.assign(Error('El recorte de logos no está disponible.'), { status: 503 });
  const source = images[index];
  let url;
  try { url = await prepareLogo({ url: source.sourceUrl || source.url, crop }); }
  catch (error) { throw Object.assign(Error(error.message || 'No se pudo recortar el logo.'), { status: 400 }); }
  if (!publicImageUrl(url)) throw Object.assign(Error('No se pudo guardar el logo recortado.'), { status: 500 });
  images[index] = { ...source, sourceUrl: source.sourceUrl || source.url, url, logoPrepared: true, visionReviewedUrl: '', visionDescription: '' };
  const brief = normalizeConversationBrief(lead.sampleAgent.brief || {}, images);
  brief.logoChoice = 'provided';
  brief.images = brief.images.filter((image) => image.index !== index && image.role !== 'logo');
  brief.images.push({ index, role: 'logo', description: 'Logo recortado y seleccionado por el asesor.', logoStatus: 'clean', logoCrop: [] });
  return { ...lead, sampleAgent: { ...lead.sampleAgent, images, brief } };
}

export function manualSampleContext(lead, fields) {
  const agent = lead.sampleAgent || {}, images = agent.images || [];
  const updates = {};
  for (const field of ['companyName', 'activity', 'objective', 'customers', 'differentiators', 'serviceArea', 'colorPreference']) {
    if (typeof fields[field] !== 'string' || fields[field].length > (field === 'activity' ? 2000 : 500)) throw Object.assign(Error('Revisa los datos del negocio.'), { status: 400 });
    updates[field] = fields[field].trim();
  }
  if (!Array.isArray(fields.offerings) || fields.offerings.length > 8 || fields.offerings.some((value) => typeof value !== 'string' || value.length > 100) || typeof fields.withoutLogo !== 'boolean') throw Object.assign(Error('Revisa los servicios y la opción del logo.'), { status: 400 });
  updates.offerings = fields.offerings;
  const brief = normalizeConversationBrief({ ...agent.brief, ...updates }, images);
  if (!fields.withoutLogo && brief.logoChoice === 'none') brief.logoChoice = 'pending';
  if (fields.withoutLogo) {
    brief.logoChoice = 'none';
    if (fields.logoImageIndex !== '' && fields.logoImageIndex != null) {
      const index = fields.logoImageIndex;
      if (!Number.isInteger(index) || !images[index]) throw Object.assign(Error('Selecciona una imagen válida para descartar como logo.'), { status: 400 });
      brief.images = brief.images.filter((image) => image.index !== index);
      brief.images.push({ index, role: 'logo', description: 'Logo descartado por el asesor; usar el nombre del negocio.', logoStatus: 'original_required', logoCrop: [] });
    }
    if (!brief.colorPreference) brief.colorPreference = brief.palette.length ? `Colores de la marca: ${brief.palette.join(', ')}` : 'Proponer una combinación de colores acorde al negocio';
  }
  return brief;
}
