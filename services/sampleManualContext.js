import { normalizeConversationBrief } from './sampleConversationAgent.js';

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
