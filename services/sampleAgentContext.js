const key = (item) => `${['assistant', 'business'].includes(item.role) ? 'business' : 'customer'}:${String(item.text || '').replace(/\s+/g, ' ').trim()}:${item.mediaType || 'text'}`;

export function compactSampleContext(history = [], context = []) {
  const recent = context.slice(-12);
  const known = new Set(recent.map(key));
  return { history: history.slice(-12).filter((item) => !known.has(key(item))), conversationContext: recent };
}

export function sampleVisionIndexes(images, message = {}) {
  const text = String(message.text || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const review = /\b(?:revisa|revisar|vuelve a ver|mira bien|te equivocaste|no es|confundiste)\b/.test(text) && /\b(?:logo|imagen|imagenes|foto|fotos|color|colores)\b/.test(text);
  const identifiesLogo = /\b(?:logo|logotipo)\b/.test(text) && /\b(?:es|seria|mande|envie|esta bien|foto|toldo|letrero|lona|fachada)\b/.test(text);
  return images.flatMap((image, index) => review || image.visionReviewedUrl !== image.url || (identifiesLogo && index >= images.length - 3) ? [index] : []);
}

// Keep a bounded visual context, reserving the logo and product examples while
// admitting recent files. All other files remain stored in the conversation.
export function mergeSampleImages(brief = {}, images = [], incoming = []) {
  const merged = [...images], known = new Set(images.flatMap((image) => [image.url, image.sourceUrl]).filter(Boolean));
  for (const image of incoming) if (image.url && !known.has(image.url)) { merged.push(image); known.add(image.url); }
  const roles = new Map((brief.images || []).filter((item) => images[item.index]).map((item) => [images[item.index].url, item]));
  const logo = merged.find((image) => roles.get(image.url)?.role === 'logo');
  const examples = merged.filter((image) => ['product', 'work'].includes(roles.get(image.url)?.role));
  const selected = new Set([...(logo ? [logo.url] : []), ...(examples.length ? examples : merged.filter((image) => image !== logo)).slice(0, 3).map((image) => image.url)]);
  for (let index = merged.length - 1; index >= 0 && selected.size < 12; index--) selected.add(merged[index].url);
  const next = merged.filter((image) => selected.has(image.url));
  return { images: next, brief: { ...brief, images: next.flatMap((image, index) => roles.has(image.url) ? [{ ...roles.get(image.url), index }] : []) } };
}
