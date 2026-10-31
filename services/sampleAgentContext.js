const key = (item) => `${['assistant', 'business'].includes(item.role) ? 'business' : 'customer'}:${String(item.text || '').replace(/\s+/g, ' ').trim()}:${item.mediaType || 'text'}`;

export function compactSampleContext(history = [], context = []) {
  const recent = context.slice(-12);
  const known = new Set(recent.map(key));
  return { history: history.slice(-12).filter((item) => !known.has(key(item))), conversationContext: recent };
}

export function sampleVisionIndexes(images, message = {}, brief = {}) {
  const text = String(message.text || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const review = /\b(?:revisa|revisar|vuelve a ver|mira bien|te equivocaste|no es|confundiste|cambia|reemplaza|sustituye|usa|usar)\b/.test(text) && /\b(?:logo|imagen|imagenes|foto|fotos|color|colores)\b/.test(text);
  const identifiesLogo = /\b(?:logo|logotipo)\b/.test(text) && /\b(?:es|seria|mande|envie|esta bien|foto|toldo|letrero|lona|fachada)\b/.test(text);
  const knownLogos = (brief.images || []).filter(item => item.role === 'logo').map(item => item.index);
  const lastImage = /\bultima\b/.test(text);
  const firstImage = /\bprimera\b/.test(text);
  const targets = lastImage ? [images.length - 1] : firstImage ? [0] : /\b(?:logo|logotipo)\b/.test(text) && knownLogos.length ? knownLogos : images.map((_, index) => index);
  return images.flatMap((image, index) => ((!image.visionDeferred && image.visionReviewedUrl !== image.url)
    || (review && targets.includes(index)) || (identifiesLogo && (lastImage ? index === images.length - 1 : index >= images.length - 3))) ? [index] : []);
}

export function canDeferExtraSamplePhotos(brief = {}, images = [], incoming = [], text = '') {
  if (String(text).trim() || !incoming.length) return false;
  const known = brief.images || [];
  const brandReady = brief.logoChoice === 'none' || (brief.logoChoice === 'provided' && brief.palette?.length >= 3
    && known.some(item => item.role === 'logo' && item.logoStatus === 'clean'));
  const photos = known.filter(item => ['product', 'work'].includes(item.role) && item.description
    && images[item.index]?.url && images[item.index].visionReviewedUrl === images[item.index].url);
  // Presentation slides and catalogs can contain facts needed for the brief, not just photos.
  const documentContext = known.some(item => /diapositiva|presentaci[oó]n|cat[aá]logo|documento|l[aá]mina/i.test(item.description || ''));
  return Boolean(brandReady && photos.length >= 3 && !documentContext && incoming.every(item => !item.caption?.trim()));
}

export function deferExtraSamplePhotos(brief, images, incoming) {
  const urls = new Set(incoming.map(item => item.url));
  const next = { ...brief, images: [...(brief.images || [])] };
  images.forEach((image, index) => {
    if (!urls.has(image.url) || image.visionReviewedUrl === image.url) return;
    images[index] = { ...image, visionDeferred: true };
    if (!next.images.some(item => item.index === index)) next.images.push({ index, role: 'reference', description: 'Foto adicional guardada sin analizar; no seleccionada para la muestra.' });
  });
  return next;
}

// Successful visual work from an interrupted batch remains on its inbox turn.
// Reuse only image descriptions/roles, never a stale reply, approval or generation.
export function recoverSampleVisionCache(brief, images, turns = []) {
  const next = [...images];
  const roles = new Map((brief.images || []).map(item => [item.index, item]));
  for (const turn of turns) for (const [oldIndex, cached] of (turn.images || []).entries()) {
    if (!cached.url || cached.visionReviewedUrl !== cached.url) continue;
    const visual = turn.brief?.images?.find(item => item.index === oldIndex);
    if (!visual?.description) continue;
    const index = next.findIndex(image => image.url === cached.url || (cached.sourceUrl && image.url === cached.sourceUrl));
    if (index < 0 || next[index].visionReviewedUrl === next[index].url) continue;
    next[index] = { ...next[index], ...cached };
    roles.set(index, { ...visual, index });
  }
  return { images: next, brief: { ...brief, images: [...roles.values()].sort((a, b) => a.index - b.index) } };
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
