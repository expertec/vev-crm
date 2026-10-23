const key = (item) => `${['assistant', 'business'].includes(item.role) ? 'business' : 'customer'}:${String(item.text || '').replace(/\s+/g, ' ').trim()}:${item.mediaType || 'text'}`;

export function compactSampleContext(history = [], context = []) {
  const recent = context.slice(-12);
  const known = new Set(recent.map(key));
  return { history: history.slice(-12).filter((item) => !known.has(key(item))), conversationContext: recent };
}

export function sampleVisionIndexes(images, message = {}) {
  const text = String(message.text || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const review = /\b(?:revisa|revisar|vuelve a ver|mira bien|te equivocaste|no es|confundiste)\b/.test(text) && /\b(?:logo|imagen|imagenes|foto|fotos|color|colores)\b/.test(text);
  return images.flatMap((image, index) => review || image.visionReviewedUrl !== image.url ? [index] : []);
}
