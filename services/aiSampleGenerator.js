import { createHash } from 'node:crypto';
import sanitizeHtml from 'sanitize-html';
import postcss from 'postcss';
import { appendSampleVideos } from './aiSampleVideos.js';
import { sampleIconPlaceholder, renderSampleIcons } from './aiSampleIcons.js';
import { AI_SAMPLE_GENERATOR_REVISION, buildSampleVisualPlan, sampleImagePrompt, SAMPLE_PAGE_INSTRUCTIONS, sampleHeroCss, SAMPLE_BRAND_HEADER_CSS } from './aiSampleVisuals.js';
export { AI_SAMPLE_GENERATOR_REVISION } from './aiSampleVisuals.js';

export const AI_SAMPLE_VERSION = 'ai-page-v1';
export const DEFAULT_SAMPLE_MODEL = 'gpt-6.1-sol';
export const DEFAULT_SAMPLE_IMAGE_MODEL = 'gpt-image-2.5-flare';
const MAX_PAGE_BYTES = 220_000;

const clean = (value, limit = 2000) => String(value || '').trim().slice(0, limit);
const escapeHtml = (value) => String(value || '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));

export function publicImageUrl(value) {
  try {
    const url = new URL(clean(value, 4096));
    const host = url.hostname.toLowerCase();
    if (url.protocol !== 'https:' || url.username || url.password || url.port
      || !host.includes('.') || /[\[\]:]/.test(host) || /^[\d.]+$/.test(host)
      || /(^|\.)(localhost|local|internal|test|invalid)$/.test(host)) return '';
    return url.href;
  } catch { return ''; }
}

export function normalizeAiSampleInput(data = {}) {
  const photos = [...new Set((Array.isArray(data.photoURLs) ? data.photoURLs : []).map(publicImageUrl).filter(Boolean))].slice(0, 3);
  const count = Number(data.aiImageCount ?? 3);
  const aiImageCount = Math.max(1, Math.min(3, Number.isFinite(count) ? Math.floor(count) : 3));
  return {
    companyInfo: clean(data.companyInfo || data.companyName, 120),
    businessStory: clean(data.businessStory || data.description, 4000),
    businessObjective: clean(data.businessObjective || data.objective, 500),
    businessSector: clean(data.businessSector, 100),
    keyItems: (Array.isArray(data.keyItems) ? data.keyItems : []).map((item) => clean(item, 100)).filter(Boolean).slice(0, 8),
    primaryColor: /^#[\da-f]{6}$/i.test(data.primaryColor || '') ? data.primaryColor : '',
    colorPreference: clean(data.colorPreference, 200),
    enhancePhotos: data.enhancePhotos !== false,
    themeMode: data.themeMode === 'dark' ? 'dark' : 'light',
    themeTone: clean(data.themeTone, 40),
    contactWhatsapp: clean(data.contactWhatsapp || data.leadPhone, 30).replace(/\D/g, ''),
    contactEmail: /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(data.contactEmail || '') ? clean(data.contactEmail, 160) : '',
    logoURL: publicImageUrl(data.logoURL),
    photoURLs: photos,
    videoURLs: [...new Set((Array.isArray(data.videoURLs) ? data.videoURLs : []).map(publicImageUrl).filter(Boolean))].slice(0, 2),
    referenceImageURLs: (Array.isArray(data.referenceImageURLs) ? data.referenceImageURLs : []).map(publicImageUrl).filter(Boolean).slice(0, 3),
    imageContext: (Array.isArray(data.imageContext) ? data.imageContext : []).filter((item) => publicImageUrl(item.url)).slice(0, 12).map((item) => ({ url: publicImageUrl(item.url), role: clean(item.role, 30), description: clean(item.description, 300) })),
    brandPalette: (Array.isArray(data.brandPalette) ? data.brandPalette : []).filter((color) => /^#[0-9a-f]{6}$/i.test(color)).slice(0, 5),
    aiImageCount,
    reusableAiAssets: (Array.isArray(data.reusableAiAssets) ? data.reusableAiAssets : []).filter((asset) => asset && /^[a-f0-9]{64}$/.test(asset.visualKey || '') && publicImageUrl(asset.url)).slice(0, 3).map((asset) => ({ url: publicImageUrl(asset.url), visualKey: asset.visualKey })),
    templateId: ['info', 'ecommerce', 'booking'].includes(data.templateId) ? data.templateId : 'info',
    slug: clean(data.slug, 100),
  };
}

export function aiSampleInputHash(data, model = process.env.OPENAI_SAMPLE_MODEL || DEFAULT_SAMPLE_MODEL, imageModel = process.env.OPENAI_SAMPLE_IMAGE_MODEL || DEFAULT_SAMPLE_IMAGE_MODEL) {
  const { reusableAiAssets, ...input } = normalizeAiSampleInput(data);
  return createHash('sha256').update(JSON.stringify({ version: AI_SAMPLE_VERSION, revision: AI_SAMPLE_GENERATOR_REVISION, model, imageModel, input })).digest('hex');
}

export async function requestSampleOpenAI(resource, body, { fetchImpl = fetch, timeoutMs = 180_000, apiKey = process.env.OPENAI_API_KEY } = {}) {
  if (!apiKey) throw new Error('Falta OPENAI_API_KEY para generar la muestra.');
  const response = await fetchImpl(`https://api.openai.com/v1/${resource}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const result = await response.json();
  if (!response.ok) throw new Error(clean(result?.error?.message || `OpenAI: HTTP ${response.status}`, 500));
  return result;
}

const PAGE_CONTRACT = {
  type: 'object', additionalProperties: false,
  properties: {
    title: { type: 'string' }, description: { type: 'string' },
    html: { type: 'string' }, css: { type: 'string' },
    palette: { type: 'array', items: { type: 'string' }, minItems: 3, maxItems: 5 },
    imagePrompts: { type: 'array', items: { type: 'string' }, maxItems: 3 },
  },
  required: ['title', 'description', 'html', 'css', 'palette', 'imagePrompts'],
};

export function sanitizeSampleCss(source) {
  let root;
  try { root = postcss.parse(source, { from: undefined }); }
  catch { throw new Error('El diseño contiene CSS inválido.'); }
  const unsafe = (value) => /[<\\\u0000-\u0008]|url\s*\(|expression\s*\(|(?:^|[^\w-])(?:image-set|-webkit-image-set)\s*\(/i.test(String(value).replace(/\/\*[\s\S]*?\*\//g, ''));
  root.walkComments((comment) => comment.remove());
  root.walkAtRules((rule) => {
    if (!['media', 'supports', 'container', 'layer', 'scope', 'starting-style', 'keyframes', '-webkit-keyframes'].includes(rule.name.toLowerCase()) || unsafe(rule.params)) rule.remove();
  });
  root.walkRules((rule) => { if (unsafe(rule.selector)) rule.remove(); });
  root.walkDecls((declaration) => {
    if (unsafe(declaration.prop) || unsafe(declaration.value) || /^(?:behavior|-moz-binding)$/i.test(declaration.prop)) declaration.remove();
  });
  const css = root.toString().trim();
  if (/[<\u0000-\u0008]/.test(css)) throw new Error('El diseño contiene CSS no permitido.');
  return css;
}

export function sanitizeSamplePage(page, { input, assets = [] }) {
  if (!page || typeof page.html !== 'string' || typeof page.css !== 'string'
    || Buffer.byteLength(page.html + page.css) > MAX_PAGE_BYTES) throw new Error('La página generada no es válida o excede el tamaño permitido.');
  // Keep the usable design while dropping external resources and active CSS.
  const css = sanitizeSampleCss(page.css);
  const digits = input.contactWhatsapp;
  const contactUrl = digits ? `https://wa.me/${digits}` : '#contacto';
  const replacements = { CONTACT_URL: contactUrl, PHONE_URL: digits ? `tel:+${digits}` : '', EMAIL_URL: input.contactEmail ? `mailto:${input.contactEmail}` : '', LOGO: input.logoURL };
  assets.forEach((asset, index) => { replacements[`IMAGE_${index}`] = asset.url; });
  const source = page.html.replace(/\{\{([A-Z_0-9]+)\}\}/g, (_, key) => key.startsWith('ICON_') ? sampleIconPlaceholder(key) : escapeHtml(replacements[key] || ''));
  const allowedImages = new Set([input.logoURL, ...assets.map((item) => item.url)].filter(Boolean));
  const emailUrl = input.contactEmail ? `mailto:${input.contactEmail}` : '';
  const phoneUrl = digits ? `tel:+${digits}` : '';
  let imageCount = 0;
  let contactCount = 0;
  let logoCount = 0;
  const usedImages = new Set();
  const html = sanitizeHtml(source, {
    allowedTags: ['header', 'nav', 'main', 'section', 'article', 'aside', 'footer', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'p', 'a', 'img', 'ul', 'ol', 'li', 'strong', 'em', 'small', 'br', 'hr', 'figure', 'figcaption', 'details', 'summary'],
    allowedAttributes: { '*': ['class', 'id', 'aria-label', 'aria-hidden'], a: ['href', 'target', 'rel'], img: ['src', 'alt', 'width', 'height', 'loading', 'decoding', 'fetchpriority'] },
    allowedSchemes: ['https', 'mailto', 'tel'], allowProtocolRelative: false,
    transformTags: {
      a: (_tag, attributes) => {
        let href = attributes.href || '';
        if (href !== contactUrl && href !== emailUrl && href !== phoneUrl && !/^#[a-zA-Z][\w-]*$/.test(href)) href = '#contacto';
        if (digits && href === contactUrl) contactCount += 1;
        return { tagName: 'a', attribs: { ...attributes, href, target: href.startsWith('https:') ? '_blank' : '_self', rel: 'noopener noreferrer' } };
      },
    },
    exclusiveFilter: (frame) => {
      if (frame.tag !== 'img') return false;
      if (!allowedImages.has(frame.attribs.src)) return true;
      usedImages.add(frame.attribs.src);
      if (frame.attribs.src !== input.logoURL) imageCount += 1;
      else logoCount += 1;
      return false;
    },
  });
  if (!/<h1[\s>]/i.test(html) || !/<main[\s>]/i.test(html) || html.replace(/<[^>]*>/g, '').trim().length < 80) throw new Error('La muestra no contiene una página completa.');
  if (assets.length && !imageCount) throw new Error('La muestra no incorporó las imágenes disponibles.');
  if (assets.some((asset) => ['ai_generated', 'ai_edited'].includes(asset.origin) && !usedImages.has(asset.url))) throw new Error('El diseño omitió imágenes generadas para la muestra.');
  if (input.logoURL && !logoCount) throw new Error('La muestra no conservó el logo proporcionado.');
  if (digits && !contactCount) throw new Error('La muestra no contiene un contacto de WhatsApp válido.');
  return { title: clean(page.title || input.companyInfo, 150), description: clean(page.description, 250), html: renderSampleIcons(html), css };
}

export function samplePageDocument(page, logoURL = '') {
  const csp = samplePageCsp([...(page.assets || []), { url: logoURL }], page.videos || []).split('; frame-ancestors')[0];
  return `<!doctype html><html lang="es-MX"><head><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="${escapeHtml(csp)}"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>${escapeHtml(page.title)}</title><meta name="description" content="${escapeHtml(page.description)}"><style>*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;overflow-wrap:break-word}img{max-width:100%;height:auto} ${page.css}\n${SAMPLE_BRAND_HEADER_CSS}</style></head><body>${page.html}</body></html>`;
}

export function samplePageCsp(assets = [], videos = []) {
  const origins = [...new Set(assets.map((item) => publicImageUrl(item.url)).filter(Boolean).map((url) => new URL(url).origin))];
  const mediaOrigins = [...new Set(videos.map(publicImageUrl).filter(Boolean).map((url) => new URL(url).origin))];
  return `default-src 'none'; script-src 'none'; style-src 'unsafe-inline'; img-src ${origins.length ? origins.join(' ') : "'none'"}; media-src ${mediaOrigins.length ? mediaOrigins.join(' ') : "'none'"}; base-uri 'none'; form-action 'none'; frame-ancestors 'self'; sandbox allow-popups allow-popups-to-escape-sandbox`;
}

export async function generateAiSample(data, { request = requestSampleOpenAI, imageStore, savedDesign = null, saveDesign = async () => {}, model = process.env.OPENAI_SAMPLE_MODEL || DEFAULT_SAMPLE_MODEL, imageModel = process.env.OPENAI_SAMPLE_IMAGE_MODEL || DEFAULT_SAMPLE_IMAGE_MODEL } = {}) {
  const input = normalizeAiSampleInput(data);
  if (!input.companyInfo || !input.businessStory) throw new Error('Faltan nombre y descripción del negocio.');
  if (!input.logoURL && !input.primaryColor && !input.colorPreference) throw new Error('¿Qué colores te gustaría usar? Elige colores o indica que deseas una propuesta.');
  const visualPlan = buildSampleVisualPlan(input, imageModel);
  const content = [{ type: 'input_text', text: JSON.stringify({ ...input, videoURLs: undefined, reusableAiAssets: undefined, imageSlots: visualPlan.assets.map((asset, slot) => ({ slot, purpose: asset.purpose, origin: asset.origin })), imageTasks: visualPlan.pending.map(({ slot, purpose, sourceUrls }) => ({ slot, purpose, sourceUrls })) }) }];
  if (input.logoURL) content.push({ type: 'input_image', image_url: input.logoURL, detail: 'high' });
  for (const url of input.photoURLs) content.push({ type: 'input_image', image_url: url, detail: 'low' });
  for (const url of input.referenceImageURLs) content.push({ type: 'input_image', image_url: url, detail: 'low' });
  const result = savedDesign ? null : await request('responses', {
    model, store: false, max_output_tokens: 16000,
    instructions: SAMPLE_PAGE_INSTRUCTIONS,
    input: [{ role: 'user', content }],
    text: { format: { type: 'json_schema', name: 'sample_page', strict: true, schema: PAGE_CONTRACT } },
  });
  if (result?.status === 'incomplete') throw new Error('OpenAI no terminó la página; intenta nuevamente.');
  const output = result?.output_text || (result?.output || []).flatMap((item) => item.content || []).filter((item) => item.type === 'output_text').map((item) => item.text).join('');
  const design = savedDesign || JSON.parse(output);
  if (!Array.isArray(design.imagePrompts) || design.imagePrompts.length !== visualPlan.pending.length) throw new Error('El diseño no respetó el límite de imágenes.');
  const palette = input.brandPalette.length >= 3 ? input.brandPalette : (Array.isArray(design.palette) ? design.palette.filter((color) => /^#[0-9a-f]{6}$/i.test(color)).slice(0, 5) : []);
  if (palette.length < 3) throw new Error('El diseño no definió una paleta de colores válida.');
  const preparedDesign = { ...design, css: `${design.css}\n${sampleHeroCss(palette)}` };
  // Check structure and image placement before paid image generation, without Chrome.
  const structure = sanitizeSamplePage(preparedDesign, { input, assets: visualPlan.assets });
  const text = sanitizeHtml(structure.html, { allowedTags: [], allowedAttributes: {} }).trim();
  const requiredSections = ['inicio', 'servicios', 'proceso', 'preguntas', 'contacto'];
  if (text.length < 400 || requiredSections.some((id) => !new RegExp(`id="${id}"`).test(structure.html))) throw new Error('La muestra necesita completar servicios, proceso, preguntas y contacto.');
  if (!/class="[^"]*\bnw-hero-media\b/.test(structure.html) || !/class="[^"]*\bnw-hero-content\b/.test(structure.html)) throw new Error('El diseño no incorporó la portada fotográfica.');
  if (!savedDesign) await saveDesign(design);
  const assets = visualPlan.assets.map((asset) => ({ ...asset }));
  for (let index = 0; index < visualPlan.pending.length; index += 1) {
    if (!imageStore) throw new Error('No se configuró almacenamiento de imágenes.');
    const task = visualPlan.pending[index];
    const prompt = sampleImagePrompt(task, clean(design.imagePrompts[index], 2500), input, palette);
    const key = createHash('sha256').update(JSON.stringify({ imageModel, prompt, sourceUrls: task.sourceUrls })).digest('hex');
    let url = await imageStore.get(key);
    if (!url) {
      const resource = task.sourceUrls.length ? 'images/edits' : 'images/generations';
      const image = await request(resource, { model: imageModel, prompt, n: 1, size: '1536x1024', quality: 'medium', output_format: 'webp', ...(task.sourceUrls.length ? { images: task.sourceUrls.map((image_url) => ({ image_url })) } : {}) });
      const b64 = image.data?.[0]?.b64_json;
      if (!b64) throw new Error('OpenAI no devolvió la imagen solicitada.');
      url = await imageStore.put(key, Buffer.from(b64, 'base64'));
    }
    if (!publicImageUrl(url)) throw new Error('No se pudo guardar una imagen pública válida.');
    assets[task.slot] = { ...assets[task.slot], url, prompt, cacheKey: key };
  }
  const page = appendSampleVideos(sanitizeSamplePage(preparedDesign, { input, assets }), input.videoURLs);
  return {
    templateId: input.templateId, slug: input.slug, designVersion: AI_SAMPLE_VERSION,
    brand: { name: input.companyInfo, logo: input.logoURL }, colors: { primary: input.logoURL && !input.colorPreference ? palette[0] : input.primaryColor || palette[0], palette },
    hero: { title: page.title, subtitle: page.description, backgroundImageUrl: assets[0].url },
    gallery: { images: assets.filter((asset) => asset.purpose !== 'hero').map((asset) => asset.url) },
    contact: { whatsapp: input.contactWhatsapp, email: input.contactEmail },
    aiPage: { ...page, videos: input.videoURLs, version: AI_SAMPLE_VERSION, generatorRevision: AI_SAMPLE_GENERATOR_REVISION, palette, model, imageModel, assets, inputHash: aiSampleInputHash(data, model, imageModel), generatedAt: new Date().toISOString() },
  };
}
