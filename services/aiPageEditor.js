import { createHash, randomUUID } from 'node:crypto';
import multer from 'multer';
import sharp from 'sharp';
import { parseDocument } from 'htmlparser2';
import render from 'dom-serializer';
import { samplePageDocument, publicImageUrl } from './aiSampleGenerator.js';
import { isAiSampleAvailable } from './aiSampleAccess.js';
import { EXTRA_GALLERY_LIMIT } from './aiPageExtraGallery.js';

const fail = (message, status = 400) => Object.assign(new Error(message), { status });
const children = (node) => node.children || [];
const classes = (node) => String(node.attribs?.class || '').split(/\s+/);
const text = (node) => node.type === 'text' ? node.data : children(node).map(text).join('');
function walk(node, visit) { visit(node); children(node).forEach((child) => walk(child, visit)); }
function ancestor(node, predicate) { for (let p = node; p; p = p.parent) if (predicate(p)) return p; return null; }
function group(node) {
  const section = ancestor(node, (n) => ['section', 'header', 'footer'].includes(n.name));
  if (!section) return 'Contenido';
  if (section.name === 'header') return 'Marca y encabezado';
  if (section.name === 'footer') return 'Pie de página';
  let title = '';
  walk(section, (n) => { if (!title && /^h[12]$/.test(n.name)) title = text(n).trim(); });
  return (title || section.attribs?.id || 'Contenido').slice(0, 90);
}
export function aiPageRevision(page) {
  return createHash('sha256').update(JSON.stringify(page || null)).digest('hex');
}

// Fields point to existing nodes. Updates never accept HTML, CSS or selectors.
function indexPage(page) {
  const root = parseDocument(page.html, { decodeEntities: true });
  const fields = [], targets = new Map(), images = new Map(), links = new Map();
  const add = (type, value, node, label) => {
    const id = `${type}-${fields.length}`;
    fields.push({ id, type, value, group: group(node), label });
    targets.set(id, [node]);
    return id;
  };
  walk(root, (node) => {
    if (ancestor(node, (n) => ['svg', 'style', 'script', 'video'].includes(n.name))) return;
    if (node.type === 'text' && node.data.trim()) {
      add('text', node.data.trim(), node, ancestor(node, (n) => /^h[1-4]$/.test(n.name)) ? 'Título' : ancestor(node, (n) => n.name === 'a') ? 'Texto del botón o enlace' : 'Texto');
    } else if (node.name === 'img' && node.attribs.src) {
      const src = node.attribs.src;
      if (images.has(src)) targets.get(images.get(src)).push(node);
      else images.set(src, add('image', src, node, ancestor(node, (n) => classes(n).includes('nw-brand')) ? 'Logo' : node.attribs.alt || 'Imagen'));
    } else if (node.name === 'a' && /^(https:\/\/|tel:|mailto:)/.test(node.attribs.href || '')) {
      const href = node.attribs.href;
      if (links.has(href)) targets.get(links.get(href)).push(node);
      else links.set(href, add('link', href, node, href.startsWith('https://wa.me/') ? 'Destino de WhatsApp' : 'Destino del enlace'));
    }
  });
  let brand = null;
  walk(root, (node) => { if (!brand && classes(node).includes('nw-brand')) brand = node; });
  if (brand && !fields.some((field) => field.type === 'image' && field.label === 'Logo')) add('image', '', brand, 'Logo');
  let gallery = null;
  const serviceCards = [];
  walk(root, (node) => {
    if (!gallery && classes(node).includes('nw-gallery-grid') && children(node).some((n) => n.name === 'figure')) gallery = node;
    if (classes(node).includes('nw-service-card') && ancestor(node.parent, (n) => classes(n).includes('nw-service-grid'))) {
      let title = false, description = false;
      walk(node, (n) => { if (n.name === 'h3') title = true; if (n.name === 'p') description = true; });
      if (title && description) serviceCards.push(node);
    }
  });
  return { root, fields, targets, gallery, serviceCards };
}

export function describeAiPage(page, logo = '') {
  const { fields, gallery, serviceCards } = indexPage(page);
  return { revision: aiPageRevision(page), fields, canAddServices: serviceCards.length > 0 && serviceCards.length < 24, serviceCount: serviceCards.length, serviceLimit: 24, canAddPhotos: Boolean(gallery), extraGallery: page.extraGallery || { title: 'Más de nuestro trabajo', items: [] }, extraGalleryLimit: EXTRA_GALLERY_LIMIT, document: samplePageDocument(page, logo), title: page.title };
}

export async function editAiPage(page, { changes = {}, photos = [], extraGallery, services = [] } = {}, { validateImage = async () => false } = {}) {
  if (!changes || typeof changes !== 'object' || Array.isArray(changes) || !Array.isArray(photos) || photos.length > 12) throw fail('Cambios inválidos.');
  if (!Array.isArray(services) || services.length > 12) throw fail('Puedes añadir hasta 12 servicios por guardado.');
  const { root, fields, targets, gallery, serviceCards } = indexPage(page);
  const byId = new Map(fields.map((field) => [field.id, field]));
  const next = { ...page, assets: [...(page.assets || [])] };
  const replacements = new Map();
  let newLogo;
  if (extraGallery !== undefined) {
    if (!extraGallery || typeof extraGallery.title !== 'string' || extraGallery.title.length > 120 || !Array.isArray(extraGallery.items) || extraGallery.items.length > EXTRA_GALLERY_LIMIT) throw fail('La galería adicional admite hasta 20 fotos y un título de hasta 120 caracteres.');
    const existing = new Set((page.extraGallery?.items || []).flatMap((item) => [item.url, item.thumbnailUrl]).filter(Boolean));
    const items = [];
    for (const photo of extraGallery.items) {
      if (!photo || typeof photo.url !== 'string' || typeof photo.caption !== 'string' || photo.caption.length > 200) throw fail('La foto o su descripción no son válidas.');
      const thumbnailUrl = photo.thumbnailUrl || photo.url;
      for (const url of new Set([photo.url, thumbnailUrl])) if (!publicImageUrl(url) || (!existing.has(url) && !await validateImage(url))) throw fail('Sube las fotos desde el editor de este negocio.');
      items.push({ url: photo.url, thumbnailUrl, caption: photo.caption });
    }
    next.extraGallery = { title: extraGallery.title.trim() || 'Más de nuestro trabajo', items };
    next.assets = next.assets.filter((asset) => asset.purpose !== 'extra_gallery');
    for (const item of items) for (const url of new Set([item.url, item.thumbnailUrl])) next.assets.push({ url, origin: 'client_upload', purpose: 'extra_gallery' });
  }
  for (const [id, value] of Object.entries(changes)) {
    const field = byId.get(id);
    if (!field || typeof value !== 'string') throw fail('Uno de los campos ya no existe. Recarga el editor.');
    if (value === field.value) continue;
    if (field.type === 'text') {
      if (value.length > 3000) throw fail('El texto es demasiado largo.');
      for (const node of targets.get(id)) {
        const leading = node.data.match(/^\s*/)[0], trailing = node.data.match(/\s*$/)[0];
        node.data = leading + value + trailing;
      }
    } else if (field.type === 'image') {
      if (!await validateImage(value)) throw fail('Sube la imagen desde el editor.');
      for (const node of targets.get(id)) {
        if (node.name === 'img') node.attribs.src = value;
        else {
          const image = parseDocument('<img alt="Logo del negocio">').children[0];
          image.attribs.src = value; image.parent = node; image.next = node.children[0] || null;
          if (image.next) image.next.prev = image;
          node.children.unshift(image);
        }
      }
      if (field.label === 'Logo') newLogo = value;
      replacements.set(field.value, value);
    } else {
      const safe = publicImageUrl(value) || (/^(?:tel:\+?\d{7,15}|mailto:[^\s<>"@]+@[^\s<>"@]+\.[^\s<>"@]+)$/.test(value) ? value : '');
      if (!safe) throw fail('Usa un enlace https, un teléfono o un correo válido.');
      for (const node of targets.get(id)) { node.attribs.href = safe; node.attribs.rel = 'noopener noreferrer'; node.attribs.target = safe.startsWith('https:') ? '_blank' : '_self'; }
    }
  }
  if (photos.length) {
    if (!gallery) throw fail('Esta página no tiene una galería compatible para añadir fotos.');
    const figures = children(gallery).filter((node) => node.name === 'figure');
    if (figures.length + photos.length > 24) throw fail('La galería admite hasta 24 fotos.');
    for (const photo of photos) {
      if (!photo || typeof photo.caption !== 'string' || photo.caption.length > 200 || !await validateImage(photo.url)) throw fail('La foto o su descripción no son válidas.');
      const clone = parseDocument(render(figures[0])).children[0];
      let imageFound = false, captionFound = false;
      walk(clone, (node) => {
        if (node.attribs) delete node.attribs.id;
        if (node.name === 'img') { node.attribs.src = photo.url; node.attribs.alt = photo.caption; delete node.attribs.srcset; imageFound = true; }
        if (node.type === 'text') {
          node.data = !captionFound && ancestor(node, (n) => n.name === 'figcaption') ? photo.caption : '';
          if (node.data) captionFound = true;
        }
        if (node.name === 'a' && !String(node.attribs.href || '').startsWith('https://wa.me/')) node.attribs.href = photo.url;
      });
      if (!imageFound) throw fail('No se pudo conservar el formato de la galería.');
      clone.parent = gallery;
      const previous = gallery.children.at(-1);
      if (previous) previous.next = clone;
      clone.prev = previous || null;
      clone.next = null;
      gallery.children.push(clone);
      next.assets.push({ url: photo.url, origin: 'client_upload', purpose: 'gallery' });
    }
  }
  next.assets = next.assets.map((asset) => replacements.has(asset.url) ? { ...asset, url: replacements.get(asset.url), origin: 'client_upload' } : asset);
  for (const url of replacements.values()) if (!next.assets.some((asset) => asset.url === url)) next.assets.push({ url, origin: 'client_upload' });
  if (services.length) {
    if (!serviceCards.length) throw fail('Esta página no tiene tarjetas de servicios compatibles.');
    if (serviceCards.length + services.length > 24) throw fail('La página admite hasta 24 servicios.');
    let template = serviceCards.at(-1);
    const simple = serviceCards.find((card) => { let image = false; walk(card, (n) => { if (n.name === 'img') image = true; }); return !image; });
    if (simple) template = simple;
    const parent = template.parent;
    const remove = (node) => {
      if (node.prev) node.prev.next = node.next;
      if (node.next) node.next.prev = node.prev;
      node.parent.children = node.parent.children.filter((n) => n !== node);
    };
    const setText = (node, value) => {
      node.children = parseDocument(value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')).children;
      node.children.forEach((child) => { child.parent = node; });
    };
    for (const service of services) {
      if (!service || typeof service.title !== 'string' || !service.title.trim() || service.title.length > 120 || typeof service.description !== 'string' || !service.description.trim() || service.description.length > 800 || (service.imageUrl && (typeof service.imageUrl !== 'string' || !await validateImage(service.imageUrl)))) throw fail('Indica nombre y descripción del servicio. La foto debe subirse desde este editor.');
      const clone = parseDocument(render(template)).children[0], oldImages = [];
      let heading, paragraph, image;
      walk(clone, (node) => {
        if (ancestor(node, (n) => n.name === 'svg')) return;
        if (node.attribs) delete node.attribs.id;
        if (!heading && node.name === 'h3') heading = node;
        if (!paragraph && node.name === 'p') paragraph = node;
        if (node.name === 'img') oldImages.push(node);
        if (node.name === 'a') {
          const href = node.attribs.href || '';
          node.attribs.href = href.startsWith('https://wa.me/') ? `${href.split('?')[0]}?text=${encodeURIComponent(`Hola, quiero información sobre ${service.title.trim()}`)}` : '#contacto';
        }
        if (node.type === 'text') node.data = ancestor(node, (n) => n.name === 'a') ? 'Consultar servicio' : '';
      });
      setText(heading, service.title.trim()); setText(paragraph, service.description.trim());
      if (service.imageUrl) {
        image = oldImages.shift();
        if (!image) { image = parseDocument('<img loading="lazy" decoding="async">').children[0]; image.parent = clone; image.next = clone.children[0] || null; if (image.next) image.next.prev = image; clone.children.unshift(image); }
        image.attribs.src = service.imageUrl; image.attribs.alt = service.title.trim(); delete image.attribs.srcset;
        image.attribs.loading = 'lazy'; image.attribs.decoding = 'async';
        next.assets.push({ url: service.imageUrl, origin: 'client_upload', purpose: 'service' });
      }
      oldImages.forEach(remove);
      clone.parent = parent; clone.prev = parent.children.at(-1) || null; clone.next = null;
      if (clone.prev) clone.prev.next = clone;
      parent.children.push(clone);
    }
  }
  next.html = render(root, { encodeEntities: 'utf8' });
  if (Buffer.byteLength(next.html + next.css) > 220_000) throw fail('La página excede el tamaño permitido.');
  next.editedAt = new Date().toISOString();
  return { page: next, replacements, newLogo };
}

export function registerAiPageEditorRoutes(app, { db, authenticate, bucket }) {
  const receiveImage = multer({ storage: multer.memoryStorage(), limits: { fileSize: 8 * 1024 * 1024, files: 1, fields: 0, parts: 2 } }).single('image');
  app.post('/api/cliente/ai-page-editor/upload', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    try {
      const auth = await authenticate(req);
      if (!auth?.negocioId) return res.status(auth?.status || 401).json({ error: auth?.error || 'Inicia sesión para subir imágenes.' });
      const data = (await db.collection('Negocios').doc(auth.negocioId).get()).data() || {};
      if (!isAiSampleAvailable(data) || data.siteTemporarilyDisabled || data.websiteArchived) throw fail('El sitio no está habilitado para edición.', 403);
      if (['queued', 'running'].includes(data.aiSampleJob?.state)) throw fail('Espera a que termine la generación de la página.', 409);
      await new Promise((resolve, reject) => receiveImage(req, res, (err) => err ? reject(err) : resolve()));
      if (!req.file || !['image/jpeg', 'image/png', 'image/webp'].includes(req.file.mimetype)) throw fail('Usa JPG, PNG o WebP de hasta 8 MB.');
      let main, thumb;
      try {
        const image = sharp(req.file.buffer, { limitInputPixels: 32_000_000, animated: false }).rotate();
        const metadata = await image.metadata();
        if (!['jpeg', 'png', 'webp'].includes(metadata.format)) throw new Error('Formato inválido');
        [main, thumb] = await Promise.all([
          image.clone().resize({ width: 1920, height: 1920, fit: 'inside', withoutEnlargement: true }).webp({ quality: 82 }).toBuffer(),
          image.clone().resize({ width: 240, height: 180, fit: 'cover', withoutEnlargement: true }).webp({ quality: 75 }).toBuffer(),
        ]);
      } catch { throw fail('El archivo no es una imagen válida o es demasiado grande.'); }
      const prefix = `${auth.negocioId}/site-editor/${randomUUID()}`;
      const store = async (buffer, suffix) => {
        const file = bucket.file(`${prefix}${suffix}.webp`);
        await file.save(buffer, { resumable: false, metadata: { contentType: 'image/webp', cacheControl: 'public,max-age=31536000,immutable' } });
        return (await file.getSignedUrl({ action: 'read', expires: '2100-01-01' }))[0];
      };
      const [url, thumbnailUrl] = await Promise.all([store(main, ''), store(thumb, '-thumb')]);
      return res.json({ url, thumbnailUrl });
    } catch (error) {
      const status = error instanceof multer.MulterError ? 413 : error.status || 500;
      if (status === 500) console.error('[ai-page-editor-upload]', error.message);
      return res.status(status).json({ error: error instanceof multer.MulterError ? 'Sube una imagen JPG, PNG o WebP de hasta 8 MB.' : error.status ? error.message : 'No se pudo subir la imagen. Intenta nuevamente.' });
    }
  });
  const validateImage = async (url, negocioId) => {
    try {
      const parsed = new URL(url);
      if (parsed.protocol !== 'https:' || parsed.username || parsed.password || parsed.port) return false;
      let path;
      if (parsed.hostname === 'firebasestorage.googleapis.com') {
        const match = parsed.pathname.match(/^\/v0\/b\/([^/]+)\/o\/(.+)$/);
        if (!match || decodeURIComponent(match[1]) !== bucket.name) return false;
        path = decodeURIComponent(match[2]);
      } else if (parsed.hostname === 'storage.googleapis.com') {
        const prefix = `/${bucket.name}/`;
        if (!parsed.pathname.startsWith(prefix)) return false;
        path = decodeURIComponent(parsed.pathname.slice(prefix.length));
      } else return false;
      if (!path.startsWith(`${negocioId}/site-editor/`) || path.split('/').some((part) => part === '..')) return false;
      const [meta] = await bucket.file(path).getMetadata();
      return ['image/jpeg', 'image/png', 'image/webp'].includes(meta.contentType) && Number(meta.size) > 0 && Number(meta.size) <= 8 * 1024 * 1024;
    } catch { return false; }
  };
  const handler = (mutate) => async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    try {
      const auth = await authenticate(req);
      if (!auth?.negocioId) return res.status(auth?.status || 401).json({ error: auth?.error || 'Inicia sesión para editar tu sitio.' });
      const ref = db.collection('Negocios').doc(auth.negocioId);
      const data = (await ref.get()).data() || {};
      if (!isAiSampleAvailable(data)) throw fail('Esta página no usa el nuevo diseño.', 404);
      if (data.siteTemporarilyDisabled || data.websiteArchived) throw fail('El sitio no está habilitado para edición.', 403);
      const current = data.schema.aiPage;
      if (!mutate) return res.json({ ...describeAiPage(current, data.schema.brand?.logo), canRestore: Boolean(data.schema.aiPagePrevious), slug: data.slug || '' });
      if (!['save', 'preview', 'restore'].includes(req.body?.action)) throw fail('Acción de edición inválida.');
      if (req.body?.revision !== aiPageRevision(current)) throw fail('La página cambió. Recarga antes de guardar para conservar los últimos cambios.', 409);
      if (['queued', 'running'].includes(data.aiSampleJob?.state)) throw fail('Espera a que termine la generación de la página.', 409);
      const restore = req.body?.action === 'restore';
      if (restore && !data.schema.aiPagePrevious) throw fail('No hay una versión anterior guardada.');
      const result = restore ? { page: data.schema.aiPagePrevious.page, replacements: new Map() } : await editAiPage(current, req.body, { validateImage: (url) => validateImage(url, auth.negocioId) });
      const logo = restore ? data.schema.aiPagePrevious.logo : result.newLogo || result.replacements.get(data.schema.brand?.logo) || data.schema.brand?.logo || '';
      if (req.body?.action !== 'preview') {
        await db.runTransaction(async (tx) => {
          const latest = (await tx.get(ref)).data() || {};
          if (aiPageRevision(latest.schema?.aiPage) !== req.body.revision || ['queued', 'running'].includes(latest.aiSampleJob?.state)) throw fail('La página cambió. Recarga antes de guardar.', 409);
          tx.update(ref, { 'schema.aiPage': result.page, 'schema.aiPagePrevious': { page: current, logo: data.schema.brand?.logo || '' }, 'schema.brand.logo': logo, updatedAt: new Date().toISOString() });
        });
      }
      return res.json({ ...describeAiPage(result.page, logo), canRestore: true, slug: data.slug || '' });
    } catch (error) {
      if (!error.status) console.error('[ai-page-editor]', error.message);
      return res.status(error.status || 500).json({ error: error.status ? error.message : 'No se pudieron guardar los cambios. Intenta nuevamente.' });
    }
  };
  app.get('/api/cliente/ai-page-editor', handler(false));
  app.post('/api/cliente/ai-page-editor', handler(true));
}
