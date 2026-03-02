import { generateCompleteSchema } from '../schemaGenerator.js';
import { NotFoundError, ProcessingError } from './processInformationErrors.js';

export const PROCESS_INFORMATION_VERSION = 'process-information-v1';

const DEFAULT_TIMEOUT_MS = Number(process.env.PROCESS_INFORMATION_TIMEOUT_MS || 45_000);
const DEFAULT_MAX_RETRIES = Number(process.env.PROCESS_INFORMATION_MAX_RETRIES || 2);

function isPlainObject(value) {
  return (
    value !== null
    && typeof value === 'object'
    && !Array.isArray(value)
    && Object.prototype.toString.call(value) === '[object Object]'
  );
}

function safeTrim(value, maxLength = 1200) {
  const text = String(value ?? '')
    .replace(/[\u0000-\u001f\u007f]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return text.slice(0, maxLength);
}

function sanitizeUrl(value) {
  const text = safeTrim(value, 2048);
  if (!text) return '';
  try {
    const parsed = new URL(text);
    if (parsed.protocol === 'http:' || parsed.protocol === 'https:') {
      return parsed.toString();
    }
  } catch {
    return '';
  }
  return '';
}

function sanitizeColor(value) {
  const text = safeTrim(value, 16);
  return /^#(?:[0-9a-f]{3}){1,2}$/i.test(text) ? text : '';
}

function sanitizeArray(values, mapper) {
  if (!Array.isArray(values)) return [];
  const out = [];
  for (const value of values) {
    const mapped = mapper(value);
    if (mapped) out.push(mapped);
  }
  return out;
}

function normalizeTemplateId(value) {
  const next = safeTrim(value, 40).toLowerCase();
  if (next === 'ecommerce' || next === 'booking' || next === 'info') return next;
  return 'info';
}

function normalizeWhatsapp(value) {
  return safeTrim(value, 30).replace(/[^\d+]/g, '');
}

function normalizeEmail(value) {
  const email = safeTrim(value, 160).toLowerCase();
  if (!email) return '';
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return '';
  return email;
}

function normalizeKeyItems(items = []) {
  const list = sanitizeArray(items, (item) => {
    if (isPlainObject(item)) {
      return safeTrim(
        item.label
          || item.name
          || item.title
          || item.text
          || item.value
          || '',
        100
      );
    }
    return safeTrim(item, 100);
  });
  return Array.from(new Set(list)).slice(0, 8);
}

function extractAdvancedBriefText(advancedBrief = '') {
  if (typeof advancedBrief === 'string') return safeTrim(advancedBrief, 4000);
  if (Array.isArray(advancedBrief)) {
    return advancedBrief
      .map((part) => extractAdvancedBriefText(part))
      .filter(Boolean)
      .join('. ')
      .slice(0, 4000);
  }
  if (isPlainObject(advancedBrief)) {
    return Object.values(advancedBrief)
      .map((part) => extractAdvancedBriefText(part))
      .filter(Boolean)
      .join('. ')
      .slice(0, 4000);
  }
  return '';
}

function sanitizePhotoUrls(rawUrls = []) {
  return Array.from(
    new Set(
      sanitizeArray(rawUrls, (url) => sanitizeUrl(url))
    )
  ).slice(0, 12);
}

function inferSlug(negocioId, data = {}) {
  const direct = safeTrim(data.slug || data?.schema?.slug || '', 90)
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '');
  if (direct) return direct;

  const fromName = safeTrim(data.companyInfo || data?.schema?.brand?.name || '', 90)
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '');
  if (fromName) return fromName;
  return `negocio-${safeTrim(negocioId, 50).toLowerCase().replace(/[^a-z0-9_-]+/g, '-') || 'site'}`;
}

function normalizeNegocioPayload(negocioId, data = {}) {
  const templateId = normalizeTemplateId(data.templateId || data?.schema?.templateId || 'info');
  const companyInfo = safeTrim(
    data.companyInfo
      || data.businessName
      || data?.schema?.brand?.name
      || 'Negocio Local',
    120
  );
  const businessStoryBase = safeTrim(
    data.businessStory
      || data.description
      || '',
    2200
  );
  const advancedBriefText = extractAdvancedBriefText(
    data.advancedBrief ?? data?.briefWeb?.advancedBrief ?? ''
  );
  const keyItems = normalizeKeyItems(
    data.keyItems || data?.schema?.keyItems || data?.briefWeb?.keyItems || []
  );
  const businessStory = [businessStoryBase, advancedBriefText]
    .filter(Boolean)
    .join('. ')
    .slice(0, 3000);
  const socialFacebook = sanitizeUrl(data.socialFacebook || data?.schema?.contact?.facebook || '');
  const socialInstagram = sanitizeUrl(data.socialInstagram || data?.schema?.contact?.instagram || '');
  const logoURL = sanitizeUrl(data.logoURL || data?.schema?.brand?.logo || '');
  const photoURLs = sanitizePhotoUrls(
    data.photoURLs
      || data?.schema?.gallery?.images
      || data?.gallery
      || []
  );
  const leadPhone = normalizeWhatsapp(data.leadPhone || '');
  const contactWhatsapp = normalizeWhatsapp(
    data.contactWhatsapp || data?.schema?.contact?.whatsapp || leadPhone
  );
  const contactEmail = normalizeEmail(data.contactEmail || data?.schema?.contact?.email || '');
  const palette = sanitizeArray(data.palette || [], (color) => sanitizeColor(color)).slice(0, 6);
  const primaryColor = sanitizeColor(data.primaryColor || palette[0] || '');
  const businessSector = safeTrim(
    data.businessSector || data?.schema?.businessSector || data?.schema?.brand?.sector || '',
    100
  );
  const locationRaw = isPlainObject(data.location) ? data.location : {};

  const location = {
    address: safeTrim(locationRaw.address || data.address || '', 180),
    city: safeTrim(locationRaw.city || data.city || '', 80),
    state: safeTrim(locationRaw.state || data.state || '', 80),
    country: safeTrim(locationRaw.country || data.country || 'Mexico', 80),
    postalCode: safeTrim(locationRaw.postalCode || data.postalCode || '', 20),
    mapUrl: sanitizeUrl(locationRaw.mapUrl || data.mapUrl || ''),
  };

  return {
    templateId,
    companyInfo,
    businessStory,
    advancedBriefText,
    businessSector,
    keyItems,
    photoURLs,
    logoURL,
    contactWhatsapp,
    contactEmail,
    socialFacebook,
    socialInstagram,
    location,
    palette,
    primaryColor,
    slug: inferSlug(negocioId, data),
    existingSchema: isPlainObject(data.schema) ? data.schema : {},
    currentSchemaVersion: Number(data.schemaVersion || 0),
  };
}

function uniqueNonEmpty(items = []) {
  return Array.from(
    new Set(
      items
        .map((item) => safeTrim(item, 200))
        .filter(Boolean)
    )
  );
}

function buildDefaultServices(payload) {
  const fromKeyItems = payload.keyItems.map((label) => ({
    icon: 'CheckCircleOutlined',
    title: label,
    text: `Atencion especializada en ${label.toLowerCase()}.`,
    imageURL: payload.photoURLs[0] || '',
  }));

  if (fromKeyItems.length) return fromKeyItems.slice(0, 6);

  return [
    {
      icon: 'CheckCircleOutlined',
      title: 'Atencion profesional',
      text: 'Soluciones enfocadas en resultados para tu negocio.',
      imageURL: payload.photoURLs[0] || '',
    },
    {
      icon: 'SafetyOutlined',
      title: 'Calidad garantizada',
      text: 'Procesos claros, calidad constante y seguimiento cercano.',
      imageURL: payload.photoURLs[1] || payload.photoURLs[0] || '',
    },
    {
      icon: 'RocketOutlined',
      title: 'Respuesta agil',
      text: 'Atendemos rapido para mantener continuidad operativa.',
      imageURL: payload.photoURLs[2] || payload.photoURLs[0] || '',
    },
  ];
}

function buildDefaultBenefits(payload) {
  return uniqueNonEmpty([
    payload.keyItems[0] ? `Resultados medibles en ${payload.keyItems[0].toLowerCase()}` : '',
    'Comunicacion directa por WhatsApp',
    'Atencion personalizada segun tus objetivos',
  ]).map((line, index) => ({
    icon: ['BulbOutlined', 'ThunderboltOutlined', 'HeartOutlined'][index] || 'CheckCircleOutlined',
    title: line.split(' ').slice(0, 4).join(' '),
    text: line,
  }));
}

function buildDefaultFaqs(payload) {
  return [
    {
      q: '¿Como puedo solicitar informacion?',
      a: 'Escríbenos por WhatsApp y te responderemos con asesoria puntual.',
    },
    {
      q: '¿En que horario atienden?',
      a: 'Atendemos en horario comercial y confirmamos tiempos de respuesta por mensaje.',
    },
    {
      q: '¿Ofrecen atencion personalizada?',
      a: 'Si, ajustamos el servicio de acuerdo con las necesidades de tu negocio.',
    },
    {
      q: '¿Donde se ubican?',
      a: payload.location.address
        ? `Nos ubicamos en ${payload.location.address}.`
        : 'Compartimos ubicacion exacta al momento de agendar contacto.',
    },
  ];
}

function buildMenu(templateId) {
  const menu = [
    { id: 'inicio', label: 'Inicio' },
    { id: 'nosotros', label: 'Nosotros' },
    { id: templateId === 'ecommerce' ? 'productos' : 'servicios', label: templateId === 'ecommerce' ? 'Productos' : 'Servicios' },
    { id: 'beneficios', label: 'Beneficios' },
    { id: 'galeria', label: 'Galeria' },
    { id: 'faq', label: 'FAQ' },
    { id: 'contacto', label: 'Contacto' },
  ];

  if (templateId === 'booking') {
    menu.splice(3, 0, { id: 'reservas', label: 'Reservas' });
  }
  return menu;
}

function removePublicAiMentions(text) {
  return safeTrim(
    String(text || '')
      .replace(/\binteligencia artificial\b/gi, 'tecnologia')
      .replace(/\bIA\b/g, '')
      .replace(/\bAI\b/g, '')
      .replace(/\s{2,}/g, ' '),
    5000
  );
}

function deepSanitizePublicText(value) {
  if (typeof value === 'string') {
    if (/^https?:\/\//i.test(value.trim())) return sanitizeUrl(value) || value.trim();
    return removePublicAiMentions(value);
  }
  if (Array.isArray(value)) return value.map((item) => deepSanitizePublicText(item));
  if (isPlainObject(value)) {
    const out = {};
    for (const [key, current] of Object.entries(value)) {
      out[key] = deepSanitizePublicText(current);
    }
    return out;
  }
  return value;
}

function deepMerge(base, override) {
  if (Array.isArray(base) && Array.isArray(override)) return [...override];
  if (!isPlainObject(base) || !isPlainObject(override)) return override;

  const output = { ...base };
  for (const [key, value] of Object.entries(override)) {
    if (isPlainObject(value) && isPlainObject(output[key])) {
      output[key] = deepMerge(output[key], value);
      continue;
    }
    output[key] = value;
  }
  return output;
}

function buildSeoSection(payload, schema) {
  const title = safeTrim(
    schema?.seo?.title
      || `${payload.companyInfo} | ${payload.templateId === 'ecommerce' ? 'Tienda en linea' : 'Sitio oficial'}`,
    70
  );

  const description = safeTrim(
    schema?.seo?.description
      || schema?.hero?.subtitle
      || schema?.about?.text
      || payload.businessStory
      || `Conoce ${payload.companyInfo} y contacta por WhatsApp.`,
    160
  );

  const keywords = uniqueNonEmpty([
    payload.companyInfo,
    payload.businessSector,
    ...payload.keyItems,
    payload.templateId === 'ecommerce' ? 'tienda online' : 'servicios',
  ]).slice(0, 12);

  return {
    title,
    description,
    keywords,
    ogImage: sanitizeUrl(schema?.seo?.ogImage || schema?.hero?.backgroundImageUrl || payload.photoURLs[0] || ''),
    canonicalPath: `/site/${payload.slug}`,
  };
}

function ensureProfessionalSchema(generatedSchema = {}, payload) {
  const base = isPlainObject(generatedSchema) ? generatedSchema : {};
  const galleryImages = sanitizePhotoUrls(
    base?.gallery?.images || payload.photoURLs
  );
  const heroImage = sanitizeUrl(
    base?.hero?.backgroundImageUrl
      || galleryImages[0]
      || payload.photoURLs[0]
      || ''
  );
  const waDigits = payload.contactWhatsapp.replace(/\D/g, '');
  const waUrl = waDigits ? `https://wa.me/${waDigits}` : '#';

  const servicesItems = Array.isArray(base?.services?.items) && base.services.items.length
    ? base.services.items
    : buildDefaultServices(payload);

  const benefits = Array.isArray(base?.benefits) && base.benefits.length
    ? base.benefits
    : buildDefaultBenefits(payload);

  const faqs = Array.isArray(base?.faqs) && base.faqs.length
    ? base.faqs
    : buildDefaultFaqs(payload);

  const schema = {
    ...base,
    templateId: payload.templateId,
    slug: payload.slug,
    brand: {
      ...(isPlainObject(base.brand) ? base.brand : {}),
      name: safeTrim(base?.brand?.name || payload.companyInfo, 120),
      logo: sanitizeUrl(base?.brand?.logo || payload.logoURL || ''),
      sector: safeTrim(base?.brand?.sector || payload.businessSector || '', 100),
    },
    businessSector: safeTrim(base?.businessSector || payload.businessSector || '', 100),
    keyItems: payload.keyItems,
    hero: {
      title: safeTrim(base?.hero?.title || payload.companyInfo, 120),
      subtitle: safeTrim(
        base?.hero?.subtitle
          || payload.businessStory
          || `Conoce ${payload.companyInfo} y descubre nuestros servicios.`,
        220
      ),
      backgroundImageUrl: heroImage,
      ctaText: safeTrim(base?.hero?.ctaText || 'Solicitar informacion', 40),
      ctaUrl: sanitizeUrl(base?.hero?.ctaUrl || waUrl) || waUrl,
      waText: safeTrim(
        base?.hero?.waText
          || `Hola, quiero mas informacion sobre ${payload.companyInfo}.`,
        240
      ),
    },
    about: {
      title: safeTrim(base?.about?.title || 'Sobre Nosotros', 80),
      text: safeTrim(
        base?.about?.text || payload.businessStory || `${payload.companyInfo} ofrece atencion profesional y cercana.`,
        1200
      ),
      mission: safeTrim(
        base?.about?.mission || 'Brindar soluciones confiables y de alto valor para cada cliente.',
        220
      ),
    },
    services: {
      title: safeTrim(
        base?.services?.title || (payload.templateId === 'ecommerce' ? 'Catalogo Principal' : 'Servicios'),
        90
      ),
      items: servicesItems,
    },
    benefits,
    gallery: {
      title: safeTrim(base?.gallery?.title || 'Galeria', 80),
      images: galleryImages.length ? galleryImages : payload.photoURLs,
    },
    faqs,
    cta: {
      title: safeTrim(base?.cta?.title || '¿Listo para avanzar?', 90),
      text: safeTrim(
        base?.cta?.text || 'Contactanos y recibe orientacion personalizada para tu negocio.',
        240
      ),
      buttonText: safeTrim(base?.cta?.buttonText || 'Contactar por WhatsApp', 40),
      buttonUrl: sanitizeUrl(base?.cta?.buttonUrl || waUrl) || waUrl,
    },
    contact: {
      whatsapp: payload.contactWhatsapp,
      email: payload.contactEmail,
      facebook: payload.socialFacebook,
      instagram: payload.socialInstagram,
      ...payload.location,
      ...(isPlainObject(base.contact) ? base.contact : {}),
    },
    menu: Array.isArray(base?.menu) && base.menu.length ? base.menu : buildMenu(payload.templateId),
    seo: buildSeoSection(payload, base),
    colors: {
      ...(isPlainObject(base.colors) ? base.colors : {}),
      ...(payload.primaryColor ? { primary: payload.primaryColor } : {}),
      ...(payload.palette.length ? { palette: payload.palette } : {}),
    },
  };

  return deepSanitizePublicText(schema);
}

export function buildDeterministicFallbackSchema(payload) {
  return ensureProfessionalSchema(
    {
      templateId: payload.templateId,
      hero: {
        title: payload.companyInfo,
        subtitle: payload.businessStory || `Descubre ${payload.companyInfo} y su propuesta de valor.`,
      },
      about: {
        title: 'Conoce Nuestro Negocio',
        text: payload.businessStory || `${payload.companyInfo} ofrece soluciones enfocadas en resultados.`,
      },
      services: {
        title: payload.templateId === 'ecommerce' ? 'Productos Destacados' : 'Servicios Principales',
      },
      cta: {
        title: 'Solicita atencion personalizada',
        text: 'Escribenos y te compartimos la mejor opcion para tu necesidad.',
      },
    },
    payload
  );
}

function withTimeout(task, timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new ProcessingError('Tiempo de espera agotado al generar schema', { code: 'TIMEOUT', retryable: true }));
    }, timeoutMs);

    Promise.resolve(task)
      .then((value) => {
        clearTimeout(timer);
        resolve(value);
      })
      .catch((error) => {
        clearTimeout(timer);
        reject(error);
      });
  });
}

async function wait(ms) {
  if (ms <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function generateSchemaWithRetries({
  schemaGenerator,
  payload,
  timeoutMs,
  maxRetries,
  logger,
}) {
  let attempts = 0;
  let lastError = null;
  const maxAttempts = Math.max(1, maxRetries + 1);

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    attempts = attempt;
    try {
      const schema = await withTimeout(
        Promise.resolve(schemaGenerator(payload)),
        timeoutMs
      );
      if (!isPlainObject(schema)) {
        throw new ProcessingError('El generador no devolvio un schema valido', {
          code: 'INVALID_SCHEMA',
          retryable: true,
        });
      }
      return { schema, attempts };
    } catch (error) {
      lastError = error;
      logger.warn(
        `[process-information] intento ${attempt}/${maxAttempts} fallido: ${error?.message || error}`
      );
      if (attempt < maxAttempts) {
        await wait(250 * attempt);
      }
    }
  }

  throw lastError || new ProcessingError('No se pudo generar schema', { retryable: true });
}

export function computeNextSchemaVersion(currentVersion) {
  const base = Number(currentVersion || 0);
  if (!Number.isFinite(base) || base < 0) return 1;
  return base + 1;
}

export async function processBusinessInformation({
  negocioId,
  source = 'informacion',
  force = false,
  triggerSequences = false,
  idempotencyHash = '',
  jobId = '',
  repository,
  schemaGenerator = generateCompleteSchema,
  logger = console,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  maxRetries = DEFAULT_MAX_RETRIES,
  sequenceService = null,
}) {
  const safeNegocioId = safeTrim(negocioId, 120);
  if (!safeNegocioId) {
    throw new ProcessingError('negocioId es requerido', { statusCode: 400, code: 'INVALID_NEGOCIO_ID' });
  }
  if (!repository || typeof repository.getNegocioById !== 'function') {
    throw new ProcessingError('Repositorio de informacion no configurado', { code: 'REPOSITORY_NOT_CONFIGURED' });
  }

  if (triggerSequences) {
    logger.warn('[process-information] triggerSequences=true fue ignorado para este endpoint');
  }
  if (sequenceService) {
    logger.debug?.('[process-information] sequenceService recibido pero no sera usado');
  }

  const startedAt = Date.now();
  const negocio = await repository.getNegocioById(safeNegocioId);
  if (!negocio) {
    throw new NotFoundError(`Negocio ${safeNegocioId} no encontrado`);
  }

  const payload = normalizeNegocioPayload(safeNegocioId, negocio);
  let generatorAttempts = 0;
  let usedFallback = false;
  let generatedSchema = null;

  try {
    const generated = await generateSchemaWithRetries({
      schemaGenerator,
      payload,
      timeoutMs: Math.max(1_000, Number(timeoutMs || DEFAULT_TIMEOUT_MS)),
      maxRetries: Math.max(0, Number(maxRetries || DEFAULT_MAX_RETRIES)),
      logger,
    });
    generatedSchema = generated.schema;
    generatorAttempts = generated.attempts;
  } catch (error) {
    usedFallback = true;
    generatorAttempts = Math.max(1, Number(maxRetries || DEFAULT_MAX_RETRIES) + 1);
    logger.error(
      `[process-information] fallback deterministico para ${safeNegocioId}: ${error?.message || error}`
    );
    generatedSchema = buildDeterministicFallbackSchema(payload);
  }

  const normalizedSchema = ensureProfessionalSchema(generatedSchema, payload);
  const mergedSchema = deepMerge(payload.existingSchema, normalizedSchema);
  const schemaVersion = computeNextSchemaVersion(payload.currentSchemaVersion);
  const durationMs = Date.now() - startedAt;

  await repository.persistProcessedInformation({
    negocioId: safeNegocioId,
    schema: mergedSchema,
    schemaVersion,
    source: safeTrim(source, 40) || 'informacion',
    durationMs,
    generatorVersion: PROCESS_INFORMATION_VERSION,
    idempotencyHash,
    usedFallback,
    attempts: generatorAttempts,
    jobId: safeTrim(jobId, 120),
    force: Boolean(force),
  });

  return {
    negocioId: safeNegocioId,
    schemaVersion,
    durationMs,
    attempts: generatorAttempts,
    usedFallback,
    idempotencyHash,
  };
}

