const GOOGLE_PLACES_TEXT_SEARCH_URL = 'https://places.googleapis.com/v1/places:searchText';
const DEFAULT_FIELD_MASK = [
  'places.id',
  'places.displayName',
  'places.formattedAddress',
  'places.nationalPhoneNumber',
  'places.internationalPhoneNumber',
  'places.websiteUri',
  'places.googleMapsUri',
  'places.rating',
  'places.userRatingCount',
  'places.businessStatus',
  'places.types',
  'nextPageToken',
  'searchUri',
].join(',');

const EMAIL_REGEX = /(?:mailto:)?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/g;
const SOCIAL_DOMAINS = {
  facebook: /(?:^|\.)facebook\.com$/i,
  instagram: /(?:^|\.)instagram\.com$/i,
  whatsapp: /(?:^|\.)wa\.me$|(?:^|\.)whatsapp\.com$/i,
};
const DEFAULT_SERVICE_PROFILE = [
  'paginas web profesionales para negocios locales',
  'CRM para administrar prospectos y clientes',
  'automatizacion de WhatsApp para seguimiento y ventas',
  'correos corporativos',
  'formularios, muestras web y embudos de captacion',
].join(', ');

function cleanText(value = '', maxLength = 280) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxLength);
}

function cleanDigits(value = '') {
  return String(value || '').replace(/\D/g, '');
}

function clampNumber(value, min, max, fallback) {
  const number = Number(value);
  if (!Number.isFinite(number)) return fallback;
  return Math.min(max, Math.max(min, number));
}

function unique(values = []) {
  const seen = new Set();
  const result = [];
  values.forEach((value) => {
    const text = cleanText(value, 500);
    const key = text.toLowerCase();
    if (!text || seen.has(key)) return;
    seen.add(key);
    result.push(text);
  });
  return result;
}

function normalizeUrl(value = '') {
  const text = cleanText(value, 600);
  if (!text) return '';
  try {
    const url = new URL(text.startsWith('http') ? text : `https://${text}`);
    if (!['http:', 'https:'].includes(url.protocol)) return '';
    url.hash = '';
    return url.toString();
  } catch {
    return '';
  }
}

function parseJsonObject(text = '') {
  const raw = String(text || '').trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    const start = raw.indexOf('{');
    const end = raw.lastIndexOf('}');
    if (start < 0 || end <= start) return null;
    try {
      return JSON.parse(raw.slice(start, end + 1));
    } catch {
      return null;
    }
  }
}

function isLikelyAssetEmail(email = '') {
  return /\.(png|jpe?g|gif|webp|svg|css|js|pdf)$/i.test(email);
}

export function extractEmails(rawHtml = '') {
  const html = String(rawHtml || '');
  const matches = [];
  for (const match of html.matchAll(EMAIL_REGEX)) {
    const email = cleanText(match[1] || '').toLowerCase();
    if (!email || isLikelyAssetEmail(email)) continue;
    if (email.includes('example.') || email.includes('sentry.io')) continue;
    matches.push(email);
  }
  return unique(matches).slice(0, 8);
}

function extractLinks(rawHtml = '', baseUrl = '') {
  const html = String(rawHtml || '');
  const links = [];
  const hrefRegex = /href=["']([^"']+)["']/gi;
  for (const match of html.matchAll(hrefRegex)) {
    const href = cleanText(match[1] || '', 800);
    if (!href || href.startsWith('#') || href.startsWith('javascript:')) continue;
    try {
      links.push(new URL(href, baseUrl).toString());
    } catch {
      // ignore invalid links
    }
  }
  return unique(links);
}

function socialLinksFromUrls(urls = []) {
  const social = {};
  urls.forEach((href) => {
    try {
      const url = new URL(href);
      const host = url.hostname.replace(/^www\./i, '');
      Object.entries(SOCIAL_DOMAINS).forEach(([key, pattern]) => {
        if (!pattern.test(host)) return;
        if (!social[key]) social[key] = normalizeUrl(url.toString());
      });
    } catch {
      // ignore invalid links
    }
  });
  return social;
}

function contactPathScore(url = '') {
  const lower = String(url || '').toLowerCase();
  if (/contacto|contact|ubicacion|location|about|acerca|privacidad|privacy/.test(lower)) return 1;
  return 0;
}

function buildCandidatePages(homeUrl = '', links = []) {
  const normalizedHome = normalizeUrl(homeUrl);
  if (!normalizedHome) return [];

  const candidates = [normalizedHome];
  const sameSiteLinks = links
    .map(normalizeUrl)
    .filter(Boolean)
    .filter((href) => {
      try {
        return new URL(href).hostname.replace(/^www\./i, '') === new URL(normalizedHome).hostname.replace(/^www\./i, '');
      } catch {
        return false;
      }
    })
    .filter(contactPathScore)
    .sort((a, b) => contactPathScore(b) - contactPathScore(a));

  try {
    const home = new URL(normalizedHome);
    ['/contacto', '/contact', '/ubicacion', '/about'].forEach((path) => {
      candidates.push(new URL(path, home.origin).toString());
    });
  } catch {
    // ignore
  }

  return unique([...candidates, ...sameSiteLinks]).slice(0, 5);
}

async function fetchText(url, { timeoutMs = 7000, maxBytes = 600000 } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        accept: 'text/html,application/xhtml+xml,text/plain;q=0.8,*/*;q=0.2',
        'user-agent': 'VEV-CRM-Prospecting/1.0 (+business-contact-discovery)',
      },
    });
    if (!response.ok) return '';
    const contentType = String(response.headers.get('content-type') || '').toLowerCase();
    if (contentType && !contentType.includes('text/html') && !contentType.includes('text/plain')) {
      return '';
    }
    const reader = response.body?.getReader?.();
    if (!reader) {
      return (await response.text()).slice(0, maxBytes);
    }
    const chunks = [];
    let total = 0;
    while (total < maxBytes) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      chunks.push(value);
    }
    return new TextDecoder('utf-8').decode(Buffer.concat(chunks).subarray(0, maxBytes));
  } catch {
    return '';
  } finally {
    clearTimeout(timer);
  }
}

export function scoreProspect({ website = '', emails = [], rating = null, userRatingCount = null } = {}) {
  let score = 50;
  const reasons = [];

  if (!website) {
    score += 30;
    reasons.push('Sin sitio web detectado');
  } else {
    reasons.push('Ya tiene sitio web');
  }

  if (!emails.length) {
    score += website ? 18 : 8;
    reasons.push('Sin correo publico detectado');
  } else {
    score -= 8;
    reasons.push('Correo publico disponible');
  }

  const reviews = Number(userRatingCount || 0);
  if (reviews >= 50) {
    score += 10;
    reasons.push('Negocio con actividad visible en Google');
  } else if (reviews <= 5) {
    score += 5;
    reasons.push('Presencia digital baja en Google');
  }

  const numericRating = Number(rating);
  if (Number.isFinite(numericRating) && numericRating < 4) {
    score += 4;
    reasons.push('Rating con margen de mejora');
  }

  const finalScore = Math.max(0, Math.min(100, Math.round(score)));
  let label = 'Media';
  if (finalScore >= 78) label = 'Alta';
  if (finalScore < 58) label = 'Baja';

  return { score: finalScore, label, reasons };
}

function inferProspectFit(item = {}, { businessType = '', serviceProfile = DEFAULT_SERVICE_PROFILE } = {}) {
  let score = Number(item?.opportunity?.score || 50);
  const reasons = [];
  const segments = [];
  const website = cleanText(item.website || '', 600);
  const reviews = Number(item.userRatingCount || 0);
  const rating = Number(item.rating || 0);
  const hasEmail = Boolean(item.primaryEmail || item.emails?.length);
  const hasPhone = Boolean(item.phone || item.phoneDigits);
  const socialCount = Object.keys(item.socialLinks || {}).filter((key) => item.socialLinks[key]).length;
  const typeText = `${businessType} ${(item.types || []).join(' ')} ${item.name || ''}`.toLowerCase();

  if (!website) {
    score += 18;
    segments.push('sin_web');
    reasons.push('No tiene sitio web visible para convertir trafico de Google.');
  } else {
    segments.push('con_web');
    reasons.push('Tiene web; conviene revisar si captura prospectos y seguimiento.');
  }

  if (hasPhone) {
    score += 7;
    segments.push('whatsapp_viable');
    reasons.push('Tiene telefono publico para seguimiento por WhatsApp.');
  }

  if (!hasEmail && website) {
    score += 8;
    segments.push('sin_correo_publico');
    reasons.push('No se detecto correo publico en su sitio.');
  } else if (hasEmail) {
    score += 4;
    segments.push('email_viable');
    reasons.push('Tiene correo publico para primer contacto.');
  }

  if (reviews >= 35) {
    score += 8;
    segments.push('demanda_probada');
    reasons.push('Tiene actividad visible en Google; probablemente ya recibe busquedas.');
  } else if (reviews <= 5) {
    score += 6;
    segments.push('presencia_debil');
    reasons.push('Poca prueba social; puede necesitar presencia digital.');
  }

  if (rating > 0 && rating < 4) {
    score += 4;
    segments.push('reputacion_mejorable');
    reasons.push('Rating con margen de mejora para reputacion y seguimiento.');
  }

  if (socialCount === 0) {
    score += 5;
    segments.push('sin_redes_detectadas');
  }

  if (/restaurant|food|meal|beauty|hair|spa|dent|doctor|clinic|gym|real_estate|lodging|school|store|car|auto|lawyer|accounting/.test(typeText)) {
    score += 5;
    segments.push('giro_local_apto');
  }

  const finalScore = Math.max(0, Math.min(100, Math.round(score)));
  let label = 'Medio';
  if (finalScore >= 80) label = 'Prioritario';
  if (finalScore < 62) label = 'Bajo';

  let pitchAngle = 'Ofrecer mejora de presencia digital, captacion y seguimiento de prospectos.';
  if (!website) {
    pitchAngle = 'Ofrecer pagina web rapida con WhatsApp, formulario y correo corporativo.';
  } else if (!hasEmail) {
    pitchAngle = 'Ofrecer optimizacion del sitio para captar prospectos y agregar correo corporativo.';
  } else if (hasPhone) {
    pitchAngle = 'Ofrecer CRM y automatizacion de WhatsApp para dar seguimiento a clientes.';
  }

  const nextAction = hasEmail
    ? 'Enviar correo personalizado'
    : hasPhone
      ? 'Contactar por WhatsApp o llamada'
      : 'Abrir Google Maps y validar contacto manual';

  return {
    score: finalScore,
    label,
    reasons: unique(reasons).slice(0, 4),
    segments: unique(segments).slice(0, 6),
    pitchAngle,
    nextAction,
    serviceProfile: cleanText(serviceProfile, 500),
    source: 'rules',
  };
}

function buildFallbackZoneSuggestions({ area = '', businessType = '' } = {}) {
  const base = cleanText(area || 'tu zona', 120);
  const type = cleanText(businessType || 'negocios locales', 80);
  const names = [
    `Zona centro de ${base}`,
    `Zona norte de ${base}`,
    `Zona sur de ${base}`,
    `Zona poniente de ${base}`,
    `Zona oriente de ${base}`,
    `Plazas comerciales en ${base}`,
    `Avenidas principales de ${base}`,
    `Colonias residenciales con comercio en ${base}`,
  ];
  return names.map((name, index) => ({
    id: `zone_${index + 1}`,
    name,
    searchArea: name,
    reason: `Buena zona para buscar ${type} con necesidad de presencia digital local.`,
    suggestedBusinessTypes: unique([type, 'clinicas', 'esteticas', 'restaurantes']).slice(0, 4),
    priority: index < 3 ? 'Alta' : index < 6 ? 'Media' : 'Exploratoria',
  }));
}

async function callOpenAiJson({ apiKey, model, messages, timeoutMs = 12000 } = {}) {
  if (!apiKey) return null;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model,
        temperature: 0.2,
        response_format: { type: 'json_object' },
        messages,
      }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) return null;
    return parseJsonObject(payload?.choices?.[0]?.message?.content || '');
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

function normalizeAiFit(value = {}, fallback = {}) {
  const score = Math.max(0, Math.min(100, Math.round(Number(value.score ?? fallback.score ?? 50))));
  let label = cleanText(value.label || fallback.label || 'Medio', 40);
  if (!['Prioritario', 'Medio', 'Bajo'].includes(label)) {
    label = score >= 80 ? 'Prioritario' : score < 62 ? 'Bajo' : 'Medio';
  }
  return {
    score,
    label,
    reasons: unique(Array.isArray(value.reasons) ? value.reasons : fallback.reasons || []).slice(0, 4),
    segments: unique(Array.isArray(value.segments) ? value.segments : fallback.segments || []).slice(0, 6),
    pitchAngle: cleanText(value.pitchAngle || fallback.pitchAngle || '', 260),
    nextAction: cleanText(value.nextAction || fallback.nextAction || '', 160),
    source: value.source || 'ai',
  };
}

function mapGooglePlace(place = {}, { emails = [], socialLinks = {}, scannedPages = [] } = {}) {
  const website = normalizeUrl(place.websiteUri || '');
  const phone = cleanText(place.internationalPhoneNumber || place.nationalPhoneNumber || '', 80);
  const rating = Number.isFinite(Number(place.rating)) ? Number(place.rating) : null;
  const userRatingCount = Number.isFinite(Number(place.userRatingCount)) ? Number(place.userRatingCount) : null;
  const opportunity = scoreProspect({
    website,
    emails,
    rating,
    userRatingCount,
  });

  return {
    placeId: cleanText(place.id || '', 180),
    name: cleanText(place.displayName?.text || place.displayName || '', 180),
    address: cleanText(place.formattedAddress || '', 260),
    phone,
    phoneDigits: cleanDigits(phone),
    website,
    googleMapsUrl: normalizeUrl(place.googleMapsUri || ''),
    rating,
    userRatingCount,
    businessStatus: cleanText(place.businessStatus || '', 80),
    types: Array.isArray(place.types) ? place.types.slice(0, 8) : [],
    emails,
    primaryEmail: emails[0] || '',
    socialLinks,
    scannedPages,
    opportunity,
    source: 'google_places',
  };
}

export async function scanBusinessWebsite(website = '') {
  const normalized = normalizeUrl(website);
  if (!normalized) {
    return { emails: [], socialLinks: {}, scannedPages: [] };
  }

  const homeHtml = await fetchText(normalized);
  const homeLinks = extractLinks(homeHtml, normalized);
  const pages = buildCandidatePages(normalized, homeLinks);
  const pageHtml = [homeHtml];
  const scannedPages = homeHtml ? [normalized] : [];

  for (const pageUrl of pages.slice(1)) {
    const html = await fetchText(pageUrl, { timeoutMs: 5000, maxBytes: 400000 });
    if (!html) continue;
    scannedPages.push(pageUrl);
    pageHtml.push(html);
  }

  const combined = pageHtml.join('\n');
  const links = unique([...homeLinks, ...pageHtml.flatMap((html) => extractLinks(html, normalized))]);
  return {
    emails: extractEmails(combined),
    socialLinks: socialLinksFromUrls(links),
    scannedPages: unique(scannedPages).slice(0, 5),
  };
}

export class ProspectingService {
  constructor({
    apiKey = process.env.GOOGLE_PLACES_API_KEY,
    openAiApiKey = process.env.OPENAI_API_KEY,
    aiModel = process.env.PROSPECTING_AI_MODEL || 'gpt-4o-mini',
    serviceProfile = process.env.PROSPECTING_SERVICE_PROFILE || DEFAULT_SERVICE_PROFILE,
    logger = console,
  } = {}) {
    this.apiKey = apiKey;
    this.openAiApiKey = openAiApiKey;
    this.aiModel = aiModel;
    this.serviceProfile = serviceProfile;
    this.logger = logger;
  }

  isConfigured() {
    return Boolean(cleanText(this.apiKey || ''));
  }

  isAiConfigured() {
    return Boolean(cleanText(this.openAiApiKey || ''));
  }

  async recommendZones({ area = '', businessType = '', serviceProfile = this.serviceProfile } = {}) {
    const safeArea = cleanText(area, 120);
    const safeBusinessType = cleanText(businessType, 100);
    const fallback = buildFallbackZoneSuggestions({ area: safeArea, businessType: safeBusinessType });
    if (!this.isAiConfigured()) {
      return { source: 'rules', items: fallback };
    }

    const payload = await callOpenAiJson({
      apiKey: this.openAiApiKey,
      model: this.aiModel,
      messages: [
        {
          role: 'system',
          content: 'Eres un analista comercial B2B para vender servicios digitales a negocios locales. Responde solo JSON valido.',
        },
        {
          role: 'user',
          content: JSON.stringify({
            task: 'Sugiere zonas de prospeccion. Pueden ser colonias, corredores, plazas, avenidas, zonas comerciales o areas dentro/cerca del area dada. No inventes direcciones exactas; usa nombres de busqueda utiles para Google Places.',
            area: safeArea,
            businessType: safeBusinessType,
            services: serviceProfile,
            schema: {
              items: [
                {
                  name: 'string',
                  searchArea: 'string',
                  reason: 'string',
                  suggestedBusinessTypes: ['string'],
                  priority: 'Alta|Media|Exploratoria',
                },
              ],
            },
          }),
        },
      ],
    });

    const items = Array.isArray(payload?.items) ? payload.items : [];
    const normalized = items.slice(0, 10).map((item, index) => ({
      id: `zone_ai_${index + 1}`,
      name: cleanText(item.name || item.searchArea || fallback[index]?.name || '', 120),
      searchArea: cleanText(item.searchArea || item.name || fallback[index]?.searchArea || '', 140),
      reason: cleanText(item.reason || fallback[index]?.reason || '', 240),
      suggestedBusinessTypes: unique(Array.isArray(item.suggestedBusinessTypes) ? item.suggestedBusinessTypes : []).slice(0, 4),
      priority: ['Alta', 'Media', 'Exploratoria'].includes(item.priority) ? item.priority : fallback[index]?.priority || 'Media',
    })).filter((item) => item.name && item.searchArea);

    return {
      source: normalized.length ? 'ai' : 'rules',
      items: normalized.length ? normalized : fallback,
    };
  }

  async classifyProspects({ items = [], area = '', businessType = '', useAi = true } = {}) {
    const fallbackItems = items.map((item) => ({
      ...item,
      fit: inferProspectFit(item, {
        area,
        businessType,
        serviceProfile: this.serviceProfile,
      }),
    }));
    if (!useAi || !this.isAiConfigured() || !fallbackItems.length) {
      return { source: 'rules', items: fallbackItems };
    }

    const compactItems = fallbackItems.slice(0, 25).map((item) => ({
      placeId: item.placeId,
      name: item.name,
      address: item.address,
      website: Boolean(item.website),
      email: Boolean(item.primaryEmail),
      phone: Boolean(item.phone || item.phoneDigits),
      rating: item.rating,
      reviews: item.userRatingCount,
      types: item.types,
      ruleFit: item.fit,
    }));
    const payload = await callOpenAiJson({
      apiKey: this.openAiApiKey,
      model: this.aiModel,
      messages: [
        {
          role: 'system',
          content: 'Eres un analista comercial B2B. Clasifica prospectos para vender paginas web, CRM, WhatsApp automatizado y correos corporativos. Responde solo JSON valido.',
        },
        {
          role: 'user',
          content: JSON.stringify({
            task: 'Clasifica cada prospecto por probabilidad de funcionar para mis servicios. Prioriza negocios locales con necesidad clara, contacto viable y actividad comercial.',
            area,
            businessType,
            services: this.serviceProfile,
            prospects: compactItems,
            schema: {
              fits: [
                {
                  placeId: 'string',
                  score: 0,
                  label: 'Prioritario|Medio|Bajo',
                  reasons: ['string'],
                  segments: ['string'],
                  pitchAngle: 'string',
                  nextAction: 'string',
                },
              ],
            },
          }),
        },
      ],
    });

    const fits = new Map();
    (Array.isArray(payload?.fits) ? payload.fits : []).forEach((fit) => {
      const key = cleanText(fit.placeId || '', 180);
      if (!key) return;
      fits.set(key, fit);
    });

    const aiItems = fallbackItems.map((item) => {
      const aiFit = fits.get(item.placeId);
      if (!aiFit) return item;
      return {
        ...item,
        fit: normalizeAiFit({ ...aiFit, source: 'ai' }, item.fit),
      };
    });

    return {
      source: fits.size ? 'ai' : 'rules',
      items: fits.size ? aiItems : fallbackItems,
    };
  }

  async search({
    area = '',
    businessType = '',
    maxResults = 20,
    scanWebsites = true,
    pageToken = '',
    useAiClassification = true,
  } = {}) {
    if (!this.isConfigured()) {
      const error = new Error('Falta configurar GOOGLE_PLACES_API_KEY en el servidor.');
      error.statusCode = 503;
      error.code = 'GOOGLE_PLACES_NOT_CONFIGURED';
      throw error;
    }

    const safeArea = cleanText(area, 120);
    const safeBusinessType = cleanText(businessType, 120);
    if (!safeArea || !safeBusinessType) {
      const error = new Error('Captura giro y zona para buscar negocios.');
      error.statusCode = 400;
      error.code = 'INVALID_PROSPECTING_QUERY';
      throw error;
    }

    const pageSize = clampNumber(maxResults, 1, 20, 20);
    const requestBody = {
      textQuery: `${safeBusinessType} en ${safeArea}`,
      pageSize,
      languageCode: 'es',
      regionCode: 'MX',
      includePureServiceAreaBusinesses: true,
    };
    if (pageToken) requestBody.pageToken = cleanText(pageToken, 600);

    const response = await fetch(GOOGLE_PLACES_TEXT_SEARCH_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': this.apiKey,
        'X-Goog-FieldMask': DEFAULT_FIELD_MASK,
      },
      body: JSON.stringify(requestBody),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const message = payload?.error?.message || payload?.error || 'No se pudo consultar Google Places.';
      const error = new Error(message);
      error.statusCode = response.status;
      error.code = 'GOOGLE_PLACES_ERROR';
      error.details = payload;
      throw error;
    }

    const places = Array.isArray(payload.places) ? payload.places : [];
    const items = [];
    for (const place of places) {
      let websiteScan = { emails: [], socialLinks: {}, scannedPages: [] };
      if (scanWebsites && place.websiteUri) {
        websiteScan = await scanBusinessWebsite(place.websiteUri);
      }
      items.push(mapGooglePlace(place, websiteScan));
    }

    const classified = await this.classifyProspects({
      items,
      area: safeArea,
      businessType: safeBusinessType,
      useAi: useAiClassification,
    });
    const sortedItems = classified.items.sort(
      (a, b) => Number(b.fit?.score || b.opportunity?.score || 0) - Number(a.fit?.score || a.opportunity?.score || 0)
    );

    const summary = {
      total: items.length,
      withoutWebsite: sortedItems.filter((item) => !item.website).length,
      withEmail: sortedItems.filter((item) => item.emails.length > 0).length,
      highOpportunity: sortedItems.filter((item) => item.opportunity.label === 'Alta').length,
      priorityFit: sortedItems.filter((item) => item.fit?.label === 'Prioritario').length,
    };

    return {
      query: {
        area: safeArea,
        businessType: safeBusinessType,
        scanWebsites: Boolean(scanWebsites),
        useAiClassification: Boolean(useAiClassification),
      },
      summary,
      items: sortedItems,
      classificationSource: classified.source,
      nextPageToken: cleanText(payload.nextPageToken || '', 600),
      googleSearchUrl: normalizeUrl(payload.searchUri || ''),
      searchedAt: new Date().toISOString(),
    };
  }
}
