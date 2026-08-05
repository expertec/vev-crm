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
  constructor({ apiKey = process.env.GOOGLE_PLACES_API_KEY, logger = console } = {}) {
    this.apiKey = apiKey;
    this.logger = logger;
  }

  isConfigured() {
    return Boolean(cleanText(this.apiKey || ''));
  }

  async search({ area = '', businessType = '', maxResults = 20, scanWebsites = true, pageToken = '' } = {}) {
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

    const summary = {
      total: items.length,
      withoutWebsite: items.filter((item) => !item.website).length,
      withEmail: items.filter((item) => item.emails.length > 0).length,
      highOpportunity: items.filter((item) => item.opportunity.label === 'Alta').length,
    };

    return {
      query: {
        area: safeArea,
        businessType: safeBusinessType,
        scanWebsites: Boolean(scanWebsites),
      },
      summary,
      items,
      nextPageToken: cleanText(payload.nextPageToken || '', 600),
      googleSearchUrl: normalizeUrl(payload.searchUri || ''),
      searchedAt: new Date().toISOString(),
    };
  }
}
