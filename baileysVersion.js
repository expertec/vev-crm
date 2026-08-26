import { fetchLatestBaileysVersion, fetchLatestWaWebVersion } from 'baileys';

const DEFAULT_WA_WEB_VERSION = [2, 3000, 1045849355];
const VERSION_CACHE_TTL_MS = 30 * 60 * 1000;

let cachedVersion = null;
let cachedAt = 0;

function compareVersion(a = [], b = []) {
  for (let i = 0; i < 3; i += 1) {
    const diff = Number(a[i] || 0) - Number(b[i] || 0);
    if (diff !== 0) return diff;
  }
  return 0;
}

function parseEnvVersion() {
  const raw = String(process.env.WA_WEB_VERSION || '').trim();
  if (!raw) return null;

  const normalized = raw.replace(/-alpha$/i, '').replace(/\./g, ',');
  const parsed = normalized.split(',').map((part) => Number(part.trim()));
  if (parsed.length !== 3 || parsed.some((part) => !Number.isInteger(part) || part < 0)) {
    console.warn(`[WA] WA_WEB_VERSION invalida (${raw}); usando ${DEFAULT_WA_WEB_VERSION.join('.')}`);
    return null;
  }
  if (compareVersion(parsed, DEFAULT_WA_WEB_VERSION) < 0) {
    console.warn(`[WA] WA_WEB_VERSION obsoleta (${raw}); usando ${DEFAULT_WA_WEB_VERSION.join('.')}`);
    return null;
  }

  return parsed;
}

export async function getWhatsAppWebVersion() {
  const envVersion = parseEnvVersion();
  if (envVersion) return envVersion;

  if (cachedVersion && Date.now() - cachedAt < VERSION_CACHE_TTL_MS) {
    return cachedVersion;
  }

  try {
    const result = await fetchLatestWaWebVersion();
    if (result?.isLatest && Array.isArray(result.version) && result.version.length === 3) {
      cachedVersion = result.version;
      cachedAt = Date.now();
      return cachedVersion;
    }
    if (result?.error) {
      console.warn('[WA] No se pudo confirmar la version mas reciente de WhatsApp Web:', result.error?.message || result.error);
    }
  } catch (error) {
    console.warn('[WA] No se pudo obtener la version de WhatsApp Web:', error?.message || error);
  }

  try {
    const result = await fetchLatestBaileysVersion();
    if (result?.isLatest && Array.isArray(result.version) && result.version.length === 3) {
      cachedVersion = result.version;
      cachedAt = Date.now();
      return cachedVersion;
    }
    if (result?.error) {
      console.warn('[WA] No se pudo confirmar la version mas reciente de Baileys:', result.error?.message || result.error);
    }
  } catch (error) {
    console.warn('[WA] No se pudo obtener la version publicada por Baileys:', error?.message || error);
  }

  cachedVersion = DEFAULT_WA_WEB_VERSION;
  cachedAt = Date.now();
  return cachedVersion;
}
