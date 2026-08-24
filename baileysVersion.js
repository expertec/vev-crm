import { fetchLatestWaWebVersion } from 'baileys';

const DEFAULT_WA_WEB_VERSION = [2, 3000, 1037641644];
const VERSION_CACHE_TTL_MS = 30 * 60 * 1000;

let cachedVersion = null;
let cachedAt = 0;

function parseEnvVersion() {
  const raw = String(process.env.WA_WEB_VERSION || '').trim();
  if (!raw) return null;

  const parsed = raw.split(',').map((part) => Number(part.trim()));
  if (parsed.length !== 3 || parsed.some((part) => !Number.isInteger(part) || part < 0)) {
    console.warn(`[WA] WA_WEB_VERSION invalida (${raw}); usando ${DEFAULT_WA_WEB_VERSION.join('.')}`);
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

  cachedVersion = DEFAULT_WA_WEB_VERSION;
  cachedAt = Date.now();
  return cachedVersion;
}
