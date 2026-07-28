const DEFAULT_WA_WEB_VERSION = [2, 3000, 1037641644];

export function getWhatsAppWebVersion() {
  const raw = String(process.env.WA_WEB_VERSION || '').trim();
  if (!raw) return DEFAULT_WA_WEB_VERSION;

  const parsed = raw.split(',').map((part) => Number(part.trim()));
  if (parsed.length !== 3 || parsed.some((part) => !Number.isInteger(part) || part < 0)) {
    console.warn(`[WA] WA_WEB_VERSION invalida (${raw}); usando ${DEFAULT_WA_WEB_VERSION.join('.')}`);
    return DEFAULT_WA_WEB_VERSION;
  }

  return parsed;
}
