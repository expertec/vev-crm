export function buildAiSamplePublicUrl(slug = '') {
  const value = String(slug || '').trim();
  if (!/^[a-z0-9][a-z0-9_-]{0,99}$/i.test(value)) return '';
  return `https://negociosweb.mx/${encodeURIComponent(value)}`;
}
