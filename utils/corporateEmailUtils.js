const ALIAS_REGEX = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
const SIMPLE_EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const DOMAIN_LABEL_REGEX = /^[a-z0-9-]{1,63}$/;

export const DEFAULT_RESERVED_ALIASES = Object.freeze([
  'admin',
  'administrator',
  'abuse',
  'postmaster',
  'support',
  'help',
  'billing',
  'invoice',
  'root',
  'security',
  'hostmaster',
  'webmaster',
  'mailer-daemon',
  'noreply',
  'no-reply',
]);

export function normalizeAlias(value = '') {
  return String(value || '').trim().toLowerCase();
}

export function normalizeEmailAddress(value = '') {
  return String(value || '').trim().toLowerCase();
}

export function normalizeDomain(value = '') {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';

  const withoutProtocol = raw.replace(/^https?:\/\//, '');
  const noPath = withoutProtocol.split('/')[0];
  const noPort = noPath.split(':')[0];
  const clean = noPort.replace(/^\.+|\.+$/g, '');
  return clean;
}

export function isValidAlias(value = '') {
  const alias = normalizeAlias(value);
  return ALIAS_REGEX.test(alias);
}

export function isValidEmailAddress(value = '') {
  const email = normalizeEmailAddress(value);
  if (!email || email.length > 254) return false;
  return SIMPLE_EMAIL_REGEX.test(email);
}

export function isValidDomain(value = '') {
  const domain = normalizeDomain(value);
  if (!domain || domain.length > 253) return false;

  const labels = domain.split('.').filter(Boolean);
  if (labels.length < 2) return false;

  return labels.every((label) => {
    if (!DOMAIN_LABEL_REGEX.test(label)) return false;
    if (label.startsWith('-') || label.endsWith('-')) return false;
    return true;
  });
}

export function buildCorporateEmailAddress({ alias = '', domain = '' } = {}) {
  const safeAlias = normalizeAlias(alias);
  const safeDomain = normalizeDomain(domain);
  if (!safeAlias || !safeDomain) return '';
  return `${safeAlias}@${safeDomain}`;
}

export function buildCorporateEmailRecordId({ alias = '', domain = '' } = {}) {
  const safeAlias = normalizeAlias(alias);
  const safeDomain = normalizeDomain(domain);
  if (!safeAlias || !safeDomain) return '';
  return `${safeAlias}__${safeDomain}`;
}

function normalizeAliasForComparison(value = '') {
  return normalizeAlias(value).replace(/-/g, '');
}

export function buildReservedAliasSet(values = []) {
  const set = new Set();
  for (const value of values) {
    const alias = normalizeAlias(value);
    if (!alias) continue;
    set.add(alias);
    set.add(normalizeAliasForComparison(alias));
  }
  return set;
}

export function isReservedAlias(alias = '', reservedAliases = new Set()) {
  const normalized = normalizeAlias(alias);
  if (!normalized) return false;
  if (reservedAliases.has(normalized)) return true;
  return reservedAliases.has(normalizeAliasForComparison(normalized));
}

