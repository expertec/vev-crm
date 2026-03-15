import axios from 'axios';
import { normalizeDomain } from '../utils/corporateEmailUtils.js';

function cleanString(value = '', maxLength = 300) {
  return String(value ?? '').trim().slice(0, maxLength);
}

function getCloudflareErrorMessage(payload = {}, fallback = 'Error en Cloudflare Email Routing') {
  const errors = Array.isArray(payload?.errors) ? payload.errors : [];
  if (errors.length > 0) {
    const first = errors[0] || {};
    const message = cleanString(first?.message || '', 500);
    if (message) return message;
  }
  const resultInfo = cleanString(payload?.result_info?.message || '', 500);
  if (resultInfo) return resultInfo;
  return fallback;
}

function getCloudflareErrorCode(payload = {}, fallback = 'CLOUDFLARE_API_ERROR') {
  const errors = Array.isArray(payload?.errors) ? payload.errors : [];
  if (errors.length > 0) {
    const firstCode = String(errors[0]?.code || '').trim();
    if (firstCode) return `CLOUDFLARE_${firstCode}`;
  }
  return fallback;
}

function mapStatusCode(status) {
  if (status === 400) return 400;
  if (status === 401 || status === 403) return 503;
  if (status === 404) return 404;
  if (status === 409) return 409;
  if (status === 429) return 503;
  if (status >= 500) return 502;
  return 502;
}

function isAlreadyEnabledDnsError(error) {
  if (!(error instanceof CloudflareEmailRoutingError)) return false;
  if (![400, 409].includes(Number(error.statusCode || 0))) return false;
  const message = cleanString(error.message || '', 500).toLowerCase();
  return /already|enabled|exist|configured|mx.*lock|record/i.test(message);
}

function buildDomainCandidates(domain = '') {
  const cleanDomain = normalizeDomain(domain);
  if (!cleanDomain) return [];
  const labels = cleanDomain.split('.');
  const candidates = [];
  for (let i = 0; i < labels.length - 1; i += 1) {
    const candidate = labels.slice(i).join('.');
    if (candidate.includes('.')) candidates.push(candidate);
  }
  return candidates;
}

export class CloudflareEmailRoutingError extends Error {
  constructor(
    message,
    {
      statusCode = 502,
      code = 'CLOUDFLARE_API_ERROR',
      details = null,
    } = {}
  ) {
    super(message);
    this.name = 'CloudflareEmailRoutingError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

export class CloudflareEmailRoutingClient {
  constructor({
    apiToken = process.env.CLOUDFLARE_API_TOKEN,
    apiBaseUrl = process.env.CLOUDFLARE_API_BASE_URL || 'https://api.cloudflare.com/client/v4',
    timeoutMs = Number(process.env.CLOUDFLARE_API_TIMEOUT_MS || 12_000),
    logger = console,
  } = {}) {
    this.apiToken = cleanString(apiToken, 400);
    this.apiBaseUrl = cleanString(apiBaseUrl, 300).replace(/\/+$/, '');
    this.timeoutMs = Number.isFinite(Number(timeoutMs)) ? Number(timeoutMs) : 12_000;
    this.logger = logger;

    this.http = axios.create({
      baseURL: this.apiBaseUrl,
      timeout: this.timeoutMs,
      headers: {
        'Content-Type': 'application/json',
      },
    });
  }

  assertConfigured() {
    if (!this.apiToken) {
      throw new CloudflareEmailRoutingError('Falta CLOUDFLARE_API_TOKEN', {
        statusCode: 503,
        code: 'CLOUDFLARE_NOT_CONFIGURED',
      });
    }
  }

  async request({
    method = 'GET',
    path = '/',
    params = undefined,
    data = undefined,
  } = {}) {
    this.assertConfigured();

    try {
      const response = await this.http.request({
        method,
        url: path,
        params,
        data,
        headers: {
          Authorization: `Bearer ${this.apiToken}`,
        },
      });

      const payload = response?.data || {};
      if (payload?.success === false) {
        throw new CloudflareEmailRoutingError(
          getCloudflareErrorMessage(payload),
          {
            statusCode: mapStatusCode(response?.status || 502),
            code: getCloudflareErrorCode(payload),
            details: payload,
          }
        );
      }

      return payload?.result;
    } catch (error) {
      if (error instanceof CloudflareEmailRoutingError) throw error;

      const responsePayload = error?.response?.data || {};
      const responseStatus = Number(error?.response?.status || 0);
      const message = getCloudflareErrorMessage(
        responsePayload,
        cleanString(error?.message || 'Error de conexión con Cloudflare', 500)
      );
      const code = getCloudflareErrorCode(responsePayload);
      throw new CloudflareEmailRoutingError(message, {
        statusCode: mapStatusCode(responseStatus),
        code,
        details: responsePayload || null,
      });
    }
  }

  async resolveZoneIdByDomain(domain = '') {
    const candidates = buildDomainCandidates(domain);
    for (const candidate of candidates) {
      const result = await this.request({
        method: 'GET',
        path: '/zones',
        params: {
          name: candidate,
          status: 'active',
          match: 'all',
          per_page: 1,
          page: 1,
        },
      });
      if (Array.isArray(result) && result.length > 0) {
        const zoneId = cleanString(result[0]?.id || '', 120);
        if (zoneId) return zoneId;
      }
    }
    return '';
  }

  async ensureEmailRoutingDnsEnabled({ zoneId } = {}) {
    const safeZoneId = cleanString(zoneId, 120);
    if (!safeZoneId) {
      throw new CloudflareEmailRoutingError('Falta zoneId para habilitar DNS de Email Routing', {
        statusCode: 400,
        code: 'CLOUDFLARE_ZONE_REQUIRED',
      });
    }

    try {
      const result = await this.request({
        method: 'POST',
        path: `/zones/${safeZoneId}/email/routing/dns`,
      });
      return {
        enabled: true,
        changed: true,
        zoneId: safeZoneId,
        result: result || null,
      };
    } catch (error) {
      if (isAlreadyEnabledDnsError(error)) {
        return {
          enabled: true,
          changed: false,
          zoneId: safeZoneId,
          alreadyEnabled: true,
        };
      }
      throw error;
    }
  }

  async createRoutingRule({
    zoneId,
    sourceEmail,
    destinationEmail,
    enabled = true,
  } = {}) {
    const safeZoneId = cleanString(zoneId, 120);
    const safeSourceEmail = cleanString(sourceEmail, 254).toLowerCase();
    const safeDestinationEmail = cleanString(destinationEmail, 254).toLowerCase();

    if (!safeZoneId) {
      throw new CloudflareEmailRoutingError('Falta zoneId para crear regla de Email Routing', {
        statusCode: 400,
        code: 'CLOUDFLARE_ZONE_REQUIRED',
      });
    }

    const result = await this.request({
      method: 'POST',
      path: `/zones/${safeZoneId}/email/routing/rules`,
      data: {
        matchers: [
          {
            type: 'literal',
            field: 'to',
            value: safeSourceEmail,
          },
        ],
        actions: [
          {
            type: 'forward',
            value: [safeDestinationEmail],
          },
        ],
        enabled: enabled !== false,
      },
    });

    return {
      id: cleanString(result?.id || '', 160),
      tag: cleanString(result?.tag || '', 160),
      enabled: result?.enabled !== false,
    };
  }

  async deleteRoutingRule({ zoneId, ruleId } = {}) {
    const safeZoneId = cleanString(zoneId, 120);
    const safeRuleId = cleanString(ruleId, 160);

    if (!safeZoneId || !safeRuleId) {
      return {
        deleted: false,
        skipped: true,
      };
    }

    try {
      await this.request({
        method: 'DELETE',
        path: `/zones/${safeZoneId}/email/routing/rules/${safeRuleId}`,
      });
      return {
        deleted: true,
        skipped: false,
        notFound: false,
      };
    } catch (error) {
      if (error instanceof CloudflareEmailRoutingError && error.statusCode === 404) {
        this.logger.warn(
          `[corporate-emails] Regla ${safeRuleId} no encontrada en Cloudflare (zone ${safeZoneId})`
        );
        return {
          deleted: false,
          skipped: false,
          notFound: true,
        };
      }
      throw error;
    }
  }
}
