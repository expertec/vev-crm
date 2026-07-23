// services/cloudflareEmailSendingClient.js
// Cliente de ENVÍO saliente con Cloudflare Email Sending (REST API oficial).
//   POST https://api.cloudflare.com/client/v4/accounts/{account_id}/email/sending/send
//   Authorization: Bearer <token con "Email Sending: Edit">
// Sustituye a Amazon SES para el botón "Enviar" del panel.
import axios from 'axios';

function cleanString(value = '', maxLength = 400) {
  return String(value ?? '').trim().slice(0, maxLength);
}

export class CloudflareEmailSendingError extends Error {
  constructor(message, { statusCode = 502, code = 'CLOUDFLARE_SEND_ERROR', details = null } = {}) {
    super(message);
    this.name = 'CloudflareEmailSendingError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

function toArray(value) {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === '') return [];
  return [value];
}

function attachmentToPayload(item = {}) {
  const filename = cleanString(item?.filename || item?.name || 'archivo-adjunto', 180);
  const type = cleanString(item?.type || item?.mimetype || 'application/octet-stream', 160)
    || 'application/octet-stream';
  let content = '';
  if (Buffer.isBuffer(item?.buffer)) {
    content = item.buffer.toString('base64');
  } else if (Buffer.isBuffer(item?.content)) {
    content = item.content.toString('base64');
  } else if (typeof item?.content === 'string') {
    content = item.encoding === 'base64'
      ? item.content
      : Buffer.from(item.content).toString('base64');
  }

  return {
    content,
    filename,
    type,
    disposition: 'attachment',
  };
}

export class CloudflareEmailSendingClient {
  constructor({
    apiToken = process.env.CLOUDFLARE_EMAIL_SENDING_API_TOKEN
      || process.env.CLOUDFLARE_EMAIL_SENDING_TOKEN
      || process.env.CLOUDFLARE_API_TOKEN,
    accountId = process.env.CLOUDFLARE_ACCOUNT_ID,
    apiBaseUrl = process.env.CLOUDFLARE_API_BASE_URL || 'https://api.cloudflare.com/client/v4',
    timeoutMs = Number(process.env.CLOUDFLARE_API_TIMEOUT_MS || 15_000),
    logger = console,
  } = {}) {
    this.apiToken = cleanString(apiToken, 400);
    this.accountId = cleanString(accountId, 120);
    this.apiBaseUrl = cleanString(apiBaseUrl, 300).replace(/\/+$/, '');
    this.timeoutMs = Number.isFinite(Number(timeoutMs)) ? Number(timeoutMs) : 15_000;
    this.logger = logger;

    this.http = axios.create({
      baseURL: this.apiBaseUrl,
      timeout: this.timeoutMs,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  isConfigured() {
    return Boolean(this.apiToken && this.accountId);
  }

  assertConfigured() {
    if (!this.apiToken) {
      throw new CloudflareEmailSendingError('Falta el token de Cloudflare Email Sending', {
        statusCode: 503,
        code: 'CLOUDFLARE_NOT_CONFIGURED',
      });
    }
    if (!this.accountId) {
      throw new CloudflareEmailSendingError('Falta CLOUDFLARE_ACCOUNT_ID', {
        statusCode: 503,
        code: 'CLOUDFLARE_ACCOUNT_REQUIRED',
      });
    }
  }

  async sendEmail({
    from,
    to = [],
    cc = [],
    bcc = [],
    replyTo = [],
    subject = '',
    html = '',
    text = '',
    attachments = [],
  } = {}) {
    this.assertConfigured();

    const safeFrom = cleanString(from, 320).toLowerCase();
    const toList = toArray(to).map((v) => cleanString(v, 320).toLowerCase()).filter(Boolean);
    const ccList = toArray(cc).map((v) => cleanString(v, 320).toLowerCase()).filter(Boolean);
    const bccList = toArray(bcc).map((v) => cleanString(v, 320).toLowerCase()).filter(Boolean);
    const replyList = toArray(replyTo).map((v) => cleanString(v, 320).toLowerCase()).filter(Boolean);

    if (!safeFrom) {
      throw new CloudflareEmailSendingError('`from` es requerido', {
        statusCode: 400,
        code: 'CLOUDFLARE_FROM_REQUIRED',
      });
    }
    if (toList.length === 0) {
      throw new CloudflareEmailSendingError('Se requiere al menos un destinatario', {
        statusCode: 400,
        code: 'CLOUDFLARE_TO_REQUIRED',
      });
    }

    const payload = { from: safeFrom, to: toList, subject: cleanString(subject, 998) };
    if (html) payload.html = String(html);
    if (text) payload.text = String(text);
    if (ccList.length) payload.cc = ccList;
    if (bccList.length) payload.bcc = bccList;
    if (replyList.length) payload.reply_to = replyList;
    const attachmentPayload = toArray(attachments)
      .map((item) => attachmentToPayload(item))
      .filter((item) => item.content && item.filename);
    if (attachmentPayload.length) payload.attachments = attachmentPayload;

    let response;
    try {
      response = await this.http.post(
        `/accounts/${this.accountId}/email/sending/send`,
        payload,
        {
          headers: { Authorization: `Bearer ${this.apiToken}` },
          validateStatus: () => true,
        }
      );
    } catch (error) {
      throw new CloudflareEmailSendingError(
        cleanString(error?.message || 'Error de conexión con Cloudflare', 300),
        { statusCode: 502, code: 'CLOUDFLARE_SEND_NETWORK' }
      );
    }

    const data = response?.data || {};
    if (data?.success !== true) {
      const errors = Array.isArray(data?.errors) ? data.errors : [];
      const first = errors[0] || {};
      const message = cleanString(first?.message || 'Cloudflare rechazó el envío', 400);
      const code = first?.code ? `CLOUDFLARE_${first.code}` : 'CLOUDFLARE_SEND_ERROR';
      const status = Number(response?.status || 0);
      throw new CloudflareEmailSendingError(message, {
        statusCode: status >= 400 && status < 500 ? 400 : 502,
        code,
        details: data,
      });
    }

    const result = data?.result || {};
    const delivered = Array.isArray(result.delivered) ? result.delivered : [];
    const queued = Array.isArray(result.queued) ? result.queued : [];
    const bounces = Array.isArray(result.permanent_bounces) ? result.permanent_bounces : [];

    // Cloudflare no devuelve un messageId único; usamos el header si viene, o vacío.
    const headers = response?.headers || {};
    const messageId = cleanString(
      headers['cf-message-id'] || headers['x-message-id'] || headers['message-id'] || '',
      200
    );

    if (bounces.length > 0 && delivered.length === 0 && queued.length === 0) {
      throw new CloudflareEmailSendingError(
        `Rebote permanente: ${bounces.join(', ')}`,
        { statusCode: 400, code: 'CLOUDFLARE_PERMANENT_BOUNCE', details: result }
      );
    }

    return { messageId, delivered, queued, bounces, raw: result };
  }
}
