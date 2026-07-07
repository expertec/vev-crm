// routes/cloudflareEmailTestRoutes.js
// ⚠️ TEMPORAL — Endpoint de VALIDACIÓN de Cloudflare Email Service (Email Sending).
// Objetivo: comprobar si la REST API oficial de Cloudflare puede reemplazar a Amazon SES.
// - NO usa Amazon SES ni ninguna dependencia AWS.
// - NO toca el módulo de correos corporativos existente.
// - Usa EXCLUSIVAMENTE la REST API oficial:
//     POST https://api.cloudflare.com/client/v4/accounts/{account_id}/email/sending/send
//   con Authorization: Bearer <API_TOKEN> (permiso "Email Sending: Edit").
//
// Para eliminar tras la validación: quitar el mount en server.js y borrar este archivo.

import express from 'express';
import axios from 'axios';

const CF_API_BASE = (process.env.CLOUDFLARE_API_BASE_URL || 'https://api.cloudflare.com/client/v4')
  .replace(/\/+$/, '');

function resolveToken() {
  return String(
    process.env.CLOUDFLARE_EMAIL_SENDING_API_TOKEN
    || process.env.CLOUDFLARE_EMAIL_SENDING_TOKEN
    || process.env.CLOUDFLARE_API_TOKEN
    || ''
  ).trim();
}

function resolveAccountId() {
  return String(process.env.CLOUDFLARE_ACCOUNT_ID || '').trim();
}

function normalizeRecipients(value) {
  if (Array.isArray(value)) {
    return value.map((item) => (typeof item === 'string' ? item.trim() : item)).filter(Boolean);
  }
  if (typeof value === 'string' && value.trim()) {
    return [value.trim()];
  }
  return [];
}

export function createCloudflareEmailTestRouter({ logger = console } = {}) {
  const router = express.Router();

  router.post('/cloudflare-email', async (req, res) => {
    // Guard opcional: si defines CLOUDFLARE_EMAIL_TEST_SECRET, exige el header x-test-secret.
    const requiredSecret = String(process.env.CLOUDFLARE_EMAIL_TEST_SECRET || '').trim();
    if (requiredSecret && String(req.header('x-test-secret') || '').trim() !== requiredSecret) {
      return res.status(401).json({
        success: false,
        code: 'CF_EMAIL_TEST_UNAUTHORIZED',
        error: 'Falta o no coincide el header x-test-secret.',
      });
    }

    const token = resolveToken();
    const accountId = resolveAccountId();

    if (!token) {
      return res.status(500).json({
        success: false,
        code: 'CF_EMAIL_TOKEN_MISSING',
        error:
          'Falta el token de Cloudflare. Define CLOUDFLARE_EMAIL_SENDING_API_TOKEN (o CLOUDFLARE_API_TOKEN) con permiso "Email Sending: Edit".',
      });
    }
    if (!accountId) {
      return res.status(500).json({
        success: false,
        code: 'CF_ACCOUNT_ID_MISSING',
        error: 'Falta CLOUDFLARE_ACCOUNT_ID.',
      });
    }

    const from = String(req.body?.from || 'welcome@negociosweb.mx').trim();
    const subject = String(req.body?.subject || 'Prueba Cloudflare Email Service').trim();
    const html =
      typeof req.body?.html === 'string' && req.body.html.trim()
        ? req.body.html
        : '<h1>Prueba Cloudflare Email Service</h1><p>Este correo valida el envío saliente vía la REST API oficial de Cloudflare Email Sending.</p>';
    const text =
      typeof req.body?.text === 'string' && req.body.text.trim()
        ? req.body.text
        : 'Prueba de envío saliente vía la REST API oficial de Cloudflare Email Sending.';
    const cc = req.body?.cc;
    const bcc = req.body?.bcc;
    const replyTo = req.body?.reply_to || req.body?.replyTo;

    const recipients = normalizeRecipients(req.body?.to);
    if (recipients.length === 0) {
      return res.status(400).json({
        success: false,
        code: 'CF_EMAIL_TO_REQUIRED',
        error: 'Indica `to` como string o array de correos (ej. ["a@gmail.com","b@outlook.com"]).',
      });
    }

    const url = `${CF_API_BASE}/accounts/${encodeURIComponent(accountId)}/email/sending/send`;
    const safeEndpoint = `${CF_API_BASE}/accounts/{account_id}/email/sending/send`;
    const timeoutMs = Number(process.env.CLOUDFLARE_API_TIMEOUT_MS || 15_000);

    // Un request por destinatario para aislar el estado de entrega por proveedor
    // (Gmail / Outlook / Yahoo / otro) y ver exactamente qué responde Cloudflare.
    const startedAll = Date.now();
    const results = [];

    for (const to of recipients) {
      const payload = { to, from, subject, html, text };
      if (cc) payload.cc = cc;
      if (bcc) payload.bcc = bcc;
      if (replyTo) payload.reply_to = replyTo;

      const startedAt = Date.now();
      try {
        const response = await axios.post(url, payload, {
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
          },
          timeout: timeoutMs,
          // No lanzar por status: queremos ver el body crudo de Cloudflare siempre.
          validateStatus: () => true,
        });

        const data = response?.data || {};
        results.push({
          to,
          httpStatus: response.status,
          cloudflareAccepted: data?.success === true,
          cloudflareResponse: data, // body VERBATIM de Cloudflare (result/errors/messages)
          requestPayload: payload,
          elapsedMs: Date.now() - startedAt,
        });
      } catch (error) {
        logger.error?.('[cloudflare-email-test] request error:', error?.message || error);
        results.push({
          to,
          httpStatus: error?.response?.status || 0,
          cloudflareAccepted: false,
          cloudflareResponse: error?.response?.data || { error: error?.message || 'network error' },
          requestPayload: payload,
          error: error?.message || 'request failed',
          elapsedMs: Date.now() - startedAt,
        });
      }
    }

    const anyAccepted = results.some((item) => item.cloudflareAccepted === true);

    return res.status(200).json({
      success: true,
      provider: 'cloudflare_email_service',
      endpoint: safeEndpoint,
      tokenConfigured: true,
      accountIdConfigured: true,
      from,
      totalRecipients: recipients.length,
      anyAccepted,
      totalElapsedMs: Date.now() - startedAll,
      results,
    });
  });

  return router;
}
