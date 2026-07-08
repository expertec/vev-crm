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

  // ⚠️ TEMPORAL — Diagnóstico de solo lectura: ¿con qué proveedor envía el panel?
  // NO expone secretos, solo si cada variable existe (true/false).
  //   GET /api/test/send-provider-status
  router.get('/send-provider-status', (req, res) => {
    const has = (name) => Boolean(String(process.env[name] || '').trim());
    const sesConfigured =
      has('AWS_REGION') && has('AWS_ACCESS_KEY_ID') && has('AWS_SECRET_ACCESS_KEY');
    const cloudflareSendConfigured =
      has('CLOUDFLARE_ACCOUNT_ID') &&
      (has('CLOUDFLARE_EMAIL_SENDING_API_TOKEN') || has('CLOUDFLARE_API_TOKEN'));
    return res.status(200).json({
      success: true,
      // Lo que el CÓDIGO del panel usa hoy para enviar:
      panelSendRoute: 'POST /api/web/empresas/:empresaId/correos-corporativos/enviar',
      panelSendHandler: 'sendAmazonSesEmail (Amazon SES / @aws-sdk/client-sesv2)',
      aws: {
        AWS_REGION: has('AWS_REGION'),
        AWS_ACCESS_KEY_ID: has('AWS_ACCESS_KEY_ID'),
        AWS_SECRET_ACCESS_KEY: has('AWS_SECRET_ACCESS_KEY'),
      },
      sesConfigured,
      cloudflareSend: {
        CLOUDFLARE_ACCOUNT_ID: has('CLOUDFLARE_ACCOUNT_ID'),
        CLOUDFLARE_EMAIL_SENDING_API_TOKEN: has('CLOUDFLARE_EMAIL_SENDING_API_TOKEN'),
        CLOUDFLARE_API_TOKEN: has('CLOUDFLARE_API_TOKEN'),
      },
      cloudflareSendConfigured,
      conclusion: sesConfigured
        ? 'El panel está enviando por Amazon SES (hay credenciales AWS configuradas).'
        : 'No hay credenciales AWS: si el envío del panel "funciona", en realidad estaría fallando o no usando este código.',
      note: 'El envío Cloudflare solo existe en /api/test/cloudflare-email (prueba, NO conectado al botón Enviar).',
    });
  });

  // ⚠️ TEMPORAL — ¿están puestas las variables del mini-mail? (sin exponer valores)
  //   GET /api/test/mailbox-config-status
  router.get('/mailbox-config-status', (req, res) => {
    const has = (name) => Boolean(String(process.env[name] || '').trim());
    return res.status(200).json({
      success: true,
      MAILBOX_INGEST_SECRET: has('MAILBOX_INGEST_SECRET'),
      MAILBOX_JWT_SECRET: has('MAILBOX_JWT_SECRET'),
      MAILBOX_ADMIN_SECRET: has('MAILBOX_ADMIN_SECRET'),
      MAILBOX_WORKER_NAME: String(process.env.MAILBOX_WORKER_NAME || '(default) negociosweb-mail-inbound'),
      note: 'Si MAILBOX_ADMIN_SECRET es false, /mailbox/setup siempre responde No autorizado. Ponla en Render y reinicia.',
    });
  });

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

  // ⚠️ TEMPORAL — Diagnóstico de RECEPCIÓN (Email Routing) de solo lectura.
  // Usa el MISMO token que la recepción (CLOUDFLARE_API_TOKEN) para revelar si le
  // faltan permisos de Email Routing (403), si el routing está habilitado, si hay
  // MX, y si los destinos están verificados.
  //   GET /api/test/cloudflare-routing-status?domain=negociosweb.mx
  router.get('/cloudflare-routing-status', async (req, res) => {
    const requiredSecret = String(process.env.CLOUDFLARE_EMAIL_TEST_SECRET || '').trim();
    if (requiredSecret && String(req.header('x-test-secret') || '').trim() !== requiredSecret) {
      return res.status(401).json({
        success: false,
        code: 'CF_EMAIL_TEST_UNAUTHORIZED',
        error: 'Falta o no coincide el header x-test-secret.',
      });
    }

    // Token de RECEPCIÓN (el que usa el módulo de correos corporativos).
    const token = String(process.env.CLOUDFLARE_API_TOKEN || '').trim();
    const accountIdEnv = resolveAccountId();
    const domain = String(req.query?.domain || '').trim().toLowerCase();

    if (!token) {
      return res.status(500).json({
        success: false,
        code: 'CF_ROUTING_TOKEN_MISSING',
        error: 'Falta CLOUDFLARE_API_TOKEN (token con permisos de Email Routing).',
      });
    }
    if (!domain) {
      return res.status(400).json({
        success: false,
        code: 'CF_DOMAIN_REQUIRED',
        error: 'Indica ?domain=tu-dominio.com',
      });
    }

    const http = axios.create({
      baseURL: CF_API_BASE,
      timeout: Number(process.env.CLOUDFLARE_API_TIMEOUT_MS || 15_000),
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      validateStatus: () => true,
    });

    const cfGet = async (path, params) => {
      const r = await http.get(path, { params });
      const data = r?.data || {};
      return {
        httpStatus: r.status,
        ok: data?.success === true,
        permissionError: r.status === 403,
        errors: Array.isArray(data?.errors) ? data.errors : [],
        result: data?.result,
      };
    };

    const steps = {};
    const diagnosis = [];

    // 1) Zona
    const zone = await cfGet('/zones', { name: domain, per_page: 1 });
    const zoneRow = Array.isArray(zone.result) ? zone.result[0] : null;
    const zoneId = zoneRow?.id || '';
    const accountId = accountIdEnv || zoneRow?.account?.id || '';
    const zoneStatus = zoneRow?.status || '';
    const zoneNameServers = Array.isArray(zoneRow?.name_servers) ? zoneRow.name_servers : [];
    steps.zone = {
      httpStatus: zone.httpStatus,
      permissionError: zone.permissionError,
      found: Boolean(zoneId),
      zoneId,
      accountId,
      status: zoneStatus,
      nameServers: zoneNameServers,
      totalZonesWithThisName: Array.isArray(zone.result) ? zone.result.length : 0,
      errors: zone.errors,
    };
    if (zoneStatus && zoneStatus !== 'active') {
      diagnosis.push(`⚠️ La zona del token está en estado "${zoneStatus}" (no "active"). Puede no ser la autoritativa.`);
    }
    if (zone.permissionError) {
      diagnosis.push('❌ El token CLOUDFLARE_API_TOKEN no tiene permiso para leer zonas (Zone: Read).');
    }
    if (!zoneId) {
      diagnosis.push(`❌ ${domain} no es una zona de esta cuenta de Cloudflare (o el token no la ve).`);
    }

    // 2) Estado de Email Routing en la zona
    if (zoneId) {
      const routing = await cfGet(`/zones/${zoneId}/email/routing`);
      const enabled = routing.result?.enabled === true;
      steps.emailRouting = {
        httpStatus: routing.httpStatus,
        permissionError: routing.permissionError,
        enabled,
        status: routing.result?.status || '',
        name: routing.result?.name || '',
        errors: routing.errors,
      };
      if (routing.permissionError) {
        diagnosis.push('❌ El token no tiene permiso de Email Routing en la zona (falta "Email Routing Rules: Edit" o "Zone: Read").');
      } else if (!enabled) {
        diagnosis.push(`❌ Email Routing NO está habilitado para ${domain}. Sin esto no se reciben correos ni llegan verificaciones.`);
      } else {
        diagnosis.push(`✅ Email Routing habilitado para ${domain} (status: ${routing.result?.status || 'n/a'}).`);
      }

      // 3) Registros MX de la zona
      const mx = await cfGet(`/zones/${zoneId}/dns_records`, { type: 'MX', per_page: 100 });
      const mxRecords = (Array.isArray(mx.result) ? mx.result : []).map((r) => ({
        name: r?.name,
        content: r?.content,
        priority: r?.priority,
        proxied: r?.proxied === true,
      }));
      steps.mx = {
        httpStatus: mx.httpStatus,
        permissionError: mx.permissionError,
        count: mxRecords.length,
        records: mxRecords,
      };
      const hasCloudflareMx = mxRecords.some((r) => /mx\.cloudflare\.net$/i.test(String(r.content || '')));
      if (mx.permissionError) {
        diagnosis.push('❌ El token no tiene permiso "DNS: Read" para revisar los MX.');
      } else if (mxRecords.length === 0) {
        diagnosis.push('❌ El dominio no tiene registros MX. Email Routing no puede recibir.');
      } else if (!hasCloudflareMx) {
        diagnosis.push('⚠️ Los MX no apuntan a *.mx.cloudflare.net. Puede que otro proveedor tenga el MX del dominio.');
      } else {
        diagnosis.push('✅ MX de Cloudflare Email Routing presentes.');
      }
    }

    // 4) Direcciones de destino (a nivel cuenta) y su verificación
    if (accountId) {
      const dest = await cfGet(`/accounts/${accountId}/email/routing/addresses`, { per_page: 200 });
      const addresses = (Array.isArray(dest.result) ? dest.result : []).map((d) => ({
        email: String(d?.email || '').toLowerCase(),
        verified: Boolean(d?.verified),
      }));
      steps.destinations = {
        httpStatus: dest.httpStatus,
        permissionError: dest.permissionError,
        count: addresses.length,
        verifiedCount: addresses.filter((a) => a.verified).length,
        addresses,
        errors: dest.errors,
      };
      if (dest.permissionError) {
        diagnosis.push('❌ El token NO tiene "Email Routing Addresses: Edit" → por eso no puede crear destinos ni enviar el correo de verificación. (Causa más probable de tu problema.)');
      } else {
        const unverified = addresses.filter((a) => !a.verified).map((a) => a.email);
        if (unverified.length > 0) {
          diagnosis.push(`⚠️ Destinos sin verificar: ${unverified.join(', ')}. Reenvía la verificación desde el dashboard.`);
        }
      }
    } else {
      diagnosis.push('⚠️ No se pudo resolver accountId; revisa CLOUDFLARE_ACCOUNT_ID.');
    }

    // Comparación con el DNS público EN VIVO (DoH a Cloudflare, sin token) para
    // detectar si el token administra una zona distinta de la autoritativa.
    try {
      const doh = await axios.get('https://cloudflare-dns.com/dns-query', {
        params: { name: domain, type: 'MX' },
        headers: { accept: 'application/dns-json' },
        timeout: 8000,
        validateStatus: () => true,
      });
      const answers = Array.isArray(doh.data?.Answer) ? doh.data.Answer : [];
      const liveMx = answers
        .map((a) => String(a?.data || '').trim())
        .filter(Boolean)
        .map((entry) => {
          const parts = entry.split(/\s+/);
          const host = String(parts[parts.length - 1] || '').replace(/\.$/, '').toLowerCase();
          const priority = Number(parts.length > 1 ? parts[0] : 0);
          return { host, priority };
        });
      const zoneMxContents = (steps.mx?.records || [])
        .map((r) => String(r.content || '').replace(/\.$/, '').toLowerCase());
      const liveHosts = liveMx.map((m) => m.host);
      const liveMatchesZone = liveHosts.length > 0 && liveHosts.every((h) => zoneMxContents.includes(h));
      steps.liveDns = {
        source: 'cloudflare-dns.com',
        mx: liveMx,
        matchesTokenZone: liveMatchesZone,
      };

      if (liveHosts.length > 0 && !liveMatchesZone) {
        diagnosis.push(
          `🚨 El MX EN VIVO (${liveHosts.join(', ')}) NO coincide con la zona del token (${zoneMxContents.join(', ') || 'vacío'}). `
          + 'Muy probablemente negociosweb.mx está en OTRA zona/cuenta de Cloudflare y el token administra la zona equivocada. '
          + 'Arreglar el routing por API en esta zona NO afectará el correo real hasta consolidar en una sola zona autoritativa.'
        );
      }
      if (liveHosts.some((h) => /_dc-mx\./i.test(h))) {
        diagnosis.push('⚠️ El MX en vivo parece de cPanel/HostGator (_dc-mx). El correo entrante hoy se entrega ahí, no en Cloudflare Routing.');
      }
    } catch (dohError) {
      steps.liveDns = { error: dohError?.message || 'DoH lookup failed' };
    }

    return res.status(200).json({
      success: true,
      domain,
      tokenUsedEnv: 'CLOUDFLARE_API_TOKEN',
      steps,
      diagnosis,
    });
  });

  // ⚠️ TEMPORAL — Arreglo de RECEPCIÓN por API (Email Routing).
  //   POST /api/test/cloudflare-routing-fix
  //   body: { "domain": "negociosweb.mx", "apply": false, "removeConflictingRootMx": false }
  // - Sin apply (default): DRY-RUN, solo reporta el plan (no modifica nada).
  // - apply:true → habilita Email Routing + añade/asegura MX+SPF de recepción.
  // - removeConflictingRootMx:true → además elimina los MX de la raíz que NO son de Cloudflare
  //   (ej. mail.negociosweb.mx) que interceptan el correo entrante.
  const handleRoutingFix = async (req, res) => {
    const requiredSecret = String(process.env.CLOUDFLARE_EMAIL_TEST_SECRET || '').trim();
    const providedSecret =
      String(req.header('x-test-secret') || req.query?.secret || '').trim();
    if (requiredSecret && providedSecret !== requiredSecret) {
      return res.status(401).json({
        success: false,
        code: 'CF_EMAIL_TEST_UNAUTHORIZED',
        error: 'Falta o no coincide el secret (header x-test-secret o ?secret=).',
      });
    }

    // Acepta parámetros por query (navegador / GET) o por body (POST).
    const src = { ...(req.query || {}), ...(req.body || {}) };
    const token = String(process.env.CLOUDFLARE_API_TOKEN || '').trim();
    const accountIdEnv = resolveAccountId();
    const domain = String(src.domain || '').trim().toLowerCase();
    const apply = src.apply === true || src.apply === 'true';
    const removeConflictingRootMx =
      src.removeConflictingRootMx === true || src.removeConflictingRootMx === 'true';

    if (!token) {
      return res.status(500).json({
        success: false,
        code: 'CF_ROUTING_TOKEN_MISSING',
        error: 'Falta CLOUDFLARE_API_TOKEN (token con permisos de Email Routing).',
      });
    }
    if (!domain) {
      return res.status(400).json({ success: false, code: 'CF_DOMAIN_REQUIRED', error: 'Indica domain en el body.' });
    }

    const http = axios.create({
      baseURL: CF_API_BASE,
      timeout: Number(process.env.CLOUDFLARE_API_TIMEOUT_MS || 15_000),
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      validateStatus: () => true,
    });
    const cf = async (method, path, { params, data } = {}) => {
      const r = await http.request({ method, url: path, params, data });
      const body = r?.data || {};
      return {
        httpStatus: r.status,
        ok: body?.success === true,
        errors: Array.isArray(body?.errors) ? body.errors : [],
        result: body?.result,
      };
    };
    const isCloudflareMx = (content) => /\.mx\.cloudflare\.net$/i.test(String(content || ''));

    // Resolver zona
    const zone = await cf('GET', '/zones', { params: { name: domain, per_page: 1 } });
    const zoneRow = Array.isArray(zone.result) ? zone.result[0] : null;
    const zoneId = zoneRow?.id || '';
    const accountId = accountIdEnv || zoneRow?.account?.id || '';
    if (!zoneId) {
      return res.status(404).json({
        success: false,
        code: 'CF_ZONE_NOT_FOUND',
        error: `No se encontró la zona ${domain} con este token.`,
        zoneStep: { httpStatus: zone.httpStatus, errors: zone.errors },
      });
    }

    // MX actuales de la raíz
    const mxBefore = await cf('GET', `/zones/${zoneId}/dns_records`, { params: { type: 'MX', per_page: 100 } });
    const mxRecords = (Array.isArray(mxBefore.result) ? mxBefore.result : []).map((r) => ({
      id: r?.id,
      name: String(r?.name || '').toLowerCase(),
      content: r?.content,
      priority: r?.priority,
    }));
    const rootMx = mxRecords.filter((r) => r.name === domain);
    const conflictingRootMx = rootMx.filter((r) => !isCloudflareMx(r.content));

    const routingBefore = await cf('GET', `/zones/${zoneId}/email/routing`);
    const enabledBefore = routingBefore.result?.enabled === true;

    const plannedActions = [];
    if (conflictingRootMx.length) {
      plannedActions.push(
        `Eliminar MX raíz conflictivo: ${conflictingRootMx.map((r) => `${r.content} (prio ${r.priority})`).join(', ')}`
      );
    }
    if (!enabledBefore) plannedActions.push('Habilitar Email Routing (POST /email/routing/enable)');
    plannedActions.push('Asegurar MX+SPF de recepción (POST /email/routing/dns)');

    if (!apply) {
      return res.status(200).json({
        success: true,
        mode: 'dry-run',
        domain,
        zoneId,
        accountId,
        currentRouting: { enabled: enabledBefore, status: routingBefore.result?.status || '' },
        rootMx,
        conflictingRootMx,
        plannedActions,
        note:
          'DRY-RUN: no se modificó nada. Reenvía con {"apply": true} para ejecutar. '
          + 'Para eliminar el MX raíz conflictivo agrega {"removeConflictingRootMx": true} '
          + '(solo si NO usas un servidor de correo propio en ese host).',
      });
    }

    // ---- APLICAR ----
    const actions = [];
    const warnings = [];

    if (conflictingRootMx.length) {
      if (removeConflictingRootMx) {
        for (const rec of conflictingRootMx) {
          const del = await cf('DELETE', `/zones/${zoneId}/dns_records/${rec.id}`);
          actions.push({
            action: 'delete_root_mx',
            content: rec.content,
            httpStatus: del.httpStatus,
            ok: del.ok,
            errors: del.errors,
          });
        }
      } else {
        warnings.push(
          `Se dejó el MX raíz conflictivo (${conflictingRootMx.map((r) => r.content).join(', ')}); `
          + 'seguirá interceptando el correo entrante. Reenvía con removeConflictingRootMx:true para eliminarlo.'
        );
      }
    }

    const enable = await cf('POST', `/zones/${zoneId}/email/routing/enable`, { data: {} });
    actions.push({ action: 'enable_routing', httpStatus: enable.httpStatus, ok: enable.ok, errors: enable.errors });

    const dns = await cf('POST', `/zones/${zoneId}/email/routing/dns`, { data: {} });
    actions.push({ action: 'ensure_routing_dns', httpStatus: dns.httpStatus, ok: dns.ok, errors: dns.errors });

    // Verificación final
    const routingAfter = await cf('GET', `/zones/${zoneId}/email/routing`);
    const mxAfterResp = await cf('GET', `/zones/${zoneId}/dns_records`, { params: { type: 'MX', per_page: 100 } });
    const rootMxAfter = (Array.isArray(mxAfterResp.result) ? mxAfterResp.result : [])
      .filter((r) => String(r?.name || '').toLowerCase() === domain)
      .map((r) => ({ content: r?.content, priority: r?.priority, isCloudflare: isCloudflareMx(r?.content) }));

    const enabledAfter = routingAfter.result?.enabled === true;
    const rootMxOk = rootMxAfter.length > 0
      && rootMxAfter.every((r) => r.isCloudflare);

    return res.status(200).json({
      success: true,
      mode: 'apply',
      domain,
      zoneId,
      accountId,
      actions,
      warnings,
      routingBefore: { enabled: enabledBefore, status: routingBefore.result?.status || '' },
      routingAfter: { enabled: enabledAfter, status: routingAfter.result?.status || '' },
      rootMxAfter,
      receptionReady: enabledAfter && rootMxOk,
      note: enabledAfter && rootMxOk
        ? '✅ Recepción lista: Email Routing habilitado y MX raíz apuntando a Cloudflare.'
        : 'Aún no queda listo: revisa actions/warnings y el MX raíz (puede tardar unos minutos en propagar).',
    });
  };

  // Mismo handler por GET (navegador) y POST. En el navegador:
  //   .../cloudflare-routing-fix?domain=negociosweb.mx                      (dry-run)
  //   .../cloudflare-routing-fix?domain=negociosweb.mx&apply=true&removeConflictingRootMx=true
  router.get('/cloudflare-routing-fix', handleRoutingFix);
  router.post('/cloudflare-routing-fix', handleRoutingFix);

  return router;
}
