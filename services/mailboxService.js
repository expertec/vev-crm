// services/mailboxService.js
// Lógica del "mini-mail": ingesta de correo entrante (desde el Email Worker),
// alta de buzón con contraseña, login (token), bandeja, lectura y envío.
// El envío reutiliza CorporateEmailService.sendCorporateEmail (Cloudflare).
import {
  hashPassword,
  verifyPassword,
  signMailboxToken,
  verifyMailboxToken,
} from '../utils/mailboxAuth.js';

function cleanString(value = '', maxLength = 300) {
  return String(value ?? '').trim().slice(0, maxLength);
}

function normalizeEmail(value = '') {
  return String(value || '').trim().toLowerCase();
}

function toIso(value) {
  if (!value) return null;
  if (typeof value?.toDate === 'function') {
    try {
      return value.toDate().toISOString();
    } catch {
      return null;
    }
  }
  if (value instanceof Date) return value.toISOString();
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}

export class MailboxServiceError extends Error {
  constructor(message, { code = 'MAILBOX_ERROR', statusCode = 400 } = {}) {
    super(message);
    this.name = 'MailboxServiceError';
    this.code = code;
    this.statusCode = statusCode;
  }
}

export class MailboxService {
  constructor({
    mailboxRepository,
    corporateEmailService,
    routingClient = null,
    workerName = '',
    ingestSecret,
    jwtSecret,
    adminSecret,
    tokenTtlSeconds = 60 * 60 * 12,
    logger = console,
  } = {}) {
    if (!mailboxRepository || !corporateEmailService) {
      throw new Error('MailboxService requiere mailboxRepository y corporateEmailService');
    }
    this.repo = mailboxRepository;
    this.corporate = corporateEmailService;
    this.routingClient = routingClient;
    this.workerName = cleanString(workerName, 200);
    this.ingestSecret = cleanString(ingestSecret, 200);
    this.jwtSecret = cleanString(jwtSecret, 200);
    this.adminSecret = cleanString(adminSecret, 200);
    this.tokenTtlSeconds = Number(tokenTtlSeconds) || 60 * 60 * 12;
    this.logger = logger;
  }

  /**
   * Apunta (por API) la regla de Email Routing del alias hacia el Worker de
   * captura. Best-effort: no rompe el alta del buzón si falla.
   */
  async pointAliasToWorker({ empresaId, correoId, data }) {
    if (!this.routingClient || !this.workerName) {
      return { pointedToWorker: false, reason: 'Worker no configurado (falta MAILBOX_WORKER_NAME).' };
    }
    const zoneId = cleanString(data?.cloudflareZoneId, 120);
    const sourceEmail = normalizeEmail(data?.email);
    const ruleId = cleanString(data?.cloudflareRuleId || data?.cloudflareRuleTag, 160);

    if (!zoneId) {
      return { pointedToWorker: false, reason: 'El alias no tiene zoneId guardado; apúntalo al Worker manualmente.' };
    }
    if (!sourceEmail) {
      return { pointedToWorker: false, reason: 'El alias no tiene dirección resoluble.' };
    }

    try {
      const rule = await this.routingClient.upsertWorkerRoutingRule({
        zoneId,
        sourceEmail,
        workerName: this.workerName,
        ruleId,
      });
      await this.repo.setMailboxConfig({
        empresaId,
        correoId,
        patch: {
          cloudflareRuleId: cleanString(rule?.id || ruleId, 160),
          mailboxRoutingWorker: true,
        },
      });
      return { pointedToWorker: true, reason: '', ruleId: cleanString(rule?.id || ruleId, 160) };
    } catch (error) {
      this.logger.error?.('[mailbox] no se pudo apuntar el alias al Worker:', error?.message || error);
      return {
        pointedToWorker: false,
        reason: cleanString(error?.message || 'No se pudo apuntar el alias al Worker', 300),
      };
    }
  }

  serializeInbound(message = {}) {
    return {
      id: cleanString(message.id, 180),
      from: normalizeEmail(message.from),
      to: Array.isArray(message.to) ? message.to : (message.to ? [message.to] : []),
      cc: Array.isArray(message.cc) ? message.cc : [],
      subject: cleanString(message.subject, 300),
      textBody: typeof message.textBody === 'string' ? message.textBody : '',
      htmlBody: typeof message.htmlBody === 'string' ? message.htmlBody : '',
      read: message.read === true,
      date: cleanString(message.date, 60),
      createdAt: toIso(message.createdAt),
    };
  }

  async ingest({ secret, to, from, subject, text, html, messageId, date, sizeBytes, inReplyTo }) {
    if (!this.ingestSecret || cleanString(secret, 200) !== this.ingestSecret) {
      throw new MailboxServiceError('No autorizado', {
        code: 'MAILBOX_INGEST_UNAUTHORIZED',
        statusCode: 401,
      });
    }
    const address = normalizeEmail(to);
    if (!address) {
      throw new MailboxServiceError('Falta el destinatario `to`', {
        code: 'MAILBOX_TO_REQUIRED',
        statusCode: 400,
      });
    }

    const found = await this.repo.findCorporateEmailByAddress(address);
    if (!found) {
      // Correo desconocido: no guardamos ni reenviamos.
      return { stored: false, forwardTo: '' };
    }

    const { empresaId, correoId, data } = found;
    const forwardTo = normalizeEmail(
      data?.forwardCopyTo || data?.destination || data?.destinationEmail || ''
    );
    const mailboxEnabled = data?.mailboxEnabled === true;

    if (mailboxEnabled) {
      const inboxId = this.repo.buildInboxMessageId(messageId);
      await this.repo.saveInboundMessage({
        empresaId,
        correoId,
        messageId: inboxId,
        payload: {
          from: normalizeEmail(from),
          to: [address],
          subject: cleanString(subject, 300),
          textBody: String(text || '').slice(0, 200000),
          htmlBody: String(html || '').slice(0, 500000),
          providerMessageId: cleanString(messageId, 200),
          inReplyTo: cleanString(inReplyTo, 200),
          date: cleanString(date, 60),
          sizeBytes: Number(sizeBytes || 0) || 0,
        },
      });
    }

    return { stored: mailboxEnabled, forwardTo };
  }

  async setupMailbox({ adminSecret, negocioId, empresaId, correoId, address, password, forwardCopyTo, displayName }) {
    if (!this.adminSecret || cleanString(adminSecret, 200) !== this.adminSecret) {
      throw new MailboxServiceError('No autorizado', {
        code: 'MAILBOX_ADMIN_UNAUTHORIZED',
        statusCode: 401,
      });
    }

    const resolvedEmpresaId = cleanString(empresaId || negocioId, 140);
    let target = null;
    if (resolvedEmpresaId && correoId) {
      target = {
        empresaId: resolvedEmpresaId,
        correoId: cleanString(correoId, 240),
        data: await this.repo.getCorporateEmailById(resolvedEmpresaId, correoId),
      };
    } else if (resolvedEmpresaId && address) {
      // Sin índices: resuelve por negocio + dirección (lectura directa).
      target = await this.repo.getCorporateEmailByNegocioAndAddress({
        empresaId: resolvedEmpresaId,
        address,
      });
    } else if (address) {
      target = await this.repo.findCorporateEmailByAddress(address);
    }
    if (!target || !target.data) {
      throw new MailboxServiceError(
        resolvedEmpresaId
          ? 'No se encontró ese correo en el negocio indicado. Revisa el negocioId y que el alias exista.'
          : 'Indica el `negocioId` del negocio para activar el buzón.',
        { code: 'MAILBOX_CORREO_NOT_FOUND', statusCode: 404 }
      );
    }

    const pass = String(password || '');
    if (pass.length < 6) {
      throw new MailboxServiceError('La contraseña debe tener al menos 6 caracteres', {
        code: 'MAILBOX_WEAK_PASSWORD',
        statusCode: 400,
      });
    }

    const patch = {
      mailboxEnabled: true,
      passwordHash: hashPassword(pass),
      displayName: cleanString(displayName, 120),
    };
    if (forwardCopyTo !== undefined) {
      patch.forwardCopyTo = normalizeEmail(forwardCopyTo);
    }

    const updated = await this.repo.setMailboxConfig({
      empresaId: target.empresaId,
      correoId: target.correoId,
      patch,
    });

    // Registra el correo en la tablita de lookup (así ingest/login funcionan sin índices).
    await this.repo.putLookup({
      address: normalizeEmail(updated?.email || target?.data?.email || address),
      empresaId: target.empresaId,
      correoId: target.correoId,
    }).catch((error) => {
      this.logger.error?.('[mailbox] no se pudo guardar el lookup:', error?.message || error);
    });

    // Apuntar (por API) el alias al Worker de captura, para que empiece a recibir.
    const routing = await this.pointAliasToWorker({
      empresaId: target.empresaId,
      correoId: target.correoId,
      data: updated || target.data,
    });

    return {
      empresaId: target.empresaId,
      correoId: target.correoId,
      email: normalizeEmail(updated?.email || target?.data?.email),
      mailboxEnabled: true,
      displayName: patch.displayName,
      forwardCopyTo: patch.forwardCopyTo ?? normalizeEmail(updated?.forwardCopyTo),
      routing,
    };
  }

  async login({ email, password }) {
    const address = normalizeEmail(email);
    if (!address || !password) {
      throw new MailboxServiceError('Correo y contraseña requeridos', {
        code: 'MAILBOX_CREDENTIALS_REQUIRED',
        statusCode: 400,
      });
    }
    if (!this.jwtSecret) {
      throw new MailboxServiceError('El servidor no tiene configurado MAILBOX_JWT_SECRET', {
        code: 'MAILBOX_JWT_NOT_CONFIGURED',
        statusCode: 503,
      });
    }

    const found = await this.repo.findCorporateEmailByAddress(address);
    const data = found?.data;
    if (!found || data?.mailboxEnabled !== true || !data?.passwordHash
      || !verifyPassword(String(password), data.passwordHash)) {
      throw new MailboxServiceError('Correo o contraseña incorrectos', {
        code: 'MAILBOX_INVALID_CREDENTIALS',
        statusCode: 401,
      });
    }

    const token = signMailboxToken(
      { empresaId: found.empresaId, correoId: found.correoId, email: address },
      { secret: this.jwtSecret, expiresInSeconds: this.tokenTtlSeconds }
    );

    return {
      token,
      mailbox: {
        email: address,
        displayName: cleanString(data.displayName, 120),
        domain: cleanString(data.domain, 200),
      },
    };
  }

  authenticate(token) {
    if (!this.jwtSecret) return null;
    const claims = verifyMailboxToken(token, { secret: this.jwtSecret });
    if (!claims || !claims.empresaId || !claims.correoId || !claims.email) return null;
    return {
      empresaId: cleanString(claims.empresaId, 140),
      correoId: cleanString(claims.correoId, 240),
      email: normalizeEmail(claims.email),
    };
  }

  async getInbox({ empresaId, correoId, limit = 50 }) {
    const rows = await this.repo.listInbox({ empresaId, correoId, limit });
    return rows.map((row) => this.serializeInbound(row));
  }

  async getMessage({ empresaId, correoId, messageId }) {
    const message = await this.repo.getInboxMessage({ empresaId, correoId, messageId });
    if (!message) {
      throw new MailboxServiceError('Mensaje no encontrado', {
        code: 'MAILBOX_MESSAGE_NOT_FOUND',
        statusCode: 404,
      });
    }
    await this.repo.markInboxRead({ empresaId, correoId, messageId }).catch(() => {});
    return this.serializeInbound({ ...message, read: true });
  }

  async send({ empresaId, mailboxEmail, to, cc, bcc, subject, text, html }) {
    // El remitente se fuerza al correo del buzón autenticado (no arbitrario).
    return this.corporate.sendCorporateEmail({
      empresaId,
      fromAlias: mailboxEmail,
      to,
      cc,
      bcc,
      subject,
      text,
      html,
      createdBy: mailboxEmail,
    });
  }

  async getSent({ empresaId, mailboxEmail }) {
    const all = await this.corporate.listCorporateEmailMessages({ empresaId, limit: 100 });
    const address = normalizeEmail(mailboxEmail);
    return (Array.isArray(all) ? all : []).filter(
      (item) => normalizeEmail(item?.fromAlias) === address
    );
  }
}
