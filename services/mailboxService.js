// services/mailboxService.js
// Lógica del "mini-mail": ingesta de correo entrante (desde el Email Worker),
// alta de buzón con contraseña, login (token), bandeja, lectura y envío.
// El envío reutiliza CorporateEmailService.sendCorporateEmail (Cloudflare).
import crypto from 'node:crypto';
import {
  hashPassword,
  verifyPassword,
  signMailboxToken,
  verifyMailboxToken,
} from '../utils/mailboxAuth.js';
import { parseMailboxImport } from './mailboxImportParser.js';

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

function parseBoolean(value, defaultValue = true) {
  if (value === undefined || value === null || value === '') return defaultValue;
  if (typeof value === 'boolean') return value;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'si', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return defaultValue;
}

function uniqueEmails(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeEmail(value))
        .filter(Boolean)
    )
  );
}

function numberOrDefault(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function safeFileName(value = '') {
  const cleaned = cleanString(value || 'adjunto', 240)
    .replace(/[\\/:*?"<>|\r\n]+/g, '_')
    .replace(/\s+/g, ' ')
    .trim();
  return cleaned || 'adjunto';
}

function normalizeContentType(value = '') {
  const cleaned = cleanString(value, 180).toLowerCase();
  return cleaned && /^[a-z0-9.+-]+\/[a-z0-9.+-]+$/i.test(cleaned)
    ? cleaned
    : 'application/octet-stream';
}

function decodeBase64Attachment(value = '') {
  const source = String(value || '')
    .replace(/^data:[^;]+;base64,/i, '')
    .replace(/\s+/g, '');
  if (!source) return null;
  const buffer = Buffer.from(source, 'base64');
  return buffer.length > 0 ? buffer : null;
}

function serializeAttachment(attachment = {}) {
  return {
    id: cleanString(attachment.id || attachment.attachmentId, 120),
    filename: safeFileName(attachment.filename || attachment.name),
    contentType: normalizeContentType(attachment.contentType || attachment.type),
    sizeBytes: Number(attachment.sizeBytes || attachment.size || 0) || 0,
    contentId: cleanString(attachment.contentId, 200),
    disposition: cleanString(attachment.disposition || 'attachment', 40),
  };
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
    maxInboundAttachments = 5,
    maxInboundAttachmentBytes = 5 * 1024 * 1024,
    maxInboundTotalAttachmentBytes = 10 * 1024 * 1024,
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
    this.maxInboundAttachments = numberOrDefault(maxInboundAttachments, 5);
    this.maxInboundAttachmentBytes = numberOrDefault(maxInboundAttachmentBytes, 5 * 1024 * 1024);
    this.maxInboundTotalAttachmentBytes = numberOrDefault(maxInboundTotalAttachmentBytes, 10 * 1024 * 1024);
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
      attachments: Array.isArray(message.attachments)
        ? message.attachments.map(serializeAttachment).filter((item) => item.id)
        : [],
    };
  }

  normalizeInboundAttachments(attachments = []) {
    const out = [];
    let totalBytes = 0;

    for (const attachment of Array.isArray(attachments) ? attachments : []) {
      if (out.length >= this.maxInboundAttachments) break;
      const buffer = decodeBase64Attachment(
        attachment?.contentBase64 || attachment?.base64 || attachment?.content
      );
      if (!buffer) continue;
      if (buffer.length > this.maxInboundAttachmentBytes) {
        this.logger.warn?.('[mailbox] adjunto entrante omitido por tamaño:', attachment?.filename || attachment?.name || '');
        continue;
      }
      if (totalBytes + buffer.length > this.maxInboundTotalAttachmentBytes) {
        this.logger.warn?.('[mailbox] adjunto entrante omitido por límite total:', attachment?.filename || attachment?.name || '');
        continue;
      }

      const filename = safeFileName(attachment?.filename || attachment?.name || `adjunto-${out.length + 1}`);
      const contentType = normalizeContentType(attachment?.contentType || attachment?.mimeType || attachment?.type);
      const attachmentId = `att_${String(out.length + 1).padStart(2, '0')}_${crypto
        .createHash('sha1')
        .update(`${filename}:${contentType}:${buffer.length}:${out.length}`)
        .digest('hex')
        .slice(0, 12)}`;

      totalBytes += buffer.length;
      out.push({
        id: attachmentId,
        filename,
        contentType,
        sizeBytes: buffer.length,
        contentId: cleanString(attachment?.contentId, 200),
        disposition: cleanString(attachment?.disposition || 'attachment', 40),
        buffer,
      });
    }

    return out;
  }

  async ingest({ secret, to, from, subject, text, html, messageId, date, sizeBytes, inReplyTo, attachments = [] }) {
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
      const existing = await this.repo.getInboxMessage({ empresaId, correoId, messageId: inboxId });
      if (!existing) {
        const storedAttachments = [];
        for (const attachment of this.normalizeInboundAttachments(attachments)) {
          const { buffer, ...metadata } = attachment;
          try {
            const stored = await this.repo.saveInboundAttachment({
              empresaId,
              correoId,
              messageId: inboxId,
              attachmentId: metadata.id,
              filename: metadata.filename,
              contentType: metadata.contentType,
              buffer,
            });
            storedAttachments.push({ ...metadata, storagePath: stored.storagePath });
          } catch (error) {
            this.logger.error?.('[mailbox] no se pudo guardar adjunto entrante:', error?.message || error);
          }
        }

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
            attachments: storedAttachments,
          },
        });
      }
    }

    return { stored: mailboxEnabled, forwardTo };
  }

  async getAttachment({ empresaId, correoId, messageId, attachmentId }) {
    const message = await this.repo.getInboxMessage({ empresaId, correoId, messageId });
    if (!message) {
      throw new MailboxServiceError('Mensaje no encontrado', {
        code: 'MAILBOX_MESSAGE_NOT_FOUND',
        statusCode: 404,
      });
    }

    const safeAttachmentId = cleanString(attachmentId, 120);
    const attachment = (Array.isArray(message.attachments) ? message.attachments : [])
      .find((item) => cleanString(item?.id || item?.attachmentId, 120) === safeAttachmentId);
    if (!attachment?.storagePath) {
      throw new MailboxServiceError('Adjunto no encontrado', {
        code: 'MAILBOX_ATTACHMENT_NOT_FOUND',
        statusCode: 404,
      });
    }

    const buffer = await this.repo.downloadInboundAttachment({ storagePath: attachment.storagePath });
    if (!buffer) {
      throw new MailboxServiceError('Adjunto no encontrado', {
        code: 'MAILBOX_ATTACHMENT_NOT_FOUND',
        statusCode: 404,
      });
    }

    return {
      ...serializeAttachment(attachment),
      buffer,
    };
  }

  async setupMailbox({ adminSecret, ...params }) {
    if (!this.adminSecret || cleanString(adminSecret, 200) !== this.adminSecret) {
      throw new MailboxServiceError('No autorizado', {
        code: 'MAILBOX_ADMIN_UNAUTHORIZED',
        statusCode: 401,
      });
    }
    return this.enableMailboxForOwner(params);
  }

  /**
   * Activa el buzón desde el panel del dueño (sin admin secret). El acceso lo
   * autoriza el propio panel, igual que el resto de endpoints de correos.
   */
  async enableMailboxForOwner({ negocioId, empresaId, correoId, address, password, forwardCopyTo, displayName }) {
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

  /**
   * Proceso único: crea el correo (buzón) + contraseña + correo de recuperación.
   * El correo funciona al instante (buzón por Worker); la recuperación queda
   * pendiente de confirmar para que también lleguen copias ahí.
   */
  async createCorporateMailbox({ empresaId, alias, password, recoveryEmail, displayName }) {
    const pass = String(password || '');
    if (pass.length < 6) {
      throw new MailboxServiceError('La contraseña debe tener al menos 6 caracteres', {
        code: 'MAILBOX_WEAK_PASSWORD',
        statusCode: 400,
      });
    }

    // 1. Crear el correo en modo buzón (ruta al Worker, sin exigir verificación).
    let corporateEmail = null;
    try {
      corporateEmail = await this.corporate.createCorporateEmail({
        empresaId,
        alias,
        destinationEmail: recoveryEmail,
        mailbox: true,
        workerName: this.workerName,
      });
    } catch (error) {
      if (String(error?.code || '') !== 'ALIAS_ALREADY_EXISTS') {
        throw error;
      }

      corporateEmail = await this.findExistingCorporateEmailForMailbox({
        empresaId,
        alias,
      });
      if (!corporateEmail) {
        throw error;
      }
    }

    // 2. Activar el buzón: contraseña + reenvío-copia + lookup.
    const mailbox = await this.enableMailboxForOwner({
      empresaId,
      correoId: corporateEmail.id,
      address: corporateEmail.email,
      password: pass,
      forwardCopyTo: recoveryEmail,
      displayName: displayName || alias,
    });

    return { corporateEmail, mailbox };
  }

  async findExistingCorporateEmailForMailbox({ empresaId, alias }) {
    const safeEmpresaId = cleanString(empresaId, 140);
    if (!safeEmpresaId || !alias) return null;

    const safeAlias = this.corporate.validateAlias(alias);
    const company = await this.corporate.getCompanyOrThrow(safeEmpresaId);
    const domain = this.corporate.resolveDomain({ company });
    const existing = await this.corporate.repository.getCorporateEmailByAliasAndDomain({
      empresaId: safeEmpresaId,
      alias: safeAlias,
      domain,
    });
    if (!existing || String(existing.status || '').toLowerCase() === 'deleted') {
      return null;
    }
    return this.corporate.serializeCorporateEmail(existing);
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

  async send({ empresaId, mailboxEmail, to, cc, bcc, subject, text, html, attachments = [] }) {
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
      attachments,
      createdBy: mailboxEmail,
    });
  }

  async importInbox({
    empresaId,
    correoId,
    mailboxEmail,
    raw,
    fileName = '',
    markAsRead = true,
    maxMessages = 1000,
    folder = 'inbox',
  }) {
    const targetFolder = String(folder || 'inbox').toLowerCase() === 'sent' ? 'sent' : 'inbox';
    const source = String(raw || '');
    if (!source.trim()) {
      throw new MailboxServiceError('Sube un archivo mbox/EML válido.', {
        code: 'MAILBOX_IMPORT_EMPTY',
        statusCode: 400,
      });
    }

    const messages = parseMailboxImport(source, { maxMessages });
    if (messages.length === 0) {
      throw new MailboxServiceError('No se encontraron mensajes para importar.', {
        code: 'MAILBOX_IMPORT_NO_MESSAGES',
        statusCode: 400,
      });
    }

    const read = parseBoolean(markAsRead, true);
    const result = {
      total: messages.length,
      imported: 0,
      duplicates: 0,
      failed: 0,
      skipped: 0,
      fileName: cleanString(fileName, 240),
      folder: targetFolder,
    };

    for (const message of messages) {
      try {
        const providerMessageId = cleanString(message.messageId || message.importHash, 260);
        if (!providerMessageId) {
          result.skipped += 1;
          continue;
        }
        const to = uniqueEmails(message.to);
        const cc = uniqueEmails(message.cc);
        if (targetFolder === 'sent') {
          if (to.length === 0 && cc.length === 0) {
            result.skipped += 1;
            continue;
          }
          const stored = await this.corporate.importCorporateEmailMessage({
            empresaId,
            fromAlias: normalizeEmail(mailboxEmail),
            to,
            cc,
            bcc: [],
            subject: cleanString(message.subject, 300),
            text: String(message.textBody || '').slice(0, 200000),
            html: String(message.htmlBody || '').slice(0, 500000),
            providerMessageId,
            date: message.date || undefined,
            sizeBytes: Number(message.sizeBytes || 0) || 0,
            importSource: cleanString(fileName || 'mailbox-import', 240),
            createdBy: normalizeEmail(mailboxEmail),
          });
          if (stored?.duplicate) {
            result.duplicates += 1;
          } else {
            result.imported += 1;
          }
          continue;
        }

        const inboxId = this.repo.buildInboxMessageId(providerMessageId);
        const stored = await this.repo.saveInboundMessage({
          empresaId,
          correoId,
          messageId: inboxId,
          payload: {
            from: normalizeEmail(message.from),
            to: to.length > 0 ? to : [normalizeEmail(mailboxEmail)],
            cc,
            subject: cleanString(message.subject, 300),
            textBody: String(message.textBody || '').slice(0, 200000),
            htmlBody: String(message.htmlBody || '').slice(0, 500000),
            providerMessageId,
            date: cleanString(message.date, 120),
            sizeBytes: Number(message.sizeBytes || 0) || 0,
            imported: true,
            importSource: cleanString(fileName || 'mailbox-import', 240),
            read,
            createdAt: message.date || undefined,
          },
        });
        if (stored?.duplicate) {
          result.duplicates += 1;
        } else {
          result.imported += 1;
        }
      } catch (error) {
        result.failed += 1;
        this.logger.warn?.('[mailbox] import message failed:', error?.message || error);
      }
    }

    return result;
  }

  async getSent({ empresaId, mailboxEmail }) {
    const all = await this.corporate.listCorporateEmailMessages({ empresaId, limit: 100 });
    const address = normalizeEmail(mailboxEmail);
    return (Array.isArray(all) ? all : []).filter(
      (item) => normalizeEmail(item?.fromAlias) === address
    );
  }

  async getContacts({ empresaId, mailboxEmail, limit = 200 }) {
    const all = await this.corporate.listCorporateEmailMessages({ empresaId, limit });
    const address = normalizeEmail(mailboxEmail);
    const contacts = new Map();

    for (const item of Array.isArray(all) ? all : []) {
      if (normalizeEmail(item?.fromAlias) !== address) continue;
      const recipients = uniqueEmails([
        ...(item?.to || []),
        ...(item?.cc || []),
        ...(item?.bcc || []),
      ]);

      for (const email of recipients) {
        const current = contacts.get(email) || { email, count: 0, lastSentAt: null };
        current.count += 1;
        current.lastSentAt = current.lastSentAt || item?.createdAt || null;
        contacts.set(email, current);
      }
    }

    return Array.from(contacts.values()).sort((a, b) => {
      const bTime = Date.parse(b.lastSentAt || '') || 0;
      const aTime = Date.parse(a.lastSentAt || '') || 0;
      return bTime - aTime || b.count - a.count || a.email.localeCompare(b.email);
    });
  }
}
