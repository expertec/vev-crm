import { Timestamp } from 'firebase-admin/firestore';
import { CloudflareEmailRoutingError } from './cloudflareEmailRoutingClient.js';
import { AmazonSesClientError } from './amazonSesClient.js';
import {
  DEFAULT_RESERVED_ALIASES,
  buildCorporateEmailAddress,
  buildCorporateEmailRecordId,
  buildReservedAliasSet,
  isReservedAlias,
  isValidAlias,
  isValidDomain,
  isValidEmailAddress,
  normalizeAlias,
  normalizeDomain,
  normalizeEmailAddress,
} from '../utils/corporateEmailUtils.js';

function cleanString(value = '', maxLength = 220) {
  return String(value ?? '').trim().slice(0, maxLength);
}

function parseBoolean(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') return defaultValue;
  if (typeof value === 'boolean') return value;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'si', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return defaultValue;
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

function parseCsvOrArray(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) => cleanString(item, 320).toLowerCase())
      .filter(Boolean);
  }
  return cleanString(value, 2000)
    .split(',')
    .map((item) => cleanString(item, 320).toLowerCase())
    .filter(Boolean);
}

function extractDomainFromEmail(value = '') {
  const safeEmail = normalizeEmailAddress(value);
  if (!safeEmail || !safeEmail.includes('@')) return '';
  const [, domain = ''] = safeEmail.split('@');
  return normalizeDomain(domain);
}

function isEmailInsideDomain({
  email = '',
  domain = '',
} = {}) {
  const safeDomain = normalizeDomain(domain);
  const emailDomain = extractDomainFromEmail(email);
  if (!safeDomain || !emailDomain) return false;
  return emailDomain === safeDomain || emailDomain.endsWith(`.${safeDomain}`);
}

export class CorporateEmailServiceError extends Error {
  constructor(
    message,
    {
      code = 'CORPORATE_EMAIL_ERROR',
      statusCode = 400,
      details = null,
    } = {}
  ) {
    super(message);
    this.name = 'CorporateEmailServiceError';
    this.code = code;
    this.statusCode = statusCode;
    this.details = details;
  }
}

export class CorporateEmailService {
  constructor({
    repository,
    cloudflareClient,
    sesClient = null,
    logger = console,
    reservedAliases = undefined,
  } = {}) {
    if (!repository || !cloudflareClient) {
      throw new Error('CorporateEmailService requiere repository y cloudflareClient');
    }

    this.repository = repository;
    this.cloudflareClient = cloudflareClient;
    this.sesClient = sesClient;
    this.logger = logger;

    const envReserved = String(process.env.CORPORATE_EMAIL_RESERVED_ALIASES || '')
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean);
    const source = Array.isArray(reservedAliases) ? reservedAliases : DEFAULT_RESERVED_ALIASES;
    this.reservedAliases = buildReservedAliasSet([...source, ...envReserved]);
  }

  serializeCorporateEmail(record = {}) {
    return {
      id: cleanString(record.id || '', 260),
      empresaId: cleanString(record.empresaId || '', 140),
      alias: cleanString(record.alias || '', 80),
      email: cleanString(record.email || '', 280),
      domain: cleanString(record.domain || '', 200),
      destinationEmail: cleanString(record.destinationEmail || '', 280),
      status: cleanString(record.status || 'active', 60),
      cloudflareZoneId: cleanString(record.cloudflareZoneId || '', 120),
      cloudflareAccountId: cleanString(record.cloudflareAccountId || '', 120),
      cloudflareRuleId: cleanString(record.cloudflareRuleId || '', 180),
      cloudflareRuleTag: cleanString(record.cloudflareRuleTag || '', 180),
      cloudflareRuleEnabled: record.cloudflareRuleEnabled !== false,
      cloudflareEmailRoutingDnsEnabled: record.cloudflareEmailRoutingDnsEnabled === true,
      cloudflareEmailRoutingDnsUpdated: record.cloudflareEmailRoutingDnsUpdated === true,
      cloudflareDestinationAddressId: cleanString(record.cloudflareDestinationAddressId || '', 120),
      destinationVerifiedAt: toIso(record.destinationVerifiedAt) || cleanString(record.destinationVerifiedAt || '', 80) || null,
      createdAt: toIso(record.createdAt),
      updatedAt: toIso(record.updatedAt),
      deletedAt: toIso(record.deletedAt),
    };
  }

  serializeDestination(record = {}) {
    return {
      id: cleanString(record.id || '', 260),
      empresaId: cleanString(record.empresaId || '', 140),
      destinationEmail: cleanString(
        record.destinationEmail || record.email || '',
        280
      ).toLowerCase(),
      email: cleanString(
        record.destinationEmail || record.email || '',
        280
      ).toLowerCase(),
      domain: cleanString(record.domain || '', 200),
      status: cleanString(record.status || 'pending_verification', 80),
      cloudflareZoneId: cleanString(record.cloudflareZoneId || '', 120),
      cloudflareAccountId: cleanString(record.cloudflareAccountId || '', 120),
      cloudflareDestinationAddressId: cleanString(record.cloudflareDestinationAddressId || '', 120),
      verified: record.verified === true,
      verificationSent: record.verificationSent === true,
      destinationCreated: record.destinationCreated === true,
      createdAt: toIso(record.createdAt),
      updatedAt: toIso(record.updatedAt),
      verifiedAt: toIso(record.verifiedAt || record.destinationVerifiedAt),
    };
  }

  serializeSesSenderProfile(record = {}) {
    return {
      id: cleanString(record.id || '', 160),
      empresaId: cleanString(record.empresaId || '', 140),
      provider: cleanString(record.provider || 'amazon_ses', 80).toLowerCase(),
      enabled: record.enabled !== false,
      domain: normalizeDomain(record.domain || ''),
      fromEmail: normalizeEmailAddress(record.fromEmail || ''),
      replyToEmail: normalizeEmailAddress(record.replyToEmail || ''),
      defaultToEmail: normalizeEmailAddress(record.defaultToEmail || ''),
      displayName: cleanString(record.displayName || '', 120),
      configurationSetName: cleanString(record.configurationSetName || '', 120),
      identityType: cleanString(record.identityType || '', 80).toLowerCase(),
      identityExists: record.identityExists === true,
      identityVerified: record.identityVerified === true,
      identityDkimStatus: cleanString(record.identityDkimStatus || '', 80).toLowerCase(),
      identityCheckedAt: toIso(record.identityCheckedAt),
      createdAt: toIso(record.createdAt),
      updatedAt: toIso(record.updatedAt),
    };
  }

  ensureEmpresaId(value = '') {
    const empresaId = cleanString(value, 140);
    if (!empresaId) {
      throw new CorporateEmailServiceError('`empresaId` es requerido', {
        code: 'EMPRESA_ID_REQUIRED',
        statusCode: 400,
      });
    }
    return empresaId;
  }

  ensureSesClient() {
    if (!this.sesClient) {
      throw new CorporateEmailServiceError(
        'Amazon SES no está configurado en este servidor',
        {
          code: 'SES_NOT_CONFIGURED',
          statusCode: 503,
        }
      );
    }
    return this.sesClient;
  }

  async getCompanyOrThrow(empresaId) {
    const company = await this.repository.getCompanyById(empresaId);
    if (!company) {
      throw new CorporateEmailServiceError('Empresa no encontrada', {
        code: 'COMPANY_NOT_FOUND',
        statusCode: 404,
      });
    }
    return company;
  }

  resolveDomain({ requestedDomain = '', company = {} } = {}) {
    const domain = normalizeDomain(
      requestedDomain
      || company?.dominio
      || company?.domain
      || company?.emailDomain
      || ''
    );

    if (!domain || !isValidDomain(domain)) {
      throw new CorporateEmailServiceError(
        'Dominio invalido o no configurado para la empresa',
        {
          code: 'INVALID_DOMAIN',
          statusCode: 400,
        }
      );
    }
    return domain;
  }

  validateAlias(alias = '') {
    const safeAlias = normalizeAlias(alias);
    if (!safeAlias) {
      throw new CorporateEmailServiceError('`alias` es requerido', {
        code: 'ALIAS_REQUIRED',
        statusCode: 400,
      });
    }
    if (!isValidAlias(safeAlias)) {
      throw new CorporateEmailServiceError(
        'Alias invalido. Usa solo letras, numeros y guiones (sin espacios)',
        {
          code: 'INVALID_ALIAS',
          statusCode: 400,
        }
      );
    }
    if (isReservedAlias(safeAlias, this.reservedAliases)) {
      throw new CorporateEmailServiceError(
        'Alias reservado. Elige uno diferente',
        {
          code: 'RESERVED_ALIAS',
          statusCode: 400,
        }
      );
    }
    return safeAlias;
  }

  validateDestinationEmail(value = '') {
    const destinationEmail = normalizeEmailAddress(value);
    if (!destinationEmail) {
      throw new CorporateEmailServiceError('`destinationEmail` es requerido', {
        code: 'DESTINATION_REQUIRED',
        statusCode: 400,
      });
    }
    if (!isValidEmailAddress(destinationEmail)) {
      throw new CorporateEmailServiceError('Correo destino invalido', {
        code: 'INVALID_DESTINATION_EMAIL',
        statusCode: 400,
      });
    }
    return destinationEmail;
  }

  validateSenderEmailForDomain({
    fromEmail = '',
    domain = '',
  } = {}) {
    const safeFromEmail = normalizeEmailAddress(fromEmail);
    if (!safeFromEmail) {
      throw new CorporateEmailServiceError('`fromEmail` es requerido', {
        code: 'SES_FROM_EMAIL_REQUIRED',
        statusCode: 400,
      });
    }
    if (!isValidEmailAddress(safeFromEmail)) {
      throw new CorporateEmailServiceError('`fromEmail` es inválido', {
        code: 'SES_FROM_EMAIL_INVALID',
        statusCode: 400,
      });
    }
    if (!isEmailInsideDomain({ email: safeFromEmail, domain })) {
      throw new CorporateEmailServiceError(
        `El remitente debe pertenecer al dominio ${domain}`,
        {
          code: 'SES_FROM_EMAIL_DOMAIN_MISMATCH',
          statusCode: 400,
        }
      );
    }
    return safeFromEmail;
  }

  validateRecipientEmails(values, {
    fieldName = 'to',
    required = false,
  } = {}) {
    const recipients = parseCsvOrArray(values);
    if (required && recipients.length === 0) {
      throw new CorporateEmailServiceError(`\`${fieldName}\` es requerido`, {
        code: 'SES_RECIPIENTS_REQUIRED',
        statusCode: 400,
      });
    }

    for (const recipient of recipients) {
      if (!isValidEmailAddress(recipient)) {
        throw new CorporateEmailServiceError(
          `Correo inválido en \`${fieldName}\`: ${recipient}`,
          {
            code: 'SES_RECIPIENT_INVALID',
            statusCode: 400,
          }
        );
      }
    }

    return recipients;
  }

  resolveDestinationOwnership(record = {}, empresaId = '') {
    const owner = cleanString(
      record?.ownerEmpresaId
      || record?.destinationOwnerEmpresaId
      || '',
      140
    );
    return owner === empresaId;
  }

  collectDestinationEmailsFromCorporateRecords(records = []) {
    const emails = new Set();
    for (const record of records) {
      if (!record || typeof record !== 'object') continue;
      const status = cleanString(record?.status || '', 60).toLowerCase();
      if (status === 'deleted') continue;
      const destinationEmail = normalizeEmailAddress(
        record?.destinationEmail
        || record?.emailDestino
        || record?.destination
        || ''
      );
      if (destinationEmail) emails.add(destinationEmail);
    }
    return emails;
  }

  async resolveZoneId({ company = {}, domain = '' } = {}) {
    const fromCompany = cleanString(
      company?.cloudflareZoneId
      || company?.zoneId
      || company?.cloudflare?.zoneId
      || '',
      120
    );

    if (fromCompany) return fromCompany;

    const zoneId = await this.cloudflareClient.resolveZoneIdByDomain(domain);
    if (!zoneId) {
      throw new CorporateEmailServiceError(
        `No se pudo resolver el zoneId de Cloudflare para ${domain}`,
        {
          code: 'CLOUDFLARE_ZONE_NOT_FOUND',
          statusCode: 400,
        }
      );
    }
    return zoneId;
  }

  async resolveAccountId({ company = {}, zoneId = '' } = {}) {
    const fromCompany = cleanString(
      company?.cloudflareAccountId
      || company?.accountId
      || company?.cloudflare?.accountId
      || '',
      120
    );
    if (fromCompany) return fromCompany;

    const fromEnv = cleanString(process.env.CLOUDFLARE_ACCOUNT_ID || '', 120);
    if (fromEnv) return fromEnv;

    const accountId = await this.cloudflareClient.resolveAccountIdByZone(zoneId);
    if (!accountId) {
      throw new CorporateEmailServiceError('No se pudo resolver accountId de Cloudflare', {
        code: 'CLOUDFLARE_ACCOUNT_NOT_FOUND',
        statusCode: 400,
      });
    }
    return accountId;
  }

  mapError(error) {
    if (error instanceof CorporateEmailServiceError) return error;

    if (error instanceof CloudflareEmailRoutingError) {
      return new CorporateEmailServiceError(
        cleanString(error.message || 'Error al sincronizar con Cloudflare', 320),
        {
          code: cleanString(error.code || 'CLOUDFLARE_API_ERROR', 120),
          statusCode: Number(error.statusCode || 502),
          details: error.details || null,
        }
      );
    }

    if (error instanceof AmazonSesClientError) {
      return new CorporateEmailServiceError(
        cleanString(error.message || 'Error al sincronizar con Amazon SES', 320),
        {
          code: cleanString(error.code || 'SES_API_ERROR', 120),
          statusCode: Number(error.statusCode || 502),
          details: error.details || null,
        }
      );
    }

    if (String(error?.code || '') === 'ALIAS_ALREADY_EXISTS') {
      return new CorporateEmailServiceError('Alias no disponible para este dominio', {
        code: 'ALIAS_ALREADY_EXISTS',
        statusCode: 409,
      });
    }

    if (String(error?.code || '') === 'COMPANY_NOT_FOUND') {
      return new CorporateEmailServiceError('Empresa no encontrada', {
        code: 'COMPANY_NOT_FOUND',
        statusCode: 404,
      });
    }

    return new CorporateEmailServiceError(
      cleanString(error?.message || 'Error interno', 320),
      {
        code: cleanString(error?.code || 'INTERNAL_ERROR', 100),
        statusCode: Number.isInteger(error?.statusCode) ? error.statusCode : 500,
      }
    );
  }

  async createCorporateEmail({
    empresaId,
    alias,
    destinationEmail,
    domain,
  }) {
    try {
      const safeEmpresaId = this.ensureEmpresaId(empresaId);
      const safeAlias = this.validateAlias(alias);
      const safeDestinationEmail = this.validateDestinationEmail(destinationEmail);

      const company = await this.getCompanyOrThrow(safeEmpresaId);
      const safeDomain = this.resolveDomain({
        requestedDomain: domain,
        company,
      });
      const email = buildCorporateEmailAddress({
        alias: safeAlias,
        domain: safeDomain,
      });

      const existing = await this.repository.getCorporateEmailByAliasAndDomain({
        empresaId: safeEmpresaId,
        alias: safeAlias,
        domain: safeDomain,
      });
      if (existing && String(existing.status || '').toLowerCase() !== 'deleted') {
        throw new CorporateEmailServiceError('Alias no disponible para este dominio', {
          code: 'ALIAS_ALREADY_EXISTS',
          statusCode: 409,
        });
      }

      const zoneId = await this.resolveZoneId({
        company,
        domain: safeDomain,
      });

      const routingDns = await this.cloudflareClient.ensureEmailRoutingDnsEnabled({
        zoneId,
      });

      const accountId = await this.resolveAccountId({
        company,
        zoneId,
      });
      const destinationStatus = await this.cloudflareClient.ensureDestinationAddress({
        accountId,
        email: safeDestinationEmail,
      });
      await this.repository.upsertCorporateEmailDestination({
        empresaId: safeEmpresaId,
        destinationEmail: safeDestinationEmail,
        payload: {
          ownerEmpresaId: safeEmpresaId,
          destinationOwnerEmpresaId: safeEmpresaId,
          source: 'create_alias',
          domain: safeDomain,
          status: destinationStatus?.verified === true ? 'verified' : 'pending_verification',
          verified: destinationStatus?.verified === true,
          verificationSent: destinationStatus?.verificationSent === true,
          destinationCreated: destinationStatus?.created === true,
          cloudflareZoneId: zoneId,
          cloudflareAccountId: accountId,
          cloudflareDestinationAddressId: cleanString(destinationStatus?.id || '', 120),
          destinationVerifiedAt: cleanString(destinationStatus?.verifiedAt || '', 80) || null,
          verifiedAt: cleanString(destinationStatus?.verifiedAt || '', 80) || null,
          cloudflareLastSyncAt: Timestamp.now(),
        },
      });
      if (destinationStatus?.verified !== true) {
        throw new CorporateEmailServiceError(
          'Correo destino pendiente de verificacion. Revisa tu bandeja y confirma el correo en Cloudflare.',
          {
            code: 'DESTINATION_EMAIL_NOT_VERIFIED',
            statusCode: 409,
            details: {
              destinationEmail: safeDestinationEmail,
              cloudflareAccountId: accountId,
              destinationAddressId: cleanString(destinationStatus?.id || '', 120),
              verificationSent: destinationStatus?.verificationSent === true,
              destinationCreated: destinationStatus?.created === true,
              verified: false,
            },
          }
        );
      }

      const rule = await this.cloudflareClient.createRoutingRule({
        zoneId,
        sourceEmail: email,
        destinationEmail: safeDestinationEmail,
      });

      const correoId = buildCorporateEmailRecordId({
        alias: safeAlias,
        domain: safeDomain,
      });
      const status = rule?.enabled === false ? 'pending_verification' : 'active';

      try {
        const created = await this.repository.createCorporateEmail({
          empresaId: safeEmpresaId,
          correoId,
          payload: {
            alias: safeAlias,
            email,
            domain: safeDomain,
            destinationEmail: safeDestinationEmail,
            status,
            cloudflareZoneId: zoneId,
            cloudflareAccountId: accountId,
            cloudflareRuleId: cleanString(rule?.id || '', 180),
            cloudflareRuleTag: cleanString(rule?.tag || '', 180),
            cloudflareRuleEnabled: rule?.enabled !== false,
            cloudflareEmailRoutingDnsEnabled: routingDns?.enabled === true,
            cloudflareEmailRoutingDnsUpdated: routingDns?.changed === true,
            cloudflareDestinationAddressId: cleanString(destinationStatus?.id || '', 120),
            destinationVerifiedAt: cleanString(destinationStatus?.verifiedAt || '', 80) || null,
            cloudflareLastSyncAt: Timestamp.now(),
          },
        });

        return this.serializeCorporateEmail(created || {});
      } catch (error) {
        const rollbackRuleId = cleanString(rule?.id || rule?.tag || '', 180);
        await this.cloudflareClient.deleteRoutingRule({
          zoneId,
          ruleId: rollbackRuleId,
        }).catch((rollbackError) => {
          this.logger.error(
            `[corporate-emails] rollback fallo para ${email}: ${rollbackError?.message || rollbackError}`
          );
        });
        throw error;
      }
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async listCorporateEmails({
    empresaId,
    includeDeleted = false,
  }) {
    try {
      const safeEmpresaId = this.ensureEmpresaId(empresaId);
      await this.getCompanyOrThrow(safeEmpresaId);

      const records = await this.repository.listCorporateEmailsByCompany(safeEmpresaId);
      const include = parseBoolean(includeDeleted, false);
      const filtered = include
        ? records
        : records.filter((item) => String(item.status || '').toLowerCase() !== 'deleted');

      return filtered.map((item) => this.serializeCorporateEmail(item));
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async deleteCorporateEmail({
    empresaId,
    correoId,
  }) {
    try {
      const safeEmpresaId = this.ensureEmpresaId(empresaId);
      const safeCorreoId = cleanString(correoId, 240);
      if (!safeCorreoId) {
        throw new CorporateEmailServiceError('`correoId` es requerido', {
          code: 'CORREO_ID_REQUIRED',
          statusCode: 400,
        });
      }

      await this.getCompanyOrThrow(safeEmpresaId);

      const existing = await this.repository.getCorporateEmailById(safeEmpresaId, safeCorreoId);
      if (!existing || String(existing.status || '').toLowerCase() === 'deleted') {
        throw new CorporateEmailServiceError('Correo corporativo no encontrado', {
          code: 'CORPORATE_EMAIL_NOT_FOUND',
          statusCode: 404,
        });
      }

      const zoneId = cleanString(existing.cloudflareZoneId || '', 120);
      const ruleId = cleanString(
        existing.cloudflareRuleId || existing.cloudflareRuleTag || '',
        180
      );
      const cloudflareDelete = await this.cloudflareClient.deleteRoutingRule({
        zoneId,
        ruleId,
      });

      const updated = await this.repository.markCorporateEmailDeleted({
        empresaId: safeEmpresaId,
        correoId: safeCorreoId,
        patch: {
          cloudflareRuleEnabled: false,
          cloudflareDeleted: cloudflareDelete?.deleted === true,
          cloudflareRuleNotFound: cloudflareDelete?.notFound === true,
          cloudflareLastSyncAt: Timestamp.now(),
        },
      });

      return {
        ...this.serializeCorporateEmail(updated || {}),
        cloudflareDelete,
      };
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async validateAliasAvailability({
    empresaId,
    alias,
    domain,
  }) {
    try {
      const safeEmpresaId = this.ensureEmpresaId(empresaId);
      const company = await this.getCompanyOrThrow(safeEmpresaId);
      const safeDomain = this.resolveDomain({
        requestedDomain: domain,
        company,
      });
      const normalizedAlias = normalizeAlias(alias);

      if (!normalizedAlias) {
        return {
          available: false,
          reason: 'alias_required',
          alias: '',
          domain: safeDomain,
          email: '',
        };
      }

      if (!isValidAlias(normalizedAlias)) {
        return {
          available: false,
          reason: 'invalid_alias',
          alias: normalizedAlias,
          domain: safeDomain,
          email: '',
        };
      }

      if (isReservedAlias(normalizedAlias, this.reservedAliases)) {
        return {
          available: false,
          reason: 'reserved_alias',
          alias: normalizedAlias,
          domain: safeDomain,
          email: buildCorporateEmailAddress({
            alias: normalizedAlias,
            domain: safeDomain,
          }),
        };
      }

      const existing = await this.repository.getCorporateEmailByAliasAndDomain({
        empresaId: safeEmpresaId,
        alias: normalizedAlias,
        domain: safeDomain,
      });
      const isTaken = existing && String(existing.status || '').toLowerCase() !== 'deleted';

      return {
        available: !isTaken,
        reason: isTaken ? 'already_exists' : 'available',
        alias: normalizedAlias,
        domain: safeDomain,
        email: buildCorporateEmailAddress({
          alias: normalizedAlias,
          domain: safeDomain,
        }),
      };
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async getDestinationVerificationStatus({
    empresaId,
    destinationEmail,
    domain,
  }) {
    try {
      const safeEmpresaId = this.ensureEmpresaId(empresaId);
      const safeDestinationEmail = this.validateDestinationEmail(destinationEmail);
      const company = await this.getCompanyOrThrow(safeEmpresaId);
      const safeDomain = this.resolveDomain({
        requestedDomain: domain,
        company,
      });
      const zoneId = await this.resolveZoneId({
        company,
        domain: safeDomain,
      });
      const accountId = await this.resolveAccountId({
        company,
        zoneId,
      });

      const destination = await this.cloudflareClient.findDestinationAddressByEmail({
        accountId,
        email: safeDestinationEmail,
      });

      await this.repository.upsertCorporateEmailDestination({
        empresaId: safeEmpresaId,
        destinationEmail: safeDestinationEmail,
        payload: {
          ownerEmpresaId: safeEmpresaId,
          destinationOwnerEmpresaId: safeEmpresaId,
          source: 'verification_check',
          domain: safeDomain,
          status: destination?.verified === true ? 'verified' : 'pending_verification',
          verified: destination?.verified === true,
          verificationSent: false,
          destinationCreated: false,
          cloudflareZoneId: zoneId,
          cloudflareAccountId: accountId,
          cloudflareDestinationAddressId: cleanString(destination?.id || '', 120),
          destinationVerifiedAt: cleanString(destination?.verifiedAt || '', 80) || null,
          verifiedAt: cleanString(destination?.verifiedAt || '', 80) || null,
          cloudflareLastSyncAt: Timestamp.now(),
        },
      });

      return {
        destinationEmail: safeDestinationEmail,
        domain: safeDomain,
        cloudflareZoneId: zoneId,
        cloudflareAccountId: accountId,
        exists: Boolean(destination),
        verified: destination?.verified === true,
        destinationAddressId: cleanString(destination?.id || '', 120),
        verifiedAt: cleanString(destination?.verifiedAt || '', 80) || null,
        createdAt: cleanString(destination?.createdAt || '', 80) || null,
        modifiedAt: cleanString(destination?.modifiedAt || '', 80) || null,
      };
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async registerDestinationEmail({
    empresaId,
    destinationEmail,
    domain,
  }) {
    try {
      const safeEmpresaId = this.ensureEmpresaId(empresaId);
      const safeDestinationEmail = this.validateDestinationEmail(destinationEmail);
      const company = await this.getCompanyOrThrow(safeEmpresaId);
      const safeDomain = this.resolveDomain({
        requestedDomain: domain,
        company,
      });
      const zoneId = await this.resolveZoneId({
        company,
        domain: safeDomain,
      });
      const accountId = await this.resolveAccountId({
        company,
        zoneId,
      });

      const destinationStatus = await this.cloudflareClient.ensureDestinationAddress({
        accountId,
        email: safeDestinationEmail,
      });

      const saved = await this.repository.upsertCorporateEmailDestination({
        empresaId: safeEmpresaId,
        destinationEmail: safeDestinationEmail,
        payload: {
          ownerEmpresaId: safeEmpresaId,
          destinationOwnerEmpresaId: safeEmpresaId,
          source: 'register_destination',
          domain: safeDomain,
          status: destinationStatus?.verified === true ? 'verified' : 'pending_verification',
          verified: destinationStatus?.verified === true,
          verificationSent: destinationStatus?.verificationSent === true,
          destinationCreated: destinationStatus?.created === true,
          cloudflareZoneId: zoneId,
          cloudflareAccountId: accountId,
          cloudflareDestinationAddressId: cleanString(destinationStatus?.id || '', 120),
          destinationVerifiedAt: cleanString(destinationStatus?.verifiedAt || '', 80) || null,
          verifiedAt: cleanString(destinationStatus?.verifiedAt || '', 80) || null,
          cloudflareLastSyncAt: Timestamp.now(),
        },
      });

      return this.serializeDestination(saved || {});
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async listDestinationEmails({
    empresaId,
    domain,
    syncWithCloudflare = true,
  }) {
    try {
      const safeEmpresaId = this.ensureEmpresaId(empresaId);
      const company = await this.getCompanyOrThrow(safeEmpresaId);
      const safeDomain = this.resolveDomain({
        requestedDomain: domain,
        company,
      });
      const zoneId = await this.resolveZoneId({
        company,
        domain: safeDomain,
      });
      const accountId = await this.resolveAccountId({
        company,
        zoneId,
      });
      const [rawDestinationRecords, corporateEmailRecords] = await Promise.all([
        this.repository.listCorporateEmailDestinationsByCompany(safeEmpresaId),
        this.repository.listCorporateEmailsByCompany(safeEmpresaId),
      ]);

      const destinationEmailsInUse = this.collectDestinationEmailsFromCorporateRecords(
        corporateEmailRecords
      );

      const allowedEmails = new Set();
      for (const record of rawDestinationRecords) {
        const destinationEmail = normalizeEmailAddress(
          record?.destinationEmail || record?.email || ''
        );
        if (!destinationEmail) continue;
        if (this.resolveDestinationOwnership(record, safeEmpresaId)) {
          allowedEmails.add(destinationEmail);
          continue;
        }
        if (destinationEmailsInUse.has(destinationEmail)) {
          // Compatibilidad legacy: permitimos destinos usados por aliases actuales.
          allowedEmails.add(destinationEmail);
        }
      }

      // Asegura que siempre podamos mostrar/sincronizar destinos ya usados por aliases.
      for (const destinationEmail of destinationEmailsInUse) {
        allowedEmails.add(destinationEmail);
      }

      if (parseBoolean(syncWithCloudflare, true) && allowedEmails.size > 0) {
        for (const destinationEmail of allowedEmails) {
          const destination = await this.cloudflareClient.findDestinationAddressByEmail({
            accountId,
            email: destinationEmail,
          });

          await this.repository.upsertCorporateEmailDestination({
            empresaId: safeEmpresaId,
            destinationEmail,
            payload: {
              ownerEmpresaId: safeEmpresaId,
              destinationOwnerEmpresaId: safeEmpresaId,
              source: 'list_sync',
              domain: safeDomain,
              status: destination?.verified === true
                ? 'verified'
                : destination
                  ? 'pending_verification'
                  : 'not_found',
              verified: destination?.verified === true,
              cloudflareZoneId: zoneId,
              cloudflareAccountId: accountId,
              cloudflareDestinationAddressId: cleanString(destination?.id || '', 120),
              destinationVerifiedAt: cleanString(destination?.verifiedAt || '', 80) || null,
              verifiedAt: cleanString(destination?.verifiedAt || '', 80) || null,
              cloudflareLastSyncAt: Timestamp.now(),
            },
          });
        }
      }

      const refreshedRecords = await this.repository.listCorporateEmailDestinationsByCompany(
        safeEmpresaId
      );
      return refreshedRecords
        .filter((record) => {
          const destinationEmail = normalizeEmailAddress(
            record?.destinationEmail || record?.email || ''
          );
          if (!destinationEmail) return false;
          if (this.resolveDestinationOwnership(record, safeEmpresaId)) return true;
          return destinationEmailsInUse.has(destinationEmail);
        })
        .map((item) => this.serializeDestination(item))
        .filter((item) => item.destinationEmail);
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async getAmazonSesConfiguration({
    empresaId,
    domain,
  }) {
    try {
      const safeEmpresaId = this.ensureEmpresaId(empresaId);
      const company = await this.getCompanyOrThrow(safeEmpresaId);
      const safeDomain = this.resolveDomain({
        requestedDomain: domain,
        company,
      });

      const profile = await this.repository.getCorporateEmailSenderProfile(safeEmpresaId);
      if (!profile) {
        return {
          empresaId: safeEmpresaId,
          provider: 'amazon_ses',
          enabled: false,
          domain: safeDomain,
          configured: false,
          fromEmail: '',
          replyToEmail: '',
          displayName: '',
          configurationSetName: '',
          identityExists: false,
          identityVerified: false,
          identityType: '',
          identityDkimStatus: '',
          identityCheckedAt: null,
        };
      }

      const serialized = this.serializeSesSenderProfile({
        ...profile,
        domain: safeDomain,
      });
      return {
        ...serialized,
        configured: Boolean(serialized.fromEmail),
      };
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async verifyAmazonSesIdentity({
    empresaId,
    domain,
    fromEmail,
  }) {
    try {
      const sesClient = this.ensureSesClient();
      const safeEmpresaId = this.ensureEmpresaId(empresaId);
      const company = await this.getCompanyOrThrow(safeEmpresaId);
      const safeDomain = this.resolveDomain({
        requestedDomain: domain,
        company,
      });

      const normalizedFromEmail = cleanString(fromEmail, 320)
        ? this.validateSenderEmailForDomain({ fromEmail, domain: safeDomain })
        : '';

      const targetIdentity = normalizedFromEmail || safeDomain;
      let identity = await sesClient.getEmailIdentityStatus({
        emailIdentity: targetIdentity,
      });

      if (!identity?.exists && normalizedFromEmail) {
        identity = await sesClient.getEmailIdentityStatus({
          emailIdentity: safeDomain,
        });
      }

      const now = Timestamp.now();
      const currentProfile = await this.repository.getCorporateEmailSenderProfile(safeEmpresaId);
      if (currentProfile) {
        await this.repository.upsertCorporateEmailSenderProfile({
          empresaId: safeEmpresaId,
          payload: {
            identityType: cleanString(identity?.identityType || '', 80).toLowerCase(),
            identityExists: identity?.exists === true,
            identityVerified: identity?.verified === true,
            identityDkimStatus: cleanString(identity?.dkimStatus || '', 80).toLowerCase(),
            identityCheckedAt: now,
          },
        });
      }

      return {
        identity: targetIdentity,
        domain: safeDomain,
        exists: identity?.exists === true,
        verified: identity?.verified === true,
        identityType: cleanString(identity?.identityType || '', 80).toLowerCase(),
        dkimStatus: cleanString(identity?.dkimStatus || '', 80).toLowerCase(),
        checkedAt: toIso(now),
      };
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async configureAmazonSesSender({
    empresaId,
    domain,
    enabled = true,
    fromEmail,
    replyToEmail,
    defaultToEmail,
    displayName,
    configurationSetName,
  }) {
    try {
      const sesClient = this.ensureSesClient();
      const safeEmpresaId = this.ensureEmpresaId(empresaId);
      const company = await this.getCompanyOrThrow(safeEmpresaId);
      const safeDomain = this.resolveDomain({
        requestedDomain: domain,
        company,
      });

      const safeFromEmail = this.validateSenderEmailForDomain({
        fromEmail,
        domain: safeDomain,
      });

      const safeReplyToEmail = cleanString(replyToEmail, 320)
        ? this.validateDestinationEmail(replyToEmail)
        : '';
      const safeDefaultToEmail = cleanString(defaultToEmail, 320)
        ? this.validateDestinationEmail(defaultToEmail)
        : '';

      const identity = await sesClient.getEmailIdentityStatus({
        emailIdentity: safeDomain,
      });

      const saved = await this.repository.upsertCorporateEmailSenderProfile({
        empresaId: safeEmpresaId,
        payload: {
          enabled: parseBoolean(enabled, true),
          domain: safeDomain,
          fromEmail: safeFromEmail,
          replyToEmail: safeReplyToEmail,
          defaultToEmail: safeDefaultToEmail,
          displayName: cleanString(displayName, 120),
          configurationSetName: cleanString(configurationSetName, 120),
          identityType: cleanString(identity?.identityType || '', 80).toLowerCase(),
          identityExists: identity?.exists === true,
          identityVerified: identity?.verified === true,
          identityDkimStatus: cleanString(identity?.dkimStatus || '', 80).toLowerCase(),
          identityCheckedAt: Timestamp.now(),
        },
      });

      return this.serializeSesSenderProfile(saved || {});
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async sendAmazonSesEmail({
    empresaId,
    domain,
    fromEmail,
    replyToEmail,
    to,
    cc,
    bcc,
    subject,
    text,
    html,
    configurationSetName,
    tags = [],
  }) {
    try {
      const sesClient = this.ensureSesClient();
      const safeEmpresaId = this.ensureEmpresaId(empresaId);
      const company = await this.getCompanyOrThrow(safeEmpresaId);
      const safeDomain = this.resolveDomain({
        requestedDomain: domain,
        company,
      });

      const profile = await this.repository.getCorporateEmailSenderProfile(safeEmpresaId);
      if (profile && profile.enabled === false) {
        throw new CorporateEmailServiceError('El envío con Amazon SES está deshabilitado', {
          code: 'SES_SENDER_DISABLED',
          statusCode: 409,
        });
      }

      const resolvedFromEmail = this.validateSenderEmailForDomain({
        fromEmail: fromEmail || profile?.fromEmail || '',
        domain: safeDomain,
      });
      const resolvedReplyToEmail = cleanString(
        replyToEmail || profile?.replyToEmail || '',
        320
      )
        ? this.validateDestinationEmail(replyToEmail || profile?.replyToEmail || '')
        : '';
      const resolvedConfigSet = cleanString(
        configurationSetName || profile?.configurationSetName || '',
        120
      );

      const toEmails = this.validateRecipientEmails(
        to || profile?.defaultToEmail || '',
        { fieldName: 'to', required: true }
      );
      const ccEmails = this.validateRecipientEmails(cc, {
        fieldName: 'cc',
        required: false,
      });
      const bccEmails = this.validateRecipientEmails(bcc, {
        fieldName: 'bcc',
        required: false,
      });

      const safeSubject = cleanString(subject, 220);
      if (!safeSubject) {
        throw new CorporateEmailServiceError('`subject` es requerido', {
          code: 'SES_SUBJECT_REQUIRED',
          statusCode: 400,
        });
      }

      const textBody = String(text ?? '').trim();
      const htmlBody = String(html ?? '').trim();
      if (!textBody && !htmlBody) {
        throw new CorporateEmailServiceError('Incluye `text` o `html` para enviar', {
          code: 'SES_BODY_REQUIRED',
          statusCode: 400,
        });
      }

      const identity = await sesClient.getEmailIdentityStatus({
        emailIdentity: safeDomain,
      });
      if (!identity?.exists || identity?.verified !== true) {
        throw new CorporateEmailServiceError(
          `La identidad SES del dominio ${safeDomain} no está verificada`,
          {
            code: 'SES_IDENTITY_NOT_VERIFIED',
            statusCode: 409,
            details: {
              domain: safeDomain,
              identityExists: identity?.exists === true,
              identityVerified: identity?.verified === true,
            },
          }
        );
      }

      const result = await sesClient.sendEmail({
        fromEmail: resolvedFromEmail,
        toEmails,
        ccEmails,
        bccEmails,
        replyToEmails: resolvedReplyToEmail ? [resolvedReplyToEmail] : [],
        subject: safeSubject,
        textBody,
        htmlBody,
        configurationSetName: resolvedConfigSet,
        tags,
      });

      await this.repository.upsertCorporateEmailSenderProfile({
        empresaId: safeEmpresaId,
        payload: {
          enabled: profile?.enabled !== false,
          domain: safeDomain,
          fromEmail: resolvedFromEmail,
          replyToEmail: resolvedReplyToEmail,
          defaultToEmail: profile?.defaultToEmail || '',
          displayName: profile?.displayName || '',
          configurationSetName: resolvedConfigSet,
          identityType: cleanString(identity?.identityType || '', 80).toLowerCase(),
          identityExists: identity?.exists === true,
          identityVerified: identity?.verified === true,
          identityDkimStatus: cleanString(identity?.dkimStatus || '', 80).toLowerCase(),
          identityCheckedAt: Timestamp.now(),
          lastSentAt: Timestamp.now(),
        },
      });

      return {
        provider: 'amazon_ses',
        domain: safeDomain,
        fromEmail: resolvedFromEmail,
        to: toEmails,
        cc: ccEmails,
        bcc: bccEmails,
        messageId: cleanString(result?.messageId || '', 180),
        sentAt: new Date().toISOString(),
      };
    } catch (error) {
      throw this.mapError(error);
    }
  }
}
