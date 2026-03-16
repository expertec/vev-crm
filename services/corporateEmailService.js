import { Timestamp } from 'firebase-admin/firestore';
import { CloudflareEmailRoutingError } from './cloudflareEmailRoutingClient.js';
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
    logger = console,
    reservedAliases = undefined,
  } = {}) {
    if (!repository || !cloudflareClient) {
      throw new Error('CorporateEmailService requiere repository y cloudflareClient');
    }

    this.repository = repository;
    this.cloudflareClient = cloudflareClient;
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
}
