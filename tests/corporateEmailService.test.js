import test from 'node:test';
import assert from 'node:assert/strict';
import { CorporateEmailService } from '../services/corporateEmailService.js';
import { CloudflareEmailRoutingError } from '../services/cloudflareEmailRoutingClient.js';
import { buildCorporateEmailRecordId } from '../utils/corporateEmailUtils.js';

function cleanTestString(value = '') {
  return String(value ?? '').trim();
}

function normalizeRecordName(value = '') {
  return cleanTestString(value).toLowerCase().replace(/\.$/, '');
}

class InMemoryCorporateEmailRepository {
  constructor({
    companies = {},
    corporateEmails = {},
    destinations = {},
    senderProfiles = {},
  } = {}) {
    this.companies = new Map(Object.entries(companies));
    this.emailsByCompany = new Map();
    this.destinationsByCompany = new Map();
    this.senderProfilesByCompany = new Map();

    for (const [empresaId, records] of Object.entries(corporateEmails)) {
      const byId = new Map();
      for (const item of records) {
        byId.set(item.id, structuredClone(item));
      }
      this.emailsByCompany.set(empresaId, byId);
    }

    for (const [empresaId, records] of Object.entries(destinations)) {
      const byId = new Map();
      for (const item of records) {
        byId.set(item.id, structuredClone(item));
      }
      this.destinationsByCompany.set(empresaId, byId);
    }

    for (const [empresaId, profile] of Object.entries(senderProfiles)) {
      this.senderProfilesByCompany.set(empresaId, structuredClone(profile));
    }
  }

  getBucket(empresaId) {
    if (!this.emailsByCompany.has(empresaId)) {
      this.emailsByCompany.set(empresaId, new Map());
    }
    return this.emailsByCompany.get(empresaId);
  }

  getDestinationBucket(empresaId) {
    if (!this.destinationsByCompany.has(empresaId)) {
      this.destinationsByCompany.set(empresaId, new Map());
    }
    return this.destinationsByCompany.get(empresaId);
  }

  async getCompanyById(empresaId) {
    const company = this.companies.get(empresaId);
    return company ? { id: empresaId, ...structuredClone(company) } : null;
  }

  async listCorporateEmailsByCompany(empresaId) {
    const bucket = this.getBucket(empresaId);
    return Array.from(bucket.values()).map((item) => structuredClone(item));
  }

  async getCorporateEmailById(empresaId, correoId) {
    const bucket = this.getBucket(empresaId);
    const record = bucket.get(correoId);
    return record ? structuredClone(record) : null;
  }

  async getCorporateEmailByAliasAndDomain({
    empresaId,
    alias,
    domain,
  }) {
    const id = buildCorporateEmailRecordId({ alias, domain });
    return this.getCorporateEmailById(empresaId, id);
  }

  async createCorporateEmail({
    empresaId,
    correoId,
    payload,
  }) {
    if (!this.companies.has(empresaId)) {
      const error = new Error('Empresa no encontrada');
      error.code = 'COMPANY_NOT_FOUND';
      throw error;
    }

    const bucket = this.getBucket(empresaId);
    const current = bucket.get(correoId);
    if (current && String(current.status || '').toLowerCase() !== 'deleted') {
      const error = new Error('Alias ya existe');
      error.code = 'ALIAS_ALREADY_EXISTS';
      throw error;
    }

    const now = new Date().toISOString();
    const next = {
      id: correoId,
      empresaId,
      ...structuredClone(payload),
      createdAt: now,
      updatedAt: now,
      deletedAt: null,
    };
    bucket.set(correoId, next);
    return structuredClone(next);
  }

  async markCorporateEmailDeleted({
    empresaId,
    correoId,
    patch = {},
  }) {
    const bucket = this.getBucket(empresaId);
    const current = bucket.get(correoId);
    if (!current) return null;
    const now = new Date().toISOString();
    const next = {
      ...current,
      ...structuredClone(patch),
      status: 'deleted',
      updatedAt: now,
      deletedAt: now,
    };
    bucket.set(correoId, next);
    return structuredClone(next);
  }

  buildDestinationId(destinationEmail) {
    const safe = String(destinationEmail || '').trim().toLowerCase();
    if (!safe) return '';
    return `dest_${safe.replace(/[^a-z0-9]/g, '_')}`;
  }

  async listCorporateEmailDestinationsByCompany(empresaId) {
    const bucket = this.getDestinationBucket(empresaId);
    return Array.from(bucket.values()).map((item) => structuredClone(item));
  }

  async getCorporateEmailDestinationByEmail(empresaId, destinationEmail) {
    const destinationId = this.buildDestinationId(destinationEmail);
    if (!destinationId) return null;
    const bucket = this.getDestinationBucket(empresaId);
    const current = bucket.get(destinationId);
    return current ? structuredClone(current) : null;
  }

  async upsertCorporateEmailDestination({
    empresaId,
    destinationEmail,
    payload = {},
  }) {
    if (!this.companies.has(empresaId)) {
      const error = new Error('Empresa no encontrada');
      error.code = 'COMPANY_NOT_FOUND';
      throw error;
    }

    const safeEmail = String(destinationEmail || '').trim().toLowerCase();
    const destinationId = this.buildDestinationId(safeEmail);
    const bucket = this.getDestinationBucket(empresaId);
    const current = bucket.get(destinationId) || null;
    const now = new Date().toISOString();
    const next = {
      id: destinationId,
      empresaId,
      destinationEmail: safeEmail,
      email: safeEmail,
      createdAt: current?.createdAt || now,
      updatedAt: now,
      ...structuredClone(payload),
    };
    bucket.set(destinationId, next);
    return structuredClone(next);
  }

  async getCorporateEmailSenderProfile(empresaId) {
    const profile = this.senderProfilesByCompany.get(empresaId);
    return profile ? structuredClone(profile) : null;
  }

  async upsertCorporateEmailSenderProfile({
    empresaId,
    payload = {},
  }) {
    if (!this.companies.has(empresaId)) {
      const error = new Error('Empresa no encontrada');
      error.code = 'COMPANY_NOT_FOUND';
      throw error;
    }

    const current = this.senderProfilesByCompany.get(empresaId) || null;
    const now = new Date().toISOString();
    const next = {
      ...(current ? structuredClone(current) : {}),
      id: current?.id || 'amazonSes',
      empresaId,
      provider: 'amazon_ses',
      createdAt: current?.createdAt || now,
      ...structuredClone(payload),
      updatedAt: now,
    };
    this.senderProfilesByCompany.set(empresaId, next);
    return structuredClone(next);
  }
}

class FakeCloudflareEmailRoutingClient {
  constructor() {
    this.created = [];
    this.deleted = [];
    this.dnsEnsures = [];
    this.dnsUpserts = [];
    this.events = [];
    this.destinations = new Map();
    this.dnsRecords = [];
  }

  async resolveZoneIdByDomain() {
    return 'zone-auto-123';
  }

  async resolveAccountIdByZone(zoneId) {
    return `acc-${zoneId || 'default'}`;
  }

  async listDestinationAddresses({
    accountId,
    page = 1,
    perPage = 200,
  } = {}) {
    void accountId;
    void page;
    void perPage;
    return Array.from(this.destinations.values()).map((item) => structuredClone(item));
  }

  async listAllDestinationAddresses({ accountId } = {}) {
    void accountId;
    return Array.from(this.destinations.values()).map((item) => structuredClone(item));
  }

  async findDestinationAddressByEmail({ email }) {
    const key = String(email || '').trim().toLowerCase();
    return this.destinations.get(key) || null;
  }

  async ensureDestinationAddress({ accountId, email }) {
    const key = String(email || '').trim().toLowerCase();
    let current = this.destinations.get(key);
    if (!current) {
      current = {
        id: `dest-${this.destinations.size + 1}`,
        email: key,
        verified: true,
        verifiedAt: new Date().toISOString(),
        exists: true,
        created: true,
        verificationSent: true,
        accountId,
      };
      this.destinations.set(key, current);
    }
    this.events.push(`destination:${key}`);
    return structuredClone(current);
  }

  async ensureEmailRoutingDnsEnabled({ zoneId }) {
    this.dnsEnsures.push(zoneId);
    this.events.push(`dns:${zoneId}`);
    return {
      enabled: true,
      changed: true,
      zoneId,
    };
  }

  async createRoutingRule({
    zoneId,
    sourceEmail,
    destinationEmail,
  }) {
    const item = {
      zoneId,
      sourceEmail,
      destinationEmail,
    };
    this.created.push(item);
    this.events.push(`rule:${zoneId}`);
    return {
      id: `rule-${this.created.length}`,
      tag: `tag-${this.created.length}`,
      enabled: true,
    };
  }

  async deleteRoutingRule({ zoneId, ruleId }) {
    this.deleted.push({ zoneId, ruleId });
    return {
      deleted: true,
      skipped: false,
      notFound: false,
    };
  }

  async listDnsRecords({
    zoneId,
    type,
    name,
  } = {}) {
    const safeZoneId = cleanTestString(zoneId);
    const safeType = cleanTestString(type).toUpperCase();
    const safeName = normalizeRecordName(name);
    return this.dnsRecords
      .filter((record) => {
        if (safeZoneId && cleanTestString(record.zoneId) !== safeZoneId) return false;
        if (safeType && cleanTestString(record.type).toUpperCase() !== safeType) return false;
        if (safeName && normalizeRecordName(record.name) !== safeName) return false;
        return true;
      })
      .map((record) => structuredClone(record));
  }

  async upsertDnsRecord({
    zoneId,
    type,
    name,
    content,
    existingRecordId = '',
  } = {}) {
    const safeZoneId = cleanTestString(zoneId);
    const safeType = cleanTestString(type).toUpperCase();
    const safeName = normalizeRecordName(name);
    const safeContent = cleanTestString(content);
    const safeExistingRecordId = cleanTestString(existingRecordId);

    const existingById = safeExistingRecordId
      ? this.dnsRecords.find((record) => record.id === safeExistingRecordId)
      : null;
    const existingByName = this.dnsRecords.find(
      (record) =>
        cleanTestString(record.zoneId) === safeZoneId
        && cleanTestString(record.type).toUpperCase() === safeType
        && normalizeRecordName(record.name) === safeName
    );
    const existing = existingById || existingByName || null;

    const next = {
      id: existing?.id || `dns-${this.dnsRecords.length + 1}`,
      zoneId: safeZoneId,
      type: safeType,
      name: safeName,
      content: safeContent,
      ttl: 1,
      proxied: false,
    };

    if (existing) {
      const index = this.dnsRecords.findIndex((record) => record.id === existing.id);
      this.dnsRecords[index] = next;
    } else {
      this.dnsRecords.push(next);
    }

    const action = existing
      ? normalizeRecordName(existing.content) === normalizeRecordName(safeContent)
        ? 'unchanged'
        : 'updated'
      : 'created';

    this.dnsUpserts.push({
      zoneId: safeZoneId,
      type: safeType,
      name: safeName,
      content: safeContent,
      action,
    });

    return {
      ...structuredClone(next),
      action,
      created: action === 'created',
      updated: action === 'updated',
    };
  }
}

class FakeAmazonSesClient {
  constructor() {
    this.identityStatus = new Map();
    this.sent = [];
    this.createdIdentities = [];
  }

  setIdentity(emailIdentity, payload = {}) {
    const key = String(emailIdentity || '').trim().toLowerCase();
    this.identityStatus.set(key, {
      exists: true,
      verified: true,
      identityType: key.includes('@') ? 'email_address' : 'domain',
      dkimStatus: 'success',
      ...structuredClone(payload),
    });
  }

  async getEmailIdentityStatus({ emailIdentity }) {
    const key = String(emailIdentity || '').trim().toLowerCase();
    if (this.identityStatus.has(key)) {
      return structuredClone(this.identityStatus.get(key));
    }
    return {
      exists: false,
      verified: false,
      identityType: key.includes('@') ? 'email_address' : 'domain',
      dkimStatus: '',
      dkimTokens: [],
    };
  }

  async createEmailIdentity({ emailIdentity }) {
    const key = String(emailIdentity || '').trim().toLowerCase();
    if (!this.identityStatus.has(key)) {
      const tokens = [
        `${key.replace(/\W/g, '')}a`,
        `${key.replace(/\W/g, '')}b`,
        `${key.replace(/\W/g, '')}c`,
      ].map((token) => token.slice(0, 24));
      this.identityStatus.set(key, {
        exists: true,
        verified: false,
        identityType: key.includes('@') ? 'email_address' : 'domain',
        dkimStatus: 'pending',
        dkimTokens: tokens,
      });
    }
    this.createdIdentities.push(key);
    const current = this.identityStatus.get(key);
    return {
      created: true,
      emailIdentity: key,
      verifiedForSendingStatus: current?.verified === true,
      dkimStatus: cleanTestString(current?.dkimStatus || '').toLowerCase(),
      dkimTokens: Array.isArray(current?.dkimTokens) ? current.dkimTokens : [],
    };
  }

  async ensureDomainIdentity(domain = '') {
    const key = String(domain || '').trim().toLowerCase();
    const current = await this.getEmailIdentityStatus({
      emailIdentity: key,
    });
    if (current?.exists) {
      return {
        created: false,
        alreadyExists: true,
        emailIdentity: key,
        identityStatus: current,
      };
    }
    await this.createEmailIdentity({
      emailIdentity: key,
    });
    const identityStatus = await this.getEmailIdentityStatus({
      emailIdentity: key,
    });
    return {
      created: true,
      alreadyExists: false,
      emailIdentity: key,
      identityStatus,
    };
  }

  async sendEmail(payload = {}) {
    this.sent.push(structuredClone(payload));
    return {
      messageId: `ses-msg-${this.sent.length}`,
    };
  }
}

function createService({
  companies = {},
  corporateEmails = {},
  destinations = {},
  senderProfiles = {},
} = {}) {
  const repository = new InMemoryCorporateEmailRepository({
    companies,
    corporateEmails,
    destinations,
    senderProfiles,
  });
  const cloudflareClient = new FakeCloudflareEmailRoutingClient();
  const sesClient = new FakeAmazonSesClient();
  const service = new CorporateEmailService({
    repository,
    cloudflareClient,
    sesClient,
    logger: { error() {}, warn() {}, info() {} },
  });
  return { service, repository, cloudflareClient, sesClient };
}

test('crea alias corporativo y guarda datos', async () => {
  const { service, cloudflareClient } = createService({
    companies: {
      n1: {
        dominio: 'cliente.com',
        cloudflareZoneId: 'zone-static-999',
      },
    },
  });

  const created = await service.createCorporateEmail({
    empresaId: 'n1',
    alias: 'ventas',
    destinationEmail: 'cliente@gmail.com',
  });

  assert.equal(created.alias, 'ventas');
  assert.equal(created.email, 'ventas@cliente.com');
  assert.equal(created.domain, 'cliente.com');
  assert.equal(created.destinationEmail, 'cliente@gmail.com');
  assert.equal(created.status, 'active');
  assert.equal(created.cloudflareZoneId, 'zone-static-999');
  assert.equal(created.cloudflareEmailRoutingDnsEnabled, true);
  assert.equal(cloudflareClient.created.length, 1);
  assert.equal(cloudflareClient.dnsEnsures.length, 1);
  assert.equal(cloudflareClient.events[0], 'dns:zone-static-999');
  assert.equal(cloudflareClient.events[1], 'destination:cliente@gmail.com');
  assert.equal(cloudflareClient.events[2], 'rule:zone-static-999');
  assert.equal(cloudflareClient.created[0].sourceEmail, 'ventas@cliente.com');
});

test('rechaza alias reservado', async () => {
  const { service, cloudflareClient } = createService({
    companies: {
      n2: {
        dominio: 'empresa.com',
      },
    },
  });

  await assert.rejects(
    () => service.createCorporateEmail({
      empresaId: 'n2',
      alias: 'support',
      destinationEmail: 'destino@outlook.com',
    }),
    (error) => error?.code === 'RESERVED_ALIAS'
  );

  assert.equal(cloudflareClient.created.length, 0);
});

test('valida disponibilidad de alias existente', async () => {
  const correoId = buildCorporateEmailRecordId({
    alias: 'info',
    domain: 'cliente.com',
  });
  const { service } = createService({
    companies: {
      n3: {
        dominio: 'cliente.com',
      },
    },
    corporateEmails: {
      n3: [
        {
          id: correoId,
          empresaId: 'n3',
          alias: 'info',
          domain: 'cliente.com',
          email: 'info@cliente.com',
          destinationEmail: 'a@b.com',
          status: 'active',
        },
      ],
    },
  });

  const result = await service.validateAliasAvailability({
    empresaId: 'n3',
    alias: 'info',
  });

  assert.equal(result.available, false);
  assert.equal(result.reason, 'already_exists');
});

test('elimina alias corporativo y marca registro como deleted', async () => {
  const correoId = buildCorporateEmailRecordId({
    alias: 'contacto',
    domain: 'miempresa.com',
  });

  const { service, cloudflareClient } = createService({
    companies: {
      n4: {
        dominio: 'miempresa.com',
      },
    },
    corporateEmails: {
      n4: [
        {
          id: correoId,
          empresaId: 'n4',
          alias: 'contacto',
          domain: 'miempresa.com',
          email: 'contacto@miempresa.com',
          destinationEmail: 'destino@outlook.com',
          status: 'active',
          cloudflareZoneId: 'zone-del-001',
          cloudflareRuleId: 'rule-del-001',
        },
      ],
    },
  });

  const deleted = await service.deleteCorporateEmail({
    empresaId: 'n4',
    correoId,
  });

  assert.equal(deleted.status, 'deleted');
  assert.equal(cloudflareClient.deleted.length, 1);
  assert.equal(cloudflareClient.deleted[0].zoneId, 'zone-del-001');
  assert.equal(cloudflareClient.deleted[0].ruleId, 'rule-del-001');
});

test('si falla habilitar DNS de Email Routing no crea regla', async () => {
  const { service, cloudflareClient } = createService({
    companies: {
      n5: {
        dominio: 'cliente.com',
        cloudflareZoneId: 'zone-static-err',
      },
    },
  });

  cloudflareClient.ensureEmailRoutingDnsEnabled = async () => {
    throw new CloudflareEmailRoutingError('Permiso insuficiente para activar DNS de Email Routing', {
      statusCode: 503,
      code: 'CLOUDFLARE_PERMISSION_DENIED',
    });
  };

  await assert.rejects(
    () => service.createCorporateEmail({
      empresaId: 'n5',
      alias: 'facturacion',
      destinationEmail: 'cliente@gmail.com',
    }),
    (error) => error?.code === 'CLOUDFLARE_PERMISSION_DENIED'
  );

  assert.equal(cloudflareClient.created.length, 0);
});

test('si destino no esta verificado devuelve error y envia verificacion', async () => {
  const { service, cloudflareClient } = createService({
    companies: {
      n6: {
        dominio: 'cliente.com',
        cloudflareZoneId: 'zone-pending-001',
        cloudflareAccountId: 'acc-zone-pending-001',
      },
    },
  });

  cloudflareClient.ensureDestinationAddress = async ({ email }) => ({
    id: 'dest-pending-001',
    email,
    verified: false,
    verifiedAt: null,
    exists: true,
    created: true,
    verificationSent: true,
  });

  await assert.rejects(
    () => service.createCorporateEmail({
      empresaId: 'n6',
      alias: 'gerencia',
      destinationEmail: 'pendiente@gmail.com',
    }),
    (error) => error?.code === 'DESTINATION_EMAIL_NOT_VERIFIED'
  );

  assert.equal(cloudflareClient.created.length, 0);
});

test('consulta estado de verificacion del destino', async () => {
  const { service, cloudflareClient } = createService({
    companies: {
      n7: {
        dominio: 'cliente.com',
        cloudflareZoneId: 'zone-status-001',
      },
    },
  });
  cloudflareClient.destinations.set('estado@gmail.com', {
    id: 'dest-status-001',
    email: 'estado@gmail.com',
    verified: false,
    verifiedAt: null,
    createdAt: '2026-03-15T00:00:00.000Z',
    modifiedAt: '2026-03-15T00:01:00.000Z',
  });

  const status = await service.getDestinationVerificationStatus({
    empresaId: 'n7',
    destinationEmail: 'estado@gmail.com',
  });

  assert.equal(status.exists, true);
  assert.equal(status.verified, false);
  assert.equal(status.destinationAddressId, 'dest-status-001');
});

test('registra destino y lo guarda para reutilizarlo', async () => {
  const { service, repository } = createService({
    companies: {
      n8: {
        dominio: 'cliente.com',
        cloudflareZoneId: 'zone-dest-001',
      },
    },
  });

  const destination = await service.registerDestinationEmail({
    empresaId: 'n8',
    destinationEmail: 'ventas.destino@gmail.com',
  });

  assert.equal(destination.destinationEmail, 'ventas.destino@gmail.com');
  assert.equal(destination.verified, true);
  assert.equal(destination.status, 'verified');

  const saved = await repository.getCorporateEmailDestinationByEmail(
    'n8',
    'ventas.destino@gmail.com'
  );
  assert.equal(saved.destinationEmail, 'ventas.destino@gmail.com');
  assert.equal(saved.verified, true);
  assert.equal(saved.ownerEmpresaId, 'n8');
});

test('lista destinos guardados y sincronizados con Cloudflare', async () => {
  const { service, cloudflareClient } = createService({
    companies: {
      n9: {
        dominio: 'cliente.com',
        cloudflareZoneId: 'zone-dest-002',
      },
    },
    destinations: {
      n9: [
        {
          id: 'dest_uno_gmail_com',
          empresaId: 'n9',
          destinationEmail: 'uno@gmail.com',
          status: 'pending_verification',
          ownerEmpresaId: 'n9',
        },
        {
          id: 'dest_dos_gmail_com',
          empresaId: 'n9',
          destinationEmail: 'dos@gmail.com',
          status: 'pending_verification',
          ownerEmpresaId: 'n9',
        },
      ],
    },
  });
  cloudflareClient.destinations.set('uno@gmail.com', {
    id: 'dest-uno',
    email: 'uno@gmail.com',
    verified: true,
    verifiedAt: '2026-03-15T00:00:00.000Z',
  });
  cloudflareClient.destinations.set('dos@gmail.com', {
    id: 'dest-dos',
    email: 'dos@gmail.com',
    verified: false,
    verifiedAt: null,
  });

  const destinations = await service.listDestinationEmails({
    empresaId: 'n9',
  });

  assert.equal(destinations.length, 2);
  const uno = destinations.find((item) => item.destinationEmail === 'uno@gmail.com');
  const dos = destinations.find((item) => item.destinationEmail === 'dos@gmail.com');
  assert.equal(uno?.verified, true);
  assert.equal(dos?.verified, false);
});

test('no mezcla destinos de otra empresa al listar destinos', async () => {
  const correoId = buildCorporateEmailRecordId({
    alias: 'ventas',
    domain: 'cliente.com',
  });
  const { service, cloudflareClient } = createService({
    companies: {
      n10: {
        dominio: 'cliente.com',
        cloudflareZoneId: 'zone-dest-003',
      },
    },
    corporateEmails: {
      n10: [
        {
          id: correoId,
          empresaId: 'n10',
          alias: 'ventas',
          domain: 'cliente.com',
          email: 'ventas@cliente.com',
          destinationEmail: 'enuso@gmail.com',
          status: 'active',
        },
      ],
    },
    destinations: {
      n10: [
        {
          id: 'dest_propio_gmail_com',
          empresaId: 'n10',
          destinationEmail: 'propio@gmail.com',
          ownerEmpresaId: 'n10',
          status: 'verified',
        },
        {
          id: 'dest_enuso_gmail_com',
          empresaId: 'n10',
          destinationEmail: 'enuso@gmail.com',
          status: 'verified',
        },
        {
          id: 'dest_ajeno_gmail_com',
          empresaId: 'n10',
          destinationEmail: 'ajeno@gmail.com',
          status: 'verified',
        },
      ],
    },
  });

  cloudflareClient.destinations.set('propio@gmail.com', {
    id: 'dest-propio',
    email: 'propio@gmail.com',
    verified: true,
    verifiedAt: '2026-03-15T00:00:00.000Z',
  });
  cloudflareClient.destinations.set('enuso@gmail.com', {
    id: 'dest-enuso',
    email: 'enuso@gmail.com',
    verified: true,
    verifiedAt: '2026-03-15T00:00:00.000Z',
  });
  cloudflareClient.destinations.set('ajeno@gmail.com', {
    id: 'dest-ajeno',
    email: 'ajeno@gmail.com',
    verified: true,
    verifiedAt: '2026-03-15T00:00:00.000Z',
  });

  const destinations = await service.listDestinationEmails({
    empresaId: 'n10',
    syncWithCloudflare: true,
  });

  const emails = destinations.map((item) => item.destinationEmail).sort();
  assert.deepEqual(
    emails,
    ['enuso@gmail.com', 'propio@gmail.com']
  );
});

test('configura perfil de Amazon SES por empresa', async () => {
  const { service, sesClient } = createService({
    companies: {
      ses1: {
        dominio: 'cliente.com',
      },
    },
  });

  sesClient.setIdentity('cliente.com', {
    exists: true,
    verified: true,
    identityType: 'domain',
    dkimStatus: 'success',
  });

  const profile = await service.configureAmazonSesSender({
    empresaId: 'ses1',
    fromEmail: 'ventas@cliente.com',
    replyToEmail: 'soporte@gmail.com',
    displayName: 'Ventas Cliente',
  });

  assert.equal(profile.fromEmail, 'ventas@cliente.com');
  assert.equal(profile.replyToEmail, 'soporte@gmail.com');
  assert.equal(profile.identityExists, true);
  assert.equal(profile.identityVerified, true);
});

test('rechaza remitente SES si no pertenece al dominio de la empresa', async () => {
  const { service, sesClient } = createService({
    companies: {
      ses2: {
        dominio: 'cliente.com',
      },
    },
  });
  sesClient.setIdentity('cliente.com', { exists: true, verified: true });

  await assert.rejects(
    () => service.configureAmazonSesSender({
      empresaId: 'ses2',
      fromEmail: 'ventas@otrodominio.com',
    }),
    (error) => error?.code === 'SES_FROM_EMAIL_DOMAIN_MISMATCH'
  );
});

test('envia correo con Amazon SES usando perfil guardado', async () => {
  const { service, sesClient } = createService({
    companies: {
      ses3: {
        dominio: 'cliente.com',
      },
    },
    senderProfiles: {
      ses3: {
        id: 'amazonSes',
        empresaId: 'ses3',
        provider: 'amazon_ses',
        enabled: true,
        domain: 'cliente.com',
        fromEmail: 'ventas@cliente.com',
        replyToEmail: 'reply@cliente.com',
      },
    },
  });
  sesClient.setIdentity('cliente.com', {
    exists: true,
    verified: true,
    identityType: 'domain',
    dkimStatus: 'success',
  });

  const sent = await service.sendAmazonSesEmail({
    empresaId: 'ses3',
    to: ['destino@gmail.com'],
    subject: 'Hola',
    text: 'Mensaje de prueba',
  });

  assert.equal(sent.messageId, 'ses-msg-1');
  assert.equal(sent.fromEmail, 'ventas@cliente.com');
  assert.deepEqual(sent.to, ['destino@gmail.com']);
  assert.equal(sesClient.sent.length, 1);
  assert.equal(sesClient.sent[0].fromEmail, 'ventas@cliente.com');
});

test('bloquea envio SES cuando la identidad de dominio no esta verificada', async () => {
  const { service, sesClient } = createService({
    companies: {
      ses4: {
        dominio: 'cliente.com',
      },
    },
    senderProfiles: {
      ses4: {
        id: 'amazonSes',
        empresaId: 'ses4',
        provider: 'amazon_ses',
        enabled: true,
        domain: 'cliente.com',
        fromEmail: 'ventas@cliente.com',
      },
    },
  });
  sesClient.setIdentity('cliente.com', {
    exists: true,
    verified: false,
    identityType: 'domain',
    dkimStatus: 'pending',
  });

  await assert.rejects(
    () => service.sendAmazonSesEmail({
      empresaId: 'ses4',
      to: ['destino@gmail.com'],
      subject: 'No deberia salir',
      text: 'Pendiente verificacion',
    }),
    (error) => error?.code === 'SES_IDENTITY_NOT_VERIFIED'
  );
  assert.equal(sesClient.sent.length, 0);
});

test('provisiona SES y DNS (DKIM/SPF/DMARC) para un dominio', async () => {
  const { service, cloudflareClient, sesClient } = createService({
    companies: {
      ses5: {
        dominio: 'cliente.com',
        cloudflareZoneId: 'zone-ses-005',
        cloudflareAccountId: 'acc-zone-ses-005',
      },
    },
  });

  sesClient.setIdentity('cliente.com', {
    exists: true,
    verified: false,
    identityType: 'domain',
    dkimStatus: 'pending',
    dkimTokens: ['dkimaaa', 'dkimbbb', 'dkimccc'],
  });

  const result = await service.provisionEmailInfrastructure({
    empresaId: 'ses5',
  });

  assert.equal(result.domain, 'cliente.com');
  assert.equal(result.cloudflare.emailRoutingDnsEnabled, true);
  assert.equal(result.ses.identityExists, true);
  assert.equal(result.ses.identityVerified, false);
  assert.equal(result.dns.dkim.length, 3);
  assert.equal(result.dns.spf.includesAmazonSes, true);
  assert.equal(result.dns.dmarc.present, true);
  assert.equal(result.status, 'pending_verification');
  assert.equal(cloudflareClient.dnsUpserts.length, 5);
});

test('consulta estado de provision y reporta ready cuando DNS+SES estan completos', async () => {
  const { service, cloudflareClient, sesClient } = createService({
    companies: {
      ses6: {
        dominio: 'cliente.com',
        cloudflareZoneId: 'zone-ses-006',
        cloudflareAccountId: 'acc-zone-ses-006',
      },
    },
  });

  sesClient.setIdentity('cliente.com', {
    exists: true,
    verified: true,
    identityType: 'domain',
    dkimStatus: 'success',
    dkimTokens: ['tok1', 'tok2', 'tok3'],
  });

  cloudflareClient.dnsRecords.push(
    {
      id: 'dns-1',
      zoneId: 'zone-ses-006',
      type: 'CNAME',
      name: 'tok1._domainkey.cliente.com',
      content: 'tok1.dkim.amazonses.com',
    },
    {
      id: 'dns-2',
      zoneId: 'zone-ses-006',
      type: 'CNAME',
      name: 'tok2._domainkey.cliente.com',
      content: 'tok2.dkim.amazonses.com',
    },
    {
      id: 'dns-3',
      zoneId: 'zone-ses-006',
      type: 'CNAME',
      name: 'tok3._domainkey.cliente.com',
      content: 'tok3.dkim.amazonses.com',
    },
    {
      id: 'dns-4',
      zoneId: 'zone-ses-006',
      type: 'TXT',
      name: 'cliente.com',
      content: 'v=spf1 include:amazonses.com ~all',
    },
    {
      id: 'dns-5',
      zoneId: 'zone-ses-006',
      type: 'TXT',
      name: '_dmarc.cliente.com',
      content: 'v=DMARC1; p=none; pct=100',
    }
  );

  const status = await service.getEmailProvisionStatus({
    empresaId: 'ses6',
  });

  assert.equal(status.status, 'ready');
  assert.equal(status.ses.identityVerified, true);
  assert.equal(status.dns.spf.includesAmazonSes, true);
  assert.equal(status.dns.dkim.every((item) => item.matches), true);
});
