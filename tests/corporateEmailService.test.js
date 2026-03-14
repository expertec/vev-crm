import test from 'node:test';
import assert from 'node:assert/strict';
import { CorporateEmailService } from '../services/corporateEmailService.js';
import { buildCorporateEmailRecordId } from '../utils/corporateEmailUtils.js';

class InMemoryCorporateEmailRepository {
  constructor({
    companies = {},
    corporateEmails = {},
  } = {}) {
    this.companies = new Map(Object.entries(companies));
    this.emailsByCompany = new Map();

    for (const [empresaId, records] of Object.entries(corporateEmails)) {
      const byId = new Map();
      for (const item of records) {
        byId.set(item.id, structuredClone(item));
      }
      this.emailsByCompany.set(empresaId, byId);
    }
  }

  getBucket(empresaId) {
    if (!this.emailsByCompany.has(empresaId)) {
      this.emailsByCompany.set(empresaId, new Map());
    }
    return this.emailsByCompany.get(empresaId);
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
}

class FakeCloudflareEmailRoutingClient {
  constructor() {
    this.created = [];
    this.deleted = [];
  }

  async resolveZoneIdByDomain() {
    return 'zone-auto-123';
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
}

function createService({ companies = {}, corporateEmails = {} } = {}) {
  const repository = new InMemoryCorporateEmailRepository({
    companies,
    corporateEmails,
  });
  const cloudflareClient = new FakeCloudflareEmailRoutingClient();
  const service = new CorporateEmailService({
    repository,
    cloudflareClient,
    logger: { error() {}, warn() {}, info() {} },
  });
  return { service, repository, cloudflareClient };
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
  assert.equal(cloudflareClient.created.length, 1);
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

