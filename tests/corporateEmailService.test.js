import test from 'node:test';
import assert from 'node:assert/strict';
import { CorporateEmailService } from '../services/corporateEmailService.js';
import { CloudflareEmailRoutingError } from '../services/cloudflareEmailRoutingClient.js';
import { buildCorporateEmailRecordId } from '../utils/corporateEmailUtils.js';

class InMemoryCorporateEmailRepository {
  constructor({
    companies = {},
    corporateEmails = {},
    destinations = {},
  } = {}) {
    this.companies = new Map(Object.entries(companies));
    this.emailsByCompany = new Map();
    this.destinationsByCompany = new Map();

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
}

class FakeCloudflareEmailRoutingClient {
  constructor() {
    this.created = [];
    this.deleted = [];
    this.dnsEnsures = [];
    this.events = [];
    this.destinations = new Map();
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
}

function createService({ companies = {}, corporateEmails = {}, destinations = {} } = {}) {
  const repository = new InMemoryCorporateEmailRepository({
    companies,
    corporateEmails,
    destinations,
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
