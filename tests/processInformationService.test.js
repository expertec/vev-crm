import test from 'node:test';
import assert from 'node:assert/strict';
import { processBusinessInformation } from '../services/processBusinessInformation.js';
import { ProcessInformationJobService } from '../services/processInformationJobService.js';
import { buildInformationIdempotencyHash } from '../services/processInformationHash.js';

class InMemoryProcessRepository {
  constructor(seed = {}) {
    this.negocios = new Map();
    this.jobs = new Map();
    for (const [id, value] of Object.entries(seed)) {
      this.negocios.set(id, structuredClone(value));
    }
  }

  async getNegocioById(negocioId) {
    const value = this.negocios.get(negocioId);
    if (!value) return null;
    return { id: negocioId, ...structuredClone(value) };
  }

  async getJobById(jobId) {
    const value = this.jobs.get(jobId);
    return value ? structuredClone(value) : null;
  }

  async acquireLockAndCreateJob({
    negocioId,
    source,
    force,
    jobId,
  }) {
    const negocio = this.negocios.get(negocioId);
    if (!negocio) return { outcome: 'not_found', negocioId };

    const idempotencyHash = buildInformationIdempotencyHash({
      negocioId,
      negocioData: negocio,
    });

    const lock = negocio?.processingMeta?.infoLock;
    const lockExpiresAt = Number(lock?.expiresAt || 0);
    if (lock?.jobId && lockExpiresAt > Date.now() && lock.jobId !== jobId) {
      return {
        outcome: 'locked',
        negocioId,
        existingJobId: lock.jobId,
        idempotencyHash,
      };
    }

    if (
      !force
      && negocio.schema
      && negocio?.processingMeta?.lastInformationHash
      && negocio.processingMeta.lastInformationHash === idempotencyHash
    ) {
      return {
        outcome: 'noop',
        negocioId,
        schemaVersion: Number(negocio.schemaVersion || 1),
        idempotencyHash,
      };
    }

    negocio.infoStatus = 'Procesando';
    negocio.processingMeta = {
      ...(negocio.processingMeta || {}),
      infoLock: {
        jobId,
        source,
        startedAt: Date.now(),
        expiresAt: Date.now() + 600_000,
      },
      pendingInformationHash: idempotencyHash,
      lastRequestedSource: source,
      lastRequestedAt: Date.now(),
    };

    this.jobs.set(jobId, {
      jobId,
      negocioId,
      source,
      force: Boolean(force),
      status: 'queued',
      attempts: 0,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });

    return {
      outcome: 'queued',
      negocioId,
      jobId,
      idempotencyHash,
    };
  }

  async markJobRunning({ jobId }) {
    const current = this.jobs.get(jobId) || {};
    this.jobs.set(jobId, {
      ...current,
      status: 'running',
      attempts: Number(current.attempts || 0) + 1,
      startedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
  }

  async markJobCompleted({
    jobId,
    schemaVersion,
    durationMs,
    usedFallback,
    attempts,
  }) {
    const current = this.jobs.get(jobId) || {};
    this.jobs.set(jobId, {
      ...current,
      status: 'completed',
      schemaVersion,
      durationMs,
      usedFallback,
      attempts,
      finishedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      error: null,
    });
  }

  async markJobFailed({
    jobId,
    errorCode,
    errorMessage,
    attempts,
  }) {
    const current = this.jobs.get(jobId) || {};
    this.jobs.set(jobId, {
      ...current,
      status: 'failed',
      attempts,
      finishedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      error: {
        code: errorCode,
        message: errorMessage,
      },
    });
  }

  async persistProcessedInformation({
    negocioId,
    schema,
    schemaVersion,
    source,
    durationMs,
    idempotencyHash,
    usedFallback,
    attempts,
    jobId,
  }) {
    const negocio = this.negocios.get(negocioId);
    if (!negocio) throw new Error('Negocio no encontrado');
    negocio.schema = structuredClone(schema);
    negocio.schemaVersion = schemaVersion;
    negocio.infoStatus = 'Informacion procesada';
    negocio.schemaUpdatedAt = Date.now();
    negocio.infoProcessedAt = Date.now();
    negocio.processingMeta = {
      ...(negocio.processingMeta || {}),
      source,
      durationMs,
      usedFallback,
      attempts,
      lastProcessedJobId: jobId,
      lastInformationHash: idempotencyHash,
      version: 'test',
    };
    delete negocio.processingMeta.infoLock;
    delete negocio.processingMeta.pendingInformationHash;
  }

  async releaseLock({ negocioId, jobId, errorMessage }) {
    const negocio = this.negocios.get(negocioId);
    if (!negocio) return;
    if (negocio?.processingMeta?.infoLock?.jobId !== jobId) return;
    delete negocio.processingMeta.infoLock;
    delete negocio.processingMeta.pendingInformationHash;
    if (errorMessage) {
      negocio.infoStatus = 'Error al procesar informacion';
      negocio.processingMeta.lastError = errorMessage;
    }
  }
}

function buildNegocio(partial = {}) {
  return {
    companyInfo: 'Clinica Vital',
    businessStory: 'Servicios integrales de bienestar',
    advancedBrief: 'Atendemos familias y empresas con planes personalizados',
    keyItems: ['Consulta inicial', 'Seguimiento'],
    photoURLs: ['https://example.com/1.jpg', 'https://example.com/2.jpg'],
    contactWhatsapp: '+5218112345678',
    contactEmail: 'hola@vital.mx',
    socialFacebook: 'https://facebook.com/vital',
    socialInstagram: 'https://instagram.com/vital',
    templateId: 'info',
    slug: 'clinica-vital',
    schemaVersion: 0,
    updatedAt: 1700000000000,
    processingMeta: {},
    ...partial,
  };
}

test('Caso feliz: procesa informacion y guarda schema', async () => {
  const repository = new InMemoryProcessRepository({
    n1: buildNegocio(),
  });

  const result = await processBusinessInformation({
    negocioId: 'n1',
    source: 'informacion',
    repository,
    schemaGenerator: async () => ({
      templateId: 'info',
      hero: { title: 'Clinica Vital' },
      about: { text: 'Atencion profesional' },
    }),
  });

  const saved = await repository.getNegocioById('n1');
  assert.equal(result.negocioId, 'n1');
  assert.equal(result.schemaVersion, 1);
  assert.equal(saved.infoStatus, 'Informacion procesada');
  assert.ok(saved.schema.hero);
  assert.ok(saved.schema.services);
  assert.ok(saved.schema.seo);
});

test('No dispara secuencias aunque triggerSequences=true', async () => {
  const repository = new InMemoryProcessRepository({
    n2: buildNegocio(),
  });
  const sequenceSpy = {
    calls: 0,
    schedule() {
      this.calls += 1;
    },
    campaign() {
      this.calls += 1;
    },
  };

  await processBusinessInformation({
    negocioId: 'n2',
    source: 'informacion',
    triggerSequences: true,
    repository,
    sequenceService: sequenceSpy,
    schemaGenerator: async () => ({
      templateId: 'info',
      hero: { title: 'Titulo' },
      about: { text: 'Texto' },
    }),
  });

  assert.equal(sequenceSpy.calls, 0);
});

test('Idempotencia: segunda llamada no reprocesa con force=false', async () => {
  const repository = new InMemoryProcessRepository({
    n3: buildNegocio(),
  });
  let generatorCalls = 0;
  const jobService = new ProcessInformationJobService({
    repository,
    schemaGenerator: async () => {
      generatorCalls += 1;
      return {
        templateId: 'info',
        hero: { title: `Titulo ${generatorCalls}` },
        about: { text: 'Texto base' },
      };
    },
  });

  const first = await jobService.requestProcessing({
    negocioId: 'n3',
    source: 'informacion',
    force: false,
    inline: true,
  });

  const second = await jobService.requestProcessing({
    negocioId: 'n3',
    source: 'informacion',
    force: false,
    inline: true,
  });

  assert.equal(first.statusCode, 200);
  assert.equal(second.statusCode, 200);
  assert.equal(second.body.idempotent, true);
  assert.equal(generatorCalls, 1);
});

test('force=true reprocesa aunque no haya cambios', async () => {
  const repository = new InMemoryProcessRepository({
    n4: buildNegocio(),
  });
  let generatorCalls = 0;
  const jobService = new ProcessInformationJobService({
    repository,
    schemaGenerator: async () => {
      generatorCalls += 1;
      return {
        templateId: 'info',
        hero: { title: `Titulo ${generatorCalls}` },
        about: { text: 'Texto base' },
      };
    },
  });

  await jobService.requestProcessing({
    negocioId: 'n4',
    source: 'informacion',
    force: false,
    inline: true,
  });

  await jobService.requestProcessing({
    negocioId: 'n4',
    source: 'informacion',
    force: true,
    inline: true,
  });

  assert.equal(generatorCalls, 2);
});

test('Error negocio inexistente', async () => {
  const repository = new InMemoryProcessRepository({});

  await assert.rejects(
    () =>
      processBusinessInformation({
        negocioId: 'no-existe',
        source: 'informacion',
        repository,
        schemaGenerator: async () => ({ hero: { title: 'x' } }),
      }),
    /no encontrado/i
  );
});

test('Error del generador usa fallback deterministico', async () => {
  const repository = new InMemoryProcessRepository({
    n6: buildNegocio({ templateId: 'booking' }),
  });

  const result = await processBusinessInformation({
    negocioId: 'n6',
    source: 'informacion',
    repository,
    schemaGenerator: async () => {
      throw new Error('fallo externo');
    },
  });

  const saved = await repository.getNegocioById('n6');
  assert.equal(result.usedFallback, true);
  assert.equal(saved.infoStatus, 'Informacion procesada');
  assert.equal(saved.schema.templateId, 'booking');
  assert.ok(saved.schema.hero.title);
  assert.ok(saved.schema.faqs.length > 0);
});

