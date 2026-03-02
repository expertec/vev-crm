import crypto from 'node:crypto';
import { processBusinessInformation } from './processBusinessInformation.js';
import { NotFoundError } from './processInformationErrors.js';

function safeTrim(value, maxLength = 200) {
  return String(value ?? '')
    .replace(/[\u0000-\u001f\u007f]+/g, ' ')
    .trim()
    .slice(0, maxLength);
}

export class ProcessInformationJobService {
  constructor({
    repository,
    logger = console,
    timeoutMs = Number(process.env.PROCESS_INFORMATION_TIMEOUT_MS || 45_000),
    maxRetries = Number(process.env.PROCESS_INFORMATION_MAX_RETRIES || 2),
    schemaGenerator = undefined,
    sequenceService = null,
  } = {}) {
    this.repository = repository;
    this.logger = logger;
    this.timeoutMs = timeoutMs;
    this.maxRetries = maxRetries;
    this.schemaGenerator = schemaGenerator;
    this.sequenceService = sequenceService;
    this.runningJobs = new Map();
  }

  async requestProcessing({
    negocioId,
    source = 'informacion',
    force = false,
    triggerSequences = false,
    inline = false,
  }) {
    const safeNegocioId = safeTrim(negocioId, 120);
    const safeSource = safeTrim(source || 'informacion', 40) || 'informacion';
    const jobId = crypto.randomUUID();

    const lockResult = await this.repository.acquireLockAndCreateJob({
      negocioId: safeNegocioId,
      source: safeSource,
      force: Boolean(force),
      jobId,
    });

    if (lockResult.outcome === 'not_found') {
      throw new NotFoundError(`Negocio ${safeNegocioId} no encontrado`);
    }

    if (lockResult.outcome === 'noop') {
      return {
        statusCode: 200,
        body: {
          success: true,
          queued: false,
          negocioId: safeNegocioId,
          schemaVersion: Number(lockResult.schemaVersion || 1),
          idempotent: true,
        },
      };
    }

    if (lockResult.outcome === 'locked') {
      return {
        statusCode: 202,
        body: {
          success: true,
          queued: true,
          locked: true,
          jobId: String(lockResult.existingJobId || ''),
          negocioId: safeNegocioId,
        },
      };
    }

    const runPayload = {
      jobId: String(lockResult.jobId || jobId),
      negocioId: safeNegocioId,
      source: safeSource,
      force: Boolean(force),
      triggerSequences: Boolean(triggerSequences),
      idempotencyHash: String(lockResult.idempotencyHash || ''),
    };

    if (inline) {
      const result = await this.runJob(runPayload);
      return {
        statusCode: 200,
        body: {
          success: true,
          queued: false,
          negocioId: safeNegocioId,
          schemaVersion: Number(result.schemaVersion || 1),
          jobId: runPayload.jobId,
        },
      };
    }

    this.runJobInBackground(runPayload);
    return {
      statusCode: 202,
      body: {
        success: true,
        queued: true,
        jobId: runPayload.jobId,
        negocioId: safeNegocioId,
      },
    };
  }

  runJobInBackground(payload) {
    setImmediate(() => {
      this.runJob(payload).catch((error) => {
        this.logger.error(
          `[process-information] job ${payload?.jobId} fallo: ${error?.message || error}`
        );
      });
    });
  }

  async runJob({
    jobId,
    negocioId,
    source,
    force,
    triggerSequences,
    idempotencyHash,
  }) {
    const safeJobId = safeTrim(jobId, 120);
    if (this.runningJobs.has(safeJobId)) {
      return this.runningJobs.get(safeJobId);
    }

    const promise = this.#executeJob({
      jobId: safeJobId,
      negocioId: safeTrim(negocioId, 120),
      source: safeTrim(source || 'informacion', 40) || 'informacion',
      force: Boolean(force),
      triggerSequences: Boolean(triggerSequences),
      idempotencyHash: safeTrim(idempotencyHash, 180),
    }).finally(() => {
      this.runningJobs.delete(safeJobId);
    });

    this.runningJobs.set(safeJobId, promise);
    return promise;
  }

  async #executeJob({
    jobId,
    negocioId,
    source,
    force,
    triggerSequences,
    idempotencyHash,
  }) {
    await this.repository.markJobRunning({ jobId });

    try {
      const result = await processBusinessInformation({
        negocioId,
        source,
        force,
        triggerSequences,
        idempotencyHash,
        jobId,
        repository: this.repository,
        logger: this.logger,
        timeoutMs: this.timeoutMs,
        maxRetries: this.maxRetries,
        schemaGenerator: this.schemaGenerator,
        sequenceService: this.sequenceService,
      });

      await this.repository.markJobCompleted({
        jobId,
        negocioId,
        source,
        schemaVersion: result.schemaVersion,
        durationMs: result.durationMs,
        usedFallback: result.usedFallback,
        attempts: result.attempts,
      });
      return result;
    } catch (error) {
      await this.repository.markJobFailed({
        jobId,
        negocioId,
        source,
        attempts: this.maxRetries + 1,
        errorCode: error?.code || 'PROCESSING_ERROR',
        errorMessage: safeTrim(error?.message || 'Error interno', 350),
      });

      await this.repository.releaseLock({
        negocioId,
        jobId,
        errorMessage: safeTrim(error?.message || 'Error interno', 350),
      });
      throw error;
    }
  }
}

