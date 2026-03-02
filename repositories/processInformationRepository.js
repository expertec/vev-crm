import { FieldValue, Timestamp } from 'firebase-admin/firestore';
import { db } from '../firebaseAdmin.js';
import { buildInformationIdempotencyHash } from '../services/processInformationHash.js';

function toMillis(value) {
  if (!value) return 0;
  if (typeof value?.toMillis === 'function') {
    try {
      return value.toMillis();
    } catch {
      return 0;
    }
  }
  if (value instanceof Date) return value.getTime();
  if (typeof value === 'number') return value;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
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

export function mapJobForResponse(job = {}) {
  return {
    jobId: String(job.jobId || ''),
    negocioId: String(job.negocioId || ''),
    status: String(job.status || 'unknown'),
    source: String(job.source || 'informacion'),
    force: Boolean(job.force),
    attempts: Number(job.attempts || 0),
    createdAt: toIso(job.createdAt),
    startedAt: toIso(job.startedAt),
    finishedAt: toIso(job.finishedAt),
    updatedAt: toIso(job.updatedAt),
    durationMs: typeof job.durationMs === 'number' ? job.durationMs : null,
    usedFallback: Boolean(job.usedFallback),
    schemaVersion: Number.isFinite(Number(job.schemaVersion))
      ? Number(job.schemaVersion)
      : null,
    error: job.error
      ? {
          code: String(job.error.code || 'PROCESSING_ERROR'),
          message: String(job.error.message || 'Error de procesamiento'),
        }
      : null,
  };
}

export class FirestoreProcessInformationRepository {
  constructor({
    dbClient = db,
    jobsCollection = 'informationProcessingJobs',
    lockTtlMs = Number(process.env.PROCESS_INFORMATION_LOCK_TTL_MS || 10 * 60 * 1000),
    logger = console,
  } = {}) {
    this.db = dbClient;
    this.jobsCollection = jobsCollection;
    this.lockTtlMs = Math.max(30_000, lockTtlMs);
    this.logger = logger;
  }

  async getNegocioById(negocioId) {
    const id = String(negocioId || '').trim();
    if (!id) return null;
    const snap = await this.db.collection('Negocios').doc(id).get();
    if (!snap.exists) return null;
    return { id: snap.id, ...(snap.data() || {}) };
  }

  async getJobById(jobId) {
    const id = String(jobId || '').trim();
    if (!id) return null;
    const snap = await this.db.collection(this.jobsCollection).doc(id).get();
    if (!snap.exists) return null;
    return { id: snap.id, ...(snap.data() || {}) };
  }

  async acquireLockAndCreateJob({
    negocioId,
    source = 'informacion',
    force = false,
    jobId,
  }) {
    const id = String(negocioId || '').trim();
    const safeJobId = String(jobId || '').trim();
    const normalizedSource = String(source || 'informacion').trim() || 'informacion';
    const nowMs = Date.now();

    if (!id || !safeJobId) {
      throw new Error('negocioId y jobId son requeridos');
    }

    const negocioRef = this.db.collection('Negocios').doc(id);
    const jobRef = this.db.collection(this.jobsCollection).doc(safeJobId);

    return this.db.runTransaction(async (tx) => {
      const negocioSnap = await tx.get(negocioRef);
      if (!negocioSnap.exists) {
        return { outcome: 'not_found', negocioId: id };
      }

      const negocioData = negocioSnap.data() || {};
      const idempotencyHash = buildInformationIdempotencyHash({
        negocioId: id,
        negocioData,
      });

      const lastHash = String(negocioData?.processingMeta?.lastInformationHash || '').trim();
      const hasExistingSchema = Boolean(
        negocioData?.schema && typeof negocioData.schema === 'object'
      );

      if (!force && hasExistingSchema && lastHash && lastHash === idempotencyHash) {
        return {
          outcome: 'noop',
          negocioId: id,
          idempotencyHash,
          schemaVersion: Number(negocioData?.schemaVersion || 1),
        };
      }

      const currentLock = negocioData?.processingMeta?.infoLock || {};
      const lockJobId = String(currentLock?.jobId || '').trim();
      const lockExpiresAtMs = toMillis(currentLock?.expiresAt);
      if (lockJobId && lockExpiresAtMs > nowMs && lockJobId !== safeJobId) {
        return {
          outcome: 'locked',
          negocioId: id,
          existingJobId: lockJobId,
          idempotencyHash,
        };
      }

      const now = Timestamp.now();
      const expiresAt = Timestamp.fromMillis(nowMs + this.lockTtlMs);

      tx.set(
        jobRef,
        {
          jobId: safeJobId,
          negocioId: id,
          source: normalizedSource,
          status: 'queued',
          force: Boolean(force),
          attempts: 0,
          createdAt: now,
          updatedAt: now,
        },
        { merge: true }
      );

      tx.set(
        negocioRef,
        {
          infoStatus: 'Procesando',
          processingMeta: {
            ...(negocioData.processingMeta || {}),
            infoLock: {
              jobId: safeJobId,
              source: normalizedSource,
              startedAt: now,
              expiresAt,
            },
            pendingInformationHash: idempotencyHash,
            lastRequestedAt: now,
            lastRequestedSource: normalizedSource,
          },
        },
        { merge: true }
      );

      return {
        outcome: 'queued',
        negocioId: id,
        jobId: safeJobId,
        idempotencyHash,
      };
    });
  }

  async markJobRunning({ jobId }) {
    const safeJobId = String(jobId || '').trim();
    if (!safeJobId) return;
    const now = Timestamp.now();
    await this.db
      .collection(this.jobsCollection)
      .doc(safeJobId)
      .set(
        {
          status: 'running',
          attempts: FieldValue.increment(1),
          startedAt: now,
          updatedAt: now,
        },
        { merge: true }
      );
  }

  async markJobCompleted({
    jobId,
    negocioId,
    schemaVersion,
    durationMs,
    usedFallback,
    source,
    attempts,
  }) {
    const safeJobId = String(jobId || '').trim();
    if (!safeJobId) return;
    const now = Timestamp.now();
    await this.db
      .collection(this.jobsCollection)
      .doc(safeJobId)
      .set(
        {
          status: 'completed',
          negocioId: String(negocioId || ''),
          source: String(source || 'informacion'),
          schemaVersion: Number(schemaVersion || 0),
          durationMs: Number(durationMs || 0),
          usedFallback: Boolean(usedFallback),
          attempts: Number(attempts || 1),
          finishedAt: now,
          updatedAt: now,
          error: FieldValue.delete(),
        },
        { merge: true }
      );
  }

  async markJobFailed({
    jobId,
    negocioId,
    source,
    attempts,
    errorMessage,
    errorCode = 'PROCESSING_ERROR',
  }) {
    const safeJobId = String(jobId || '').trim();
    if (!safeJobId) return;
    const now = Timestamp.now();
    await this.db
      .collection(this.jobsCollection)
      .doc(safeJobId)
      .set(
        {
          status: 'failed',
          negocioId: String(negocioId || ''),
          source: String(source || 'informacion'),
          attempts: Number(attempts || 1),
          finishedAt: now,
          updatedAt: now,
          error: {
            code: String(errorCode || 'PROCESSING_ERROR'),
            message: String(errorMessage || 'Error de procesamiento'),
          },
        },
        { merge: true }
      );
  }

  async persistProcessedInformation({
    negocioId,
    schema,
    schemaVersion,
    source,
    durationMs,
    generatorVersion,
    idempotencyHash,
    usedFallback,
    attempts,
    jobId,
  }) {
    const id = String(negocioId || '').trim();
    if (!id) throw new Error('negocioId es requerido');
    const ref = this.db.collection('Negocios').doc(id);
    const now = Timestamp.now();

    await ref.set(
      {
        schema,
        infoStatus: 'Informacion procesada',
        schemaUpdatedAt: now,
        schemaVersion: Number(schemaVersion || 1),
        infoProcessedAt: now,
      },
      { merge: true }
    );

    await ref.update({
      'processingMeta.durationMs': Number(durationMs || 0),
      'processingMeta.version': String(generatorVersion || 'process-information-v1'),
      'processingMeta.source': String(source || 'informacion'),
      'processingMeta.attempts': Number(attempts || 1),
      'processingMeta.usedFallback': Boolean(usedFallback),
      'processingMeta.lastInformationHash': String(idempotencyHash || ''),
      'processingMeta.lastProcessedAt': now,
      'processingMeta.lastProcessedJobId': String(jobId || ''),
      'processingMeta.pendingInformationHash': FieldValue.delete(),
      'processingMeta.infoLock': FieldValue.delete(),
    });
  }

  async releaseLock({
    negocioId,
    jobId,
    errorMessage = '',
  }) {
    const id = String(negocioId || '').trim();
    const safeJobId = String(jobId || '').trim();
    if (!id || !safeJobId) return;
    const ref = this.db.collection('Negocios').doc(id);
    const now = Timestamp.now();
    const cleanError = String(errorMessage || '').trim().slice(0, 400);

    await this.db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists) return;
      const data = snap.data() || {};
      const lockJobId = String(data?.processingMeta?.infoLock?.jobId || '').trim();
      if (lockJobId && lockJobId !== safeJobId) return;

      const patch = {
        'processingMeta.infoLock': FieldValue.delete(),
        'processingMeta.pendingInformationHash': FieldValue.delete(),
      };

      if (cleanError) {
        patch.infoStatus = 'Error al procesar informacion';
        patch['processingMeta.lastError'] = cleanError;
        patch['processingMeta.lastErrorAt'] = now;
      }

      tx.update(ref, patch);
    });
  }
}

