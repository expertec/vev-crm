import { ValidationError } from '../services/processInformationErrors.js';

function sanitizeString(value, maxLength = 200) {
  return String(value ?? '')
    .replace(/[\u0000-\u001f\u007f]+/g, ' ')
    .trim()
    .slice(0, maxLength);
}

function parseBoolean(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') return defaultValue;
  if (typeof value === 'boolean') return value;
  const lower = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'si', 'y'].includes(lower)) return true;
  if (['0', 'false', 'no', 'n'].includes(lower)) return false;
  return defaultValue;
}

function validatePayload(body = {}) {
  const negocioId = sanitizeString(body.negocioId, 120);
  if (!negocioId) {
    throw new ValidationError('`negocioId` es requerido');
  }

  return {
    negocioId,
    source: sanitizeString(body.source || 'informacion', 40) || 'informacion',
    force: Boolean(body.force === true),
    triggerSequences: Boolean(body.triggerSequences === true),
  };
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

function mapJobResponse(job = {}) {
  return {
    jobId: sanitizeString(job.jobId || job.id || '', 120),
    negocioId: sanitizeString(job.negocioId || '', 120),
    status: sanitizeString(job.status || 'unknown', 40),
    source: sanitizeString(job.source || 'informacion', 40),
    force: Boolean(job.force),
    attempts: Number(job.attempts || 0),
    schemaVersion: Number.isFinite(Number(job.schemaVersion))
      ? Number(job.schemaVersion)
      : null,
    durationMs: typeof job.durationMs === 'number' ? job.durationMs : null,
    usedFallback: Boolean(job.usedFallback),
    createdAt: toIso(job.createdAt),
    startedAt: toIso(job.startedAt),
    finishedAt: toIso(job.finishedAt),
    updatedAt: toIso(job.updatedAt),
    error: job.error
      ? {
          code: sanitizeString(job.error.code || 'PROCESSING_ERROR', 80),
          message: sanitizeString(job.error.message || 'Error de procesamiento', 300),
        }
      : null,
  };
}

function resolveErrorStatus(error) {
  if (Number.isInteger(error?.statusCode) && error.statusCode >= 400 && error.statusCode <= 599) {
    return error.statusCode;
  }
  return 500;
}

function resolveSafeMessage(error) {
  const status = resolveErrorStatus(error);
  if (status >= 500) return 'Error interno al procesar la solicitud';
  return sanitizeString(error?.message || 'Solicitud invalida', 250) || 'Solicitud invalida';
}

export function createProcessInformationController({
  jobService,
  repository,
  logger = console,
}) {
  if (!jobService || !repository) {
    throw new Error('createProcessInformationController requiere jobService y repository');
  }

  return {
    postProcessInformation: async (req, res) => {
      try {
        const payload = validatePayload(req.body || {});
        const inline = parseBoolean(req.query?.inline, false);

        if (payload.triggerSequences) {
          logger.warn(
            `[process-information] triggerSequences=true ignorado para negocio ${payload.negocioId}`
          );
        }

        const response = await jobService.requestProcessing({
          ...payload,
          inline,
        });

        return res.status(response.statusCode).json(response.body);
      } catch (error) {
        logger.error('[process-information] POST error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json({
          success: false,
          error: resolveSafeMessage(error),
        });
      }
    },

    getProcessInformationStatus: async (req, res) => {
      try {
        const jobId = sanitizeString(req.params?.jobId, 120);
        if (!jobId) {
          throw new ValidationError('`jobId` es requerido');
        }

        const job = await repository.getJobById(jobId);
        if (!job) {
          return res.status(404).json({
            success: false,
            error: 'Job no encontrado',
          });
        }

        return res.status(200).json({
          success: true,
          job: mapJobResponse(job),
        });
      } catch (error) {
        logger.error('[process-information] GET status error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json({
          success: false,
          error: resolveSafeMessage(error),
        });
      }
    },
  };
}

