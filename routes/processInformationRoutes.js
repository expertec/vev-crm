import express from 'express';
import { createInternalApiKeyMiddleware } from '../middlewares/internalApiKeyMiddleware.js';
import { createProcessInformationController } from '../controllers/processInformationController.js';
import { FirestoreProcessInformationRepository } from '../repositories/processInformationRepository.js';
import { ProcessInformationJobService } from '../services/processInformationJobService.js';

export function createProcessInformationRouter({ logger = console } = {}) {
  const repository = new FirestoreProcessInformationRepository({ logger });
  const jobService = new ProcessInformationJobService({
    repository,
    logger,
  });
  const controller = createProcessInformationController({
    repository,
    jobService,
    logger,
  });

  const router = express.Router();
  const internalApiKeyMiddleware = createInternalApiKeyMiddleware({ logger });

  router.post(
    '/process-information',
    internalApiKeyMiddleware,
    controller.postProcessInformation
  );

  router.get(
    '/process-information/:jobId',
    internalApiKeyMiddleware,
    controller.getProcessInformationStatus
  );

  return router;
}

