import express from 'express';
import { createCorporateEmailController } from '../controllers/corporateEmailController.js';
import { FirestoreCorporateEmailRepository } from '../repositories/corporateEmailRepository.js';
import { CorporateEmailService } from '../services/corporateEmailService.js';
import { CloudflareEmailRoutingClient } from '../services/cloudflareEmailRoutingClient.js';

export function createCorporateEmailRouter({
  logger = console,
} = {}) {
  const repository = new FirestoreCorporateEmailRepository();
  const cloudflareClient = new CloudflareEmailRoutingClient({ logger });
  const service = new CorporateEmailService({
    repository,
    cloudflareClient,
    logger,
  });
  const controller = createCorporateEmailController({
    service,
    logger,
  });

  const router = express.Router();

  router.get(
    '/empresas/:empresaId/correos-corporativos/disponibilidad',
    controller.validateAliasAvailability
  );

  router.get(
    '/empresas/:empresaId/correos-corporativos/destinos/verificacion',
    controller.getDestinationVerificationStatus
  );

  router.post(
    '/empresas/:empresaId/correos-corporativos',
    controller.createCorporateEmail
  );

  router.get(
    '/empresas/:empresaId/correos-corporativos',
    controller.listCorporateEmails
  );

  router.delete(
    '/empresas/:empresaId/correos-corporativos/:correoId',
    controller.deleteCorporateEmail
  );

  return router;
}
