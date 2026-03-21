import express from 'express';
import { createCorporateEmailController } from '../controllers/corporateEmailController.js';
import { FirestoreCorporateEmailRepository } from '../repositories/corporateEmailRepository.js';
import { CorporateEmailService } from '../services/corporateEmailService.js';
import { CloudflareEmailRoutingClient } from '../services/cloudflareEmailRoutingClient.js';
import { AmazonSesClient } from '../services/amazonSesClient.js';

export function createCorporateEmailRouter({
  logger = console,
} = {}) {
  const repository = new FirestoreCorporateEmailRepository();
  const cloudflareClient = new CloudflareEmailRoutingClient({ logger });
  const sesClient = new AmazonSesClient({ logger });
  const service = new CorporateEmailService({
    repository,
    cloudflareClient,
    sesClient,
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
    '/empresas/:empresaId/correos-corporativos/plan',
    controller.getEmailPlanStatus
  );

  router.post(
    '/empresas/:empresaId/correos-corporativos/plan/solicitudes',
    controller.requestEmailPlanExpansion
  );

  router.post(
    '/empresas/:empresaId/correos-corporativos/destinos',
    controller.registerDestinationEmail
  );

  router.get(
    '/empresas/:empresaId/correos-corporativos/destinos',
    controller.listDestinationEmails
  );

  router.get(
    '/empresas/:empresaId/correos-corporativos/destinos/verificacion',
    controller.getDestinationVerificationStatus
  );

  router.post(
    '/empresas/:empresaId/correos-corporativos/provision',
    controller.provisionEmailInfrastructure
  );

  router.get(
    '/empresas/:empresaId/correos-corporativos/provision-status',
    controller.getEmailProvisionStatus
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

  router.get(
    '/empresas/:empresaId/correos-corporativos/ses/configuracion',
    controller.getAmazonSesConfiguration
  );

  router.put(
    '/empresas/:empresaId/correos-corporativos/ses/configuracion',
    controller.configureAmazonSesSender
  );

  router.post(
    '/empresas/:empresaId/correos-corporativos/ses/verificar-identidad',
    controller.verifyAmazonSesIdentity
  );

  router.post(
    '/empresas/:empresaId/correos-corporativos/ses/enviar',
    controller.sendAmazonSesEmail
  );

  return router;
}
