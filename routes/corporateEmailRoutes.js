import express from 'express';
import { createCorporateEmailController } from '../controllers/corporateEmailController.js';
import { FirestoreCorporateEmailRepository } from '../repositories/corporateEmailRepository.js';
import { CorporateEmailService } from '../services/corporateEmailService.js';
import { CloudflareEmailRoutingClient } from '../services/cloudflareEmailRoutingClient.js';
import { CloudflareEmailSendingClient } from '../services/cloudflareEmailSendingClient.js';
import { AmazonSesClient } from '../services/amazonSesClient.js';
import { FirestoreMailboxRepository } from '../repositories/mailboxRepository.js';
import { MailboxService } from '../services/mailboxService.js';

export function createCorporateEmailRouter({
  logger = console,
} = {}) {
  const repository = new FirestoreCorporateEmailRepository();
  const cloudflareClient = new CloudflareEmailRoutingClient({ logger });
  const sesClient = new AmazonSesClient({ logger });
  const emailSendingClient = new CloudflareEmailSendingClient({ logger });
  const service = new CorporateEmailService({
    repository,
    cloudflareClient,
    sesClient,
    emailSendingClient,
    logger,
  });
  const controller = createCorporateEmailController({
    service,
    logger,
  });

  // Servicio de buzones (para el botón "Crear buzón" del panel del dueño).
  const mailboxService = new MailboxService({
    mailboxRepository: new FirestoreMailboxRepository(),
    corporateEmailService: service,
    routingClient: cloudflareClient,
    workerName: process.env.MAILBOX_WORKER_NAME || 'negociosweb-mail-inbound',
    ingestSecret: process.env.MAILBOX_INGEST_SECRET,
    jwtSecret: process.env.MAILBOX_JWT_SECRET,
    adminSecret: process.env.MAILBOX_ADMIN_SECRET,
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

  // Rutas "de cara al cliente" (envío directo desde la plataforma).
  // Internamente usan Amazon SES; el frontend consume estos paths neutrales.
  router.post(
    '/empresas/:empresaId/correos-corporativos/enviar',
    controller.sendAmazonSesEmail
  );

  router.get(
    '/empresas/:empresaId/correos-corporativos/mensajes',
    controller.listCorporateEmailMessages
  );

  router.get(
    '/empresas/:empresaId/correos-corporativos/sending-status',
    controller.getSendingStatus
  );

  // Crear/activar buzón (mini-mail) para un correo, desde el panel del dueño.
  router.post(
    '/empresas/:empresaId/correos-corporativos/:correoId/buzon',
    async (req, res) => {
      try {
        const mailbox = await mailboxService.enableMailboxForOwner({
          empresaId: req.params?.empresaId,
          correoId: req.params?.correoId,
          address: req.body?.address,
          password: req.body?.password,
          forwardCopyTo: req.body?.forwardCopyTo,
          displayName: req.body?.displayName,
        });
        return res.status(200).json({ success: true, mailbox });
      } catch (error) {
        logger.error('[corporate-emails] enable mailbox error:', error?.message || error);
        const status = Number.isInteger(error?.statusCode)
          && error.statusCode >= 400 && error.statusCode <= 599
          ? error.statusCode
          : 500;
        return res.status(status).json({
          success: false,
          code: String(error?.code || 'MAILBOX_ERROR'),
          error: status >= 500 ? 'Error interno al procesar la solicitud' : String(error?.message || 'Solicitud inválida'),
        });
      }
    }
  );

  return router;
}
