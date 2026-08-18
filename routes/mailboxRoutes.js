// routes/mailboxRoutes.js
// Rutas del "mini-mail" (buzones de agentes).
//   POST /api/internal/mailbox/ingest   (Worker → guarda entrante)   [x-ingest-secret]
//   POST /api/mailbox/setup             (alta/enable buzón + password) [x-mailbox-admin-secret]
//   POST /api/mailbox/login             (email + password → token)
//   POST /api/mailbox/refresh           (refresh token → token nuevo)
//   PUT  /api/mailbox/password          (auth) cambiar contraseña propia
//   GET  /api/mailbox/inbox             (auth) bandeja
//   GET  /api/mailbox/messages/:id      (auth) leer mensaje (marca leído)
//   GET  /api/mailbox/messages/:id/attachments/:attachmentId (auth) descargar adjunto
//   GET  /api/mailbox/sent              (auth) enviados
//   POST /api/mailbox/send              (auth) enviar (Cloudflare)
//   POST /api/mailbox/import            (auth) importar mbox/EML a inbox o sent (?folder=sent)
//   GET  /api/mailbox/contacts          (auth) contactos de enviados
//   GET/POST/PUT/DELETE /api/mailbox/lists (auth) listas de correo
//   GET/POST/PUT/DELETE /api/mailbox/drafts (auth) borradores
import express from 'express';
import multer from 'multer';
import { FirestoreMailboxRepository } from '../repositories/mailboxRepository.js';
import { FirestoreCorporateEmailRepository } from '../repositories/corporateEmailRepository.js';
import { CorporateEmailService } from '../services/corporateEmailService.js';
import { CloudflareEmailRoutingClient } from '../services/cloudflareEmailRoutingClient.js';
import { CloudflareEmailSendingClient } from '../services/cloudflareEmailSendingClient.js';
import { AmazonSesClient } from '../services/amazonSesClient.js';
import { MailboxService } from '../services/mailboxService.js';
import { createMailboxController } from '../controllers/mailboxController.js';

function runUpload(uploadMiddleware) {
  return (req, res, next) => {
    uploadMiddleware(req, res, (error) => {
      if (!error) return next();
      if (error instanceof multer.MulterError) {
        return res.status(400).json({
          success: false,
          code: error.code || 'MAILBOX_UPLOAD_ERROR',
          error:
            error.code === 'LIMIT_FILE_SIZE'
              ? 'Uno de los archivos supera el tamaño permitido.'
              : 'Los adjuntos superan el límite permitido.',
        });
      }
      return next(error);
    });
  };
}

export function createMailboxRouter({ logger = console } = {}) {
  const mailboxRepository = new FirestoreMailboxRepository();
  const corporateRepository = new FirestoreCorporateEmailRepository();
  const cloudflareClient = new CloudflareEmailRoutingClient({ logger });
  const corporateEmailService = new CorporateEmailService({
    repository: corporateRepository,
    cloudflareClient,
    sesClient: new AmazonSesClient({ logger }),
    emailSendingClient: new CloudflareEmailSendingClient({ logger }),
    logger,
  });
  const service = new MailboxService({
    mailboxRepository,
    corporateEmailService,
    routingClient: cloudflareClient,
    workerName: process.env.MAILBOX_WORKER_NAME || 'negociosweb-mail-inbound',
    ingestSecret: process.env.MAILBOX_INGEST_SECRET,
    jwtSecret: process.env.MAILBOX_JWT_SECRET,
    adminSecret: process.env.MAILBOX_ADMIN_SECRET,
    tokenTtlSeconds: process.env.MAILBOX_TOKEN_TTL_SECONDS,
    refreshTokenTtlSeconds: process.env.MAILBOX_REFRESH_TOKEN_TTL_SECONDS,
    maxInboundAttachments: process.env.MAILBOX_INBOUND_MAX_ATTACHMENTS,
    maxInboundAttachmentBytes: process.env.MAILBOX_INBOUND_MAX_ATTACHMENT_BYTES,
    maxInboundTotalAttachmentBytes: process.env.MAILBOX_INBOUND_MAX_TOTAL_ATTACHMENT_BYTES,
    logger,
  });
  const controller = createMailboxController({ service, logger });

  const router = express.Router();
  const importUpload = multer({
    storage: multer.memoryStorage(),
    limits: {
      fileSize: Number(process.env.MAILBOX_IMPORT_MAX_BYTES || 25 * 1024 * 1024),
    },
  });
  const sendUpload = multer({
    storage: multer.memoryStorage(),
    limits: {
      files: Number(process.env.MAILBOX_SEND_MAX_ATTACHMENTS || 3),
      fileSize: Number(process.env.MAILBOX_SEND_MAX_ATTACHMENT_BYTES || 20 * 1024 * 1024),
    },
  });

  router.post('/internal/mailbox/ingest', controller.ingest);
  router.post('/mailbox/setup', controller.setup);
  router.post('/mailbox/login', controller.login);
  router.post('/mailbox/refresh', controller.refresh);
  router.put('/mailbox/password', controller.requireAuth, controller.changePassword);
  router.get('/mailbox/inbox', controller.requireAuth, controller.inbox);
  router.get('/mailbox/sent', controller.requireAuth, controller.sent);
  router.get('/mailbox/messages/:id', controller.requireAuth, controller.message);
  router.get('/mailbox/messages/:id/attachments/:attachmentId', controller.requireAuth, controller.attachment);
  router.post('/mailbox/send', controller.requireAuth, runUpload(sendUpload.array('attachments', 3)), controller.send);
  router.post('/mailbox/import', controller.requireAuth, runUpload(importUpload.single('file')), controller.importInbox);
  router.get('/mailbox/contacts', controller.requireAuth, controller.contacts);
  router.get('/mailbox/lists', controller.requireAuth, controller.mailingLists);
  router.post('/mailbox/lists', controller.requireAuth, controller.saveMailingList);
  router.put('/mailbox/lists/:listId', controller.requireAuth, controller.saveMailingList);
  router.delete('/mailbox/lists/:listId', controller.requireAuth, controller.deleteMailingList);
  router.get('/mailbox/drafts', controller.requireAuth, controller.drafts);
  router.post('/mailbox/drafts', controller.requireAuth, controller.saveDraft);
  router.put('/mailbox/drafts/:draftId', controller.requireAuth, controller.saveDraft);
  router.delete('/mailbox/drafts/:draftId', controller.requireAuth, controller.deleteDraft);

  return router;
}
