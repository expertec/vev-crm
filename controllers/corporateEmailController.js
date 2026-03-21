function cleanString(value = '', maxLength = 240) {
  return String(value ?? '').trim().slice(0, maxLength);
}

function resolveErrorStatus(error) {
  if (Number.isInteger(error?.statusCode) && error.statusCode >= 400 && error.statusCode <= 599) {
    return error.statusCode;
  }
  return 500;
}

function resolveSafeMessage(error) {
  const status = resolveErrorStatus(error);
  const code = cleanString(error?.code || '', 120).toUpperCase();
  const safeMessage = cleanString(error?.message || 'Solicitud invalida', 300);

  const shouldExposeMessage =
    code === 'DESTINATION_EMAIL_NOT_VERIFIED'
    || code === 'PLAN_ALIAS_LIMIT_REACHED'
    || code === 'PLAN_UPGRADE_OPTION_INVALID'
    || code === 'PLAN_UPGRADE_REQUEST_PENDING'
    || code === 'CLOUDFLARE_NOT_CONFIGURED'
    || code === 'CLOUDFLARE_ZONE_NOT_FOUND'
    || code === 'CLOUDFLARE_ACCOUNT_NOT_FOUND'
    || code.startsWith('CLOUDFLARE_')
    || code.startsWith('SES_');

  if (shouldExposeMessage && safeMessage) return safeMessage;
  if (status >= 500) return 'Error interno al procesar la solicitud';
  return safeMessage || 'Solicitud invalida';
}

function parseBoolean(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') return defaultValue;
  if (typeof value === 'boolean') return value;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'si', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return defaultValue;
}

function buildErrorResponse(error) {
  const body = {
    success: false,
    code: cleanString(error?.code || 'INTERNAL_ERROR', 120),
    error: resolveSafeMessage(error),
  };

  if (error?.details && typeof error.details === 'object') {
    body.details = error.details;
  }
  return body;
}

export function createCorporateEmailController({
  service,
  logger = console,
}) {
  if (!service) {
    throw new Error('createCorporateEmailController requiere service');
  }

  return {
    createCorporateEmail: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const alias = cleanString(req.body?.alias || req.body?.nombreAlias || '', 80);
        const destinationEmail = cleanString(
          req.body?.destinationEmail || req.body?.correoDestino || req.body?.forwardTo || '',
          280
        );
        const domain = cleanString(req.body?.domain || req.body?.dominio || '', 200);

        const corporateEmail = await service.createCorporateEmail({
          empresaId,
          alias,
          destinationEmail,
          domain,
        });

        return res.status(201).json({
          success: true,
          corporateEmail,
        });
      } catch (error) {
        logger.error('[corporate-emails] create error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    listCorporateEmails: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const includeDeleted = parseBoolean(req.query?.includeDeleted, false);
        const corporateEmails = await service.listCorporateEmails({
          empresaId,
          includeDeleted,
        });

        return res.status(200).json({
          success: true,
          corporateEmails,
        });
      } catch (error) {
        logger.error('[corporate-emails] list error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    deleteCorporateEmail: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const correoId = cleanString(req.params?.correoId || '', 240);
        const corporateEmail = await service.deleteCorporateEmail({
          empresaId,
          correoId,
        });

        return res.status(200).json({
          success: true,
          corporateEmail,
        });
      } catch (error) {
        logger.error('[corporate-emails] delete error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    validateAliasAvailability: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const alias = cleanString(req.query?.alias || req.query?.nombreAlias || '', 80);
        const domain = cleanString(req.query?.domain || req.query?.dominio || '', 200);
        const result = await service.validateAliasAvailability({
          empresaId,
          alias,
          domain,
        });

        return res.status(200).json({
          success: true,
          ...result,
        });
      } catch (error) {
        logger.error('[corporate-emails] availability error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    getEmailPlanStatus: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const includeRequests = parseBoolean(req.query?.includeRequests, true);
        const requestLimit = Number.parseInt(
          cleanString(req.query?.requestLimit || req.query?.limit || '', 10),
          10
        );

        const result = await service.getEmailPlanStatus({
          empresaId,
          includeRequests,
          requestLimit: Number.isFinite(requestLimit) ? requestLimit : 20,
        });

        return res.status(200).json({
          success: true,
          ...result,
        });
      } catch (error) {
        logger.error('[corporate-emails] get plan status error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    requestEmailPlanExpansion: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const extraAliases = Number.parseInt(
          cleanString(
            req.body?.extraAliases
            || req.body?.requestedExtraAliases
            || req.body?.paquete
            || req.body?.planExtra
            || '',
            10
          ),
          10
        );
        const requestedByName = cleanString(
          req.body?.requestedByName || req.body?.nombreSolicitante || '',
          160
        );
        const requestedByEmail = cleanString(
          req.body?.requestedByEmail || req.body?.emailSolicitante || '',
          280
        );
        const note = cleanString(req.body?.note || req.body?.nota || '', 1000);

        const result = await service.requestEmailPlanExpansion({
          empresaId,
          extraAliases: Number.isFinite(extraAliases) ? extraAliases : 0,
          requestedByName,
          requestedByEmail,
          note,
        });

        return res.status(201).json({
          success: true,
          ...result,
        });
      } catch (error) {
        logger.error('[corporate-emails] request plan expansion error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    registerDestinationEmail: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const destinationEmail = cleanString(
          req.body?.destinationEmail || req.body?.correoDestino || req.body?.email || '',
          280
        );
        const domain = cleanString(req.body?.domain || req.body?.dominio || '', 200);

        const destination = await service.registerDestinationEmail({
          empresaId,
          destinationEmail,
          domain,
        });

        return res.status(201).json({
          success: true,
          destination,
        });
      } catch (error) {
        logger.error('[corporate-emails] register destination error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    listDestinationEmails: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const domain = cleanString(req.query?.domain || req.query?.dominio || '', 200);
        const syncWithCloudflare = parseBoolean(req.query?.syncWithCloudflare, true);

        const destinations = await service.listDestinationEmails({
          empresaId,
          domain,
          syncWithCloudflare,
        });

        return res.status(200).json({
          success: true,
          destinations,
        });
      } catch (error) {
        logger.error('[corporate-emails] list destinations error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    getDestinationVerificationStatus: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const destinationEmail = cleanString(
          req.query?.destinationEmail || req.query?.correoDestino || req.query?.email || '',
          280
        );
        const domain = cleanString(req.query?.domain || req.query?.dominio || '', 200);

        const verification = await service.getDestinationVerificationStatus({
          empresaId,
          destinationEmail,
          domain,
        });

        return res.status(200).json({
          success: true,
          verification,
        });
      } catch (error) {
        logger.error('[corporate-emails] destination verification error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    provisionEmailInfrastructure: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const domain = cleanString(
          req.body?.domain
          || req.body?.dominio
          || req.query?.domain
          || req.query?.dominio
          || '',
          200
        );
        const result = await service.provisionEmailInfrastructure({
          empresaId,
          domain,
        });
        return res.status(200).json({
          success: true,
          result,
        });
      } catch (error) {
        logger.error('[corporate-emails] provision infrastructure error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    getEmailProvisionStatus: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const domain = cleanString(
          req.query?.domain
          || req.query?.dominio
          || req.body?.domain
          || req.body?.dominio
          || '',
          200
        );
        const refresh = parseBoolean(req.query?.refresh || req.body?.refresh, false);
        const result = await service.getEmailProvisionStatus({
          empresaId,
          domain,
          refresh,
        });
        return res.status(200).json({
          success: true,
          result,
        });
      } catch (error) {
        logger.error('[corporate-emails] get provision status error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    getAmazonSesConfiguration: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const domain = cleanString(req.query?.domain || req.query?.dominio || '', 200);
        const configuration = await service.getAmazonSesConfiguration({
          empresaId,
          domain,
        });
        return res.status(200).json({
          success: true,
          configuration,
        });
      } catch (error) {
        logger.error('[corporate-emails] ses get configuration error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    configureAmazonSesSender: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const domain = cleanString(req.body?.domain || req.body?.dominio || '', 200);
        const enabled = parseBoolean(req.body?.enabled, true);
        const fromEmail = cleanString(req.body?.fromEmail || req.body?.correoOrigen || '', 280);
        const replyToEmail = cleanString(req.body?.replyToEmail || req.body?.correoRespuesta || '', 280);
        const defaultToEmail = cleanString(req.body?.defaultToEmail || req.body?.correoDestinoDefault || '', 280);
        const displayName = cleanString(req.body?.displayName || req.body?.nombreRemitente || '', 120);
        const configurationSetName = cleanString(
          req.body?.configurationSetName || req.body?.sesConfigurationSet || '',
          120
        );

        const configuration = await service.configureAmazonSesSender({
          empresaId,
          domain,
          enabled,
          fromEmail,
          replyToEmail,
          defaultToEmail,
          displayName,
          configurationSetName,
        });
        return res.status(200).json({
          success: true,
          configuration,
        });
      } catch (error) {
        logger.error('[corporate-emails] ses configure error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    verifyAmazonSesIdentity: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const domain = cleanString(
          req.body?.domain
          || req.body?.dominio
          || req.query?.domain
          || req.query?.dominio
          || '',
          200
        );
        const fromEmail = cleanString(
          req.body?.fromEmail
          || req.query?.fromEmail
          || req.body?.correoOrigen
          || '',
          280
        );
        const identity = await service.verifyAmazonSesIdentity({
          empresaId,
          domain,
          fromEmail,
        });
        return res.status(200).json({
          success: true,
          identity,
        });
      } catch (error) {
        logger.error('[corporate-emails] ses verify identity error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    sendAmazonSesEmail: async (req, res) => {
      try {
        const empresaId = cleanString(req.params?.empresaId || '', 140);
        const domain = cleanString(req.body?.domain || req.body?.dominio || '', 200);
        const fromEmail = cleanString(req.body?.fromEmail || req.body?.correoOrigen || '', 280);
        const replyToEmail = cleanString(
          req.body?.replyToEmail || req.body?.correoRespuesta || '',
          280
        );
        const to = req.body?.to || req.body?.toEmails || req.body?.correoDestino || '';
        const cc = req.body?.cc || req.body?.ccEmails || '';
        const bcc = req.body?.bcc || req.body?.bccEmails || '';
        const subject = cleanString(req.body?.subject || req.body?.asunto || '', 220);
        const text = cleanString(req.body?.text || req.body?.texto || '', 40000);
        const html = String(req.body?.html || req.body?.htmlBody || '').trim().slice(0, 200000);
        const configurationSetName = cleanString(
          req.body?.configurationSetName || req.body?.sesConfigurationSet || '',
          120
        );
        const tags = Array.isArray(req.body?.tags) ? req.body.tags : [];

        const result = await service.sendAmazonSesEmail({
          empresaId,
          domain,
          fromEmail,
          replyToEmail,
          to,
          cc,
          bcc,
          subject,
          text,
          html,
          configurationSetName,
          tags,
        });

        return res.status(202).json({
          success: true,
          result,
        });
      } catch (error) {
        logger.error('[corporate-emails] ses send error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },
  };
}
