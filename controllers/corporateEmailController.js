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
  if (status >= 500) return 'Error interno al procesar la solicitud';
  const message = cleanString(error?.message || 'Solicitud invalida', 300);
  return message || 'Solicitud invalida';
}

function parseBoolean(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') return defaultValue;
  if (typeof value === 'boolean') return value;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'si', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return defaultValue;
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
        return res.status(resolveErrorStatus(error)).json({
          success: false,
          code: cleanString(error?.code || 'INTERNAL_ERROR', 120),
          error: resolveSafeMessage(error),
        });
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
        return res.status(resolveErrorStatus(error)).json({
          success: false,
          code: cleanString(error?.code || 'INTERNAL_ERROR', 120),
          error: resolveSafeMessage(error),
        });
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
        return res.status(resolveErrorStatus(error)).json({
          success: false,
          code: cleanString(error?.code || 'INTERNAL_ERROR', 120),
          error: resolveSafeMessage(error),
        });
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
        return res.status(resolveErrorStatus(error)).json({
          success: false,
          code: cleanString(error?.code || 'INTERNAL_ERROR', 120),
          error: resolveSafeMessage(error),
        });
      }
    },
  };
}

