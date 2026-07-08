// controllers/mailboxController.js
function cleanString(value = '', maxLength = 300) {
  return String(value ?? '').trim().slice(0, maxLength);
}

function resolveErrorStatus(error) {
  if (Number.isInteger(error?.statusCode) && error.statusCode >= 400 && error.statusCode <= 599) {
    return error.statusCode;
  }
  return 500;
}

function buildErrorResponse(error) {
  const status = resolveErrorStatus(error);
  return {
    success: false,
    code: cleanString(error?.code || 'MAILBOX_ERROR', 120),
    error: status >= 500 ? 'Error interno al procesar la solicitud' : cleanString(error?.message || 'Solicitud inválida', 300),
  };
}

export function createMailboxController({ service, logger = console }) {
  if (!service) throw new Error('createMailboxController requiere service');

  const requireAuth = (req, res, next) => {
    const header = String(req.headers?.authorization || '');
    const token = header.startsWith('Bearer ') ? header.slice(7).trim() : '';
    const claims = service.authenticate(token);
    if (!claims) {
      return res.status(401).json({
        success: false,
        code: 'MAILBOX_UNAUTHORIZED',
        error: 'Sesión inválida o expirada.',
      });
    }
    req.mailbox = claims;
    return next();
  };

  return {
    requireAuth,

    ingest: async (req, res) => {
      try {
        const out = await service.ingest({
          secret: req.header('x-ingest-secret'),
          to: req.body?.to,
          from: req.body?.from,
          subject: req.body?.subject,
          text: req.body?.text,
          html: req.body?.html,
          messageId: req.body?.messageId,
          date: req.body?.date,
          sizeBytes: req.body?.sizeBytes,
          inReplyTo: req.body?.inReplyTo,
        });
        return res.status(200).json({ success: true, ...out });
      } catch (error) {
        logger.error?.('[mailbox] ingest error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    setup: async (req, res) => {
      try {
        const mailbox = await service.setupMailbox({
          adminSecret: req.header('x-mailbox-admin-secret'),
          negocioId: req.body?.negocioId,
          empresaId: req.body?.empresaId,
          correoId: req.body?.correoId,
          address: req.body?.address,
          password: req.body?.password,
          forwardCopyTo: req.body?.forwardCopyTo,
          displayName: req.body?.displayName,
        });
        return res.status(200).json({ success: true, mailbox });
      } catch (error) {
        logger.error?.('[mailbox] setup error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    login: async (req, res) => {
      try {
        const out = await service.login({ email: req.body?.email, password: req.body?.password });
        return res.status(200).json({ success: true, ...out });
      } catch (error) {
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    inbox: async (req, res) => {
      try {
        const items = await service.getInbox({
          empresaId: req.mailbox.empresaId,
          correoId: req.mailbox.correoId,
          limit: req.query?.limit,
        });
        return res.status(200).json({ success: true, items });
      } catch (error) {
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    message: async (req, res) => {
      try {
        const item = await service.getMessage({
          empresaId: req.mailbox.empresaId,
          correoId: req.mailbox.correoId,
          messageId: req.params?.id,
        });
        return res.status(200).json({ success: true, item });
      } catch (error) {
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    sent: async (req, res) => {
      try {
        const items = await service.getSent({
          empresaId: req.mailbox.empresaId,
          mailboxEmail: req.mailbox.email,
        });
        return res.status(200).json({ success: true, items });
      } catch (error) {
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },

    send: async (req, res) => {
      try {
        const result = await service.send({
          empresaId: req.mailbox.empresaId,
          mailboxEmail: req.mailbox.email,
          to: req.body?.to,
          cc: req.body?.cc,
          bcc: req.body?.bcc,
          subject: req.body?.subject,
          text: req.body?.text || req.body?.bodyText,
          html: req.body?.html || req.body?.bodyHtml,
        });
        return res.status(202).json({ success: true, result, message: result?.message || null });
      } catch (error) {
        logger.error?.('[mailbox] send error:', error?.message || error);
        return res.status(resolveErrorStatus(error)).json(buildErrorResponse(error));
      }
    },
  };
}
