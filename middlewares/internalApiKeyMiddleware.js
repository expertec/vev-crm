function readBearerToken(authHeader = '') {
  const value = String(authHeader || '').trim();
  const match = value.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

export function createInternalApiKeyMiddleware({ logger = console } = {}) {
  return function internalApiKeyMiddleware(req, res, next) {
    const configuredKey = String(
      process.env.INTERNAL_PROCESS_INFORMATION_API_KEY
        || process.env.INTERNAL_API_KEY
        || ''
    ).trim();

    if (!configuredKey) {
      logger.error(
        '[process-information] Falta INTERNAL_PROCESS_INFORMATION_API_KEY/INTERNAL_API_KEY'
      );
      return res.status(503).json({
        success: false,
        error: 'Servicio no configurado',
      });
    }

    const headerKey = String(req.get('x-internal-api-key') || '').trim();
    const bearerKey = readBearerToken(req.get('authorization') || '');
    const incomingKey = headerKey || bearerKey;

    if (!incomingKey || incomingKey !== configuredKey) {
      return res.status(401).json({
        success: false,
        error: 'No autorizado',
      });
    }

    return next();
  };
}

