function readBearerToken(authHeader = '') {
  const value = String(authHeader || '').trim();
  const match = value.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

function parseBooleanEnv(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') return defaultValue;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'si', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return defaultValue;
}

export function createInternalApiKeyMiddleware({ logger = console } = {}) {
  return function internalApiKeyMiddleware(req, res, next) {
    const requireApiKey = parseBooleanEnv(
      process.env.PROCESS_INFORMATION_REQUIRE_API_KEY,
      false
    );
    const configuredKey = String(
      process.env.INTERNAL_PROCESS_INFORMATION_API_KEY
        || process.env.INTERNAL_API_KEY
        || ''
    ).trim();

    if (!configuredKey) {
      if (requireApiKey) {
        logger.error(
          '[process-information] PROCESS_INFORMATION_REQUIRE_API_KEY=1 pero falta INTERNAL_PROCESS_INFORMATION_API_KEY/INTERNAL_API_KEY'
        );
        return res.status(503).json({
          success: false,
          error: 'Servicio no configurado',
        });
      }
      return next();
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
