export class ValidationError extends Error {
  constructor(message, details = null) {
    super(message);
    this.name = 'ValidationError';
    this.statusCode = 400;
    this.code = 'VALIDATION_ERROR';
    this.details = details;
  }
}

export class UnauthorizedError extends Error {
  constructor(message = 'No autorizado') {
    super(message);
    this.name = 'UnauthorizedError';
    this.statusCode = 401;
    this.code = 'UNAUTHORIZED';
  }
}

export class NotFoundError extends Error {
  constructor(message = 'Recurso no encontrado') {
    super(message);
    this.name = 'NotFoundError';
    this.statusCode = 404;
    this.code = 'NOT_FOUND';
  }
}

export class ConflictError extends Error {
  constructor(message = 'Conflicto de procesamiento') {
    super(message);
    this.name = 'ConflictError';
    this.statusCode = 409;
    this.code = 'CONFLICT';
  }
}

export class ProcessingError extends Error {
  constructor(message = 'No fue posible procesar la solicitud', options = {}) {
    super(message);
    this.name = 'ProcessingError';
    this.statusCode = options.statusCode || 500;
    this.code = options.code || 'PROCESSING_ERROR';
    this.retryable = Boolean(options.retryable);
  }
}

