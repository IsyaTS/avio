class HttpError extends Error {
  constructor(statusCode, code, message, details) {
    super(message || code);
    this.name = 'HttpError';
    this.statusCode = statusCode;
    this.code = code || 'error';
    this.details = details;
    Error.captureStackTrace?.(this, HttpError);
  }
}

module.exports = { HttpError };
