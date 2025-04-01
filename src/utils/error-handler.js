const { writeJsonReport } = require('./logging');

/**
 * Custom error class for application errors
 */
class AppError extends Error {
  constructor(message, code, details = {}) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
    this.details = details;
    Error.captureStackTrace(this, this.constructor);
  }
}

/**
 * Database error class
 */
class DatabaseError extends AppError {
  constructor(message, details = {}) {
    super(message, 'DATABASE_ERROR', details);
  }
}

/**
 * Airtable error class
 */
class AirtableError extends AppError {
  constructor(message, details = {}) {
    super(message, 'AIRTABLE_ERROR', details);
  }
}

/**
 * Validation error class
 */
class ValidationError extends AppError {
  constructor(message, details = {}) {
    super(message, 'VALIDATION_ERROR', details);
  }
}

/**
 * Handle errors in a consistent way
 * @param {Error} error - Error to handle
 * @param {String} context - Context where the error occurred
 * @param {Object} data - Additional data for error reporting
 */
function handleError(error, context, data = {}) {
  // Determine error type
  const errorType = error instanceof AppError ? error.code : 'UNKNOWN_ERROR';
  
  // Log error with context
  console.error(`[${errorType}] Error in ${context}: ${error.message}`);
  
  // Add additional data for debugging
  if (Object.keys(data).length > 0) {
    console.error('Additional data:', JSON.stringify(data, null, 2));
  }
  
  // Log stack trace for development
  if (process.env.NODE_ENV !== 'production') {
    console.error(error.stack);
  }
  
  // Write error report
  const errorReport = {
    type: errorType,
    message: error.message,
    context,
    timestamp: new Date().toISOString(),
    details: error instanceof AppError ? error.details : {},
    data
  };
  
  // Determine report category based on context
  let category = 'errors';
  if (context.includes('employee')) category = 'employee_data';
  if (context.includes('guard')) category = 'guard_cards';
  if (context.includes('role')) category = 'roles';
  if (context.includes('duplicate')) category = 'duplicates';
  if (context.includes('training')) category = 'training';
  
  writeJsonReport(`${context.replace(/\s+/g, '_')}_error.json`, errorReport, category);
  
  return errorReport;
}

/**
 * Try to execute a function with error handling
 * @param {Function} fn - Function to execute
 * @param {String} context - Context for error handling
 * @param {Object} data - Additional data for error reporting
 * @returns {Promise} - Promise that resolves to the function result or rejects with handled error
 */
async function tryCatch(fn, context = 'unknown', data = {}) {
  try {
    return await fn();
  } catch (error) {
    handleError(error, context, data);
    throw error;
  }
}

module.exports = {
  AppError,
  DatabaseError,
  AirtableError,
  ValidationError,
  handleError,
  tryCatch
}; 