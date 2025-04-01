require('dotenv').config();

/**
 * Configuration validation
 * @param {Object} config - Configuration object to validate
 * @returns {Array} - Array of validation errors
 */
function validateConfig(config) {
  const errors = [];
  
  // Required fields
  const requiredFields = [
    'AIRTABLE_API_KEY',
    'AIRTABLE_BASE_ID',
    'AIRTABLE_TABLE_NAME',
    'PG_CONNECTION_STRING'
  ];
  
  requiredFields.forEach(field => {
    if (!config[field]) {
      errors.push(`Missing required configuration: ${field}`);
    }
  });
  
  // Validate PostgreSQL connection string format
  if (config.PG_CONNECTION_STRING && !config.PG_CONNECTION_STRING.startsWith('postgres://')) {
    errors.push('Invalid PostgreSQL connection string format. Should start with postgres://');
  }
  
  return errors;
}

// Base configuration
const config = {
  // Airtable configuration
  airtable: {
    apiKey: process.env.AIRTABLE_API_KEY,
    baseId: process.env.AIRTABLE_BASE_ID,
    tables: {
      employees: process.env.AIRTABLE_TABLE_NAME || 'Employees',
      roles: process.env.AIRTABLE_TABLE_NAME_2 || 'Employee Roles',
      roleTypes: process.env.AIRTABLE_TABLE_NAME_3 || 'Roles',
      lmsData: 'LMS DATA'
    }
  },
  
  // PostgreSQL configuration
  postgres: {
    connectionString: process.env.PG_CONNECTION_STRING,
    ssl: { rejectUnauthorized: false },
    tables: {
      licenses: process.env.PG_TABLE_NAME_1 || 'employee_licenses',
      emails: process.env.PG_TABLE_NAME_2 || 'emails',
      employees: process.env.PG_TABLE_NAME_3 || 'employees',
      roleTypes: process.env.PG_TABLE_NAME_4 || 'role_types',
      employeeRoles: process.env.PG_TABLE_NAME_5 || 'employee_roles'
    }
  },
  
  // LMS API configuration
  lms: {
    apiKey: process.env.LMS_API_KEY,
    urls: {
      users: process.env.LMS_API_URL_USERS,
      courses: process.env.LMS_API_URL_COURSES,
      usersCourses: process.env.LMS_API_URL_USERS_COURSES
    }
  },
  
  // Application settings
  app: {
    logLevel: process.env.LOG_LEVEL || 'info',
    reportDir: process.env.REPORT_DIR || 'reports',
    rateLimiting: {
      airtableDelay: parseInt(process.env.AIRTABLE_RATE_LIMIT_DELAY) || 250,
      maxRetries: parseInt(process.env.MAX_RETRIES) || 3
    }
  },
  
  // Mapping constants
  mappings: {
    tenantStates: {
      '2': 'CA',
      '3': 'LA',
      '4': 'GA',
      '5': 'NM',
      '6': 'CA',
      '13': 'UK'
    },
    statusMapping: {
      active: {
        statusRSPG: 'Active',
        status: 'Hired'
      },
      inactive: {
        statusRSPG: 'Inactive',
        status: 'Separated'
      }
    }
  }
};

// Validate configuration
const validationErrors = validateConfig({
  AIRTABLE_API_KEY: config.airtable.apiKey,
  AIRTABLE_BASE_ID: config.airtable.baseId,
  AIRTABLE_TABLE_NAME: config.airtable.tables.employees,
  PG_CONNECTION_STRING: config.postgres.connectionString
});

if (validationErrors.length > 0) {
  console.error('Configuration validation errors:');
  validationErrors.forEach(error => console.error(`- ${error}`));
  
  if (process.env.NODE_ENV !== 'test') {
    throw new Error('Invalid configuration. Please check your .env file.');
  }
}

module.exports = config; 