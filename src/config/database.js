const { Pool } = require('pg');
const Airtable = require('airtable');
const config = require('./config');

// PostgreSQL connection setup
const pool = new Pool({
  connectionString: config.postgres.connectionString,
  ssl: config.postgres.ssl
});

// Airtable connection setup
const base = new Airtable({ apiKey: config.airtable.apiKey }).base(config.airtable.baseId);

// Table names
const AIRTABLE_TABLE = config.airtable.tables.employees;
const AIRTABLE_ROLES_TABLE = config.airtable.tables.roles;
const AIRTABLE_ROLE_TYPES_TABLE = config.airtable.tables.roleTypes;
const AIRTABLE_LMS_TABLE = config.airtable.tables.lmsData;

const PG_EMAILS_TABLE = config.postgres.tables.emails;
const PG_EMPLOYEES_TABLE = config.postgres.tables.employees;
const PG_LICENSES_TABLE = config.postgres.tables.licenses;
const PG_ROLE_TYPES_TABLE = config.postgres.tables.roleTypes;
const PG_EMPLOYEE_ROLES_TABLE = config.postgres.tables.employeeRoles;

// Test database connections
async function testConnections() {
  try {
    // Test PostgreSQL connection
    const pgClient = await pool.connect();
    console.log('✅ PostgreSQL connection successful');
    pgClient.release();
    
    // Test Airtable connection
    const airtableRecords = await base(AIRTABLE_TABLE).select({ maxRecords: 1 }).firstPage();
    console.log('✅ Airtable connection successful');
    
    return true;
  } catch (error) {
    console.error('❌ Database connection test failed:', error);
    throw error;
  }
}

module.exports = {
  pool,
  base,
  AIRTABLE_TABLE,
  AIRTABLE_ROLES_TABLE,
  AIRTABLE_ROLE_TYPES_TABLE,
  AIRTABLE_LMS_TABLE,
  PG_EMAILS_TABLE,
  PG_EMPLOYEES_TABLE,
  PG_LICENSES_TABLE,
  PG_ROLE_TYPES_TABLE,
  PG_EMPLOYEE_ROLES_TABLE,
  testConnections
};