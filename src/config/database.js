require('dotenv').config();
const { Pool } = require('pg');
const Airtable = require('airtable');

// PostgreSQL connection setup
const pool = new Pool({
  connectionString: process.env.PG_CONNECTION_STRING,
  ssl: { rejectUnauthorized: false }
});

// Airtable connection setup
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Table names
const AIRTABLE_TABLE = process.env.AIRTABLE_TABLE_NAME || 'Employees_dev';
const PG_EMAILS_TABLE = process.env.PG_TABLE_NAME_2 || 'emails';
const PG_EMPLOYEES_TABLE = process.env.PG_TABLE_NAME_3 || 'employees';
const PG_LICENSES_TABLE = process.env.PG_TABLE_NAME_1 || 'employee_licenses';

module.exports = {
  pool,
  base,
  AIRTABLE_TABLE,
  PG_EMAILS_TABLE,
  PG_EMPLOYEES_TABLE,
  PG_LICENSES_TABLE
};