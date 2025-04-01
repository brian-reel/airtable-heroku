const { pool } = require('../config/database');
const config = require('../config/config');
const { DatabaseError, tryCatch } = require('../utils/error-handler');

/**
 * Get ranked employee records (active first, then most recent)
 * @param {Object} filters - Optional filters for the query
 * @returns {Promise<Array>} - Array of employee records
 */
async function getRankedEmployees(filters = {}) {
  return tryCatch(async () => {
    const whereClause = buildWhereClause(filters);
    
    const query = `
      WITH RankedEmployees AS (
        SELECT 
          emp.id as employee_id,
          emp.first_name || ' ' || emp.last_name as name,
          emp.tenant_id,
          emp.active,
          emp.mobile_phone,
          emp.hire_date,
          emp.last_name,
          e.address as email,
          ROW_NUMBER() OVER (
            PARTITION BY emp.first_name || ' ' || emp.last_name
            ORDER BY 
              emp.active DESC,
              emp.updated_at DESC,
              emp.created_at DESC
          ) as rn
        FROM ${config.postgres.tables.employees} emp
        LEFT JOIN ${config.postgres.tables.emails} e ON emp.id = e.emailable_id 
          AND e.emailable_type = 'Employee' 
          AND e."primary" = true
        ${whereClause}
      )
      SELECT * FROM RankedEmployees 
      WHERE rn = 1
      ORDER BY employee_id
    `;
    
    const result = await pool.query(query);
    console.log(`Fetched ${result.rows.length} ranked employee records from PostgreSQL`);
    return result.rows;
  }, 'postgres_get_ranked_employees', { filters });
}

/**
 * Get employee emails with ranking (active first, then most recent)
 * @returns {Promise<Array>} - Array of email records
 */
async function getEmailsFromPostgres() {
  return tryCatch(async () => {
    const result = await pool.query(`
      WITH RankedEmails AS (
        SELECT 
          e.emailable_id,
          e.address,
          emp.mobile_phone,
          emp.active,
          emp.created_at,
          ROW_NUMBER() OVER (
            PARTITION BY e.address 
            ORDER BY emp.active DESC, emp.created_at DESC
          ) as rn
        FROM ${config.postgres.tables.emails} e
        LEFT JOIN ${config.postgres.tables.employees} emp ON e.emailable_id = emp.id
        WHERE e.emailable_type = 'Employee' 
        AND e.address IS NOT NULL
        AND e."primary" = true
      )
      SELECT * FROM RankedEmails WHERE rn = 1
      ORDER BY active DESC, created_at DESC
    `);
    console.log(`Fetched ${result.rows.length} email records from PostgreSQL`);
    return result.rows;
  }, 'postgres_get_emails');
}

/**
 * Get employee phone numbers with ranking (active first, then most recent)
 * @returns {Promise<Array>} - Array of phone records
 */
async function getPhoneNumbersFromPostgres() {
  return tryCatch(async () => {
    const result = await pool.query(`
      WITH RankedPhones AS (
        SELECT 
          emp.id,
          emp.mobile_phone,
          emp.active,
          emp.created_at,
          e.address as email,
          ROW_NUMBER() OVER (
            PARTITION BY emp.mobile_phone 
            ORDER BY emp.active DESC, emp.created_at DESC
          ) as rn
        FROM ${config.postgres.tables.employees} emp
        LEFT JOIN ${config.postgres.tables.emails} e ON emp.id = e.emailable_id 
        AND e.emailable_type = 'Employee' 
        AND e."primary" = true
        WHERE emp.mobile_phone IS NOT NULL
      )
      SELECT * FROM RankedPhones WHERE rn = 1
      ORDER BY active DESC, created_at DESC
    `);
    console.log(`Fetched ${result.rows.length} phone records from PostgreSQL`);
    return result.rows;
  }, 'postgres_get_phones');
}

/**
 * getEmployeeLicenses
 * @param {any} licenseTypes = ['1'] - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
function getEmployeeLicenses(licenseTypes = ['1']) {
  return tryCatch(async () => {
    const licenseTypesStr = licenseTypes.map(t => `'${t}'`).join(',');
    
    const query = `
      WITH EmployeeRanking AS (
        SELECT 
          e.id as emp_id,
          e.first_name || ' ' || e.last_name as name,
          e.active,
          ROW_NUMBER() OVER (
            PARTITION BY e.id 
            ORDER BY e.active DESC, e.updated_at DESC
          ) as emp_rank
        FROM ${config.postgres.tables.employees} e
      ),
      RankedLicenses AS (
        SELECT 
          l.id,
          l.license_type_id,
          l.employee_id,
          er.name,
          l.expires_on,
          l.number,
          l.issued_on,
          l.state,
          er.active,
          l.created_at,
          l.updated_at,
          l.status,
          ROW_NUMBER() OVER (
            PARTITION BY l.employee_id 
            ORDER BY l.expires_on DESC NULLS LAST
          ) as license_rank
        FROM ${config.postgres.tables.licenses} l
        JOIN EmployeeRanking er ON l.employee_id = er.emp_id
        WHERE er.emp_rank = 1
        AND l.license_type_id IN (${licenseTypesStr})
      )
      SELECT * FROM RankedLicenses 
      WHERE license_rank = 1
      ORDER BY employee_id
    `;
    
    const result = await pool.query(query);
    console.log(`Fetched ${result.rows.length} employee license records from PostgreSQL`);
    return result.rows;
  }, 'postgres_get_employee_licenses', { licenseTypes });
}

/**
 * Execute a custom query with error handling
 * @param {String} query - SQL query to execute
 * @param {Array} params - Query parameters
 * @returns {Promise<Object>} - Query result
 */
async function executeQuery(query, params = []) {
  return tryCatch(async () => {
    const result = await pool.query(query, params);
    return result;
  }, 'postgres_execute_query', { query });
}

/**
 * Helper function to build WHERE clauses
 * @param {Object} filters - Filters to apply
 * @returns {String} - WHERE clause
 */
function buildWhereClause(filters = {}) {
  // Standard exclusion filters for test accounts
  const standardExclusions = `
    NOT (
      emp.first_name ILIKE ANY(ARRAY[
        'SER-N-%', 'SER-D-%', 'ESP-D-%', 'ESP-N-%',
        'PRONE-D-%', 'DF-D-%', 'ULT-N-%', 'ULT-D-%',
        'SS-N-%', 'SS-D-%', 'AVS-N-%', 'AVS-D-%',
        'DF-N-%', 'East%', 'Eastern', 'Mountain', 'Central'
      ])
      OR emp.last_name = 'SUB'
      OR emp.last_name ILIKE '%Tester%'
      OR emp.last_name ILIKE '%test%'
    )
  `;
  
  // Add custom filters
  let whereClause = `WHERE ${standardExclusions}`;
  
  if (filters.active === true) {
    whereClause += ` AND emp.active = true`;
  } else if (filters.active === false) {
    whereClause += ` AND emp.active = false`;
  }
  
  if (filters.employeeId) {
    whereClause += ` AND emp.id = '${filters.employeeId}'`;
  }
  
  return whereClause;
}

module.exports = {
  getRankedEmployees,
  getEmailsFromPostgres,
  getPhoneNumbersFromPostgres,
  getEmployeeLicenses,
  executeQuery
};