const { pool, PG_EMAILS_TABLE, PG_EMPLOYEES_TABLE, PG_LICENSES_TABLE } = require('../config/database');

async function getEmailsFromPostgres() {
  try {
    const result = await pool.query(`
      SELECT e.emailable_id, e.address, emp.mobile_phone, emp.active 
      FROM ${PG_EMAILS_TABLE} e
      LEFT JOIN ${PG_EMPLOYEES_TABLE} emp ON e.emailable_id = emp.id
      WHERE e.emailable_type = 'Employee' 
      AND e.address IS NOT NULL
      AND e."primary" = true
    `);
    console.log(`Fetched ${result.rows.length} email records from Postgres`);
    return result.rows;
  } catch (error) {
    console.error('Error querying Postgres emails:', error);
    throw error;
  }
}

async function getPhoneNumbersFromPostgres() {
  try {
    const result = await pool.query(`
      SELECT emp.id, emp.mobile_phone, emp.active, e.address as email
      FROM ${PG_EMPLOYEES_TABLE} emp
      LEFT JOIN ${PG_EMAILS_TABLE} e ON emp.id = e.emailable_id
      WHERE emp.mobile_phone IS NOT NULL
      AND e."primary" = true
    `);
    console.log(`Fetched ${result.rows.length} phone records from Postgres`);
    return result.rows;
  } catch (error) {
    console.error('Error querying Postgres phones:', error);
    throw error;
  }
}

async function getEmployeesFromPostgres() {
  try {
    const result = await pool.query(`
      WITH RankedLicenses AS (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY employee_id 
            ORDER BY expires_on DESC NULLS LAST
          ) as rn
        FROM ${PG_LICENSES_TABLE}
        WHERE license_type_id = '1'
      )
      SELECT * FROM RankedLicenses 
      WHERE rn = 1
    `);
    return result.rows;
  } catch (error) {
    console.error('Error querying Postgres:', error);
    throw error;
  }
}

module.exports = {
  getEmailsFromPostgres,
  getPhoneNumbersFromPostgres,
  getEmployeesFromPostgres
};