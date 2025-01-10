require('dotenv').config();
const { pool, base, AIRTABLE_TABLE } = require('../config/database');
const { clearReportFiles, writeJsonReport } = require('../utils/logging');
const fs = require('fs');

// Table names
const AIRTABLE_ROLES_TABLE = process.env.AIRTABLE_TABLE_NAME_2 || 'Employee_Roles';
const PG_ROLE_TYPES_TABLE = process.env.PG_TABLE_NAME_4 || 'role_types';
const PG_EMPLOYEE_ROLES_TABLE = process.env.PG_TABLE_NAME_5 || 'employee_roles';
const PG_EMPLOYEES_TABLE = process.env.PG_TABLE_NAME_3 || 'employees';

async function getAirtableEmployeeMap() {
  const records = await base(AIRTABLE_TABLE)
    .select({
      fields: ['Name']
    })
    .all();

  // Create a map of employee names to their Airtable record IDs
  return records.reduce((acc, record) => {
    if (record.fields['Name']) {
      acc[record.fields['Name'].trim()] = record.id;
    }
    return acc;
  }, {});
}

async function getAirtableRoleMap() {
  const records = await base(process.env.AIRTABLE_TABLE_NAME_3)
    .select({
      fields: ['Name', 'roles_record_id']
    })
    .all();

  return records.reduce((acc, record) => {
    if (record.fields['Name'] && record.fields['roles_record_id']) {
      acc[record.fields['Name'].trim()] = record.fields['roles_record_id'];
    }
    return acc;
  }, {});
}

async function syncEmployeeRoles() {
  try {
    // Clear previous files
    console.log('Clearing previous data files...');
    clearReportFiles([
      'roles_data.json',
      'role_sync_errors.json'
    ]);
    
    console.log('Starting employee roles sync...');
    
    // Get both maps first
    console.log('Fetching Airtable employee and role records...');
    const [airtableEmployeeMap, airtableRoleMap] = await Promise.all([
      getAirtableEmployeeMap(),
      getAirtableRoleMap()
    ]);

    // Rest of the PostgreSQL query remains the same...
    const result = await pool.query(`
      WITH RankedRoles AS (
        SELECT 
          emp.first_name || ' ' || emp.last_name as employee_name,
          rt.title as role_title,
          er.created_at,
          ROW_NUMBER() OVER (
            PARTITION BY er.employee_id, er.role_type_id 
            ORDER BY er.created_at DESC
          ) as rn
        FROM ${PG_EMPLOYEE_ROLES_TABLE} er
        JOIN ${PG_EMPLOYEES_TABLE} emp ON er.employee_id = emp.id
        JOIN ${PG_ROLE_TYPES_TABLE} rt ON er.role_type_id = rt.id
        WHERE emp.active = true
        AND NOT (
          emp.first_name ILIKE ANY(ARRAY[
            'SER-N-%', 'SER-D-%', 'ESP-D-%', 'ESP-N-%',
            'PRONE-D-%', 'DF-D-%', 'ULT-N-%', 'ULT-D-%',
            'SS-N-%', 'SS-D-%', 'AVS-N-%', 'AVS-D-%',
            'DF-N-%', 'East%', 'Eastern', 'Mountain', 'Central'
          ])
          OR emp.last_name = 'SUB'
          OR emp.last_name ILIKE '%test%'
        )
      )
      SELECT 
        employee_name,
        role_title
      FROM RankedRoles
      WHERE rn = 1
      ORDER BY employee_name, role_title
    `);

    // Write the raw data to a file with more detailed information
    writeJsonReport('roles_data.json', {
      postgresData: result.rows,
      airtableEmployeeMap: Object.entries(airtableEmployeeMap).map(([name, id]) => ({
        employeeName: name,
        airtableId: id
      })),
      airtableRoleMap: Object.entries(airtableRoleMap).map(([name, id]) => ({
        roleName: name,
        airtableId: id
      })),
      employeeRoleCombinations: result.rows.map(row => ({
        employeeName: row.employee_name,
        matchFound: !!airtableEmployeeMap[row.employee_name.trim()],
        roleMatchFound: !!airtableRoleMap[row.role_title.trim()],
        airtableFields: {
          current: {
            Employee: [],
            Role: []
          },
          toCreate: {
            Employee: [airtableEmployeeMap[row.employee_name.trim()]],
            Role: [airtableRoleMap[row.role_title.trim()]]
          }
        }
      }))
    });

    console.log('Raw roles data saved to roles_data.json');

    // Get existing role records
    const airtableRoles = await base(AIRTABLE_ROLES_TABLE)
      .select({
        fields: ['Employee', 'Role']
      })
      .all();

    // Create set of existing employee+role combinations using record IDs
    const existingCombos = new Set(
      airtableRoles.map(record => {
        const employeeId = record.fields['Employee']?.[0] || '';
        const roleId = record.fields['Role']?.[0] || '';
        return `${employeeId}|${roleId}`;
      })
    );

    // Filter for new combinations
    const newRoles = result.rows.filter(row => {
      const employeeId = airtableEmployeeMap[row.employee_name.trim()];
      const roleId = airtableRoleMap[row.role_title.trim()];
      return employeeId && roleId && !existingCombos.has(`${employeeId}|${roleId}`);
    });

    console.log(`Found ${newRoles.length} new employee role combinations to add`);

    // Add new records to Airtable
    let successCount = 0;
    const errors = [];

    for (const role of newRoles) {
      try {
        const employeeId = airtableEmployeeMap[role.employee_name.trim()];
        if (!employeeId) {
          throw new Error('No matching Airtable record found for employee');
        }

        await base(AIRTABLE_ROLES_TABLE).create({
          'Employee': [employeeId],
          'Role': [airtableRoleMap[role.role_title.trim()]]
        });
        successCount++;
        console.log(`Added role for ${role.employee_name}: ${role.role_title}`);
        await new Promise(resolve => setTimeout(resolve, 250)); // Rate limiting
      } catch (error) {
        console.error(`Failed to add role for ${role.employee_name}:`, error);
        errors.push({
          employee: role.employee_name,
          role: role.role_title,
          error: error.message
        });
      }
    }

    // Final summary
    console.log('\nSync Summary:');
    console.log(`Total PG records: ${result.rows.length}`);
    console.log(`New combinations to add: ${newRoles.length}`);
    console.log(`Successfully added: ${successCount}`);
    console.log(`Failed: ${errors.length}`);

    if (errors.length > 0) {
      writeJsonReport('reports/role_sync_errors.json', errors);
    }

  } catch (error) {
    console.error('Error in employee roles sync:', error);
    return {
      success: false,
      error: error.message
    };
  }
}

module.exports = { syncEmployeeRoles };
