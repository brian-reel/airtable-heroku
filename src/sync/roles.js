require('dotenv').config();
const { pool, base, AIRTABLE_TABLE } = require('../config/database');
const { clearReportFiles, writeJsonReport, logSyncStart, logSyncEnd, logToHistory } = require('../utils/logging');
const fs = require('fs');
const { executeQuery } = require('../services/postgres');
const { getAirtableRecords, updateAirtableRecord, createAirtableRecord } = require('../services/airtable');
const { validateUpdateFields } = require('../utils/validation');
const { handleError, tryCatch } = require('../utils/error-handler');
const config = require('../config/config');
const { AIRTABLE_ROLES_TABLE } = require('../config/database');

// Table names
const PG_ROLE_TYPES_TABLE = process.env.PG_TABLE_NAME_4 || 'role_types';
const PG_EMPLOYEE_ROLES_TABLE = process.env.PG_TABLE_NAME_5 || 'employee_roles';
const PG_EMPLOYEES_TABLE = process.env.PG_TABLE_NAME_3 || 'employees';

/**
 * Get Airtable Employee Map
 * @returns {Promise<any>} - Description of return value
 */
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

/**
 * Get Airtable Role Map
 * @returns {Promise<any>} - Description of return value
 */
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

/**
 * Sync employee roles from PostgreSQL to Airtable
 */
async function syncEmployeeRoles() {
  return tryCatch(async () => {
    await logToHistory('Starting employee roles sync...');
    console.log('Starting employee roles sync...');
    
    // Clear previous report files
    clearReportFiles([
      'roles_sync.json',
      'roles_errors.json'
    ]);
    
    logSyncStart('employee roles sync');
    
    // 1. Get employee roles from PostgreSQL
    const pgRoles = await getEmployeeRolesFromPostgres();
    
    // 2. Get existing Airtable records
    const airtableRecords = await getAirtableRecords(AIRTABLE_ROLES_TABLE, [
      'RSC Emp ID', 'Role', 'Department', 'Status'
    ]);
    
    // 3. Create lookup map for Airtable records
    const airtableMap = {};
    for (const record of airtableRecords) {
      if (record.fields['RSC Emp ID']) {
        const employeeId = record.fields['RSC Emp ID'];
        if (!airtableMap[employeeId]) {
          airtableMap[employeeId] = [];
        }
        airtableMap[employeeId].push(record);
      }
    }
    
    // 4. Process updates and new records
    const updates = [];
    const newRecords = [];
    
    for (const role of pgRoles) {
      const employeeId = role.employee_id.toString();
      const airtableEmployeeRoles = airtableMap[employeeId] || [];
      
      // Try to find a matching role record
      const matchingRole = airtableEmployeeRoles.find(record => 
        record.fields['Role'] === role.role_name && 
        record.fields['Department'] === role.department_name
      );
      
      if (matchingRole) {
        // Check if update is needed
        const updateFields = {};
        
        // Update status if needed
        const currentStatus = matchingRole.fields['Status'];
        const desiredStatus = role.active ? 'Active' : 'Inactive';
        
        if (currentStatus !== desiredStatus) {
          updateFields['Status'] = desiredStatus;
        }
        
        if (Object.keys(updateFields).length > 0) {
          // Validate update fields
          const validation = validateUpdateFields(updateFields);
          if (validation.isValid) {
            updates.push({
              recordId: matchingRole.id,
              fields: updateFields
            });
          } else {
            console.warn(`Skipping invalid update for ${matchingRole.id}:`, validation.errors);
          }
        }
      } else if (role.active) {
        // Create new record if role is active
        newRecords.push({
          'RSC Emp ID': employeeId,
          'Role': role.role_name,
          'Department': role.department_name,
          'Status': 'Active'
        });
      }
    }
    
    // 5. Perform updates
    console.log(`Found ${updates.length} roles that need updating`);
    let updateSuccessCount = 0;
    const updateErrors = [];
    
    for (const update of updates) {
      try {
        await updateAirtableRecord(AIRTABLE_ROLES_TABLE, update.recordId, update.fields);
        updateSuccessCount++;
        console.log(`Updated role record ${update.recordId}`);
      } catch (error) {
        handleError(error, 'role_update', { recordId: update.recordId, fields: update.fields });
        updateErrors.push({
          recordId: update.recordId,
          error: error.message
        });
      }
    }
    
    // 6. Create new records
    console.log(`Found ${newRecords.length} new roles to add`);
    let createSuccessCount = 0;
    const createErrors = [];
    
    for (const newRecord of newRecords) {
      try {
        await createAirtableRecord(AIRTABLE_ROLES_TABLE, newRecord);
        createSuccessCount++;
        console.log(`Added new role for employee ${newRecord['RSC Emp ID']}: ${newRecord['Role']}`);
      } catch (error) {
        handleError(error, 'role_create', { 
          employeeId: newRecord['RSC Emp ID'], 
          role: newRecord['Role'] 
        });
        createErrors.push({
          employeeId: newRecord['RSC Emp ID'],
          role: newRecord['Role'],
          error: error.message
        });
      }
    }
    
    // 7. Write reports
    if (updateErrors.length > 0 || createErrors.length > 0) {
      writeJsonReport('roles_errors.json', {
        updateErrors,
        createErrors
      });
    }
    
    // 8. Log summary
    logSyncEnd('employee roles sync', {
      'Total roles processed': pgRoles.length,
      'Updates needed': updates.length,
      'Successful updates': updateSuccessCount,
      'Failed updates': updateErrors.length,
      'New records': newRecords.length,
      'Successfully created': createSuccessCount,
      'Failed creations': createErrors.length
    });
    
    await logToHistory('Employee roles sync completed');
    console.log('Employee roles sync completed');
    
    return updateSuccessCount > 0 || createSuccessCount > 0;
  }, 'sync_employee_roles');
}

/**
 * Get employee roles from PostgreSQL
 */
async function getEmployeeRolesFromPostgres() {
  return tryCatch(async () => {
    const query = `
      WITH ranked_roles AS (
        SELECT
          er.employee_id,
          r.name AS role_name,
          d.name AS department_name,
          er.active,
          er.created_at,
          ROW_NUMBER() OVER (
            PARTITION BY er.employee_id, r.name, d.name
            ORDER BY 
              er.active DESC,
              er.created_at DESC
          ) AS row_num
        FROM employee_roles er
        JOIN roles r ON er.role_id = r.id
        JOIN departments d ON r.department_id = d.id
        JOIN employees e ON er.employee_id = e.id
        WHERE e.email NOT LIKE '%test%'
          AND e.email NOT LIKE '%example%'
      )
      SELECT
        employee_id,
        role_name,
        department_name,
        active,
        created_at
      FROM ranked_roles
      WHERE row_num = 1
      ORDER BY employee_id, department_name, role_name;
    `;
    
    return await executeQuery(query);
  }, 'get_employee_roles_from_postgres');
}

module.exports = {
  syncEmployeeRoles
};
