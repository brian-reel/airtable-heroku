require('dotenv').config();
const { pool, base, AIRTABLE_TABLE } = require('../config/database');
const { clearReportFiles, writeJsonReport } = require('../utils/logging');
const fs = require('fs');
const { updateAirtableRecord } = require('../services/airtable');

const tableName = process.env.AIRTABLE_TABLE_NAME || 'Employees_dev';
const pgEmailsTable = process.env.PG_TABLE_NAME_2 || 'emails'; // Default to 'emails' if not set
const pgEmployeesTable = process.env.PG_TABLE_NAME_3 || 'employees';

/**
 * writeEmailMatchReport
 * @param {any} matches - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
function writeEmailMatchReport(matches) {
  const report = matches.map(match => ({
    airtableId: match.airtableId,
    email: match.airtableEmail,
    pgEmployeeId: match.pgEmployeeId,
    currentFields: {
      rscEmpId: match.currentFields['RSC Emp ID'] || 'BLANK',
      email: match.currentFields['Email'] || 'BLANK'
    }
  }));

  fs.writeFileSync('email_match_report.json', JSON.stringify(report, null, 2));
  console.log('Email match report saved to email_match_report.json');
}

/**
 * getEmailsFromPostgres
 * @param {any} ...args - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
async function getEmailsFromPostgres(...args) {
  try {
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
        FROM ${pgEmailsTable} e
        LEFT JOIN ${pgEmployeesTable} emp ON e.emailable_id = emp.id
        WHERE e.emailable_type = 'Employee' 
        AND e.address IS NOT NULL
        AND e."primary" = true
      )
      SELECT * FROM RankedEmails WHERE rn = 1
      ORDER BY active DESC, created_at DESC
    `);
    console.log(`Fetched ${result.rows.length} email records from Postgres`);
    return result.rows;
  } catch (error) {
    handleError(error, 'Error querying Postgres emails:');
    throw error;
  }
}

/**
 * getPhoneNumbersFromPostgres
 * @param {any} ...args - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
async function getPhoneNumbersFromPostgres(...args) {
  try {
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
        FROM ${pgEmployeesTable} emp
        LEFT JOIN ${pgEmailsTable} e ON emp.id = e.emailable_id 
        AND e.emailable_type = 'Employee' 
        AND e."primary" = true
        WHERE emp.mobile_phone IS NOT NULL
      )
      SELECT * FROM RankedPhones WHERE rn = 1
      ORDER BY active DESC, created_at DESC
    `);
    console.log(`Fetched ${result.rows.length} phone records from Postgres`);
    return result.rows;
  } catch (error) {
    handleError(error, 'Error querying Postgres phones:');
    throw error;
  }
}

// Modify the getAirtableRecords function to fetch both fields
/**
 * getAirtableRecords
 * @param {any} ...args - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
async function getAirtableRecords(...args) {
  return tryCatch(async () => {
    
  const records = [];
  try {
    await base(tableName)
      .select({
        fields: ['RSC Emp ID', 'Email', 'Phone', 'Status-RSPG', 'Status', 'Name']
      })
      .eachPage((pageRecords, fetchNextPage) => {
        records.push(...pageRecords);
        fetchNextPage();
      });
    return records;
  } catch (error) {
    handleError(error, 'Error fetching Airtable data:');
    throw error;
  }
})

// Helper function to determine what needs updating
/**
 * determineUpdates
 * @param {any} airtableFields, pgRecord - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
function determineUpdates(airtableFields, pgRecord) {
  return {
    needsEmpId: !airtableFields['RSC Emp ID'],
    needsPhone: !airtableFields['Phone'] && pgRecord.mobile_phone,
    needsEmail: !airtableFields['Email'] && pgRecord.email,
    needsStatusRSPG: airtableFields['Status-RSPG'] !== (pgRecord.active ? 'Active' : 'Inactive'),
    needsStatus: airtableFields['Status'] !== (pgRecord.active ? 'Hired' : 'Separated'),
    needsName: airtableFields['Name'] !== pgRecord.name
  };
}

/**
 * formatDateToString
 * @param {any} date - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
function formatDateToString(date) {
  if (!date) return '';
  // Ensure we're working with a date object
  const d = new Date(date);
  // Use UTC methods to avoid timezone shifts
  return `${(d.getUTCMonth() + 1).toString().padStart(2, '0')}/${d.getUTCDate().toString().padStart(2, '0')}/${d.getUTCFullYear()}`;
}

// Add this helper function to determine which record to use
function determinePreferredRecord(records) {
  // Sort records by priority:
  // 1. Active status
  // 2. Most recent created_at date
  return records.sort((a, b) => {
    if (a.active && !b.active) return -1;
    if (!a.active && b.active) return 1;
    return new Date(b.created_at) - new Date(a.created_at);
  })[0];
}

// Update the matchAndUpdateEmails function
/**
 * matchAndUpdateEmails
 * @param {any} ...args - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
async function matchAndUpdateEmails() {
  return tryCatch(async () => {
    // Clear previous files
    console.log('Clearing previous data files...');
    clearReportFiles([
      'email_match_report.json',
      'pg_emails_sample.json',
      'airtable_records_sample.json',
      'email_sync_errors.json'
    ]);

    console.log('Fetching emails from Postgres...');
    const pgEmails = await getEmailsFromPostgres();
    
    console.log('Fetching records from Airtable...');
    const airtableRecords = await base(AIRTABLE_TABLE)
      .select({
        fields: ['RSC Emp ID', 'Phone', 'Email', 'Status-RSPG', 'Status', 'Name', 'RSC Hire Date']
      })
      .all();

    const matches = [];
    const updates = [];
    let blankEmpIdCount = 0;
    let emailMatchCount = 0;

    // Modify the pgEmailMap creation in matchAndUpdateEmails
    const pgEmailMap = pgEmails.reduce((acc, row) => {
      const email = row.address.toLowerCase();
      if (!acc[email]) {
        acc[email] = [];
      }
      acc[email].push({
        emailable_id: row.emailable_id,
        mobile_phone: row.mobile_phone,
        active: row.active,
        created_at: row.created_at
      });
      return acc;
    }, {});

    // Then modify the email matching logic
    for (const record of airtableRecords) {
      const airtableEmail = record.fields['Email']?.toLowerCase();
      
      if (airtableEmail && pgEmailMap[airtableEmail]) {
        emailMatchCount++;
        const pgRecord = pgEmailMap[airtableEmail];
        
        // Check if this email belongs to a different employee ID
        const existingRecordWithId = airtableRecords.find(r => 
          r.fields['RSC Emp ID'] === pgRecord.emailable_id.toString() &&
          r.id !== record.id
        );

        if (existingRecordWithId) {
          console.log(`Skipping update for ${airtableEmail} - already exists with RSC Emp ID ${pgRecord.emailable_id}`);
          continue;
        }

        const updates = determineUpdates(record.fields, pgRecord);
        
        // Only update if the new data is from an active record or the existing record is blank
        if (Object.values(updates).some(Boolean) && 
            (pgRecord.active || !record.fields['RSC Emp ID'])) {
          matches.push({
            airtableId: record.id,
            airtableEmail: airtableEmail,
            pgEmployeeId: pgRecord.emailable_id,
            pgPhone: pgRecord.mobile_phone,
            currentFields: record.fields,
            updates,
            isActive: pgRecord.active,
            created_at: pgRecord.created_at
          });
        }
      }
    }

    // Log statistics
    console.log('\nMatching Statistics:');
    console.log(`Total Airtable records: ${airtableRecords.length}`);
    console.log(`Records with blank RSC Emp ID: ${blankEmpIdCount}`);
    console.log(`Email matches found: ${emailMatchCount}`);

    // Write match report
    writeJsonReport('email_match_report.json', matches.map(match => ({
      airtableId: match.airtableId,
      email: match.airtableEmail,
      pgEmployeeId: match.pgEmployeeId,
      currentFields: {
        rscEmpId: match.currentFields['RSC Emp ID'] || 'BLANK',
        email: match.currentFields['Email'] || 'BLANK'
      }
    })));

    // Perform updates
    console.log('\nProceeding with updates...');
    let successCount = 0;
    const errors = [];

    for (const match of matches) {
      try {
        const updateFields = {};
        if (match.updates.needsEmpId) updateFields['RSC Emp ID'] = match.pgEmployeeId.toString();
        if (match.updates.needsPhone) updateFields['Phone'] = match.pgPhone;
        if (match.updates.needsStatusRSPG) updateFields['Status-RSPG'] = match.isActive ? 'Active' : 'Inactive';
        if (match.updates.needsStatus) updateFields['Status'] = match.isActive ? 'Hired' : 'Separated';
        if (match.updates.needsName) updateFields['Name'] = match.name;
        if (match.updates.needsHireDate) updateFields['RSC Hire Date'] = match.hireDate;

        if (Object.keys(updateFields).length > 0) {
          const success = await updateWithRetry(match.airtableId, updateFields);
          if (success) {
            successCount++;
            console.log(`Updated record ${match.airtableId} with:`, updateFields);
          }
        }
      } catch (error) {
        handleError(error, 'Failed to update record for email ${match.airtableEmail}:');
        errors.push({
          email: match.airtableEmail,
          error: error.message
        });
      }
    }

    // Final summary
    console.log('\nUpdate Summary:');
    console.log(`Successful updates: ${successCount}`);
    console.log(`Failed updates: ${errors.length}`);

    if (errors.length > 0) {
      writeJsonReport('email_sync_errors.json', errors);
      console.log('Error details saved to reports/email_sync_errors.json');
    }

  });
}

// Helper function to standardize phone number format
/**
 * standardizePhoneNumber
 * @param {any} phone - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
function standardizePhoneNumber(phone) {
  if (!phone) return null;
  // Remove all non-numeric characters
  return phone.replace(/\D/g, '').slice(-10);
}

// Update the matchAndUpdatePhones function similarly
/**
 * matchAndUpdatePhones
 * @param {any} ...args - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
async function matchAndUpdatePhones() {
  return tryCatch(async () => {
    console.log('Clearing previous phone match files...');
    clearReportFiles([
      'phone_match_report.json',
      'phone_sync_errors.json'
    ]);

    console.log('Fetching phone numbers from Postgres...');
    const pgPhones = await getPhoneNumbersFromPostgres();
    
    console.log('Fetching records from Airtable...');
    const airtableRecords = await base(AIRTABLE_TABLE)
      .select({
        fields: ['RSC Emp ID', 'Phone', 'Email', 'Status-RSPG', 'Status', 'Name', 'RSC Hire Date']
      })
      .all();

    const matches = [];
    let phoneMatchCount = 0;

    // Similarly modify the phone matching logic
    const pgPhoneMap = pgPhones.reduce((acc, row) => {
      const standardizedPhone = standardizePhoneNumber(row.mobile_phone);
      if (standardizedPhone) {
        if (!acc[standardizedPhone]) {
          acc[standardizedPhone] = [];
        }
        acc[standardizedPhone].push({
          id: row.id,
          email: row.email,
          active: row.active,
          created_at: row.created_at
        });
      }
      return acc;
    }, {});

    // And update the phone matching loop similarly
    for (const record of airtableRecords) {
      const airtablePhone = standardizePhoneNumber(record.fields['Phone']);
      
      if (airtablePhone && pgPhoneMap[airtablePhone]) {
        phoneMatchCount++;
        const pgRecord = pgPhoneMap[airtablePhone];
        
        // Check if this phone belongs to a different employee ID
        const existingRecordWithId = airtableRecords.find(r => 
          r.fields['RSC Emp ID'] === pgRecord.id.toString() &&
          r.id !== record.id
        );

        if (existingRecordWithId) {
          console.log(`Skipping update for phone ${airtablePhone} - already exists with RSC Emp ID ${pgRecord.id}`);
          continue;
        }

        const updates = determineUpdates(record.fields, pgRecord);
        
        if (Object.values(updates).some(Boolean)) {
          matches.push({
            airtableId: record.id,
            airtablePhone: record.fields['Phone'],
            pgEmployeeId: pgRecord.id,
            pgEmail: pgRecord.email,
            currentFields: record.fields,
            updates,
            isActive: pgRecord.active,
            created_at: pgRecord.created_at
          });
        }
      }
    }

    // Write match report
    writeJsonReport('phone_match_report.json', matches);

    // Log statistics
    console.log('\nPhone Matching Statistics:');
    console.log(`Total phone matches found: ${phoneMatchCount}`);
    console.log(`Records needing updates: ${matches.length}`);

    let successCount = 0;
    const errors = [];

    for (const match of matches) {
      try {
        const updateFields = {};
        if (match.updates.needsEmpId) updateFields['RSC Emp ID'] = match.pgEmployeeId.toString();
        if (match.updates.needsEmail) updateFields['Email'] = match.pgEmail;
        if (match.updates.needsStatusRSPG) updateFields['Status-RSPG'] = match.isActive ? 'Active' : 'Inactive';
        if (match.updates.needsStatus) updateFields['Status'] = match.isActive ? 'Hired' : 'Separated';
        if (match.updates.needsName) updateFields['Name'] = match.name;

        if (Object.keys(updateFields).length > 0) {
          await base(AIRTABLE_TABLE).update(match.airtableId, updateFields);
          successCount++;
          console.log(`Updated record ${match.airtableId} with fields:`, updateFields);
        }
      } catch (error) {
        handleError(error, 'Failed to update record for phone ${match.airtablePhone}:');
        errors.push({
          phone: match.airtablePhone,
          error: error.message
        });
      }
    }

    // Final summary
    console.log('\nPhone Update Summary:');
    console.log(`Successful updates: ${successCount}`);
    console.log(`Failed updates: ${errors.length}`);

    if (errors.length > 0) {
      writeJsonReport('phone_sync_errors.json', errors);
      console.log('Error details saved to reports/phone_sync_errors.json');
    }

  });
}

/**
 * getTenantState
 * @param {any} tenantId - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
function getTenantState(tenantId) {
  const stateMap = {
    '2': 'CA',
    '3': 'LA',
    '4': 'GA',
    '5': 'NM',
    '6': 'CA',
    '13': 'UK'
  };
  return stateMap[tenantId.toString()] || 'Unknown';
}

/**
 * addNewEmployees
 * @param {any} ...args - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
async function addNewEmployees(...args) {
  try {
    console.log('Fetching active employees from Postgres...');
    const result = await pool.query(`
      WITH RankedEmails AS (
        SELECT 
          emp.id as employee_id,
          emp.first_name || ' ' || emp.last_name as name,
          emp.tenant_id,
          emp.active,
          emp.mobile_phone,
          emp.hire_date,
          e.address as email,
          ROW_NUMBER() OVER (PARTITION BY emp.id ORDER BY emp.created_at DESC) as rn
        FROM ${pgEmployeesTable} emp
        LEFT JOIN ${pgEmailsTable} e ON emp.id = e.emailable_id 
        AND e.emailable_type = 'Employee' 
        AND e."primary" = true
        WHERE emp.active = true
        AND NOT (
          emp.first_name ILIKE ANY(ARRAY[
            'SER-N-%',
            'SER-D-%',
            'ESP-D-%',
            'ESP-N-%',
            'PRONE-D-%',
            'DF-D-%',
            'ULT-N-%',
            'ULT-D-%',
            'SS-N-%',
            'SS-D-%',
            'AVS-N-%',
            'AVS-D-%',
            'DF-N-%',
            'East%',
            'Eastern',
            'Mountain',
            'Central'
          ])
          OR emp.last_name = 'SUB'
          OR emp.last_name = 'Tester'
          OR emp.last_name ILIKE '%Tester%'
        )
      )
      SELECT * FROM RankedEmails WHERE rn = 1
    `);
    
    console.log('Fetching existing Airtable records...');
    const airtableRecords = await base(AIRTABLE_TABLE)
      .select({
        fields: ['RSC Emp ID']
      })
      .all();
    
    // Create set of existing RSC Emp IDs in Airtable
    const existingEmpIds = new Set(
      airtableRecords
        .map(record => record.fields['RSC Emp ID'])
        .filter(Boolean)
    );

    // Filter for new employees
    const newEmployees = result.rows.filter(emp => !existingEmpIds.has(emp.employee_id.toString()));
    
    console.log(`Found ${newEmployees.length} new active employees to add`);

    // Add new employees to Airtable
    let successCount = 0;
    const errors = [];

    for (const emp of newEmployees) {
      try {
        const formattedHireDate = emp.hire_date 
          ? new Date(emp.hire_date).toLocaleDateString('en-US', {
              month: '2-digit',
              day: '2-digit',
              year: 'numeric'
            })
          : '';

        const newRecord = {
          'Name': emp.name,
          'RSC Emp ID': emp.employee_id.toString(),
          'Region': getTenantState(emp.tenant_id),
          'Status-RSPG': emp.active ? 'Active' : 'Inactive',
          'Status': emp.active ? 'Hired' : 'Separated',
          'Email': emp.email || '',
          'Phone': emp.mobile_phone || '',
          'RSC Hire Date': formattedHireDate
        };

        await base(AIRTABLE_TABLE).create(newRecord);
        successCount++;
        console.log(`Added new employee: ${emp.name} (${emp.employee_id})`);
      } catch (error) {
        handleError(error, 'Failed to add employee ${emp.employee_id}:');
        errors.push({
          employeeId: emp.employee_id,
          name: emp.name,
          error: error.message
        });
      }
    }

    console.log(`\nNew Employee Addition Summary:`);
    console.log(`Successfully added: ${successCount}`);
    console.log(`Failed to add: ${errors.length}`);

    if (errors.length > 0) {
      writeJsonReport('new_employee_errors.json', errors);
    }

    return successCount > 0;
  } catch (error) {
    handleError(error, 'Error in addNewEmployees:');
    throw error;
  }
})

/**
 * Synchronize employee data between PostgreSQL and Airtable
 * @returns {Promise<boolean>} - Success status
 */
async function syncEmployeeData() {
  try {
    await logToHistory('Starting employeedata sync...');
    console.log('Starting employeedata sync...');
    logSyncStart('employeedata sync');
    
    console.log('Starting employee data sync...');
    
    // 1. Get all employees from PG with proper ranking for active/inactive status
    const pgEmployees = await pool.query(`
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
            PARTITION BY emp.first_name || ' ' || emp.last_name  -- Partition by full name to catch duplicates
            ORDER BY 
              emp.active DESC,  -- Active records first
              emp.updated_at DESC,  -- Most recently updated
              emp.created_at DESC  -- Most recently created
          ) as rn
        FROM ${pgEmployeesTable} emp
        LEFT JOIN ${pgEmailsTable} e ON emp.id = e.emailable_id 
        AND e.emailable_type = 'Employee' 
        AND e."primary" = true
        WHERE NOT (
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
      )
      SELECT * FROM RankedEmployees 
      WHERE rn = 1  -- Only take the best record for each employee
      ORDER BY employee_id
    `);

    // 2. Get all Airtable records
    const airtableRecords = await base(AIRTABLE_TABLE)
      .select({
        fields: ['RSC Emp ID', 'Email', 'Phone', 'Status-RSPG', 'Status', 'Name', 'RSC Hire Date']
      })
      .all();

    // 3. Create lookup maps
    const pgEmployeeMap = pgEmployees.rows.reduce((acc, emp) => {
      acc[emp.employee_id] = emp;
      if (emp.email) acc[emp.email.toLowerCase()] = emp;
      if (emp.mobile_phone) acc[standardizePhoneNumber(emp.mobile_phone)] = emp;
      return acc;
    }, {});

    // 4. Find matches and determine updates
    const updates = [];
    for (const record of airtableRecords) {
      let pgMatch = null;
      
      // Try to match by RSC Emp ID first
      if (record.fields['RSC Emp ID']) {
        pgMatch = pgEmployeeMap[record.fields['RSC Emp ID']];
      }
      
      // If no match, try email
      if (!pgMatch && record.fields['Email']) {
        pgMatch = pgEmployeeMap[record.fields['Email'].toLowerCase()];
      }
      
      // If still no match, try phone
      if (!pgMatch && record.fields['Phone']) {
        const standardizedPhone = standardizePhoneNumber(record.fields['Phone']);
        pgMatch = pgEmployeeMap[standardizedPhone];
      }

      if (pgMatch) {
        const updateFields = {};
        
        // Add explicit status checking
        const currentStatusRSPG = record.fields['Status-RSPG'];
        const currentStatus = record.fields['Status'];
        const shouldBeActive = pgMatch.active;
        
        // Always update status if it doesn't match PostgreSQL
        if (shouldBeActive) {
          if (currentStatusRSPG !== 'Active') updateFields['Status-RSPG'] = 'Active';
          if (currentStatus !== 'Hired') updateFields['Status'] = 'Hired';
        } else {
          if (currentStatusRSPG !== 'Inactive') updateFields['Status-RSPG'] = 'Inactive';
          if (currentStatus !== 'Separated') updateFields['Status'] = 'Separated';
        }
        
        // Log status changes for debugging
        if (updateFields['Status-RSPG'] || updateFields['Status']) {
          console.log(`Status update for ${record.fields['Name']}:`, {
            current: { statusRSPG: currentStatusRSPG, status: currentStatus },
            new: { statusRSPG: updateFields['Status-RSPG'] || currentStatusRSPG, status: updateFields['Status'] || currentStatus },
            pgActive: shouldBeActive
          });
        }
        
        // Compare and add fields that need updating
        if (record.fields['Name'] !== pgMatch.name) {
          updateFields['Name'] = pgMatch.name;
        }
        
        // Only update hire date if it's different or missing
        if (pgMatch.hire_date) {
          const pgDate = formatDateToString(pgMatch.hire_date);
          const airtableDate = formatDateToString(record.fields['RSC Hire Date']);
          
          // Only update if dates are actually different (ignoring format)
          if (!record.fields['RSC Hire Date'] || pgDate !== airtableDate) {
            // Log the actual difference for verification
            console.log(`Hire date difference for ${record.fields['Name']}:`, {
              original: {
                pg: pgMatch.hire_date,
                airtable: record.fields['RSC Hire Date']
              },
              formatted: {
                pg: pgDate,
                airtable: airtableDate
              }
            
            // Only add to updateFields if truly different
            if (pgDate !== airtableDate) {
              updateFields['RSC Hire Date'] = pgDate;
            }
          }
        }

        if (Object.keys(updateFields).length > 0) {
          updates.push({
            recordId: record.id,
            fields: updateFields
          });
        }
      }
    }

    // 5. Perform updates with rate limiting
    console.log(`Found ${updates.length} records that need updating`);
    let successCount = 0;
    const errors = [];

    for (const update of updates) {
      try {
        await updateAirtableRecord(update.recordId, update.fields);
        await sleep(250); // Rate limit
        successCount++;
        console.log(`Updated record ${update.recordId}:`, update.fields);
      } catch (error) {
        console.error(`Failed to update record ${update.recordId}:`, error.message);
        errors.push({
          recordId: update.recordId,
          error: error.message
        });
      }
    }

    // 6. Final summary
    console.log('\nSync Summary:');
    console.log(`Total records processed: ${airtableRecords.length}`);
    console.log(`Updates needed: ${updates.length}`);
    console.log(`Successful updates: ${successCount}`);
    console.log(`Failed updates: ${errors.length}`);

    if (errors.length > 0) {
      writeJsonReport('sync_errors.json', errors);
    }
    
    logSyncEnd('employeedata sync', {
      'Total records': airtableRecords.length,
      'Updates needed': updates.length,
      'Successful updates': successCount,
      'Failed updates': errors.length
    });
    
    await logToHistory('Employeedata sync completed');
    console.log('Employeedata sync completed');
    
    return true;
  } catch (error) {
    console.error('Error in employee sync:', error.message);
    if (error.stack) console.error(error.stack);
    return false;
  }
}

module.exports = {
  matchAndUpdateEmails,
  matchAndUpdatePhones,
  addNewEmployees,
  syncEmployeeData
}; 