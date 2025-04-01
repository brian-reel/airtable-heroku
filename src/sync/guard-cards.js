require('dotenv').config();
const { pool, base, AIRTABLE_TABLE, PG_LICENSES_TABLE, PG_EMPLOYEES_TABLE } = require('../config/database');
const { clearReportFiles, writeJsonReport, logSyncStart, logSyncEnd, logToHistory } = require('../utils/logging');
const fs = require('fs');
const { executeQuery } = require('../services/postgres');
const { getAirtableRecords, updateAirtableRecord, createAirtableRecord } = require('../services/airtable');
const { formatDate } = require('../utils/formatters');
const { validateUpdateFields } = require('../utils/validation');
const { handleError, tryCatch } = require('../utils/error-handler');
const config = require('../config/config');
const { AIRTABLE_GUARD_CARD_TABLE } = require('../config/database');

// Fetch employees from PostgreSQL
/**
 * getEmployeesFromPostgres
 * @param {any}  - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
async function getEmployeesFromPostgres() {
  try {
    console.log('PostgreSQL tables:', {
      pgLicensesTable: PG_LICENSES_TABLE,
      pgEmployeesTable: PG_EMPLOYEES_TABLE
    });
    
    // Modified query to properly handle active/inactive records
    const result = await pool.query(`
      WITH EmployeeRanking AS (
        -- First rank employees by active status
        SELECT 
          e.id as emp_id,
          e.first_name || ' ' || e.last_name as name,
          e.active,
          ROW_NUMBER() OVER (
            PARTITION BY e.id 
            ORDER BY e.active DESC, e.updated_at DESC
          ) as emp_rank
        FROM ${PG_EMPLOYEES_TABLE} e
      ),
      RankedLicenses AS (
        -- Then get the latest license for each employee
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
        FROM ${PG_LICENSES_TABLE} l
        JOIN EmployeeRanking er ON l.employee_id = er.emp_id
        WHERE er.emp_rank = 1  -- Only consider the primary employee record
        AND l.license_type_id IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')
      )
      SELECT * FROM RankedLicenses 
      WHERE license_rank = 1
      ORDER BY employee_id
    `);
    
    return result.rows;
  } catch (error) {
    handleError(error, 'Error fetching PostgreSQL data:');
    throw error;
  }
}

// Fetch employees from Airtable
/**
 * getEmployeesFromAirtable
 * @param {any}  - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
async function getEmployeesFromAirtable() {
  const records = [];
  try {
    await base(AIRTABLE_TABLE)
      .select({
        fields: ['RSC Emp ID', 'GC - RSPG', 'GC Exp Date - RSPG', 'license type id - RSPG', 'Status-RSPG', 'Status']
      })
      .eachPage((pageRecords, fetchNextPage) => {
        records.push(...pageRecords);
        fetchNextPage();
      });
    return records.map((record) => ({
      id: record.id,
      fields: {
        'RSC Emp ID': record.fields['RSC Emp ID'] || '',
        'GC - RSPG': record.fields['GC - RSPG'] || '',
        'GC Exp Date - RSPG': record.fields['GC Exp Date - RSPG'] 
          ? (() => {
              try {
                const [year, month, day] = record.fields['GC Exp Date - RSPG'].split('-');
                return `${month}/${day}/${year}`; // Rearrange to MM/DD/YYYY
              } catch (e) {
                return record.fields['GC Exp Date - RSPG']; // Return as-is if format is unexpected
              }
            })()
          : '',
        'license type id - RSPG': record.fields['license type id - RSPG'] || '',
        'Status-RSPG': record.fields['Status-RSPG'] || '',
        'Status': record.fields['Status'] || ''
      }
    }));
  } catch (error) {
    handleError(error, 'Error fetching Airtable data:');
    throw error;
  }
}

// Move the logEmployeeDetails function definition to the top of the file
/**
 * logEmployeeDetails
 * @param {any} employeeId, data - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
function logEmployeeDetails(employeeId, data) {
  // Find the employee in the provided data
  const employeeData = data.find(match => match.postgres.employee_id === employeeId);
  if (employeeData) {
    console.log('EMPLOYEE DETAILS CHECK:', {
      employeeId,
      pgActive: employeeData.postgres.active,
      pgName: employeeData.postgres.name,
      airtableStatus: {
        statusRSPG: employeeData.airtable['Status-RSPG'],
        status: employeeData.airtable['Status']
      },
      updateFields: employeeData.updateFields,
      needsUpdate: employeeData.needsUpdate
    });
  } else {
    console.log(`Employee ${employeeId} not found in matched data`);
  }
}

// Sync data from PostgreSQL to Airtable
/**
 * syncPostgresToAirtable
 * @param {any}  - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
async function syncPostgresToAirtable() {
  try {
    // Clear previous files
    console.log('Clearing previous data files...');
    clearReportFiles([
      'emp_data.json',
      'matched_data.json',
      'sync_errors.json',
      'update_report.json'
    ]);

    console.log('Fetching employees from Postgres...');
    const postgresEmployees = await getEmployeesFromPostgres();
    console.log(`Fetched ${postgresEmployees.length} employees from Postgres.`);

    console.log('Fetching employees from Airtable...');
    const airtableEmployees = await getEmployeesFromAirtable();
    console.log(`Fetched ${airtableEmployees.length} employees from Airtable.`);

    // Save sample data for debugging
    writeJsonReport('emp_data.json', {
      postgres: postgresEmployees.slice(0, 100),
      airtable: airtableEmployees.slice(0, 100).map(record => ({
        id: record.id,
        fields: record.fields,
        empId: record.fields['RSC Emp ID']
      }))
    });

    // Rest of the matching logic remains the same
    const airtableById = airtableEmployees.reduce((acc, record) => {
      const empId = record.fields['RSC Emp ID'];
      if (empId) acc[empId] = record;
      return acc;
    }, {});

    const matchedData = postgresEmployees
      .filter(pgEmployee => pgEmployee.license_type_id === "1")
      .map(pgEmployee => {
        const matchingAirtableRecord = airtableById[pgEmployee.employee_id];
        
        // If we found a matching record in Airtable
        if (matchingAirtableRecord) {
          // Format PG date to MM/DD/YYYY if it exists
          const formattedExpDate = pgEmployee.expires_on 
            ? (() => {
                const expiresDate = new Date(pgEmployee.expires_on);
                return `${(expiresDate.getMonth() + 1).toString().padStart(2, '0')}/${expiresDate.getDate().toString().padStart(2, '0')}/${expiresDate.getFullYear()}`;
              })()
            : ''; 
          
          const currentGuardCard = matchingAirtableRecord.fields['GC - RSPG'] || '';
          const currentExpDate = matchingAirtableRecord.fields['GC Exp Date - RSPG'] 
            ? matchingAirtableRecord.fields['GC Exp Date - RSPG'].replace(/-/g, '/') 
            : '';
          const currentLicenseType = matchingAirtableRecord.fields['license type id - RSPG'] || '';
          const currentRscEmpId = matchingAirtableRecord.fields['RSC Emp ID'] || '';

          const needsUpdate = 
            // Check if RSC Emp ID needs updating
            (currentRscEmpId !== pgEmployee.employee_id.toString()) ||
            // Update if PG has data and Airtable is blank
            (pgEmployee.number && !currentGuardCard) ||
            (pgEmployee.expires_on && !currentExpDate) ||
            (pgEmployee.license_type_id && !currentLicenseType) ||
            // Or if both have data but it's different
            (pgEmployee.number && currentGuardCard && currentGuardCard.trim() !== pgEmployee.number.trim()) ||
            (pgEmployee.expires_on && currentExpDate && currentExpDate.trim() !== formattedExpDate.trim()) ||
            (pgEmployee.license_type_id && currentLicenseType && currentLicenseType !== pgEmployee.license_type_id) ||
            // Add status checks
            (matchingAirtableRecord.fields['Status-RSPG'] !== (pgEmployee.active ? 'Active' : 'Inactive')) ||
            (matchingAirtableRecord.fields['Status'] !== (pgEmployee.active ? 'Hired' : 'Separated'));

          if (needsUpdate) {
            return {
              postgres: pgEmployee,
              airtable: matchingAirtableRecord.fields,
              needsUpdate,
              updateFields: {
                'RSC Emp ID': pgEmployee.employee_id.toString(),  // Always include RSC Emp ID in updates
                'GC - RSPG': pgEmployee.number || '',
                ...(pgEmployee.expires_on === null 
                  ? { 'GC Exp Date - RSPG': null }
                  : { 'GC Exp Date - RSPG': formattedExpDate }),
                'license type id - RSPG': pgEmployee.license_type_id,
                'Status-RSPG': pgEmployee.active ? 'Active' : 'Inactive',
                'Status': pgEmployee.active ? 'Hired' : 'Separated'
              },
              currentValues: {
                guardCard: currentGuardCard || 'BLANK',
                expDate: currentExpDate || 'BLANK',
                licenseType: currentLicenseType || 'BLANK',
                rscEmpId: currentRscEmpId || 'BLANK',
                statusRSPG: matchingAirtableRecord.fields['Status-RSPG'] || 'BLANK',
                status: matchingAirtableRecord.fields['Status'] || 'BLANK',
                pgActive: pgEmployee.active
              }
            };
          }

          // Always return the match, but include whether it needs updating
          return {
            postgres: pgEmployee,
            airtable: matchingAirtableRecord.fields,
            needsUpdate,
            updateFields: {
              'RSC Emp ID': pgEmployee.employee_id.toString(),  // Always include RSC Emp ID
              'GC - RSPG': pgEmployee.number || '',
              ...(pgEmployee.expires_on === null 
                ? { 'GC Exp Date - RSPG': null }
                : { 'GC Exp Date - RSPG': formattedExpDate }),
              'license type id - RSPG': pgEmployee.license_type_id,
              'Status-RSPG': pgEmployee.active ? 'Active' : 'Inactive',
              'Status': pgEmployee.active ? 'Hired' : 'Separated'
            },
            currentValues: {
              guardCard: currentGuardCard || 'BLANK',
              expDate: currentExpDate || 'BLANK',
              licenseType: currentLicenseType || 'BLANK',
              rscEmpId: currentRscEmpId || 'BLANK',
              statusRSPG: matchingAirtableRecord.fields['Status-RSPG'] || 'BLANK',
              status: matchingAirtableRecord.fields['Status'] || 'BLANK',
              pgActive: pgEmployee.active
            }
          };
        }
        return null; // Return null if no matching Airtable record found
      })
      .filter(match => match !== null);
  
      // Log the total matches and how many need updates
      const recordsNeedingUpdate = matchedData.filter(match => match.needsUpdate);
      console.log(`Found ${matchedData.length} total matches`);
      console.log(`Found ${recordsNeedingUpdate.length} records that need updating`);
  
      // Check specific employees
      logEmployeeDetails('11398', matchedData);
  
      // Add this line
      writeUpdateReport(recordsNeedingUpdate);
  
      // Save all matched data to file
      fs.writeFileSync('matched_data.json', JSON.stringify(matchedData, null, 2));
      console.log('All matched data saved to matched_data.json');
  
      if (recordsNeedingUpdate.length === 0) {
        console.log('No updates needed. Aborting.');
        return;
      }
  
      console.log('Proceeding with updates...');
      const errors = [];
      let successCount = 0;

      // Add this line to log the first few records that need updating
      console.log('Sample records needing update:', recordsNeedingUpdate.slice(0, 3).map(r => ({
        id: r.postgres.employee_id,
        name: r.postgres.name,
        updateFields: r.updateFields,
        needsUpdate: r.needsUpdate
      })));

      for (const match of recordsNeedingUpdate) {
        try {
          const recordId = airtableById[match.postgres.employee_id].id;
          
          // Log the specific update being attempted
          console.log(`Attempting to update record ${recordId} for ${match.postgres.name || 'Unknown'}:`, {
            employeeId: match.postgres.employee_id,
            active: match.postgres.active,
            updateFields: match.updateFields
          });
          
          // Add a delay to avoid rate limiting
          await new Promise(resolve => setTimeout(resolve, 200));
          
          await updateAirtableRecord(AIRTABLE_TABLE, recordId, match.updateFields);
          console.log(`Successfully updated Airtable record ${recordId}`);
          successCount++;
        } catch (error) {
          handleError({
            error: error.message,
            statusCode: error.statusCode,
            recordDetails: {
              employeeId: match.postgres.employee_id,
              name: match.postgres.name,
              active: match.postgres.active
            }
          }, 'Failed to update record for employee ${match.postgres.employee_id}:');
          errors.push({
            employeeId: match.postgres.employee_id,
            error: error.message,
            statusCode: error.statusCode
          });
          continue; // Continue with next record even if this one fails
        }
      }

      // Final summary
      console.log('\nSync Summary:');
      console.log(`Total records processed: ${matchedData.length}`);
      console.log(`Records needing update: ${recordsNeedingUpdate.length}`);
      console.log(`Successful updates: ${successCount}`);
      console.log(`Failed updates: ${errors.length}`);

      if (errors.length > 0) {
        console.log('\nFailed Updates:');
        console.log(JSON.stringify(errors, null, 2));
        // Optionally save errors to a file
        writeJsonReport('sync_errors.json', errors);
        console.log('Detailed error log saved to sync_errors.json');
      }

      console.log('Sync process completed.');
    } catch (error) {
      handleError(error, 'Fatal error during sync:');
      throw error;
    }
}

// Add this function near the top with other file operations
/**
 * writeUpdateReport
 * @param {any} recordsNeedingUpdate - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
function writeUpdateReport(recordsNeedingUpdate) {
  const report = recordsNeedingUpdate.map(record => ({
    employeeId: record.postgres.employee_id,
    name: record.postgres.name,
    changes: {
      guardCard: {
        needsUpdate: record.currentValues.guardCard !== (record.postgres.number || 'BLANK'),
        current: record.currentValues.guardCard,
        new: record.postgres.number || 'BLANK'
      },
      expDate: {
        needsUpdate: record.currentValues.expDate !== (record.updateFields['GC Exp Date - RSPG'] || 'BLANK'),
        current: record.currentValues.expDate,
        new: record.updateFields['GC Exp Date - RSPG'] || 'BLANK'
      },
      licenseType: {
        needsUpdate: record.currentValues.licenseType !== record.postgres.license_type_id,
        current: record.currentValues.licenseType,
        new: record.postgres.license_type_id
      },
      statusRSPG: {
        needsUpdate: record.currentValues.statusRSPG !== record.postgres.statusRSPG,
        current: record.currentValues.statusRSPG,
        new: record.postgres.statusRSPG
      },
      status: {
        needsUpdate: record.currentValues.status !== record.postgres.status,
        current: record.currentValues.status,
        new: record.postgres.status
      },
      pgActive: {
        needsUpdate: record.currentValues.pgActive !== record.postgres.active,
        current: record.currentValues.pgActive,
        new: record.postgres.active
      }
    }
  }));

  fs.writeFileSync('update_report.json', JSON.stringify(report, null, 2));
  console.log('Update report saved to update_report.json');
}

// This is likely in the formatDate function or where dates are being processed
/**
 * formatLocalDate
 * @param {any} date - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
function formatLocalDate(date) {
  if (!date) return '';
  
  try {
    const pgDate = new Date(date);
    const formattedDate = pgDate.toLocaleDateString('en-US', {
      month: '2-digit',
      day: '2-digit',
      year: 'numeric'
    });
    
    return formattedDate;
  } catch (error) {
    handleError(error, 'Error formatting date:');
    return '';
  }
}

// ... or it might be in the comparison function
/**
 * needsUpdate
 * @param {any} airtableRecord, pgData - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
function needsUpdate(airtableRecord, pgData) {
  // ... existing code ...
  
  // ... rest of function
}

/**
 * Sync guard card information from PostgreSQL to Airtable
 */
async function syncGuardCards() {
  return tryCatch(async () => {
    await logToHistory('Starting guard card sync...');
    console.log('Starting guard card sync...');
    
    // Clear previous report files
    clearReportFiles([
      'guard_card_sync.json',
      'guard_card_errors.json'
    ]);
    
    logSyncStart('guard card sync');
    
    // 1. Get guard card data from PostgreSQL
    const pgGuardCards = await getGuardCardsFromPostgres();
    
    // 2. Get existing Airtable records
    const airtableRecords = await getAirtableRecords(AIRTABLE_GUARD_CARD_TABLE, [
      'RSC Emp ID', 'Guard Card Number', 'Expiration Date', 'Issue Date', 'Status'
    ]);
    
    // 3. Create lookup map for Airtable records
    const airtableMap = airtableRecords.reduce((acc, record) => {
      if (record.fields['RSC Emp ID']) {
        acc[record.fields['RSC Emp ID']] = record;
      }
      return acc;
    }, {});
    
    // 4. Process updates and new records
    const updates = [];
    const newRecords = [];
    
    for (const guardCard of pgGuardCards) {
      const employeeId = guardCard.employee_id.toString();
      const airtableRecord = airtableMap[employeeId];
      
      if (airtableRecord) {
        // Check if update is needed
        const updateFields = {};
        
        if (guardCard.card_number && airtableRecord.fields['Guard Card Number'] !== guardCard.card_number) {
          updateFields['Guard Card Number'] = guardCard.card_number;
        }
        
        if (guardCard.expiration_date) {
          const pgExpirationDate = formatDate(guardCard.expiration_date);
          const airtableExpirationDate = formatDate(airtableRecord.fields['Expiration Date']);
          
          if (pgExpirationDate !== airtableExpirationDate) {
            updateFields['Expiration Date'] = pgExpirationDate;
          }
        }
        
        if (guardCard.issue_date) {
          const pgIssueDate = formatDate(guardCard.issue_date);
          const airtableIssueDate = formatDate(airtableRecord.fields['Issue Date']);
          
          if (pgIssueDate !== airtableIssueDate) {
            updateFields['Issue Date'] = pgIssueDate;
          }
        }
        
        // Update status if needed
        const currentStatus = airtableRecord.fields['Status'];
        const shouldBeActive = guardCard.active && new Date(guardCard.expiration_date) > new Date();
        const desiredStatus = shouldBeActive ? 'Active' : 'Inactive';
        
        if (currentStatus !== desiredStatus) {
          updateFields['Status'] = desiredStatus;
        }
        
        if (Object.keys(updateFields).length > 0) {
          // Validate update fields
          const validation = validateUpdateFields(updateFields);
          if (validation.isValid) {
            updates.push({
              recordId: airtableRecord.id,
              fields: updateFields
            });
          } else {
            console.warn(`Skipping invalid update for ${airtableRecord.id}:`, validation.errors);
          }
        }
      } else {
        // Create new record if guard card is active
        if (guardCard.active && new Date(guardCard.expiration_date) > new Date()) {
          newRecords.push({
            'RSC Emp ID': employeeId,
            'Guard Card Number': guardCard.card_number || '',
            'Expiration Date': formatDate(guardCard.expiration_date) || '',
            'Issue Date': formatDate(guardCard.issue_date) || '',
            'Status': 'Active'
          });
        }
      }
    }
    
    // 5. Perform updates
    console.log(`Found ${updates.length} guard cards that need updating`);
    let updateSuccessCount = 0;
    const updateErrors = [];
    
    for (const update of updates) {
      try {
        await updateAirtableRecord(AIRTABLE_GUARD_CARD_TABLE, update.recordId, update.fields);
        updateSuccessCount++;
        console.log(`Updated guard card record ${update.recordId}`);
      } catch (error) {
        handleError(error, 'guard_card_update', { recordId: update.recordId, fields: update.fields });
        updateErrors.push({
          recordId: update.recordId,
          error: error.message
        });
      }
    }
    
    // 6. Create new records
    console.log(`Found ${newRecords.length} new guard cards to add`);
    let createSuccessCount = 0;
    const createErrors = [];
    
    for (const newRecord of newRecords) {
      try {
        await createAirtableRecord(AIRTABLE_GUARD_CARD_TABLE, newRecord);
        createSuccessCount++;
        console.log(`Added new guard card for employee ${newRecord['RSC Emp ID']}`);
      } catch (error) {
        handleError(error, 'guard_card_create', { employeeId: newRecord['RSC Emp ID'] });
        createErrors.push({
          employeeId: newRecord['RSC Emp ID'],
          error: error.message
        });
      }
    }
    
    // 7. Write reports
    if (updateErrors.length > 0 || createErrors.length > 0) {
      writeJsonReport('guard_card_errors.json', {
        updateErrors,
        createErrors
      });
    }
    
    // 8. Log summary
    logSyncEnd('guard card sync', {
      'Total guard cards processed': pgGuardCards.length,
      'Updates needed': updates.length,
      'Successful updates': updateSuccessCount,
      'Failed updates': updateErrors.length,
      'New records': newRecords.length,
      'Successfully created': createSuccessCount,
      'Failed creations': createErrors.length
    });
    
    await logToHistory('Guard card sync completed');
    console.log('Guard card sync completed');
    
    return updateSuccessCount > 0 || createSuccessCount > 0;
  }, 'sync_guard_cards');
}

/**
 * Get guard card data from PostgreSQL
 */
async function getGuardCardsFromPostgres() {
  return tryCatch(async () => {
    const query = `
      WITH ranked_cards AS (
        SELECT
          gc.*,
          e.active AS employee_active,
          ROW_NUMBER() OVER (
            PARTITION BY gc.employee_id
            ORDER BY 
              gc.active DESC,
              gc.expiration_date DESC,
              gc.created_at DESC
          ) AS row_num
        FROM guard_cards gc
        JOIN employees e ON gc.employee_id = e.id
        WHERE e.email NOT LIKE '%test%'
          AND e.email NOT LIKE '%example%'
      )
      SELECT
        employee_id,
        card_number,
        issue_date,
        expiration_date,
        active,
        created_at
      FROM ranked_cards
      WHERE row_num = 1
      ORDER BY employee_id;
    `;
    
    return await executeQuery(query);
  }, 'get_guard_cards_from_postgres');
}

module.exports = {
  syncGuardCards
};
