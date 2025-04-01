const { getRankedEmployees, getEmailsFromPostgres, getPhoneNumbersFromPostgres } = require('../services/postgres');
const { getAirtableRecords, updateAirtableRecord, createAirtableRecord } = require('../services/airtable');
const { clearReportFiles, writeJsonReport, logSyncStart, logSyncEnd, logToHistory } = require('../utils/logging');
const { standardizePhoneNumber, formatDate } = require('../utils/formatters');
const { validateEmployee, validateUpdateFields } = require('../utils/validation');
const { handleError, tryCatch } = require('../utils/error-handler');
const config = require('../config/config');
const { AIRTABLE_TABLE } = require('../config/database');

/**
 * Check if a record already exists in Airtable
 * @param {Object} newEmployee - Employee data to check
 * @param {Array} airtableRecords - Existing Airtable records
 * @returns {Object|null} - Matching record or null
 */
function findExistingRecord(newEmployee, airtableRecords) {
  let match = null;
  let matchReason = '';

  // Try to match by RSC Emp ID first
  if (newEmployee.employee_id) {
    match = airtableRecords.find(record => 
      record.fields['RSC Emp ID'] === newEmployee.employee_id.toString()
    );
    if (match) {
      matchReason = 'RSC Emp ID';
      console.log(`Found existing record for ${newEmployee.name} by ${matchReason} (${newEmployee.employee_id})`);
      return { match, matchReason };
    }
  }

  // Try to match by email (case insensitive)
  if (newEmployee.email) {
    match = airtableRecords.find(record => 
      record.fields['Email']?.toLowerCase() === newEmployee.email.toLowerCase()
    );
    if (match) {
      matchReason = 'Email';
      console.log(`Found existing record for ${newEmployee.name} by ${matchReason} (${newEmployee.email})`);
      return { match, matchReason };
    }
  }

  // Try to match by phone (standardized format)
  if (newEmployee.mobile_phone) {
    const standardizedNewPhone = standardizePhoneNumber(newEmployee.mobile_phone);
    match = airtableRecords.find(record => {
      const standardizedExistingPhone = standardizePhoneNumber(record.fields['Phone']);
      return standardizedNewPhone === standardizedExistingPhone;
    });
    if (match) {
      matchReason = 'Phone';
      console.log(`Found existing record for ${newEmployee.name} by ${matchReason} (${newEmployee.mobile_phone})`);
      return { match, matchReason };
    }
  }

  // Try to match by name as a last resort (case insensitive)
  if (newEmployee.name) {
    match = airtableRecords.find(record => 
      record.fields['Name']?.toLowerCase() === newEmployee.name.toLowerCase()
    );
    if (match) {
      matchReason = 'Name';
      console.log(`Found existing record for ${newEmployee.name} by ${matchReason}`);
      return { match, matchReason };
    }
  }

  return { match: null, matchReason: null };
}

/**
 * Main function to sync all employee data
 */
async function syncAllEmployeeData() {
  return tryCatch(async () => {
    await logToHistory('Starting comprehensive employee data sync...');
    console.log('Starting comprehensive employee data sync...');
    
    // Clear previous report files
    clearReportFiles([
      'employee_data.json',
      'sync_errors.json'
    ]);
    
    // 1. Sync basic employee data
    await syncEmployeeData();
    
    // 2. Add new employees
    await addNewEmployees();
    
    // 3. Match and update contact info
    await matchAndUpdateContactInfo();
    
    await logToHistory('Employee data sync completed successfully');
    console.log('Employee data sync completed successfully!');
    
    return true;
  }, 'sync_all_employee_data');
}

/**
 * Sync employee data from PostgreSQL to Airtable
 */
async function syncEmployeeData() {
  return tryCatch(async () => {
    logSyncStart('employee data sync');
    
    // Verify Airtable configuration
    console.log('\n=== AIRTABLE CONFIGURATION ===');
    console.log('Base ID:', config.airtable.baseId);
    console.log('Table:', AIRTABLE_TABLE);
    console.log('API Key (first 6 chars):', config.airtable.apiKey.substring(0, 6) + '...');
    
    // Test write permission
    try {
      const testRecord = await getAirtableRecords(AIRTABLE_TABLE, ['RSC Emp ID'])[0];
      if (testRecord) {
        await updateAirtableRecord(AIRTABLE_TABLE, testRecord.id, {
          'Last Sync Attempt': new Date().toISOString()
        });
        console.log('✅ Airtable write permission confirmed');
      }
    } catch (error) {
      console.error('❌ Airtable write permission test failed:', error.message);
      throw new Error('Airtable write permission test failed. Please check API key permissions.');
    }
    
    // 1. Get active employees from PG
    const pgEmployees = await getRankedEmployees({ active: true });
    
    // 2. Get all Airtable records
    const airtableRecords = await getAirtableRecords(AIRTABLE_TABLE, [
      'RSC Emp ID', 'Email', 'Phone', 'Status-RSPG', 'Status', 'Name', 'RSC Hire Date'
    ]);
    
    // First, add this helper function at the top of the file
    function determinePreferredRecord(records) {
      // First, try to find the most recent active record
      const activeRecords = records.filter(r => r.active);
      if (activeRecords.length > 0) {
        // Sort active records by recency
        return activeRecords.sort((a, b) => {
          const aDate = new Date(a.updated_at || a.created_at);
          const bDate = new Date(b.updated_at || b.created_at);
          return bDate - aDate;
        })[0];
      }

      // If no active records, get the most recent inactive record
      const inactiveRecords = records.sort((a, b) => {
        const aDate = new Date(a.updated_at || a.created_at);
        const bDate = new Date(b.updated_at || b.created_at);
        return bDate - aDate;
      });

      console.log(`No active records found for employee, using most recent inactive record from ${inactiveRecords[0].updated_at || inactiveRecords[0].created_at}`);
      return inactiveRecords[0];
    }

    // Then modify the lookup map creation in syncEmployeeData
    // 3. Create lookup maps
    const pgEmployeeMap = {};
    const duplicateMap = {};

    // First pass: Group employees by identifiers
    for (const emp of pgEmployees) {
      const validation = validateEmployee(emp);
      if (!validation.isValid) {
        console.warn(`Skipping invalid employee data for ${emp.name || emp.employee_id}:`, validation.errors);
        continue;
      }

      // Group by each identifier
      const identifiers = [
        emp.employee_id.toString(),
        emp.email?.toLowerCase(),
        emp.mobile_phone ? standardizePhoneNumber(emp.mobile_phone) : null,
        emp.name?.toLowerCase()
      ].filter(Boolean);

      identifiers.forEach(id => {
        if (!duplicateMap[id]) {
          duplicateMap[id] = [];
        }
        duplicateMap[id].push(emp);
      });
    }

    // Second pass: Determine preferred record for each identifier
    for (const [identifier, records] of Object.entries(duplicateMap)) {
      if (records.length > 0) {
        const preferredRecord = determinePreferredRecord(records);
        pgEmployeeMap[identifier] = preferredRecord;
        
        if (records.length > 1) {
          console.log(`Found ${records.length} records for identifier ${identifier}, using most recent ${preferredRecord.active ? 'active' : 'inactive'} record`);
        }
      }
    }

    // 4. Find matches and determine updates
    const updates = [];
    for (const record of airtableRecords) {
      let pgMatch = null;
      let matchSource = '';
      
      // Try to match by RSC Emp ID first
      if (record.fields['RSC Emp ID']) {
        pgMatch = pgEmployeeMap[record.fields['RSC Emp ID']];
        matchSource = 'RSC Emp ID';
      }
      
      // If no match, try email
      if (!pgMatch && record.fields['Email']) {
        pgMatch = pgEmployeeMap[record.fields['Email'].toLowerCase()];
        matchSource = 'Email';
      }
      
      // If still no match, try phone
      if (!pgMatch && record.fields['Phone']) {
        const standardizedPhone = standardizePhoneNumber(record.fields['Phone']);
        pgMatch = pgEmployeeMap[standardizedPhone];
        matchSource = 'Phone';
      }
      
      if (pgMatch) {
        const updateFields = {};
        
        // Log the match details for debugging
        console.log(`\nProcessing match for ${record.fields['Name']}:`, {
          matchSource,
          pgMatchActive: pgMatch.active,
          pgMatchUpdatedAt: pgMatch.updated_at,
          pgMatchCreatedAt: pgMatch.created_at,
          currentStatus: record.fields['Status'],
          currentStatusRSPG: record.fields['Status-RSPG']
        });

        // Only update non-status fields if the PG record is active
        // or if the Airtable record has no data
        const shouldUpdateFields = pgMatch.active || 
          (!record.fields['RSC Emp ID'] && !record.fields['Email'] && !record.fields['Phone']);

        // Always update status fields based on the matched record's active status
        const shouldBeActive = pgMatch.active;
        const currentStatusRSPG = record.fields['Status-RSPG'];
        const currentStatus = record.fields['Status'];

        if (shouldBeActive) {
          if (currentStatusRSPG !== config.mappings.statusMapping.active.statusRSPG) {
            updateFields['Status-RSPG'] = config.mappings.statusMapping.active.statusRSPG;
          }
          if (currentStatus !== config.mappings.statusMapping.active.status) {
            updateFields['Status'] = config.mappings.statusMapping.active.status;
          }
        } else {
          if (currentStatusRSPG !== config.mappings.statusMapping.inactive.statusRSPG) {
            updateFields['Status-RSPG'] = config.mappings.statusMapping.inactive.statusRSPG;
          }
          if (currentStatus !== config.mappings.statusMapping.inactive.status) {
            updateFields['Status'] = config.mappings.statusMapping.inactive.status;
          }
        }

        // Only update other fields if we should update fields
        if (shouldUpdateFields) {
          // Compare and add fields that need updating
          if (record.fields['Name'] !== pgMatch.name) {
            updateFields['Name'] = pgMatch.name;
          }
          
          if (pgMatch.hire_date) {
            const pgDate = formatDate(pgMatch.hire_date);
            const airtableDate = record.fields['RSC Hire Date'];
            
            const normalizedPgDate = new Date(pgDate).toISOString().split('T')[0];
            const normalizedAirtableDate = airtableDate ? new Date(airtableDate).toISOString().split('T')[0] : null;
            
            if (!airtableDate || normalizedPgDate !== normalizedAirtableDate) {
              updateFields['RSC Hire Date'] = normalizedPgDate;
            }
          }

          // Add detailed logging about the update decision
          console.log(`Update decision for ${record.fields['Name']}:`, {
            shouldUpdateFields,
            fieldsToUpdate: Object.keys(updateFields),
            reason: pgMatch.active ? 'Active record' : 'Blank Airtable record'
          });
        } else {
          console.log(`Skipping non-status updates for ${record.fields['Name']} - inactive PG record`);
        }

        if (Object.keys(updateFields).length > 0) {
          // Validate update fields
          const validation = validateUpdateFields(updateFields);
          if (validation.isValid) {
            updates.push({
              recordId: record.id,
              fields: updateFields,
              reason: `Matched by ${matchSource}, Active: ${pgMatch.active}`
            });
          } else {
            console.warn(`Skipping invalid update for ${record.id}:`, validation.errors);
          }
        }
      }
    }
    
    // 5. Perform updates with rate limiting
    console.log(`Found ${updates.length} records that need updating`);
    let successCount = 0;
    const errors = [];
    
    for (const update of updates) {
      try {
        await updateAirtableRecord(AIRTABLE_TABLE, update.recordId, update.fields);
        successCount++;
        console.log(`Updated record ${update.recordId}`);
      } catch (error) {
        handleError(error, 'employee_data_update', { recordId: update.recordId, fields: update.fields });
        errors.push({
          recordId: update.recordId,
          error: error.message
        });
      }
    }
    
    // 6. Write report and log summary
    if (errors.length > 0) {
      writeJsonReport('sync_errors.json', errors, 'employee_data');
    }
    
    logSyncEnd('employee data sync', {
      'Total records processed': airtableRecords.length,
      'Updates needed': updates.length,
      'Successful updates': successCount,
      'Failed updates': errors.length
    });
    
    return { successCount, errorCount: errors.length };
  }, 'sync_employee_data');
}

/**
 * Add new employees from PostgreSQL to Airtable
 */
async function addNewEmployees() {
  return tryCatch(async () => {
    logSyncStart('adding new employees');
    
    // 1. Get active employees from PG
    const result = await getRankedEmployees({ active: true });
    
    // 2. Get existing Airtable records
    const airtableRecords = await getAirtableRecords(AIRTABLE_TABLE, ['RSC Emp ID']);
    
    // 3. Create set of existing RSC Emp IDs in Airtable
    const existingEmpIds = new Set(
      airtableRecords
        .map(record => record.fields['RSC Emp ID'])
        .filter(Boolean)
    );
    
    // 4. Filter for new employees
    const newEmployees = result.filter(emp => !existingEmpIds.has(emp.employee_id.toString()));
    
    console.log(`Found ${newEmployees.length} new active employees to add`);
    
    // 5. Add new employees to Airtable
    let successCount = 0;
    const errors = [];
    
    for (const emp of newEmployees) {
      try {
        // Validate employee data
        const validation = validateEmployee(emp);
        if (!validation.isValid) {
          console.warn(`Skipping invalid employee data for ${emp.name || emp.employee_id}:`, validation.errors);
          continue;
        }
        
        const formattedHireDate = formatDate(emp.hire_date) || '';
        
        const newRecord = {
          'Name': emp.name,
          'RSC Emp ID': emp.employee_id.toString(),
          'Region': config.mappings.tenantStates[emp.tenant_id.toString()] || 'Unknown',
          'Status-RSPG': emp.active ? config.mappings.statusMapping.active.statusRSPG : config.mappings.statusMapping.inactive.statusRSPG,
          'Status': emp.active ? config.mappings.statusMapping.active.status : config.mappings.statusMapping.inactive.status,
          'Email': emp.email || '',
          'Phone': emp.mobile_phone || '',
          'RSC Hire Date': formattedHireDate
        };
        
        await createAirtableRecord(AIRTABLE_TABLE, newRecord);
        successCount++;
        console.log(`Added new employee: ${emp.name} (${emp.employee_id})`);
      } catch (error) {
        handleError(error, 'add_new_employee', { employeeId: emp.employee_id, name: emp.name });
        errors.push({
          employeeId: emp.employee_id,
          name: emp.name,
          error: error.message
        });
      }
    }
    
    // 6. Write report and log summary
    if (errors.length > 0) {
      writeJsonReport('new_employee_errors.json', errors, 'employee_data');
    }
    
    logSyncEnd('adding new employees', {
      'New employees found': newEmployees.length,
      'Successfully added': successCount,
      'Failed to add': errors.length
    });
    
    return successCount > 0;
  }, 'add_new_employees');
}

/**
 * Match and update contact information (emails and phones)
 */
async function matchAndUpdateContactInfo() {
  return tryCatch(async () => {
    logSyncStart('contact info sync');
    
    // Clear previous files
    clearReportFiles([
      'email_match_report.json',
      'phone_match_report.json',
      'email_sync_errors.json',
      'phone_sync_errors.json'
    ], 'contact_info');
    
    // 1. Match and update emails
    const emailResults = await matchAndUpdateEmails();
    
    // 2. Match and update phones
    const phoneResults = await matchAndUpdatePhones();
    
    logSyncEnd('contact info sync', {
      'Email matches found': emailResults.matchCount,
      'Email updates': emailResults.updateCount,
      'Phone matches found': phoneResults.matchCount,
      'Phone updates': phoneResults.updateCount
    });
    
    return {
      emailResults,
      phoneResults
    };
  }, 'match_and_update_contact_info');
}

/**
 * Match and update email information
 */
async function matchAndUpdateEmails() {
  // More organized version of the email sync logic
}

/**
 * Match and update phone information
 */
async function matchAndUpdatePhones() {
  // More organized version of the phone sync logic
}

module.exports = {
  syncAllEmployeeData,
  syncEmployeeData,
  addNewEmployees,
  matchAndUpdateContactInfo,
  matchAndUpdateEmails,
  matchAndUpdatePhones
}; 