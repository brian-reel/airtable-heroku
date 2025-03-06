require('dotenv').config();
const { pool, base, AIRTABLE_TABLE, PG_LICENSES_TABLE } = require('../config/database');
const { clearReportFiles, writeJsonReport } = require('../utils/logging');
const fs = require('fs');


// Fetch employees from PostgreSQL
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

// Fetch employees from Airtable
async function getEmployeesFromAirtable() {
  const records = [];
  try {
    await base(AIRTABLE_TABLE)
      .select({
        fields: ['RSC Emp ID', 'GC - RSPG', 'GC Exp Date - RSPG', 'license type id - RSPG']
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
              const [year, month, day] = record.fields['GC Exp Date - RSPG'].split('-');
              return `${month}/${day}/${year}`; // Rearrange to MM/DD/YYYY
            })()
          : '',
        'license type id - RSPG': record.fields['license type id - RSPG'] || ''
      }
    }));
  } catch (error) {
    console.error('Error fetching Airtable data:', error);
    throw error;
  }
}

// Update an Airtable record
async function updateAirtableRecord(recordId, updatedFields) {
  try {
    const updatedRecord = await base(AIRTABLE_TABLE).update(recordId, updatedFields);
    return updatedRecord;
  } catch (error) {
    console.error(`Error updating Airtable record ${recordId}:`, error);
    throw error;
  }
}

// Sync data from PostgreSQL to Airtable

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
            : ''; // Return empty string if expires_on is null
          
          const currentGuardCard = matchingAirtableRecord.fields['GC - RSPG'] || '';
          // Get the Airtable date and ensure it's in MM/DD/YYYY format
          const currentExpDate = matchingAirtableRecord.fields['GC Exp Date - RSPG'] 
            ? matchingAirtableRecord.fields['GC Exp Date - RSPG'].replace(/-/g, '/') // Replace any hyphens with slashes
            : '';
          const currentLicenseType = matchingAirtableRecord.fields['license type id - RSPG'] || '';

          const needsUpdate = 
            // Update if PG has data and Airtable is blank
            (pgEmployee.number && !currentGuardCard) ||
            (pgEmployee.expires_on && !currentExpDate) ||
            (pgEmployee.license_type_id && !currentLicenseType) ||
            // Or if both have data but it's different
            (pgEmployee.number && currentGuardCard && currentGuardCard.trim() !== pgEmployee.number.trim()) ||
            (pgEmployee.expires_on && currentExpDate && currentExpDate.trim() !== formattedExpDate.trim()) ||
            (pgEmployee.license_type_id && currentLicenseType && currentLicenseType !== pgEmployee.license_type_id);

          if (needsUpdate) {
            return {
              postgres: pgEmployee,
              airtable: matchingAirtableRecord.fields,
              needsUpdate,
              updateFields: {
                'GC - RSPG': pgEmployee.number || '',
                ...(pgEmployee.expires_on === null 
                  ? { 'GC Exp Date - RSPG': null }  // Set to null if PG is null
                  : { 'GC Exp Date - RSPG': formattedExpDate }), // Otherwise use formatted date
                'license type id - RSPG': pgEmployee.license_type_id
              },
              currentValues: {
                guardCard: currentGuardCard || 'BLANK',
                expDate: currentExpDate || 'BLANK',
                licenseType: currentLicenseType || 'BLANK'
              }
            };
          }

          // Always return the match, but include whether it needs updating
          return {
            postgres: pgEmployee,
            airtable: matchingAirtableRecord.fields,
            needsUpdate,
            updateFields: {
              'GC - RSPG': pgEmployee.number || '',
              ...(pgEmployee.expires_on === null 
                ? { 'GC Exp Date - RSPG': null }  // Set to null if PG is null
                : { 'GC Exp Date - RSPG': formattedExpDate }), // Otherwise use formatted date
              'license type id - RSPG': pgEmployee.license_type_id
            },
            currentValues: {
              guardCard: currentGuardCard || 'BLANK',
              expDate: currentExpDate || 'BLANK',
              licenseType: currentLicenseType || 'BLANK'
            }
          };
        }
        return null; // Return null if no matching Airtable record found
      })
      .filter(match => match !== null); // Remove any non-matches
  
      // Log the total matches and how many need updates
      const recordsNeedingUpdate = matchedData.filter(match => match.needsUpdate);
      console.log(`Found ${matchedData.length} total matches`);
      console.log(`Found ${recordsNeedingUpdate.length} records that need updating`);
  
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

      for (const match of recordsNeedingUpdate) {
        try {
          const recordId = airtableById[match.postgres.employee_id].id;
          await updateAirtableRecord(recordId, match.updateFields);
          console.log(`Successfully updated Airtable record ${recordId}`);
          successCount++;
        } catch (error) {
          console.error(`Failed to update record for employee ${match.postgres.employee_id}:`, {
            error: error.message,
            statusCode: error.statusCode,
            recordDetails: match
          });
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
      console.error('Fatal error during sync:', error);
      throw error;
    }
}

// Add this function near the top with other file operations
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
      }
    }
  }));

  fs.writeFileSync('update_report.json', JSON.stringify(report, null, 2));
  console.log('Update report saved to update_report.json');
}

// This is likely in the formatDate function or where dates are being processed
function formatDate(date) {
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
    console.error('Error formatting date:', error);
    return '';
  }
}

// ... or it might be in the comparison function
function needsUpdate(airtableRecord, pgData) {
  // ... existing code ...
  
  // ... rest of function
}

module.exports = { syncPostgresToAirtable };
