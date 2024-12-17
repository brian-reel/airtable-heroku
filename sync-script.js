require('dotenv').config();
const { Pool } = require('pg');
const Airtable = require('airtable');

// PostgreSQL connection setup
const pool = new Pool({
  connectionString: process.env.PG_CONNECTION_STRING,
  ssl: { rejectUnauthorized: false }, // Required for Heroku-managed databases
});

// Airtable connection setup
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Airtable table name, configurable through .env
const tableName = process.env.AIRTABLE_TABLE_NAME || 'Employees_dev'; // Default to 'Employees' if not set

// Add this near the other configuration constants
const pgTableName = process.env.PG_TABLE_NAME || 'employee_licenses'; // Default to 'employees' if not set

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
        FROM ${pgTableName}
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
    await base(tableName)
      .select({
        fields: ['RSC Emp ID', 'Guard Card from Scheduling', 'Guard Card Exp Date - RSPG', 'license_type_id-sche']
      })
      .eachPage((pageRecords, fetchNextPage) => {
        records.push(...pageRecords);
        fetchNextPage();
      });
    return records.map((record) => ({
      id: record.id,
      fields: {
        'RSC Emp ID': record.fields['RSC Emp ID'] || '',
        'Guard Card from Scheduling': record.fields['Guard Card from Scheduling'] || '',
        'Guard Card Exp Date - RSPG': record.fields['Guard Card Exp Date - RSPG'] 
          ? (() => {
              const [year, month, day] = record.fields['Guard Card Exp Date - RSPG'].split('-');
              return `${month}/${day}/${year}`; // Rearrange to MM/DD/YYYY
            })()
          : '',
        'license_type_id-sche': record.fields['license_type_id-sche'] || ''
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
    const updatedRecord = await base(tableName).update(recordId, updatedFields);
    return updatedRecord;
  } catch (error) {
    console.error(`Error updating Airtable record ${recordId}:`, error);
    throw error;
  }
}

// Sync data from PostgreSQL to Airtable
const fs = require('fs'); // For writing data to a file

async function syncPostgresToAirtable() {
    try {
      // Clear out any existing data files at the start
      console.log('Clearing previous data files...');
      if (fs.existsSync('emp_data.json')) fs.unlinkSync('emp_data.json');
      if (fs.existsSync('matched_data.json')) fs.unlinkSync('matched_data.json');
      if (fs.existsSync('sync_errors.json')) fs.unlinkSync('sync_errors.json');
      
      // Create empty files
      fs.writeFileSync('matched_data.json', JSON.stringify([], null, 2));
      fs.writeFileSync('sync_errors.json', JSON.stringify([], null, 2));
      
      console.log('Fetching employees from Postgres...');
      const postgresEmployees = await getEmployeesFromPostgres();
      console.log(`Fetched ${postgresEmployees.length} employees from Postgres.`);
  
      console.log('Fetching employees from Airtable...');
      const airtableEmployees = await getEmployeesFromAirtable();
      console.log(`Fetched ${airtableEmployees.length} employees from Airtable.`);
  
      // Save first 100 records from each source to file for debugging
      fs.writeFileSync('emp_data.json', JSON.stringify({
        postgres: postgresEmployees.slice(0, 100),
        airtable: airtableEmployees.slice(0, 100).map(record => ({
          id: record.id,
          fields: record.fields,
          empId: record.fields['RSC Emp ID']
        }))
      }, null, 2));
      console.log('First 100 records from each source saved to emp_data.json');
  
      console.log('Matching employees...');
      const airtableById = airtableEmployees.reduce((acc, record) => {
        const empId = record.fields['RSC Emp ID'];
        if (empId) acc[empId] = record;
        return acc;
      }, {});
  
      const matchedData = postgresEmployees
        // First, filter to only get guard card records (type 1)
        .filter(pgEmployee => pgEmployee.license_type_id === "1")
        // Then map through these records to find matches in Airtable
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
            
            const currentGuardCard = matchingAirtableRecord.fields['Guard Card from Scheduling'] || '';
            // Get the Airtable date and ensure it's in MM/DD/YYYY format
            const currentExpDate = matchingAirtableRecord.fields['Guard Card Exp Date - RSPG'] 
              ? matchingAirtableRecord.fields['Guard Card Exp Date - RSPG'].replace(/-/g, '/') // Replace any hyphens with slashes
              : '';
            const currentLicenseType = matchingAirtableRecord.fields['license_type_id-sche'] || '';

            // Debug log to see the actual formats
            console.log(`Date formats for employee ${pgEmployee.employee_id}:`, {
              rawPgDate: pgEmployee.expires_on,
              formattedPgDate: formattedExpDate,
              airtableDate: currentExpDate
            });

            const needsUpdate = 
              // Update if PG has data and Airtable is blank
              (pgEmployee.number && !currentGuardCard) ||
              (pgEmployee.expires_on && !currentExpDate) ||
              (pgEmployee.license_type_id && !currentLicenseType) ||
              // Or if both have data but it's different
              (pgEmployee.number && currentGuardCard && currentGuardCard.trim() !== pgEmployee.number.trim()) ||
              (pgEmployee.expires_on && currentExpDate && currentExpDate.trim() !== formattedExpDate.trim()) ||
              (pgEmployee.license_type_id && currentLicenseType && currentLicenseType !== pgEmployee.license_type_id);

            // Add debug logging for exact comparison values
            if (currentExpDate && formattedExpDate && currentExpDate !== formattedExpDate) {
              console.log(`Date mismatch for employee ${pgEmployee.employee_id}:`, {
                currentExpDate: `"${currentExpDate}"`,
                formattedExpDate: `"${formattedExpDate}"`,
                currentLength: currentExpDate.length,
                formattedLength: formattedExpDate.length
              });
            }

            if (needsUpdate) {
              console.log(`Update needed for employee ${pgEmployee.employee_id}:`, {
                reason: {
                  blankGuardCard: !currentGuardCard && pgEmployee.number,
                  blankExpDate: !currentExpDate && pgEmployee.expires_on,
                  guardCardMismatch: currentGuardCard !== pgEmployee.number,
                  expDateMismatch: currentExpDate !== formattedExpDate,
                  licenseTypeMismatch: currentLicenseType !== pgEmployee.license_type_id
                },
                current: {
                  guardCard: currentGuardCard || 'BLANK',
                  expDate: currentExpDate || 'BLANK',
                  licenseType: currentLicenseType || 'BLANK'
                },
                new: {
                  guardCard: pgEmployee.number || 'BLANK',
                  expDate: formattedExpDate || 'BLANK',
                  licenseType: pgEmployee.license_type_id || 'BLANK'
                }
              });
            }

            // Always return the match, but include whether it needs updating
            return {
              postgres: pgEmployee,
              airtable: matchingAirtableRecord.fields,
              needsUpdate,
              updateFields: {
                'Guard Card from Scheduling': pgEmployee.number || '',
                ...(pgEmployee.expires_on === null 
                  ? { 'Guard Card Exp Date - RSPG': null }  // Set to null if PG is null
                  : { 'Guard Card Exp Date - RSPG': formattedExpDate }), // Otherwise use formatted date
                'license_type_id-sche': pgEmployee.license_type_id
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
        fs.writeFileSync('sync_errors.json', JSON.stringify(errors, null, 2));
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
        needsUpdate: record.currentValues.expDate !== (record.updateFields['Guard Card Exp Date - RSPG'] || 'BLANK'),
        current: record.currentValues.expDate,
        new: record.updateFields['Guard Card Exp Date - RSPG'] || 'BLANK'
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

module.exports = { syncPostgresToAirtable };
