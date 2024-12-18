require('dotenv').config();
const { Pool } = require('pg');
const Airtable = require('airtable');
const fs = require('fs');

// Reuse connection setup from sync-script.js
const pool = new Pool({
  connectionString: process.env.PG_CONNECTION_STRING,
  ssl: { rejectUnauthorized: false }
});

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const tableName = process.env.AIRTABLE_TABLE_NAME || 'Employees_dev';
const pgEmailsTable = process.env.PG_TABLE_NAME_2 || 'emails'; // Default to 'emails' if not set
const pgEmployeesTable = process.env.PG_TABLE_NAME_3 || 'employees';

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

async function getEmailsFromPostgres() {
  try {
    const result = await pool.query(`
      SELECT e.emailable_id, e.address, emp.mobile_phone, emp.active 
      FROM ${pgEmailsTable} e
      LEFT JOIN ${pgEmployeesTable} emp ON e.emailable_id = emp.id
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
      FROM ${pgEmployeesTable} emp
      LEFT JOIN ${pgEmailsTable} e ON emp.id = e.emailable_id 
      AND e.emailable_type = 'Employee' 
      AND e."primary" = true
      WHERE emp.mobile_phone IS NOT NULL
    `);
    console.log(`Fetched ${result.rows.length} phone records from Postgres`);
    return result.rows;
  } catch (error) {
    console.error('Error querying Postgres phones:', error);
    throw error;
  }
}

// Modify the getAirtableRecords function to fetch both fields
async function getAirtableRecords() {
  const records = [];
  try {
    await base(tableName)
      .select({
        fields: ['RSC Emp ID', 'Email', 'Phone', 'Status-RSPG', 'Status']
      })
      .eachPage((pageRecords, fetchNextPage) => {
        records.push(...pageRecords);
        fetchNextPage();
      });
    return records;
  } catch (error) {
    console.error('Error fetching Airtable data:', error);
    throw error;
  }
}

// Helper function to determine what needs updating
function determineUpdates(airtableFields, pgRecord) {
  return {
    needsEmpId: !airtableFields['RSC Emp ID'],
    needsPhone: !airtableFields['Phone'] && pgRecord.mobile_phone,
    needsEmail: !airtableFields['Email'] && pgRecord.email,
    needsStatusRSPG: airtableFields['Status-RSPG'] !== (pgRecord.active ? 'Active' : 'Inactive'),
    needsStatus: airtableFields['Status'] !== (pgRecord.active ? 'Hired' : 'Separated')
  };
}

// Update the matchAndUpdateEmails function
async function matchAndUpdateEmails() {
  // Reference existing setup code
  try {
    // Clear previous files
    console.log('Clearing previous data files...');
    ['email_match_report.json', 'pg_emails_sample.json', 'airtable_records_sample.json']
      .forEach(file => {
        if (fs.existsSync(file)) fs.unlinkSync(file);
      });

    console.log('Fetching emails from Postgres...');
    const pgEmails = await getEmailsFromPostgres();
    
    console.log('Fetching records from Airtable...');
    const airtableRecords = await getAirtableRecords();

    const matches = [];
    const updates = [];
    let blankEmpIdCount = 0;
    let emailMatchCount = 0;

    // Create lookup object for PG emails
    const pgEmailMap = pgEmails.reduce((acc, row) => {
      acc[row.address.toLowerCase()] = row.emailable_id;
      return acc;
    }, {});

    // Modified match processing
    for (const record of airtableRecords) {
      const airtableEmail = record.fields['Email']?.toLowerCase();
      
      if (airtableEmail && pgEmailMap[airtableEmail]) {
        const pgRecord = pgEmails.find(r => r.address.toLowerCase() === airtableEmail);
        const updates = determineUpdates(record.fields, pgRecord);
        
        // Only process if any updates are needed
        if (Object.values(updates).some(Boolean)) {
          emailMatchCount++;
          matches.push({
            airtableId: record.id,
            airtableEmail: airtableEmail,
            pgEmployeeId: pgRecord.emailable_id,
            pgPhone: pgRecord.mobile_phone,
            currentFields: record.fields,
            updates,
            isActive: pgRecord.active
          });
        }
      }
    }

    // Log statistics
    console.log('\nMatching Statistics:');
    console.log(`Total Airtable records: ${airtableRecords.length}`);
    console.log(`Records with blank RSC Emp ID: ${blankEmpIdCount}`);
    console.log(`Email matches found: ${emailMatchCount}`);

    writeEmailMatchReport(matches);

    // Perform updates with additional phone field
    console.log('\nProceeding with updates...');
    let successCount = 0;
    const errors = [];

    for (const match of matches) {
      try {
        const updateFields = {};
        if (match.updates.needsEmpId) {
          updateFields['RSC Emp ID'] = match.pgEmployeeId.toString();
        }
        if (match.updates.needsPhone) {
          updateFields['Phone'] = match.pgPhone;
        }
        if (match.updates.needsStatusRSPG) {
          updateFields['Status-RSPG'] = match.isActive ? 'Active' : 'Inactive';
        }
        if (match.updates.needsStatus) {
          updateFields['Status'] = match.isActive ? 'Hired' : 'Separated';
        }

        // Only perform update if there are fields to update
        if (Object.keys(updateFields).length > 0) {
          await base(tableName).update(match.airtableId, updateFields);
        }
        successCount++;
        updates.push({
          success: true,
          email: match.airtableEmail,
          pgEmployeeId: match.pgEmployeeId
        });
      } catch (error) {
        console.error(`Failed to update record for email ${match.airtableEmail}:`, error);
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
      fs.writeFileSync('email_sync_errors.json', JSON.stringify(errors, null, 2));
      console.log('Error details saved to email_sync_errors.json');
    }

  } catch (error) {
    console.error('Error in email matching process:', error);
    throw error;
  }
}

// Helper function to standardize phone number format
function standardizePhoneNumber(phone) {
  if (!phone) return null;
  // Remove all non-numeric characters
  return phone.replace(/\D/g, '').slice(-10);
}

// Update the matchAndUpdatePhones function similarly
async function matchAndUpdatePhones() {
  try {
    console.log('Fetching phone numbers from Postgres...');
    const pgPhones = await getPhoneNumbersFromPostgres();
    
    console.log('Fetching records from Airtable...');
    const airtableRecords = await base(tableName)
      .select({
        fields: ['RSC Emp ID', 'Phone', 'Email', 'Status-RSPG', 'Status']
      })
      .all();

    const matches = [];
    let phoneMatchCount = 0;

    // Create lookup object for PG phones
    const pgPhoneMap = pgPhones.reduce((acc, row) => {
      const standardizedPhone = standardizePhoneNumber(row.mobile_phone);
      if (standardizedPhone) {
        acc[standardizedPhone] = row.id;
      }
      return acc;
    }, {});

    // Modified match processing
    for (const record of airtableRecords) {
      const airtablePhone = standardizePhoneNumber(record.fields['Phone']);
      
      if (airtablePhone && pgPhoneMap[airtablePhone]) {
        const pgRecord = pgPhones.find(r => standardizePhoneNumber(r.mobile_phone) === airtablePhone);
        const updates = determineUpdates(record.fields, pgRecord);
        
        // Only process if any updates are needed
        if (Object.values(updates).some(Boolean)) {
          phoneMatchCount++;
          matches.push({
            airtableId: record.id,
            airtablePhone: record.fields['Phone'],
            pgEmployeeId: pgRecord.id,
            pgEmail: pgRecord.email,
            currentFields: record.fields,
            updates,
            isActive: pgRecord.active
          });
        }
      }
    }

    // Log statistics
    console.log('\nPhone Matching Statistics:');
    console.log(`Total phone matches found: ${phoneMatchCount}`);
    console.log(`Records needing updates: ${matches.length}`);

    // Perform updates
    console.log('\nProceeding with phone match updates...');
    let successCount = 0;
    const errors = [];

    for (const match of matches) {
      try {
        const updateFields = {};
        
        if (match.updates.needsEmpId) {
          updateFields['RSC Emp ID'] = match.pgEmployeeId.toString();
        }
        if (match.updates.needsEmail) {
          updateFields['Email'] = match.pgEmail;
        }
        if (match.updates.needsStatusRSPG) {
          updateFields['Status-RSPG'] = match.isActive ? 'Active' : 'Inactive';
        }
        if (match.updates.needsStatus) {
          updateFields['Status'] = match.isActive ? 'Hired' : 'Separated';
        }

        // Only perform update if there are fields to update
        if (Object.keys(updateFields).length > 0) {
          await base(tableName).update(match.airtableId, updateFields);
          successCount++;
          console.log(`Updated record ${match.airtableId} with fields:`, updateFields);
        }
      } catch (error) {
        console.error(`Failed to update record for phone ${match.airtablePhone}:`, error);
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
      fs.writeFileSync('phone_sync_errors.json', JSON.stringify(errors, null, 2));
      console.log('Phone error details saved to phone_sync_errors.json');
    }

  } catch (error) {
    console.error('Error in phone matching process:', error);
    throw error;
  }
}

module.exports = { matchAndUpdateEmails, matchAndUpdatePhones }; 