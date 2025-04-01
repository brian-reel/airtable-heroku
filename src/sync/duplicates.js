require('dotenv').config();
const { base, AIRTABLE_TABLE } = require('../config/database');
const { clearReportFiles, writeJsonReport } = require('../utils/logging');
const { getAirtableRecords, updateAirtableRecord } = require('../services/airtable');
const { standardizePhoneNumber } = require('../utils/formatters');
const { handleError, tryCatch } = require('../utils/error-handler');
const config = require('../config/config');
const { logSyncStart, logSyncEnd, logToHistory } = require('../utils/logging');

// Add rate limiting and retry logic
/**
 * Fetch Airtable Records
 * @returns {Promise<any>} - Description of return value
 */
async function fetchAirtableRecords() {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 1000;
  let retries = 0;
  
  while (retries < MAX_RETRIES) {
    try {
      const records = await base(AIRTABLE_TABLE)
        .select({
          fields: ['Name', 'Email', 'Phone', 'Potential Duplicate'],
          pageSize: 100 // Process in smaller chunks
        })
        .all();
      return records;
    } catch (error) {
      retries++;
      console.log(`Attempt ${retries} failed. Retrying in ${RETRY_DELAY}ms...`);
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
      
      if (retries === MAX_RETRIES) {
        throw new Error(`Failed to fetch records after ${MAX_RETRIES} attempts: ${error.message}`);
      }
    }
  }
}

/**
 * Flag Duplicate Records
 * @returns {Promise<any>} - Description of return value
 */
async function flagDuplicateRecords() {
  try {
    console.log('Starting duplicate detection...');
    
    // Clear previous files
    clearReportFiles([
      'duplicate_report.json',
      'duplicate_errors.json'
    ]);

    // Define the field name constant
    const POTENTIAL_DUPLICATE_FIELD = 'Potential Duplicate';

    // Fetch all records from Airtable
    console.log('Fetching records from Airtable...');
    const records = await fetchAirtableRecords();

    console.log(`Fetched ${records.length} records`);

    // Create lookup maps for each field we want to check
    const emailMap = new Map();
    const phoneMap = new Map();
    const nameMap = new Map();
    
    // Set to store IDs of duplicate records
    const duplicateIds = new Set();

    // First pass: Build lookup maps
    records.forEach(record => {
      const email = record.fields['Email']?.toLowerCase()?.trim();
      const phone = record.fields['Phone']?.trim();
      const name = record.fields['Name']?.toLowerCase()?.trim();

      if (email) {
        if (emailMap.has(email)) {
          duplicateIds.add(record.id);
          duplicateIds.add(emailMap.get(email));
        } else {
          emailMap.set(email, record.id);
        }
      }

      if (phone) {
        if (phoneMap.has(phone)) {
          duplicateIds.add(record.id);
          duplicateIds.add(phoneMap.get(phone));
        } else {
          phoneMap.set(phone, record.id);
        }
      }

      if (name) {
        if (nameMap.has(name)) {
          duplicateIds.add(record.id);
          duplicateIds.add(nameMap.get(name));
        } else {
          nameMap.set(name, record.id);
        }
      }
    });

    // Update records flagged as duplicates
    console.log(`Found ${duplicateIds.size} potential duplicate records`);
    
    const updates = [];
    const errors = [];

    for (const recordId of duplicateIds) {
      try {
        const record = records.find(r => r.id === recordId);
        if (!record) {
          throw new Error(`Record ${recordId} not found in fetched records`);
        }

        if (record.fields[POTENTIAL_DUPLICATE_FIELD] !== 'Yes') {
          await base(AIRTABLE_TABLE).update(recordId, {
            [POTENTIAL_DUPLICATE_FIELD]: 'Yes'
          });
          updates.push({
            recordId,
            name: record.fields['Name'],
            email: record.fields['Email'],
            phone: record.fields['Phone']
          });
        }
      } catch (error) {
        handleError(error, 'Failed to update record ${recordId}:');
        errors.push({
          recordId,
          error: error.message
        });
      }
    }

    // Write report
    writeJsonReport('duplicate_report.json', {
      totalRecords: records.length,
      duplicatesFound: duplicateIds.size,
      updatedRecords: updates,
      errors: errors
    });

    console.log('\nDuplicate Detection Summary:');
    console.log(`Total records processed: ${records.length}`);
    console.log(`Potential duplicates found: ${duplicateIds.size}`);
    console.log(`Records updated: ${updates.length}`);
    console.log(`Errors: ${errors.length}`);

  } catch (error) {
    handleError(error, 'Error in duplicate detection:');
    throw error;
  }
}

/**
 * Detect and flag potential duplicate records in Airtable
 */
async function detectDuplicates() {
  return tryCatch(async () => {
    await logToHistory('Starting duplicate detection...');
    console.log('Starting duplicate detection...');
    
    // Clear previous report files
    clearReportFiles([
      'duplicates.json',
      'duplicate_errors.json'
    ]);
    
    logSyncStart('duplicate detection');
    
    // 1. Get all Airtable records
    const airtableRecords = await getAirtableRecords(AIRTABLE_TABLE, [
      'RSC Emp ID', 'Email', 'Phone', 'Name', 'Status-RSPG', 'Status', 'Potential Duplicate'
    ]);
    
    // 2. Create lookup maps for email and phone
    const emailMap = {};
    const phoneMap = {};
    const employeeIdMap = {};
    
    for (const record of airtableRecords) {
      // Skip records already marked as duplicates
      if (record.fields['Potential Duplicate'] === 'Yes') {
        continue;
      }
      
      // Add to employee ID map
      if (record.fields['RSC Emp ID']) {
        const empId = record.fields['RSC Emp ID'];
        if (!employeeIdMap[empId]) {
          employeeIdMap[empId] = [];
        }
        employeeIdMap[empId].push(record);
      }
      
      // Add to email map
      if (record.fields['Email']) {
        const email = record.fields['Email'].toLowerCase();
        if (!emailMap[email]) {
          emailMap[email] = [];
        }
        emailMap[email].push(record);
      }
      
      // Add to phone map
      if (record.fields['Phone']) {
        const phone = standardizePhoneNumber(record.fields['Phone']);
        if (phone && !phoneMap[phone]) {
          phoneMap[phone] = [];
        }
        if (phone) {
          phoneMap[phone].push(record);
        }
      }
    }
    
    // 3. Find potential duplicates
    const duplicates = {
      byEmployeeId: findDuplicatesInMap(employeeIdMap),
      byEmail: findDuplicatesInMap(emailMap),
      byPhone: findDuplicatesInMap(phoneMap)
    };
    
    // 4. Combine all duplicates
    const allDuplicates = [
      ...duplicates.byEmployeeId,
      ...duplicates.byEmail,
      ...duplicates.byPhone
    ];
    
    // 5. Remove duplicates from the list (a record might be flagged multiple times)
    const uniqueDuplicateIds = new Set();
    const uniqueDuplicates = [];
    
    for (const dup of allDuplicates) {
      if (!uniqueDuplicateIds.has(dup.recordId)) {
        uniqueDuplicateIds.add(dup.recordId);
        uniqueDuplicates.push(dup);
      }
    }
    
    // 6. Flag duplicates in Airtable
    console.log(`Found ${uniqueDuplicates.length} potential duplicate records`);
    let updateSuccessCount = 0;
    const updateErrors = [];
    
    for (const duplicate of uniqueDuplicates) {
      try {
        console.log(`Attempting to flag record ${duplicate.recordId} as duplicate`);
        console.log('Record details:', {
          name: duplicate.name,
          duplicateType: duplicate.duplicateType,
          duplicateWith: duplicate.duplicateWith?.name
        });

        await updateAirtableRecord(AIRTABLE_TABLE, duplicate.recordId, {
          'Potential Duplicate': 'Yes',  // Changed from true to 'Yes' since it's a text field
          'Duplicate Notes': `Possible duplicate ${duplicate.duplicateType} match with ${duplicate.duplicateWith?.name || 'another record'}`
        });
        
        updateSuccessCount++;
        console.log(`Successfully flagged record ${duplicate.recordId} as potential duplicate`);
      } catch (error) {
        console.error(`Failed to flag record ${duplicate.recordId}:`, error.message);
        console.error('Error details:', error);
        handleError(error, 'duplicate_flag', { 
          recordId: duplicate.recordId,
          name: duplicate.name,
          duplicateType: duplicate.duplicateType
        });
        updateErrors.push({
          recordId: duplicate.recordId,
          name: duplicate.name,
          duplicateType: duplicate.duplicateType,
          error: error.message
        });
      }
    }
    
    // 7. Write reports
    writeJsonReport('duplicates.json', {
      byEmployeeId: duplicates.byEmployeeId,
      byEmail: duplicates.byEmail,
      byPhone: duplicates.byPhone,
      totalUnique: uniqueDuplicates.length
    });
    
    if (updateErrors.length > 0) {
      writeJsonReport('duplicate_errors.json', updateErrors);
    }
    
    // 8. Log summary
    logSyncEnd('duplicate detection', {
      'Total records processed': airtableRecords.length,
      'Duplicates by Employee ID': duplicates.byEmployeeId.length,
      'Duplicates by Email': duplicates.byEmail.length,
      'Duplicates by Phone': duplicates.byPhone.length,
      'Total unique duplicates': uniqueDuplicates.length,
      'Successfully flagged': updateSuccessCount,
      'Failed to flag': updateErrors.length
    });
    
    await logToHistory('Duplicate detection completed');
    console.log('Duplicate detection completed');
    
    return updateSuccessCount > 0;
  }, 'detect_duplicates');
}

/**
 * Find duplicates in a map where multiple records share the same key
 */
function findDuplicatesInMap(map) {
  const duplicates = [];
  
  for (const [key, records] of Object.entries(map)) {
    if (records.length > 1) {
      // Sort records by status (active first)
      const sortedRecords = [...records].sort((a, b) => {
        const aActive = a.fields['Status'] === 'Active' || a.fields['Status-RSPG'] === 'Active';
        const bActive = b.fields['Status'] === 'Active' || b.fields['Status-RSPG'] === 'Active';
        
        if (aActive && !bActive) return -1;
        if (!aActive && bActive) return 1;
        return 0;
      });
      
      // Skip the first record (keep the active one)
      for (let i = 1; i < sortedRecords.length; i++) {
        duplicates.push({
          recordId: sortedRecords[i].id,
          name: sortedRecords[i].fields['Name'],
          email: sortedRecords[i].fields['Email'],
          phone: sortedRecords[i].fields['Phone'],
          employeeId: sortedRecords[i].fields['RSC Emp ID'],
          status: sortedRecords[i].fields['Status'],
          statusRSPG: sortedRecords[i].fields['Status-RSPG'],
          duplicateType: key.includes('@') ? 'email' : (key.match(/^\d+$/) ? 'employeeId' : 'phone'),
          duplicateValue: key,
          duplicateWith: {
            recordId: sortedRecords[0].id,
            name: sortedRecords[0].fields['Name'],
            email: sortedRecords[0].fields['Email'],
            phone: sortedRecords[0].fields['Phone'],
            employeeId: sortedRecords[0].fields['RSC Emp ID'],
            status: sortedRecords[0].fields['Status'],
            statusRSPG: sortedRecords[0].fields['Status-RSPG']
          }
        });
      }
    }
  }
  
  return duplicates;
}

// Add this at the bottom of the file
if (require.main === module) {
  (async () => {
    try {
      await detectDuplicates();
      console.log('Duplicate detection complete!');
    } catch (error) {
      handleError(error, 'Error running duplicate detection:');
      process.exit(1);
    }
  })();
}

module.exports = { detectDuplicates }; 