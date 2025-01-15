require('dotenv').config();
const { base, AIRTABLE_TABLE } = require('../config/database');
const { clearReportFiles, writeJsonReport } = require('../utils/logging');

// Add rate limiting and retry logic
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

        // Log current state for debugging
        console.log(`Processing record ${recordId}:`, {
          currentValue: record.fields[POTENTIAL_DUPLICATE_FIELD],
          name: record.fields['Name']
        });

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
          console.log(`Updated record ${recordId} to mark as duplicate`);
        } else {
          console.log(`Record ${recordId} already marked as duplicate`);
        }
      } catch (error) {
        console.error(`Failed to update record ${recordId}:`, error);
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
    console.error('Error in duplicate detection:', error);
    throw error;
  }
}

// Add this at the bottom of the file
if (require.main === module) {
  (async () => {
    try {
      await flagDuplicateRecords();
      console.log('Duplicate detection complete!');
    } catch (error) {
      console.error('Error running duplicate detection:', error);
      process.exit(1);
    }
  })();
}

module.exports = { flagDuplicateRecords }; 