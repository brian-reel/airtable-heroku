const { base } = require('../config/database');
const config = require('../config/config');
const { validateUpdateFields } = require('../utils/validation');
const { AirtableError, tryCatch } = require('../utils/error-handler');

/**
 * getAirtableRecords
 * @param {any} tableName, fields = ['RSC Emp ID', 'Email', 'Phone', 'Status-RSPG', 'Status'], options = {} - Description of parameters
 * @returns {Promise<any>} - Description of return value
 */
function getAirtableRecords(tableName, fields = ['RSC Emp ID', 'Email', 'Phone', 'Status-RSPG', 'Status'], options = {}) {
  return tryCatch(async () => {
    const records = [];
    const selectOptions = { fields, ...options };
    
    await base(tableName)
      .select(selectOptions)
      .eachPage((pageRecords, fetchNextPage) => {
        records.push(...pageRecords);
        fetchNextPage();
      });
    
    console.log(`Fetched ${records.length} records from Airtable table ${tableName}`);
    return records;
  }, 'airtable_get_records', { tableName, fields });
}

/**
 * Update an Airtable record
 * @param {String} tableName - Table name
 * @param {String} recordId - Record ID to update
 * @param {Object} fields - Fields to update
 * @param {Object} options - Additional options
 * @returns {Promise<Object>} - Updated record
 */
function updateAirtableRecord(tableName, recordId, fields, options = {}) {
  return tryCatch(async () => {
    console.log(`\n=== STARTING AIRTABLE UPDATE ===`);
    console.log(`Table: ${tableName}`);
    console.log(`Record ID: ${recordId}`);
    console.log('Fields to update:', JSON.stringify(fields, null, 2));
    
    try {
      // Attempt the update
      const updatedRecord = await base(tableName).update(recordId, fields);
      
      console.log('\nUpdate successful!');
      console.log('Updated fields:', JSON.stringify(updatedRecord.fields, null, 2));
      
      // Add delay for rate limiting
      const delay = config.app.rateLimiting?.airtableDelay || 1000;
      await new Promise(resolve => setTimeout(resolve, delay));
      
      return updatedRecord;
    } catch (error) {
      console.error('\nAirtable update failed:');
      console.error('Error message:', error.message);
      console.error('Error details:', error.error || error);
      throw new AirtableError(`Failed to update record ${recordId}`, {
        tableName,
        recordId,
        fields,
        originalError: error.message
      });
    } finally {
      console.log('=== END AIRTABLE UPDATE ===\n');
    }
  }, 'airtable_update_record', { tableName, recordId, fields });
}

/**
 * Create a new Airtable record
 * @param {String} tableName - Table name
 * @param {Object} fields - Fields for the new record
 * @returns {Promise<Object>} - Created record
 */
async function createAirtableRecord(tableName, fields) {
  // Validate fields
  const validation = validateUpdateFields(fields);
  if (!validation.isValid) {
    throw new AirtableError('Invalid record fields', { 
      fields, 
      validationErrors: validation.errors 
    });
  }
  
  return tryCatch(async () => {
    const newRecord = await base(tableName).create(fields);
    await new Promise(resolve => setTimeout(resolve, config.app.rateLimiting.airtableDelay)); // Rate limiting
    return newRecord;
  }, 'airtable_create_record', { tableName, fields });
}

/**
 * Delete an Airtable record
 * @param {String} tableName - Table name
 * @param {String} recordId - Record ID to delete
 * @returns {Promise<Object>} - Deleted record
 */
async function deleteAirtableRecord(tableName, recordId) {
  return tryCatch(async () => {
    const deletedRecord = await base(tableName).destroy(recordId);
    await new Promise(resolve => setTimeout(resolve, config.app.rateLimiting.airtableDelay)); // Rate limiting
    return deletedRecord;
  }, 'airtable_delete_record', { tableName, recordId });
}

module.exports = {
  getAirtableRecords,
  updateAirtableRecord,
  createAirtableRecord,
  deleteAirtableRecord
};