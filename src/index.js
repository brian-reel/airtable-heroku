require('dotenv').config();
const fs = require('fs');
const { syncEmployeeData } = require('./sync/contact-info');
const { syncPostgresToAirtable } = require('./sync/guard-cards');
const { syncEmployeeRoles } = require('./sync/roles');
const { flagDuplicateRecords } = require('./sync/duplicates');

async function logToFile(message) {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp}: ${message}\n`;
  fs.appendFileSync('sync_history.log', logMessage);
}

(async () => {
  await logToFile('Starting nightly sync process...');
  
  try {
    await logToFile('Starting employee data sync...');
    await syncEmployeeData();
    
    await logToFile('Starting guard card sync...');
    await syncPostgresToAirtable();
    
    await logToFile('Starting employee roles sync...');
    await syncEmployeeRoles();
    
    await logToFile('Starting duplicate detection...');
    await flagDuplicateRecords();
    
    await logToFile('All processes complete!');
  } catch (error) {
    await logToFile(`Error in sync process: ${error.message}`);
    throw error;
  }
})(); 