require('dotenv').config();
const { syncEmployeeData } = require('./sync/contact-info');
const { syncPostgresToAirtable } = require('./sync/guard-cards');
const { syncEmployeeRoles } = require('./sync/roles');
const { flagDuplicateRecords } = require('./sync/duplicates');

(async () => {
  console.log('Starting employee data sync...');
  await syncEmployeeData();
  
  console.log('\nStarting guard card sync...');
  await syncPostgresToAirtable();
  
  console.log('\nStarting employee roles sync...');
  await syncEmployeeRoles();
  
  console.log('\nStarting duplicate detection...');
  await flagDuplicateRecords();
  
  console.log('All processes complete!');
})(); 