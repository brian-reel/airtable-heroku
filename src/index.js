require('dotenv').config();
const { syncEmployeeData } = require('./sync/contact-info');
const { syncPostgresToAirtable } = require('./sync/guard-cards');

(async () => {
  console.log('Starting employee data sync...');
  await syncEmployeeData();
  
  console.log('\nStarting guard card sync...');
  await syncPostgresToAirtable();
  
  console.log('All processes complete!');
})(); 