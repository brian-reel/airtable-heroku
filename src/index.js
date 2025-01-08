require('dotenv').config();
const { syncPostgresToAirtable } = require('./sync/guard-cards');
const { matchAndUpdateEmails, matchAndUpdatePhones, addNewEmployees } = require('./sync/contact-info');

(async () => {
  console.log('Starting new employee addition process...');
  await addNewEmployees();
  
  console.log('\nStarting email matching process...');
  await matchAndUpdateEmails();
  
  console.log('\nStarting phone matching process...');
  await matchAndUpdatePhones();
  
  console.log('\nStarting guard card sync process...');
  await syncPostgresToAirtable();
  
  console.log('All processes complete!');
})(); 