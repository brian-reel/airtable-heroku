require('dotenv').config();
const { matchAndUpdateEmails, matchAndUpdatePhones } = require('./match-by-email');

(async () => {
  console.log('Starting email matching process...');
  await matchAndUpdateEmails();
  
  console.log('\nStarting phone matching process...');
  await matchAndUpdatePhones();
  
  console.log('All matching processes complete!');
})(); 