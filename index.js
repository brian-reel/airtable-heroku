require('dotenv').config();
const { syncPostgresToAirtable } = require('./sync-script');

(async () => {
  console.log('Starting data sync...');
  await syncPostgresToAirtable();
  console.log('Data sync complete!');
})();
