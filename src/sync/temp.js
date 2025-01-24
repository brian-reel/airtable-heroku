require('dotenv').config();
const axios = require('axios');
const { base } = require('../config/database');

const AIRTABLE_LMS_TABLE = 'LMS DATA';
const LMS_API_URL = process.env.LMS_API_URL_USERS;
const LMS_API_KEY = process.env.LMS_API_KEY;

async function fetchLMSUsers() {
  try {
    const auth = Buffer.from(`${LMS_API_KEY}:`).toString('base64');
    const response = await axios.get(LMS_API_URL, {
      headers: { 'Authorization': `Basic ${auth}` }
    });
    return response.data;
  } catch (error) {
    console.error('Error fetching LMS users:', error);
    throw error;
  }
}

async function deleteUser(userId) {
  try {
    const auth = Buffer.from(`${LMS_API_KEY}:`).toString('base64');
    const baseUrl = LMS_API_URL.split('/v1')[0];
    
    // Format parameters as URL-encoded form data
    const params = new URLSearchParams();
    params.append('user_id', userId);
    params.append('permanent', 'yes');
    
    const response = await axios.post(`${baseUrl}/v1/deleteuser`, params, {
      headers: { 
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });
    return response.data;
  } catch (error) {
    if (error.response?.data) {
      console.error(`Error deleting user ${userId}:`, error.response.data);
    } else {
      console.error(`Error deleting user ${userId}:`, error.message);
    }
    return null;
  }
}

async function deleteInactiveUsers() {
  try {
    console.log('Starting LMS user deletion process...');

    // Step 1: Get all LMS users to create email-to-id mapping
    console.log('Fetching current LMS users...');
    const lmsUsers = await fetchLMSUsers();
    const lmsUserMap = lmsUsers.reduce((acc, user) => {
      if (user.email) {
        acc[user.email.toLowerCase()] = user.id;
      }
      return acc;
    }, {});
    console.log(`Found ${Object.keys(lmsUserMap).length} LMS users`);

    // Step 2: Get records marked for deletion from Airtable
    console.log('\nFetching Airtable records marked for deletion...');
    const records = await base(AIRTABLE_LMS_TABLE)
      .select({
        fields: ['Email', 'Add or Delete'],
        filterByFormula: "{Add or Delete} = 'Delete'"
      })
      .all();
    console.log(`Found ${records.length} records marked for deletion`);

    // Step 3: Process deletions
    console.log('\nProcessing deletions...');
    let successCount = 0;
    let failureCount = 0;
    let notFoundCount = 0;

    for (const record of records) {
      const email = record.fields['Email']?.toLowerCase();
      const userId = lmsUserMap[email];

      if (!email) {
        console.log(`Skipping record ${record.id} - no email found`);
        continue;
      }

      if (!userId) {
        console.log(`User not found in LMS for email: ${email}`);
        notFoundCount++;
        continue;
      }

      console.log(`Deleting user: ${email} (ID: ${userId})`);
      const result = await deleteUser(userId);
      
      if (result) {
        successCount++;
        console.log(`Successfully deleted user: ${email}`);
      } else {
        failureCount++;
        console.log(`Failed to delete user: ${email}`);
      }

      // Rate limiting
      await new Promise(resolve => setTimeout(resolve, 250));
    }

    // Final Summary
    console.log('\nDeletion Process Complete!');
    console.log('Summary:');
    console.log(`- Total records processed: ${records.length}`);
    console.log(`- Successfully deleted: ${successCount}`);
    console.log(`- Failed to delete: ${failureCount}`);
    console.log(`- Users not found in LMS: ${notFoundCount}`);

  } catch (error) {
    console.error('Error in deletion process:', error);
    process.exit(1);
  }
}

// Run the script
if (require.main === module) {
  deleteInactiveUsers();
}
