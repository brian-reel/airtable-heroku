require('dotenv').config();
const axios = require('axios');
const { base, AIRTABLE_TABLE } = require('../config/database');

const LMS_API_URL_USERS = process.env.LMS_API_URL_USERS; // e.g., https://reel.talentlms.com/api/v1/users
const LMS_API_URL_USERS_COURSES_BASE = process.env.LMS_API_URL_USERS_COURSES; // e.g., https://reel.talentlms.com/api/v1/users
const LMS_API_KEY = process.env.LMS_API_KEY; // Your API key

async function fetchLMSUsers() {
  try {
    const auth = Buffer.from(`${LMS_API_KEY}:`).toString('base64');
    const response = await axios.get(LMS_API_URL_USERS, {
      headers: {
        'Authorization': `Basic ${auth}`
      }
    });
    return response.data; // Assuming the data is an array of users
  } catch (error) {
    console.error('Error fetching LMS users:', error.message);
    throw error;
  }
}

async function fetchAirtableRecords() {
  const records = [];
  try {
    await base(AIRTABLE_TABLE)
      .select({
        fields: ['Email', 'LMS_id', 'LMS_course_data'] // Ensure to include the fields you need
      })
      .eachPage((pageRecords, fetchNextPage) => {
        records.push(...pageRecords);
        fetchNextPage();
      });
    return records;
  } catch (error) {
    console.error('Error fetching Airtable data:', error);
    throw error;
  }
}

async function fetchUserCourses(lmsUserId) {
  try {
    const auth = Buffer.from(`${LMS_API_KEY}:`).toString('base64');
    const url = `${LMS_API_URL_USERS_COURSES_BASE}${lmsUserId}`; // Construct the URL dynamically
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Basic ${auth}`
      }
    });
    return response.data.courses; // Assuming the courses are in the "courses" section of the response
  } catch (error) {
    console.error(`Error fetching courses for LMS user ID ${lmsUserId}:`, error.message);
    return null; // Return null if there's an error
  }
}

function formatCourseData(courses) {
  if (!courses || courses.length === 0) {
    return 'No courses enrolled';
  }
  
  return courses.map(course => {
    // Safely convert timestamps to dates
    let enrolledDate = '';
    let completedDate = '';

    try {
      if (course.enrolled_on) {
        enrolledDate = new Date(course.enrolled_on * 1000).toLocaleString();
      }
    } catch (error) {
      console.log(`Error converting enrolled_on timestamp for record:`, course.enrolled_on);
      enrolledDate = course.enrolled_on || '';
    }

    try {
      if (course.completed_on) {
        completedDate = new Date(course.completed_on * 1000).toLocaleString();
      }
    } catch (error) {
      console.log(`Error converting completed_on timestamp for record:`, course.completed_on);
      completedDate = course.completed_on || '';
    }

    return {
      course_id: String(course.id || ''),
      name: String(course.name || ''),
      role: String(course.role || ''),
      enrolled_on: String(enrolledDate),
      enrolled_on_timestamp: String(course.enrolled_on || ''),
      completed_on: String(completedDate),
      completed_on_timestamp: String(course.completed_on_timestamp || ''),
      completion_status: String(course.completion_status || ''),
      completion_status_formatted: String(course.completion_status === 'not_attempted' ? 'Not Started' : course.completion_status),
      completion_percentage: String(course.completion_percentage || ''),
      expired_on: String(course.expired_on || ''),
      expired_on_timestamp: String(course.expired_on_timestamp || '')
    };
  });
}

async function updateAirtableRecord(recordId, lmsCourseData) {
  try {
    const formattedCourseData = formatCourseData(lmsCourseData); // Format the course data
    const updatedRecord = await base(AIRTABLE_TABLE).update(recordId, {
      'LMS_course_data': JSON.stringify(formattedCourseData, null, 2) // Properly stringify the JSON data
    });
    return updatedRecord;
  } catch (error) {
    console.error(`Error updating Airtable record ${recordId}:`, error);
    throw error;
  }
}

// Add this function to check if update is needed
function needsUpdate(existingData, newData) {
  if (!existingData || existingData === 'No courses enrolled') {
    return true;
  }

  try {
    const currentData = JSON.parse(existingData);
    // If lengths are different, update is needed
    if (!Array.isArray(currentData) || currentData.length !== newData.length) {
      return true;
    }

    // Create maps of current and new data by course_id for easy comparison
    const currentMap = new Map(currentData.map(course => [course.course_id, course]));
    const newMap = new Map(newData.map(course => [course.course_id, course]));

    // Check if any courses have different data
    for (const [courseId, newCourse] of newMap) {
      const currentCourse = currentMap.get(courseId);
      if (!currentCourse) return true;

      // Compare relevant fields
      if (
        newCourse.completion_status !== currentCourse.completion_status ||
        newCourse.completion_percentage !== currentCourse.completion_percentage ||
        newCourse.completed_on_timestamp !== currentCourse.completed_on_timestamp
      ) {
        return true;
      }
    }

    return false;
  } catch (error) {
    console.error('Error parsing existing course data:', error);
    return true; // If we can't parse the existing data, update it
  }
}

// Add this function to match and set LMS IDs
async function matchAndSetLMSIds(lmsUsers, airtableRecords) {
  console.log('Matching LMS users to Airtable records...');
  let updateCount = 0;
  let skippedCount = 0;

  // Create map of LMS users by email
  const lmsUserMap = lmsUsers.reduce((acc, user) => {
    if (user.email) {
      acc[user.email.toLowerCase()] = user;
    }
    return acc;
  }, {});

  for (const record of airtableRecords) {
    const airtableEmail = record.fields['Email']?.toLowerCase();
    
    if (airtableEmail && lmsUserMap[airtableEmail]) {
      const lmsUser = lmsUserMap[airtableEmail];
      
      // Only update if LMS_id is missing or different
      if (!record.fields['LMS_id'] || record.fields['LMS_id'] !== lmsUser.id.toString()) {
        try {
          await base(AIRTABLE_TABLE).update(record.id, {
            'LMS_id': lmsUser.id.toString()
          });
          updateCount++;
          console.log(`Updated LMS_id for record ${record.id} (${airtableEmail})`);
        } catch (error) {
          console.error(`Failed to update LMS_id for record ${record.id}:`, error);
        }
      } else {
        skippedCount++;
      }
    }
  }

  console.log('\nLMS ID Matching Summary:');
  console.log(`Updated: ${updateCount} records`);
  console.log(`Skipped: ${skippedCount} records`);
  console.log(`Total processed: ${updateCount + skippedCount} records`);
}

async function syncTrainingData() {
  try {
    console.log('Starting LMS training sync...');
    
    // Step 1: Fetch all data
    console.log('\nFetching data...');
    const [lmsUsers, airtableRecords] = await Promise.all([
      fetchLMSUsers(),
      fetchAirtableRecords()
    ]);
    console.log(`- Fetched ${lmsUsers.length} LMS users`);
    console.log(`- Fetched ${airtableRecords.length} Airtable records`);

    // Step 2: Match and set LMS IDs
    console.log('\nMatching LMS users to Airtable records...');
    await matchAndSetLMSIds(lmsUsers, airtableRecords);

    // Step 3: Fetch fresh Airtable data to get updated LMS IDs
    console.log('\nFetching updated Airtable records...');
    const updatedAirtableRecords = await fetchAirtableRecords();
    console.log(`Fetched ${updatedAirtableRecords.length} updated records`);

    // Step 4: Update course data for matched records
    console.log('\nUpdating course data...');
    let courseUpdateCount = 0;
    let courseSkippedCount = 0;

    // Process records that have an LMS_id
    const recordsWithLmsId = updatedAirtableRecords.filter(record => record.fields['LMS_id']);
    console.log(`Found ${recordsWithLmsId.length} records with LMS IDs`);

    for (const record of recordsWithLmsId) {
      const lmsUserId = record.fields['LMS_id'];
      try {
        const lmsCourseData = await fetchUserCourses(lmsUserId);
        if (lmsCourseData) {
          const formattedData = formatCourseData(lmsCourseData);
          if (needsUpdate(record.fields['LMS_course_data'], formattedData)) {
            await updateAirtableRecord(record.id, lmsCourseData);
            courseUpdateCount++;
            console.log(`Updated course data for record ${record.id} (${record.fields['Email']})`);
          } else {
            courseSkippedCount++;
            console.log(`Skipped course update for record ${record.id} - no changes needed`);
          }
        }
      } catch (error) {
        console.error(`Error processing course data for record ${record.id}:`, error);
      }
    }

    // Final Summary
    console.log('\nSync Complete!');
    console.log(`Course Data Updates:`);
    console.log(`- Updated: ${courseUpdateCount} records`);
    console.log(`- Skipped: ${courseSkippedCount} records`);
    console.log(`- Total Processed: ${courseUpdateCount + courseSkippedCount} records`);

  } catch (error) {
    console.error('Error in training sync:', error);
    throw error;
  }
}

// Update the execution block
if (require.main === module) {
  (async () => {
    try {
      await syncTrainingData();
    } catch (error) {
      console.error('Error running training sync:', error);
      process.exit(1);
    }
  })();
}

module.exports = { syncTrainingData };