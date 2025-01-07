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

  const filteredCourses = courses.filter(course => course.id === "142");
  if (filteredCourses.length === 0) {
    return 'No relevant courses found';
  }

  return filteredCourses.map(course => {
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

(async () => {
  try {
    console.log('Fetching LMS users...');
    const lmsUsers = await fetchLMSUsers();
    console.log(`Fetched ${lmsUsers.length} users from LMS.`);

    console.log('Fetching Airtable records...');
    const airtableRecords = await fetchAirtableRecords();
    console.log(`Fetched ${airtableRecords.length} records from Airtable.`);

    let updateCount = 0;
    for (const record of airtableRecords) {
      const lmsUserId = record.fields['LMS_id'];
      if (lmsUserId) {
        const lmsCourseData = await fetchUserCourses(lmsUserId);
        if (lmsCourseData) {
          await updateAirtableRecord(record.id, lmsCourseData);
          updateCount++;
          console.log(`Updated record ${record.id} with LMS course data.`);
        }
      }
    }

    console.log(`Total records updated with course data: ${updateCount}`);
  } catch (error) {
    console.error('Error in sync process:', error);
  }
})();