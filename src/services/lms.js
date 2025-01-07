require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const path = require('path'); // Import path module for handling file paths

const LMS_API_URL_USERS = process.env.LMS_API_URL_USERS; // e.g., https://reel.talentlms.com/api/v1/users
const LMS_API_URL_COURSES = process.env.LMS_API_URL_COURSES; // e.g., https://reel.talentlms.com/api/v1/courses
const LMS_API_KEY = process.env.LMS_API_KEY; // Your API key

async function fetchLMSData() {
  try {
    // Create Basic Auth header
    const auth = Buffer.from(`${LMS_API_KEY}:`).toString('base64'); // API key as username, empty password

    // Fetch users data
    const usersResponse = await axios.get(LMS_API_URL_USERS, {
      headers: {
        'Authorization': `Basic ${auth}`
      }
    });
    const usersData = usersResponse.data;

    // Save users data to the existing JSON file in the reports folder
    fs.writeFileSync(path.join(__dirname, '../reports/lms_users_data.json'), JSON.stringify(usersData, null, 2));
    console.log('LMS users data saved to reports/lms_users_data.json');

    // Fetch courses data
    const coursesResponse = await axios.get(LMS_API_URL_COURSES, {
      headers: {
        'Authorization': `Basic ${auth}`
      }
    });
    const coursesData = coursesResponse.data;

    // Save courses data to the existing JSON file in the reports folder
    fs.writeFileSync(path.join(__dirname, '../reports/lms_courses_data.json'), JSON.stringify(coursesData, null, 2));
    console.log('LMS courses data saved to reports/lms_courses_data.json');
  } catch (error) {
    console.error('Error fetching data from LMS API:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
      console.error('Response status:', error.response.status);
      console.error('Response headers:', error.response.headers);
    }
  }
}

// Call the function to fetch data
fetchLMSData();