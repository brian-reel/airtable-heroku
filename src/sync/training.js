require('dotenv').config();
const axios = require('axios');
const { base, AIRTABLE_TABLE } = require('../config/database');
const { executeQuery } = require('../services/postgres');
const { getAirtableRecords, updateAirtableRecord, createAirtableRecord } = require('../services/airtable');
const { clearReportFiles, writeJsonReport, logSyncStart, logSyncEnd, logToHistory } = require('../utils/logging');
const { formatDate } = require('../utils/formatters');
const { validateUpdateFields } = require('../utils/validation');
const { handleError, tryCatch } = require('../utils/error-handler');
const config = require('../config/config');
const { AIRTABLE_TRAINING_TABLE } = require('../config/database');

const LMS_API_URL_USERS = process.env.LMS_API_URL_USERS; // e.g., https://reel.talentlms.com/api/v1/users
const LMS_API_URL_USERS_COURSES_BASE = process.env.LMS_API_URL_USERS_COURSES; // e.g., https://reel.talentlms.com/api/v1/users
const LMS_API_KEY = process.env.LMS_API_KEY; // Your API key

/**
 * Fetch L M S Users
 * @returns {Promise<any>} - Description of return value
 */
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
    handleError(error.message, 'Error fetching LMS users:');
    throw error;
  }
}

/**
 * Fetch Airtable Records
 * @returns {Promise<any>} - Description of return value
 */
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
    handleError(error, 'Error fetching Airtable data:');
    throw error;
  }
}

/**
 * Fetch User Courses
 * @param {any} lmsUserId - Description of lmsUserId
 * @returns {Promise<any>} - Description of return value
 */
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
    handleError(error.message, 'Error fetching courses for LMS user ID ${lmsUserId}:');
    return null; // Return null if there's an error
  }
}

/**
 * Format Course Data
 * @param {any} courses - Description of courses
 * @returns {Promise<any>} - Description of return value
 */
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

/**
 * Update an Airtable record with LMS course data
 * @param {String} recordId - Airtable record ID
 * @param {Object} lmsCourseData - LMS course data
 * @returns {Promise<Object>} - Updated record
 */
async function updateAirtableTrainingRecord(recordId, lmsCourseData) {
  return tryCatch(async () => {
    const fields = {
      'LMS User ID': lmsCourseData.userId.toString(),
      'Courses Completed': lmsCourseData.completedCourses,
      'Last Course Date': lmsCourseData.lastCourseDate,
      'Total Courses': lmsCourseData.totalCourses.toString()
    };
    
    return await updateAirtableRecord(AIRTABLE_TRAINING_TABLE, recordId, fields);
  }, 'update_airtable_training_record', { recordId });
}

// Add this function to check if update is needed
/**
 * Needs Update
 * @param {any} existingData - Description of existingData
 * @param {any} newData - Description of newData
 * @returns {Promise<any>} - Description of return value
 */
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
    handleError(error, 'Error parsing existing course data:');
    return true; // If we can't parse the existing data, update it
  }
}

// Add this function to match and set LMS IDs
/**
 * Match And Set L M S Ids
 * @param {any} lmsUsers - Description of lmsUsers
 * @param {any} airtableRecords - Description of airtableRecords
 * @returns {Promise<any>} - Description of return value
 */
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
          handleError(error, 'Failed to update LMS_id for record ${record.id}:');
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

/**
 * Sync Training Data
 * @returns {Promise<any>} - Description of return value
 */
async function syncTrainingData() {
  return tryCatch(async () => {
    await logToHistory('Starting training data sync...');
    console.log('Starting training data sync...');
    
    // Clear previous report files
    clearReportFiles([
      'training_sync.json',
      'training_errors.json'
    ]);
    
    logSyncStart('training data sync');
    
    // 1. Get training data from PostgreSQL
    const pgTrainingData = await getTrainingDataFromPostgres();
    
    // 2. Get training data from LMS API if configured
    let lmsTrainingData = [];
    if (config.lmsApi.url && config.lmsApi.key) {
      lmsTrainingData = await getTrainingDataFromLMS();
    }
    
    // 3. Combine training data
    const combinedTrainingData = [...pgTrainingData];
    
    // Add LMS data that doesn't exist in PG data
    for (const lmsTraining of lmsTrainingData) {
      const existsInPg = pgTrainingData.some(pgTraining => 
        pgTraining.employee_id === lmsTraining.employee_id && 
        pgTraining.course_name === lmsTraining.course_name
      );
      
      if (!existsInPg) {
        combinedTrainingData.push(lmsTraining);
      }
    }
    
    // 4. Get existing Airtable records
    const airtableRecords = await getAirtableRecords(AIRTABLE_TRAINING_TABLE, [
      'RSC Emp ID', 'Course Name', 'Completion Date', 'Expiration Date', 'Status'
    ]);
    
    // 5. Create lookup map for Airtable records
    const airtableMap = {};
    for (const record of airtableRecords) {
      if (record.fields['RSC Emp ID'] && record.fields['Course Name']) {
        const key = `${record.fields['RSC Emp ID']}-${record.fields['Course Name']}`;
        airtableMap[key] = record;
      }
    }
    
    // 6. Process updates and new records
    const updates = [];
    const newRecords = [];
    
    for (const training of combinedTrainingData) {
      const employeeId = training.employee_id.toString();
      const key = `${employeeId}-${training.course_name}`;
      const airtableRecord = airtableMap[key];
      
      if (airtableRecord) {
        // Check if update is needed
        const updateFields = {};
        
        if (training.completion_date) {
          const pgCompletionDate = formatDate(training.completion_date);
          const airtableCompletionDate = formatDate(airtableRecord.fields['Completion Date']);
          
          if (pgCompletionDate !== airtableCompletionDate) {
            updateFields['Completion Date'] = pgCompletionDate;
          }
        }
        
        if (training.expiration_date) {
          const pgExpirationDate = formatDate(training.expiration_date);
          const airtableExpirationDate = formatDate(airtableRecord.fields['Expiration Date']);
          
          if (pgExpirationDate !== airtableExpirationDate) {
            updateFields['Expiration Date'] = pgExpirationDate;
          }
        }
        
        // Update status if needed
        const currentStatus = airtableRecord.fields['Status'];
        const isExpired = training.expiration_date && new Date(training.expiration_date) < new Date();
        const desiredStatus = isExpired ? 'Expired' : 'Active';
        
        if (currentStatus !== desiredStatus) {
          updateFields['Status'] = desiredStatus;
        }
        
        if (Object.keys(updateFields).length > 0) {
          // Validate update fields
          const validation = validateUpdateFields(updateFields);
          if (validation.isValid) {
            updates.push({
              recordId: airtableRecord.id,
              fields: updateFields
            });
          } else {
            console.warn(`Skipping invalid update for ${airtableRecord.id}:`, validation.errors);
          }
        }
      } else {
        // Create new record
        const isExpired = training.expiration_date && new Date(training.expiration_date) < new Date();
        
        newRecords.push({
          'RSC Emp ID': employeeId,
          'Course Name': training.course_name,
          'Completion Date': formatDate(training.completion_date) || '',
          'Expiration Date': formatDate(training.expiration_date) || '',
          'Status': isExpired ? 'Expired' : 'Active'
        });
      }
    }
    
    // 7. Perform updates
    console.log(`Found ${updates.length} training records that need updating`);
    let updateSuccessCount = 0;
    const updateErrors = [];
    
    for (const update of updates) {
      try {
        await updateAirtableTrainingRecord(update.recordId, update.fields);
        updateSuccessCount++;
        console.log(`Updated training record ${update.recordId}`);
      } catch (error) {
        handleError(error, 'training_update', { recordId: update.recordId, fields: update.fields });
        updateErrors.push({
          recordId: update.recordId,
          error: error.message
        });
      }
    }
    
    // 8. Create new records
    console.log(`Found ${newRecords.length} new training records to add`);
    let createSuccessCount = 0;
    const createErrors = [];
    
    for (const newRecord of newRecords) {
      try {
        await createAirtableRecord(AIRTABLE_TRAINING_TABLE, newRecord);
        createSuccessCount++;
        console.log(`Added new training record for employee ${newRecord['RSC Emp ID']}: ${newRecord['Course Name']}`);
      } catch (error) {
        handleError(error, 'training_create', { 
          employeeId: newRecord['RSC Emp ID'], 
          course: newRecord['Course Name'] 
        });
        createErrors.push({
          employeeId: newRecord['RSC Emp ID'],
          course: newRecord['Course Name'],
          error: error.message
        });
      }
    }
    
    // 9. Write reports
    if (updateErrors.length > 0 || createErrors.length > 0) {
      writeJsonReport('training_errors.json', {
        updateErrors,
        createErrors
      });
    }
    
    // 10. Log summary
    logSyncEnd('training data sync', {
      'Total training records processed': combinedTrainingData.length,
      'Updates needed': updates.length,
      'Successful updates': updateSuccessCount,
      'Failed updates': updateErrors.length,
      'New records': newRecords.length,
      'Successfully created': createSuccessCount,
      'Failed creations': createErrors.length
    });
    
    await logToHistory('Training data sync completed');
    console.log('Training data sync completed');
    
    return updateSuccessCount > 0 || createSuccessCount > 0;
  }, 'sync_training_data');
}

/**
 * Get training data from PostgreSQL
 */
async function getTrainingDataFromPostgres() {
  return tryCatch(async () => {
    const query = `
      WITH ranked_training AS (
        SELECT
          t.employee_id,
          c.name AS course_name,
          t.completion_date,
          t.expiration_date,
          t.created_at,
          ROW_NUMBER() OVER (
            PARTITION BY t.employee_id, c.name
            ORDER BY 
              t.completion_date DESC,
              t.created_at DESC
          ) AS row_num
        FROM training t
        JOIN courses c ON t.course_id = c.id
        JOIN employees e ON t.employee_id = e.id
        WHERE e.email NOT LIKE '%test%'
          AND e.email NOT LIKE '%example%'
      )
      SELECT
        employee_id,
        course_name,
        completion_date,
        expiration_date,
        created_at
      FROM ranked_training
      WHERE row_num = 1
      ORDER BY employee_id, course_name;
    `;
    
    return await executeQuery(query);
  }, 'get_training_data_from_postgres');
}

/**
 * Get training data from LMS API
 */
async function getTrainingDataFromLMS() {
  return tryCatch(async () => {
    if (!config.lmsApi.url || !config.lmsApi.key) {
      console.log('LMS API not configured, skipping');
      return [];
    }
    
    console.log('Fetching training data from LMS API...');
    
    const response = await axios.get(`${config.lmsApi.url}/training`, {
      headers: {
        'Authorization': `Bearer ${config.lmsApi.key}`,
        'Content-Type': 'application/json'
      }
    });
    
    if (!response.data || !Array.isArray(response.data)) {
      console.warn('Invalid response from LMS API');
      return [];
    }
    
    // Transform LMS data to match our format
    return response.data.map(item => ({
      employee_id: item.employeeId,
      course_name: item.courseName,
      completion_date: item.completedDate,
      expiration_date: item.expirationDate,
      created_at: new Date().toISOString()
    }));
  }, 'get_training_data_from_lms');
}

// Update the execution block
if (require.main === module) {
  (async () => {
    try {
      await syncTrainingData();
    } catch (error) {
      handleError(error, 'Error running training sync:');
      process.exit(1);
    }
  })();
}

module.exports = { syncTrainingData };