const { syncAllEmployeeData } = require('./sync/employee-sync');
const { syncGuardCards } = require('./sync/guard-cards');
const { syncEmployeeRoles } = require('./sync/roles');
const { syncTrainingData } = require('./sync/training');
const { detectDuplicates } = require('./sync/duplicates');
const { logToHistory, logSyncStart, logSyncEnd } = require('./utils/logging');
const { handleError, tryCatch } = require('./utils/error-handler');
const { testConnections } = require('./config/database');
const config = require('./config/config');

/**
 * Main function to run all sync processes
 */
async function runAllSyncProcesses() {
  return tryCatch(async () => {
    await logToHistory('Starting all sync processes');
    console.log('Starting all sync processes');
    
    // Test database connections before proceeding
    const connectionsValid = await testConnections();
    if (!connectionsValid) {
      throw new Error('Database connections failed. Aborting sync processes.');
    }
    
    logSyncStart('all sync processes');
    
    // Track results for summary
    const results = {
      employeeSync: false,
      guardCardSync: false,
      rolesSync: false,
      trainingSync: false,
      duplicateDetection: false
    };
    
    try {
      // 1. Sync employee data
      console.log('\n=== STARTING EMPLOYEE DATA SYNC ===');
      results.employeeSync = await syncAllEmployeeData();
      console.log('=== COMPLETED EMPLOYEE DATA SYNC ===\n');
    } catch (error) {
      console.error('Employee data sync failed:', error);
      handleError(error, 'employee_sync');
    }
    
    try {
      // 2. Sync guard cards
      console.log('\n=== STARTING GUARD CARD SYNC ===');
      results.guardCardSync = await syncGuardCards();
      console.log('=== COMPLETED GUARD CARD SYNC ===\n');
    } catch (error) {
      console.error('Guard card sync failed:', error);
      handleError(error, 'guard_card_sync');
    }
    
    try {
      // 3. Sync employee roles
      console.log('\n=== STARTING EMPLOYEE ROLES SYNC ===');
      results.rolesSync = await syncEmployeeRoles();
      console.log('=== COMPLETED EMPLOYEE ROLES SYNC ===\n');
    } catch (error) {
      console.error('Employee roles sync failed:', error);
      handleError(error, 'roles_sync');
    }
    
    try {
      // 4. Sync training data
      console.log('\n=== STARTING TRAINING DATA SYNC ===');
      results.trainingSync = await syncTrainingData();
      console.log('=== COMPLETED TRAINING DATA SYNC ===\n');
    } catch (error) {
      console.error('Training data sync failed:', error);
      handleError(error, 'training_sync');
    }
    
    try {
      // 5. Detect duplicates
      console.log('\n=== STARTING DUPLICATE DETECTION ===');
      results.duplicateDetection = await detectDuplicates();
      console.log('=== COMPLETED DUPLICATE DETECTION ===\n');
    } catch (error) {
      console.error('Duplicate detection failed:', error);
      handleError(error, 'duplicate_detection');
    }
    
    // Log completion of all processes
    logSyncEnd('all sync processes', {
      'Employee Sync': results.employeeSync ? 'Success' : 'Failed',
      'Guard Card Sync': results.guardCardSync ? 'Success' : 'Failed',
      'Roles Sync': results.rolesSync ? 'Success' : 'Failed',
      'Training Sync': results.trainingSync ? 'Success' : 'Failed',
      'Duplicate Detection': results.duplicateDetection ? 'Success' : 'Failed'
    });
    
    await logToHistory('All sync processes completed');
    console.log('All sync processes completed');
    
    return results;
  }, 'run_all_sync_processes');
}

/**
 * Run the sync process if this file is executed directly
 */
if (require.main === module) {
  // Set environment variables if needed
  if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config();
  }
  
  // Run the sync process
  runAllSyncProcesses()
    .then(results => {
      const allSuccessful = Object.values(results).every(result => result);
      process.exit(allSuccessful ? 0 : 1);
    })
    .catch(error => {
      handleError(error, 'Fatal error in sync process:');
      process.exit(1);
    });
}

module.exports = {
  runAllSyncProcesses
}; 