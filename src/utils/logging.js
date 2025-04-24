const path = require('path');
const fs = require('fs');
// Import handleError if it's used in this file
// const { handleError } = require('./error-handler');

const LOG_LEVELS = {
  ERROR: 0,
  WARN: 1,
  INFO: 2,
  DEBUG: 3
};

const CURRENT_LOG_LEVEL = process.env.LOG_LEVEL ? 
  LOG_LEVELS[process.env.LOG_LEVEL.toUpperCase()] : 
  LOG_LEVELS.INFO;

function shouldLog(level) {
  return CURRENT_LOG_LEVEL >= LOG_LEVELS[level];
}

function log(level, message, data = null) {
  if (!shouldLog(level)) return;
  
  const logMessage = data ? 
    `${message} ${JSON.stringify(data)}` : 
    message;
    
  console.log(logMessage);
}

/**
 * Get the path to a report file
 * @param {String} filename - Report filename
 * @param {String} category - Report category (subdirectory)
 * @returns {String} - Full path to the report file
 */
function getReportPath(filename, category = '') {
  const reportsDir = path.join(__dirname, '..', 'reports');
  
  // Create reports directory if it doesn't exist
  if (!fs.existsSync(reportsDir)) {
    fs.mkdirSync(reportsDir, { recursive: true });
  }
  
  // If category is provided, create category subdirectory
  if (category) {
    const categoryDir = path.join(reportsDir, category);
    if (!fs.existsSync(categoryDir)) {
      fs.mkdirSync(categoryDir, { recursive: true });
    }
    return path.join(categoryDir, filename);
  }
  
  return path.join(reportsDir, filename);
}

/**
 * Clear report files
 * @param {Array} files - Array of filenames to clear
 * @param {String} category - Report category (subdirectory)
 */
function clearReportFiles(files, category = '') {
  files.forEach(file => {
    const filePath = getReportPath(file, category);
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
  });
}

/**
 * Write JSON data to a report file
 * @param {String} filename - Report filename
 * @param {Object} data - Data to write
 * @param {String} category - Report category (subdirectory)
 */
function writeJsonReport(filename, data, category = '') {
  const filePath = getReportPath(filename, category);
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
  
  const displayPath = category ? `reports/${category}/${filename}` : `reports/${filename}`;
  console.log(`Report saved to ${displayPath}`);
}

/**
 * Log to a history file with timestamp
 * @param {String} message - Message to log
 * @param {String} logFile - Log filename
 */
async function logToHistory(message, logFile = 'sync_history.log') {
  try {
    const timestamp = new Date().toISOString();
    const logMessage = `${timestamp}: ${message}\n`;
    
    fs.appendFileSync(logFile, logMessage);
  } catch (error) {
    console.error(`Error writing to log file ${logFile}:`, error.message);
  }
}

/**
 * Log the start of a sync process
 * @param {String} processName - Name of the sync process
 */
function logSyncStart(processName) {
  console.log(`\n========== STARTING ${processName.toUpperCase()} ==========\n`);
}

/**
 * Log the end of a sync process with summary
 * @param {String} processName - Name of the sync process
 * @param {Object} stats - Statistics about the sync
 */
function logSyncEnd(processName, stats = {}) {
  console.log(`\n========== ${processName.toUpperCase()} COMPLETE ==========`);
  
  if (Object.keys(stats).length > 0) {
    console.log('\nSummary:');
    Object.entries(stats).forEach(([key, value]) => {
      console.log(`- ${key}: ${value}`);
    });
  }
  
  console.log('\n');
}

module.exports = {
  getReportPath,
  clearReportFiles,
  writeJsonReport,
  logToHistory,
  logSyncStart,
  logSyncEnd
};
