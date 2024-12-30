const path = require('path');
const fs = require('fs');

function getReportPath(filename) {
  const reportsDir = path.join(__dirname, '..', 'reports');
  
  // Create reports directory if it doesn't exist
  if (!fs.existsSync(reportsDir)) {
    fs.mkdirSync(reportsDir, { recursive: true });
  }
  
  return path.join(reportsDir, filename);
}

function clearReportFiles(files) {
  files.forEach(file => {
    const filePath = getReportPath(file);
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
  });
}

function writeJsonReport(filename, data) {
  fs.writeFileSync(getReportPath(filename), JSON.stringify(data, null, 2));
  console.log(`Report saved to reports/${filename}`);
}

module.exports = {
  getReportPath,
  clearReportFiles,
  writeJsonReport
};
