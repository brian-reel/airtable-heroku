const { base, AIRTABLE_TABLE } = require('../config/database');

async function getAirtableRecords(fields = ['RSC Emp ID', 'Email', 'Phone', 'Status-RSPG', 'Status']) {
  const records = [];
  try {
    await base(AIRTABLE_TABLE)
      .select({ fields })
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

async function updateAirtableRecord(recordId, fields) {
  try {
    const updatedRecord = await base(AIRTABLE_TABLE).update(recordId, fields);
    return updatedRecord;
  } catch (error) {
    console.error(`Error updating Airtable record ${recordId}:`, error);
    throw error;
  }
}

module.exports = {
  getAirtableRecords,
  updateAirtableRecord
};