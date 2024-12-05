const express = require('express');
const Airtable = require('airtable');

const app = express();
const PORT = process.env.PORT || 3000;

// Airtable setup
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Endpoint to fetch Airtable data
app.get('/records', async (req, res) => {
  try {
    const records = [];
    await base('Table Name') // Replace with your Airtable table name
      .select()
      .eachPage((pageRecords, fetchNextPage) => {
        records.push(...pageRecords);
        fetchNextPage();
      });
    res.json(records.map((record) => record.fields));
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
