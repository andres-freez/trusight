// export-contacts.js
//
// Exports HubSpot contacts to data/contacts.csv
// Reads HUBSPOT_PERSONAL_ACCESS_KEY from .env

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const hubspot = require('@hubspot/api-client');
const { createObjectCsvWriter } = require('csv-writer');

async function main() {
  // 1. Load env + validate
  const accessToken = process.env.HUBSPOT_PERSONAL_ACCESS_KEY;
  if (!accessToken) {
    console.error('❌ Missing HUBSPOT_PERSONAL_ACCESS_KEY in .env');
    process.exit(1);
  }

  // 2. Initialize HubSpot client
  const hubspotClient = new hubspot.Client({ accessToken });

  // 3. Pagination setup
  const limit = 100; // contacts per page
  let after = undefined;
  const allContacts = [];

  // Properties to pull from HubSpot
  const properties = ['email', 'firstname', 'lastname', 'phone'];

  console.log('🚀 Fetching contacts from HubSpot...');

  while (true) {
    const response = await hubspotClient.crm.contacts.basicApi.getPage(
      limit,
      after,
      properties
      // we let all other arguments use their default values
    );

    allContacts.push(...response.results);

    if (!response.paging || !response.paging.next) {
      break;
    }

    after = response.paging.next.after;
    console.log(`...fetched ${allContacts.length} so far (after=${after})`);
  }

  console.log(`✅ Finished fetching. Total contacts: ${allContacts.length}`);

  // 4. Normalize contacts into plain objects for CSV
  const rows = allContacts.map((c) => {
    const p = c.properties || {};
    return {
      id: c.id,
      email: p.email || '',
      firstname: p.firstname || '',
      lastname: p.lastname || '',
      phone: p.phone || '',
    };
  });

  // 5. Ensure data directory exists
  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  const outputPath = path.join(dataDir, 'contacts.csv');

  // 6. Write CSV
  const csvWriter = createObjectCsvWriter({
    path: outputPath,
    header: [
      { id: 'id', title: 'id' },
      { id: 'email', title: 'email' },
      { id: 'firstname', title: 'firstname' },
      { id: 'lastname', title: 'lastname' },
      { id: 'phone', title: 'phone' },
    ],
  });

  await csvWriter.writeRecords(rows);

  console.log(`📁 Contacts exported to: ${outputPath}`);
}

main().catch((err) => {
  // Try to show nice HubSpot error details if available
  const body = err?.response?.body;
  if (body) {
    console.error('❌ Error exporting contacts (HubSpot API):');
    console.error(JSON.stringify(body, null, 2));
  } else {
    console.error('❌ Error exporting contacts:', err);
  }
  process.exit(1);
});

