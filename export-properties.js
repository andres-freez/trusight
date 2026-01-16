// export-properties.js
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { createObjectCsvWriter } = require('csv-writer');
const hubspot = require('@hubspot/api-client');

const HUBSPOT_TOKEN = process.env.HUBSPOT_PERSONAL_ACCESS_KEY;

if (!HUBSPOT_TOKEN) {
  console.error('❌ Missing HUBSPOT_PERSONAL_ACCESS_KEY in .env');
  process.exit(1);
}

const hubspotClient = new hubspot.Client({
  accessToken: HUBSPOT_TOKEN,
});

// We ONLY want these three
const OBJECT_TYPES = ['contacts', 'companies', 'deals'];

async function fetchPropertiesForObject(objectType) {
  console.log(`📥 Fetching properties for: ${objectType}...`);

  // archived = false
  const res = await hubspotClient.crm.properties.coreApi.getAll(
    objectType,
    false
  );

  const results = res.results || res.body?.results || [];
  console.log(`  → ${results.length} properties for ${objectType}`);

  return results.map((p) => ({
    objectType, // hard-coded so we don't get random objectType values
    name: p.name || '',
    label: p.label || '',
    description: p.description || '',
    groupName: p.groupName || '',
    type: p.type || '',
    fieldType: p.fieldType || '',
    hubspotDefined: p.hubspotDefined ?? '',
    createdAt: p.createdAt || '',
    updatedAt: p.updatedAt || '',
    options_json: JSON.stringify(p.options || []),
  }));
}

async function main() {
  try {
    let allProps = [];

    for (const objType of OBJECT_TYPES) {
      const props = await fetchPropertiesForObject(objType);
      allProps = allProps.concat(props);
    }

    // Safety filter: in case anything weird sneaks in, keep ONLY our 3 types
    allProps = allProps.filter((p) => OBJECT_TYPES.includes(p.objectType));

    if (!fs.existsSync('data')) {
      fs.mkdirSync('data');
    }

    const filePath = path.join('data', 'properties_export.csv');

    const csvWriter = createObjectCsvWriter({
      path: filePath,
      header: [
        { id: 'objectType', title: 'objectType' },
        { id: 'name', title: 'name' },
        { id: 'label', title: 'label' },
        { id: 'description', title: 'description' },
        { id: 'groupName', title: 'groupName' },
        { id: 'type', title: 'type' },
        { id: 'fieldType', title: 'fieldType' },
        { id: 'hubspotDefined', title: 'hubspotDefined' },
        { id: 'createdAt', title: 'createdAt' },
        { id: 'updatedAt', title: 'updatedAt' },
        { id: 'options_json', title: 'options_json' },
      ],
    });

    await csvWriter.writeRecords(allProps);

    console.log(`✅ Wrote ${allProps.length} rows to ${filePath}`);
  } catch (err) {
    console.error('❌ Error exporting properties:');
    console.error(err.response?.body || err);
    process.exit(1);
  }
}

main();
