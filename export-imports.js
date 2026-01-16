// export-imports.js
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

async function fetchAllImports() {
  const limit = 100;
  let after = undefined;
  const all = [];

  console.log('🚀 Fetching HubSpot imports...');

  while (true) {
    // Signature: getPage(after?, before?, limit?)
    const page = await hubspotClient.crm.imports.coreApi.getPage(
      after,
      undefined,
      limit
    );

    const results = page.results || [];
    console.log(
      `  → fetched batch: ${results.length} imports (after=${after ?? 'null'})`
    );

    all.push(...results);

    const next = page.paging?.next?.after;
    if (!next) break;
    after = next;
  }

  console.log(`✅ Total imports fetched: ${all.length}`);
  return all;
}

function mapImportToRow(imp) {
  const meta = imp.metadata || {};

  return {
    id: imp.id || '',
    importName: imp.importName || imp.name || '',
    state: imp.state || '',
    createdAt: imp.createdAt || '',
    updatedAt: imp.updatedAt || '',
    startedAt: imp.startedAt || '',
    completedAt: imp.completedAt || '',

    numRows: meta.numRows || '',
    numSucceeded: meta.numSucceeded || '',
    numFailed: meta.numFailed || '',
    fileIds: Array.isArray(meta.fileIds) ? meta.fileIds.join('|') : '',
    objectTypeIds: Array.isArray(meta.objectTypeIds)
      ? meta.objectTypeIds.join('|')
      : '',

    metadata_json: JSON.stringify(meta),
  };
}

async function main() {
  try {
    const imports = await fetchAllImports();

    if (!fs.existsSync('data')) {
      fs.mkdirSync('data');
    }

    const filePath = path.join('data', 'imports_export.csv');

    if (!imports.length) {
      console.warn('⚠️ No imports returned by the API.');
    }

    const csvWriter = createObjectCsvWriter({
      path: filePath,
      header: [
        { id: 'id', title: 'id' },
        { id: 'importName', title: 'importName' },
        { id: 'state', title: 'state' },
        { id: 'createdAt', title: 'createdAt' },
        { id: 'updatedAt', title: 'updatedAt' },
        { id: 'startedAt', title: 'startedAt' },
        { id: 'completedAt', title: 'completedAt' },
        { id: 'numRows', title: 'numRows' },
        { id: 'numSucceeded', title: 'numSucceeded' },
        { id: 'numFailed', title: 'numFailed' },
        { id: 'fileIds', title: 'fileIds' },
        { id: 'objectTypeIds', title: 'objectTypeIds' },
        { id: 'metadata_json', title: 'metadata_json' },
      ],
    });

    const records = imports.map(mapImportToRow);
    await csvWriter.writeRecords(records);

    console.log(`📁 Wrote ${records.length} rows to ${filePath}`);
  } catch (err) {
    console.error('❌ Error exporting imports');
    console.error(err.response?.body || err);
    process.exit(1);
  }
}

main();
