// export-companies.js
// Exports companies with extra fields + associated contact IDs

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const hubspot = require('@hubspot/api-client');
const { createObjectCsvWriter } = require('csv-writer');

const HUBSPOT_TOKEN = process.env.HUBSPOT_PERSONAL_ACCESS_KEY;
const HUBSPOT_ACCOUNT_ID = process.env.HUBSPOT_ACCOUNT_ID;

if (!HUBSPOT_TOKEN) {
  console.error('❌ HUBSPOT_PERSONAL_ACCESS_KEY is missing in .env');
  process.exit(1);
}

console.log(`🔑 Using HubSpot account: ${HUBSPOT_ACCOUNT_ID || '(not set)'}`);

const hubspotClient = new hubspot.Client({ accessToken: HUBSPOT_TOKEN });

async function fetchAllCompanies() {
  const limit = 100;
  let after = undefined;
  const all = [];

  const properties = [
    'name',
    'domain',
    'industry',
    'city',
    'state',
    'country',
    'numberofemployees',
    'annualrevenue',
    'lifecyclestage',
    'createdate',
    'lastmodifieddate'
  ];

  console.log('🚀 Fetching companies with associated contacts...');

  while (true) {
    const res = await hubspotClient.crm.companies.basicApi.getPage(
      limit,
      after,
      properties,
      undefined,
      ['contacts'] // associations
    );

    all.push(...res.results);

    if (!res.paging || !res.paging.next) break;
    after = res.paging.next.after;
    console.log(`...fetched ${all.length} so far`);
  }

  console.log(`✅ Finished fetching ${all.length} companies`);

  return all.map((c) => {
    const p = c.properties || {};
    const a = c.associations || {};
    const contactIds =
      a.contacts?.results?.map((r) => r.id).filter(Boolean) || [];

    return {
      id: c.id,
      name: p.name || '',
      domain: p.domain || '',
      industry: p.industry || '',
      city: p.city || '',
      state: p.state || '',
      country: p.country || '',
      numberofemployees: p.numberofemployees || '',
      annualrevenue: p.annualrevenue || '',
      lifecyclestage: p.lifecyclestage || '',
      createdate: p.createdate || '',
      lastmodifieddate: p.lastmodifieddate || '',
      contact_ids: contactIds.join('|')
    };
  });
}

async function main() {
  const companies = await fetchAllCompanies();

  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  const outPath = path.join(dataDir, 'companies_export.csv');

  const csvWriter = createObjectCsvWriter({
    path: outPath,
    header: [
      { id: 'id', title: 'id' },
      { id: 'name', title: 'name' },
      { id: 'domain', title: 'domain' },
      { id: 'industry', title: 'industry' },
      { id: 'city', title: 'city' },
      { id: 'state', title: 'state' },
      { id: 'country', title: 'country' },
      { id: 'numberofemployees', title: 'numberofemployees' },
      { id: 'annualrevenue', title: 'annualrevenue' },
      { id: 'lifecyclestage', title: 'lifecyclestage' },
      { id: 'createdate', title: 'createdate' },
      { id: 'lastmodifieddate', title: 'lastmodifieddate' },
      { id: 'contact_ids', title: 'contact_ids' }
    ]
  });

  await csvWriter.writeRecords(companies);
  console.log(`📁 Companies export written to: ${outPath}`);
}

main().catch((err) => {
  const body = err?.response?.body;
  console.error('❌ Error exporting companies');
  if (body) {
    console.error(JSON.stringify(body, null, 2));
  } else {
    console.error(err);
  }
  process.exit(1);
});
