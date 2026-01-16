// export-deals.js
// Exports deals with extra fields + associated contact & company IDs

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

async function fetchAllDeals() {
  const limit = 100;
  let after = undefined;
  const all = [];

  const properties = [
    'dealname',
    'amount',
    'closedate',
    'dealstage',
    'pipeline',
    'hubspot_owner_id',
    'createdate',
    'lastmodifieddate'
  ];

  console.log('🚀 Fetching deals with associated contacts & companies...');

  while (true) {
    const res = await hubspotClient.crm.deals.basicApi.getPage(
      limit,
      after,
      properties,
      undefined,
      ['contacts', 'companies'] // associations
    );

    all.push(...res.results);

    if (!res.paging || !res.paging.next) break;
    after = res.paging.next.after;
    console.log(`...fetched ${all.length} so far`);
  }

  console.log(`✅ Finished fetching ${all.length} deals`);

  return all.map((d) => {
    const p = d.properties || {};
    const a = d.associations || {};

    const contactIds =
      a.contacts?.results?.map((r) => r.id).filter(Boolean) || [];
    const companyIds =
      a.companies?.results?.map((r) => r.id).filter(Boolean) || [];

    return {
      id: d.id,
      dealname: p.dealname || '',
      amount: p.amount || '',
      closedate: p.closedate || '',
      dealstage: p.dealstage || '',
      pipeline: p.pipeline || '',
      hubspot_owner_id: p.hubspot_owner_id || '',
      createdate: p.createdate || '',
      lastmodifieddate: p.lastmodifieddate || '',
      contact_ids: contactIds.join('|'),
      company_ids: companyIds.join('|')
    };
  });
}

async function main() {
  const deals = await fetchAllDeals();

  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  const outPath = path.join(dataDir, 'deals_export.csv');

  const csvWriter = createObjectCsvWriter({
    path: outPath,
    header: [
      { id: 'id', title: 'id' },
      { id: 'dealname', title: 'dealname' },
      { id: 'amount', title: 'amount' },
      { id: 'closedate', title: 'closedate' },
      { id: 'dealstage', title: 'dealstage' },
      { id: 'pipeline', title: 'pipeline' },
      { id: 'hubspot_owner_id', title: 'hubspot_owner_id' },
      { id: 'createdate', title: 'createdate' },
      { id: 'lastmodifieddate', title: 'lastmodifieddate' },
      { id: 'contact_ids', title: 'contact_ids' },
      { id: 'company_ids', title: 'company_ids' }
    ]
  });

  await csvWriter.writeRecords(deals);
  console.log(`📁 Deals export written to: ${outPath}`);
}

main().catch((err) => {
  const body = err?.response?.body;
  console.error('❌ Error exporting deals');
  if (body) {
    console.error(JSON.stringify(body, null, 2));
  } else {
    console.error(err);
  }
  process.exit(1);
});
