#!/usr/bin/env node

// cli.js
// Usage:
//   node cli.js export-with-imports

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const hubspot = require('@hubspot/api-client');
const { createObjectCsvWriter } = require('csv-writer');
const csv = require('csv-parser');

// ----- env -----
const HUBSPOT_ACCOUNT_ID = process.env.HUBSPOT_ACCOUNT_ID;
const HUBSPOT_PERSONAL_ACCESS_KEY = process.env.HUBSPOT_PERSONAL_ACCESS_KEY;

if (!HUBSPOT_PERSONAL_ACCESS_KEY) {
  console.error("❌ HUBSPOT_PERSONAL_ACCESS_KEY is missing in .env");
  process.exit(1);
}

console.log(`🔑 Using HubSpot account ${HUBSPOT_ACCOUNT_ID}`);
console.log("🔐 Token loaded:", HUBSPOT_PERSONAL_ACCESS_KEY.slice(0, 8) + "...");

// HubSpot client
const hubspotClient = new hubspot.Client({
  accessToken: HUBSPOT_PERSONAL_ACCESS_KEY
});

// ----- paths -----
const DATA_DIR = path.join(__dirname, 'data');
const IMPORTS_DIR = path.join(__dirname, 'imports');

// ----- helpers -----
async function fetchAllContacts() {
  const limit = 100;
  let after = undefined;
  const all = [];
  const properties = ['email', 'firstname', 'lastname', 'phone'];

  console.log("🚀 Fetching contacts from HubSpot...");

  while (true) {
    const res = await hubspotClient.crm.contacts.basicApi.getPage(
      limit,
      after,
      properties
    );

    all.push(...res.results);

    if (!res.paging || !res.paging.next) break;
    after = res.paging.next.after;
    console.log(`...fetched ${all.length} so far`);
  }

  console.log(`✅ Finished fetching ${all.length} contacts`);

  return all.map(c => {
    const p = c.properties || {};
    return {
      id: c.id,
      email: (p.email || '').toLowerCase(),
      firstname: p.firstname || '',
      lastname: p.lastname || '',
      phone: p.phone || ''
    };
  });
}

function readCsv(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", row => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", err => reject(err));
  });
}

async function loadImportMemberships() {
  if (!fs.existsSync(IMPORTS_DIR)) {
    console.log("ℹ️ No imports/ folder found");
    return new Map();
  }

  const files = fs.readdirSync(IMPORTS_DIR).filter(f => f.endsWith(".csv"));
  const importMap = new Map();

  console.log("📂 Reading import list CSVs...");

  for (const file of files) {
    const fp = path.join(IMPORTS_DIR, file);
    const tag = path.basename(file, ".csv");

    console.log(`  → Processing ${file}`);
    const rows = await readCsv(fp);

    for (const row of rows) {
      const email = String(row.email || row.Email || row.EMAIL || "").toLowerCase();
      if (!email) continue;

      if (!importMap.has(email)) importMap.set(email, []);
      const arr = importMap.get(email);
      if (!arr.includes(tag)) arr.push(tag);
    }
  }

  console.log(`✅ Built import tags for ${importMap.size} emails`);
  return importMap;
}

async function writeCsv(contacts, importMap) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  const out = path.join(DATA_DIR, "contacts_with_import_history.csv");

  const rows = contacts.map(c => ({
    ...c,
    import_segments: (importMap.get(c.email) || []).join("|")
  }));

  const writer = createObjectCsvWriter({
    path: out,
    header: [
      { id: "id", title: "id" },
      { id: "email", title: "email" },
      { id: "firstname", title: "firstname" },
      { id: "lastname", title: "lastname" },
      { id: "phone", title: "phone" },
      { id: "import_segments", title: "import_segments" }
    ]
  });

  await writer.writeRecords(rows);
  console.log("📁 Wrote:", out);
}

// ----- main -----
async function main() {
  const cmd = process.argv[2];

  if (cmd === "export-with-imports") {
    const contacts = await fetchAllContacts();
    const importMap = await loadImportMemberships();
    await writeCsv(contacts, importMap);
    console.log("✨ Done!");
    return;
  }

  console.log(`
Usage:
  node cli.js export-with-imports
`);
}

main();

