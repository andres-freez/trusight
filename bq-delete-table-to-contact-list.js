/**
 * bq-delete-table-to-contact-list.js
 *
 * Reads contact IDs from BQ_DELETE_TABLE and writes them into a BigQuery "contact list" table.
 *
 * Usage:
 *   node bq-delete-table-to-contact-list.js --listName="TruSight_Delete_Candidates" [--limit=5000]
 *
 * .env required:
 *   HUBSPOT_PERSONAL_ACCESS_KEY=pat-...
 *   BQ_PROJECT_ID=trusight-480718
 *   BQ_LOCATION=US
 *   BQ_DELETE_TABLE=trusight-480718.hubspot.contacts_delete_candidates   (example)
 *   BQ_DATASET=hubspot_exports
 *   BQ_LIST_TABLE=contact_lists
 */

require("dotenv").config();
const hubspot = require("@hubspot/api-client");
const { BigQuery } = require("@google-cloud/bigquery");

// -------------------- ENV --------------------
const HUBSPOT_TOKEN = process.env.HUBSPOT_PERSONAL_ACCESS_KEY;
const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID;
const BQ_LOCATION = process.env.BQ_LOCATION || "US";

const BQ_DELETE_TABLE = process.env.BQ_DELETE_TABLE;

const BQ_DATASET = process.env.BQ_DATASET;
const BQ_LIST_TABLE = process.env.BQ_LIST_TABLE;

if (!BQ_PROJECT_ID) throw new Error("Missing BQ_PROJECT_ID in .env");
if (!BQ_DELETE_TABLE) throw new Error("Missing BQ_DELETE_TABLE in .env");
if (!BQ_DATASET) throw new Error("Missing BQ_DATASET in .env");
if (!BQ_LIST_TABLE) throw new Error("Missing BQ_LIST_TABLE in .env");

// HubSpot token is optional unless you want enrichment
const hsClient = HUBSPOT_TOKEN ? new hubspot.Client({ accessToken: HUBSPOT_TOKEN }) : null;

// -------------------- CLI ARGS --------------------
const args = process.argv.slice(2);
const getArg = (name) => {
  const a = args.find((x) => x.startsWith(`${name}=`));
  return a ? a.split("=").slice(1).join("=") : null;
};

const listName = getArg("--listName") || "BQ_DELETE_TABLE_IDS";
const limitArg = getArg("--limit");
const LIMIT = limitArg ? parseInt(limitArg, 10) : null;

const ENRICH = args.includes("--enrich"); // if present, fetch email from HubSpot

// -------------------- HELPERS --------------------
function s(v) { return (v ?? "").toString().trim(); }
function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function bqQuery(bigquery, query) {
  const [job] = await bigquery.createQueryJob({ query, location: BQ_LOCATION });
  const [rows] = await job.getQueryResults();
  return rows;
}

async function ensureBQ(bigquery) {
  const dataset = bigquery.dataset(BQ_DATASET);
  const [datasetExists] = await dataset.exists();
  if (!datasetExists) {
    await dataset.create({ location: BQ_LOCATION });
    console.log(`✅ Created dataset: ${BQ_DATASET}`);
  }

  const table = dataset.table(BQ_LIST_TABLE);
  const [tableExists] = await table.exists();
  if (!tableExists) {
    const schema = [
      { name: "list_name", type: "STRING", mode: "REQUIRED" },
      { name: "source_table", type: "STRING", mode: "REQUIRED" },
      { name: "contact_id", type: "STRING", mode: "REQUIRED" },
      { name: "email", type: "STRING", mode: "NULLABLE" },
      { name: "recommended_action", type: "STRING", mode: "NULLABLE" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
    ];
    await table.create({ schema });
    console.log(`✅ Created table: ${BQ_DATASET}.${BQ_LIST_TABLE}`);
  }
  return table;
}

async function insertRows(table, rows) {
  if (!rows.length) return;
  for (const batch of chunk(rows, 500)) {
    try {
      await table.insert(batch);
      console.log(`✅ inserted ${batch.length} rows`);
    } catch (err) {
      console.error("❌ BigQuery insert error:", err?.message || err);
      if (err?.errors) console.error(JSON.stringify(err.errors, null, 2));
    }
  }
}

// -------------------- MAIN --------------------
(async () => {
  const bigquery = new BigQuery({ projectId: BQ_PROJECT_ID });
  const outTable = await ensureBQ(bigquery);

  // ✅ THIS is the "gets contact ids" part:
  const query = `
    SELECT
      CAST(id AS STRING) AS contact_id,
      CAST(email AS STRING) AS email,
      CAST(recommended_action AS STRING) AS recommended_action
    FROM \`${BQ_DELETE_TABLE}\`
    WHERE id IS NOT NULL
    ${LIMIT ? `LIMIT ${LIMIT}` : ""}
  `;

  const rows = await bqQuery(bigquery, query);
  const ids = rows.map(r => s(r.contact_id)).filter(Boolean);

  console.log(`Fetched ${ids.length} contact ids from ${BQ_DELETE_TABLE}`);

  // Optional: enrich email from HubSpot (only if you don't trust BQ email)
  let idToEmail = new Map();
  if (ENRICH) {
    if (!hsClient) throw new Error("Missing HUBSPOT_PERSONAL_ACCESS_KEY in .env (needed for --enrich)");

    console.log("Enriching emails from HubSpot...");
    for (const batch of chunk(ids, 100)) {
      try {
        const payload = {
          inputs: batch.map(id => ({ id })),
          properties: ["email"]
        };
        const res = await hsClient.crm.contacts.batchApi.read(payload);
        for (const c of (res?.results || [])) {
          idToEmail.set(s(c.id), s(c.properties?.email || ""));
        }
      } catch (err) {
        console.error("❌ enrichment error:", err?.response?.body ? JSON.stringify(err.response.body) : (err?.message || err));
      }
      await sleep(150);
    }
  }

  const pulledAt = new Date().toISOString();

  const outRows = rows
    .map(r => {
      const contact_id = s(r.contact_id);
      if (!contact_id) return null;

      const emailFromBQ = s(r.email);
      const emailFromHS = s(idToEmail.get(contact_id));
      const email = (ENRICH ? (emailFromHS || emailFromBQ) : (emailFromBQ || null));

      return {
        list_name: listName,
        source_table: BQ_DELETE_TABLE,
        contact_id,
        email: email || null,
        recommended_action: s(r.recommended_action) || null,
        pulled_at: pulledAt
      };
    })
    .filter(Boolean);

  await insertRows(outTable, outRows);

  console.log(`\n✅ Done. Wrote ${outRows.length} rows into ${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_LIST_TABLE}`);
})().catch(err => {
  console.error("❌ Failed:", err?.message || err);
  process.exit(1);
});
