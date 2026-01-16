// normalized_contacts.js
require("dotenv").config();

const path = require("path");
const hubspot = require("@hubspot/api-client");
const { BigQuery } = require("@google-cloud/bigquery");

const { syncHubSpotPropertiesFromBigQuery } = require(path.join(__dirname, "hubspot_update.js"));

const HUBSPOT_TOKEN = process.env.HUBSPOT_PERSONAL_ACCESS_KEY;
if (!HUBSPOT_TOKEN) {
  console.error("❌ Missing HUBSPOT_PERSONAL_ACCESS_KEY in .env");
  process.exit(1);
}

const hubspotClient = new hubspot.Client({ accessToken: HUBSPOT_TOKEN });

const bigquery = new BigQuery({
  projectId: process.env.BQ_PROJECT_ID || "trusight-480718",
});

async function main() {
  const result = await syncHubSpotPropertiesFromBigQuery({
    object: "contacts",
    sql: `
        SELECT
        id,
        country_normalized,
        state_normalized
        FROM \`trusight-480718.hubspot.contacts_clean_with_actions\`
        WHERE
        country_normalized is not null
    `,
    property: {
        country: "country_normalized",
        state: "state_normalized"
    },
    matching_id: "id",
    matchKeyType: "id",

    apply: true,
    limit:50,

    // ✅ small test

    // ✅ ok for small test only
    validateMatches: true,
    includeMissingIds: true,

    // ✅ write CSV
    outputCsvPath: process.env.LOCAL_CONTACTS_EDITS || "./data/contact_updates.csv",

    hubspotClient,
    bigqueryClient: bigquery,
  });

  console.log(JSON.stringify(result, null, 2));
}

main().catch((err) => {
  console.error("❌ Script failed");
  console.error(err?.response?.body ? JSON.stringify(err.response.body, null, 2) : err);
  process.exit(1);
});
