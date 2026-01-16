// hubspot-import-cli.js
//
// Usage:
//   node hubspot-import-cli.js update         (dry-run)
//   node hubspot-import-cli.js delete         (dry-run)
//   node hubspot-import-cli.js merge          (dry-run)
//   node hubspot-import-cli.js all            (dry-run)
//
// Add --apply to actually write to HubSpot:
//   node hubspot-import-cli.js update --apply
//   node hubspot-import-cli.js delete --apply
//   node hubspot-import-cli.js merge  --apply
//   node hubspot-import-cli.js all    --apply
//
// Optional:
//   --limit=500   (limit rows per action)

require("dotenv").config();
const hubspot = require("@hubspot/api-client");
const { BigQuery } = require("@google-cloud/bigquery");

// -------------------- ENV --------------------
const HUBSPOT_TOKEN = process.env.HUBSPOT_PERSONAL_ACCESS_KEY;

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID;
const BQ_LOCATION = process.env.BQ_LOCATION || "US";

const BQ_UPDATE_TABLE = process.env.BQ_UPDATE_TABLE;
const BQ_DELETE_TABLE = process.env.BQ_DELETE_TABLE;
const BQ_MERGE_TABLE  = process.env.BQ_MERGE_TABLE;

if (!HUBSPOT_TOKEN) throw new Error("Missing HUBSPOT_PERSONAL_ACCESS_KEY in .env");
if (!BQ_PROJECT_ID) throw new Error("Missing BQ_PROJECT_ID in .env");

// -------------------- CLI ARGS --------------------
const cmd = process.argv[2];
const APPLY = process.argv.includes("--apply");

const limitArg = process.argv.find(a => a.startsWith("--limit="));
const LIMIT = limitArg ? parseInt(limitArg.split("=")[1], 10) : null;

if (!cmd || !["update", "delete", "merge", "all"].includes(cmd)) {
  console.log("Usage:");
  console.log("  node hubspot-import-cli.js update|delete|merge|all [--apply] [--limit=500]");
  process.exit(0);
}

// -------------------- HELPERS --------------------
function s(v) { return (v ?? "").toString().trim(); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function bqQuery(bigquery, query) {
  const [job] = await bigquery.createQueryJob({ query, location: BQ_LOCATION });
  const [rows] = await job.getQueryResults();
  return rows;
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/**
 * HubSpot Merge endpoint:
 * POST /crm/v3/objects/contacts/merge
 * body: { primaryObjectId: "123", objectIdToMerge: "456" }
 *
 * The official client may not expose a typed helper; we call apiRequest directly.
 */
async function mergeContact(hsClient, primaryId, mergeId) {
  return hsClient.apiRequest({
    method: "POST",
    path: "/crm/v3/objects/contacts/merge",
    body: {
      primaryObjectId: primaryId,
      objectIdToMerge: mergeId
    }
  });
}

// -------------------- ACTION: UPDATE (by email) --------------------
async function runUpdate(bigquery, hsClient) {
  if (!BQ_UPDATE_TABLE) throw new Error("Missing BQ_UPDATE_TABLE in .env");

  console.log(`\n=== UPDATE (source: ${BQ_UPDATE_TABLE}) ===`);

  const query = `
    SELECT * FROM \`${BQ_UPDATE_TABLE}\`
    ${LIMIT ? `LIMIT ${LIMIT}` : ""}
  `;
  const rows = await bqQuery(bigquery, query);
  console.log(`Fetched ${rows.length} update rows`);

  // Build inputs for batch upsert (idProperty=email)
  // Only include non-empty properties so you don't overwrite with blanks.
  const inputs = rows.map(r => {
    const email = s(r.email).toLowerCase();
    const props = {};

    const maybe = (k, v) => {
      const t = s(v);
      if (t !== "") props[k] = t;
    };

    maybe("firstname", r.firstname);
    maybe("lastname", r.lastname);
    maybe("phone", r.phone);
    maybe("jobtitle", r.jobtitle);
    maybe("lifecyclestage", r.lifecyclestage);
    maybe("hs_lead_status", r.hs_lead_status);
    maybe("city", r.city);
    // These might be your custom fields or your chosen mapping:
    maybe("state", r.state);
    maybe("country", r.country);

    // If you created custom props like country_normalized/state_normalized in HubSpot, use these instead:
    // maybe("country_normalized", r.country);
    // maybe("state_normalized", r.state);

    return { email, props };
  }).filter(x => x.email);

  console.log(`Prepared ${inputs.length} upsert inputs (email required)`);

  if (!APPLY) {
    console.log("DRY RUN: not sending updates to HubSpot. Use --apply to execute.");
    return;
  }

  // Batch size: HubSpot batch endpoints typically accept up to 100 inputs
  const batches = chunk(inputs, 100);

  let done = 0;
  for (const b of batches) {
    // Convert to HubSpot batch upsert format
    const payload = {
      inputs: b.map(x => ({
        id: x.email,
        properties: x.props
      }))
    };

    try {
      // createOrUpdate using email as idProperty
      // If your client version differs, this is the correct HubSpot API concept.
      await hsClient.crm.contacts.batchApi.createOrUpdate("email", payload);
      done += b.length;
      console.log(`✅ updated ${done}/${inputs.length}`);
    } catch (err) {
      const body = err?.response?.body;
      console.error("❌ update batch error:", body ? JSON.stringify(body) : (err?.message || err));
      // continue
    }

    // gentle pacing
    await sleep(250);
  }
}

// -------------------- ACTION: DELETE (archive by id) --------------------
async function runDelete(bigquery, hsClient) {
  if (!BQ_DELETE_TABLE) throw new Error("Missing BQ_DELETE_TABLE in .env");

  console.log(`\n=== DELETE (archive) (source: ${BQ_DELETE_TABLE}) ===`);

  const query = `
    SELECT id, email, recommended_action
    FROM \`${BQ_DELETE_TABLE}\`
    ${LIMIT ? `LIMIT ${LIMIT}` : ""}
  `;
  const rows = await bqQuery(bigquery, query);
  console.log(`Fetched ${rows.length} delete rows`);

  const ids = rows.map(r => s(r.id)).filter(Boolean);
  console.log(`Prepared ${ids.length} ids to archive`);

  if (!APPLY) {
    console.log("DRY RUN: not archiving in HubSpot. Use --apply to execute.");
    return;
  }

  const batches = chunk(ids, 100);
  let done = 0;

  for (const b of batches) {
    try {
      await hsClient.crm.contacts.batchApi.archive({ inputs: b.map(id => ({ id })) });
      done += b.length;
      console.log(`✅ archived ${done}/${ids.length}`);
    } catch (err) {
      const body = err?.response?.body;
      console.error("❌ archive batch error:", body ? JSON.stringify(body) : (err?.message || err));
    }

    await sleep(250);
  }
}

// -------------------- ACTION: MERGE (primary_id + merge_these_ids[]) --------------------
async function runMerge(bigquery, hsClient) {
  if (!BQ_MERGE_TABLE) throw new Error("Missing BQ_MERGE_TABLE in .env");

  console.log(`\n=== MERGE (source: ${BQ_MERGE_TABLE}) ===`);

  const query = `
    SELECT
      merge_key,
      primary_id,
      merge_these_ids
    FROM \`${BQ_MERGE_TABLE}\`
    ${LIMIT ? `LIMIT ${LIMIT}` : ""}
  `;
  const rows = await bqQuery(bigquery, query);
  console.log(`Fetched ${rows.length} merge groups`);

  // Normalize merge_these_ids: BigQuery returns arrays already in many clients; handle string too.
  const groups = rows
    .map(r => ({
      merge_key: s(r.merge_key),
      primary_id: s(r.primary_id),
      merge_these_ids: Array.isArray(r.merge_these_ids)
        ? r.merge_these_ids.map(x => s(x)).filter(Boolean)
        : s(r.merge_these_ids).split(",").map(x => s(x)).filter(Boolean)
    }))
    .filter(g => g.primary_id && g.merge_these_ids.length);

  console.log(`Prepared ${groups.length} merge groups`);

  if (!APPLY) {
    console.log("DRY RUN: not merging in HubSpot. Use --apply to execute.");
    console.log("Example group:", groups[0] || "(none)");
    return;
  }

  let mergedPairs = 0;

  // Merge is one-call-per-pair (primary + one merge id)
  for (const g of groups) {
    for (const mergeId of g.merge_these_ids) {
      try {
        await mergeContact(hsClient, g.primary_id, mergeId);
        mergedPairs += 1;
        console.log(`✅ merged into ${g.primary_id} <- ${mergeId} (${g.merge_key}) [pairs=${mergedPairs}]`);
      } catch (err) {
        const body = err?.response?.body;
        console.error(`❌ merge error primary=${g.primary_id} merge=${mergeId}:`, body ? JSON.stringify(body) : (err?.message || err));
      }

      // merges can be rate-limited; pace more
      await sleep(400);
    }
  }
}

// -------------------- MAIN --------------------
(async () => {
  const bigquery = new BigQuery({ projectId: BQ_PROJECT_ID });
  const hsClient = new hubspot.Client({ accessToken: HUBSPOT_TOKEN });

  console.log(`Mode: ${APPLY ? "APPLY (writes to HubSpot)" : "DRY RUN (no writes)"}`);
  console.log(`Command: ${cmd}${LIMIT ? ` (limit=${LIMIT})` : ""}`);

  if (cmd === "update") await runUpdate(bigquery, hsClient);
  if (cmd === "delete") await runDelete(bigquery, hsClient);
  if (cmd === "merge")  await runMerge(bigquery, hsClient);

  if (cmd === "all") {
    // Recommended order:
    // 1) Merge first (reduces duplicate collisions)
    // 2) Update next (now updates land on the surviving record)
    // 3) Delete last (archive empties/junk after merges)
    await runMerge(bigquery, hsClient);
    await runUpdate(bigquery, hsClient);
    await runDelete(bigquery, hsClient);
  }

  console.log("\n✅ Done.");
})().catch(err => {
  const body = err?.response?.body;
  console.error("❌ Failed:", body ? JSON.stringify(body) : (err?.message || err));
  process.exit(1);
});
