// hubspot_update.js
const fs = require("fs");
const path = require("path");

/** ---------- CSV helpers ---------- */
function csvEscape(v) {
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function ensureDirForFile(filePath) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function writeCsvAppend(filePath, rows, headers) {
  ensureDirForFile(filePath);
  const exists = fs.existsSync(filePath);
  const lines = [];

  if (!exists) lines.push(headers.map(csvEscape).join(","));
  for (const r of rows) lines.push(headers.map((h) => csvEscape(r[h])).join(","));

  fs.appendFileSync(filePath, lines.join("\n") + "\n", "utf8");
}

/** ---------- Concurrency helper ---------- */
async function runWithConcurrency(items, concurrency, workerFn) {
  let idx = 0;
  const workers = Array.from({ length: concurrency }, async () => {
    while (true) {
      const i = idx++;
      if (i >= items.length) break;
      await workerFn(items[i], i);
    }
  });
  await Promise.all(workers);
}

/** ---------- Checkpoint (skip already processed IDs) ---------- */
function loadCheckpoint(checkpointPath) {
  try {
    if (!checkpointPath) return new Set();
    if (!fs.existsSync(checkpointPath)) return new Set();
    const raw = fs.readFileSync(checkpointPath, "utf8");
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) return new Set();
    return new Set(arr.map(String));
  } catch {
    return new Set();
  }
}

function saveCheckpoint(checkpointPath, set) {
  if (!checkpointPath) return;
  ensureDirForFile(checkpointPath);
  const arr = Array.from(set.values());
  fs.writeFileSync(checkpointPath, JSON.stringify(arr, null, 2), "utf8");
}

/** ---------- BigQuery pagination (stream-like) ---------- */
async function fetchBigQueryPage(job, pageToken, pageSize) {
  const opts = { maxResults: pageSize };
  if (pageToken) opts.pageToken = pageToken;

  const [rows, , resp] = await job.getQueryResults(opts);
  return { rows, nextPageToken: resp?.pageToken || null };
}

/** ---------- HubSpot single update (proven path) ---------- */
async function updateSingleByObjectType(hubspotClient, object, id, properties) {
  if (object === "contacts") return hubspotClient.crm.contacts.basicApi.update(id, { properties });
  if (object === "companies") return hubspotClient.crm.companies.basicApi.update(id, { properties });
  if (object === "deals") return hubspotClient.crm.deals.basicApi.update(id, { properties });
  if (object === "tickets") return hubspotClient.crm.tickets.basicApi.update(id, { properties });
  return hubspotClient.crm.objects.basicApi.update(object, id, { properties });
}

/**
 * Sync HubSpot properties from BigQuery results, paginating through the whole table.
 * Also supports a checkpoint file so the same ID is never updated twice across reruns.
 */
async function syncHubSpotPropertiesFromBigQuery({
  object,
  sql,
  property,
  column_name,
  matching_id,
  matchKeyType = "id",
  uniqueProperty = null,

  apply = false,

  // BigQuery paging
  pageSize = 5000,

  // Update behavior
  updateBatchSize = 500,     // group size to process
  updateConcurrency = 10,    // parallel updates inside each group
  dropBlanks = true,

  // Idempotency / skip duplicates
  checkpointPath = "./data/processed_contact_ids.json",
  skipAlreadyProcessed = true,

  // Output
  outputCsvPath = null,

  hubspotClient,
  bigqueryClient,
}) {
  if (!object) throw new Error("Missing required arg: object");
  if (!sql) throw new Error("Missing required arg: sql");
  if (!property) throw new Error("Missing required arg: property");
  if (!matching_id) throw new Error("Missing required arg: matching_id");
  if (!hubspotClient) throw new Error("Missing required arg: hubspotClient");
  if (!bigqueryClient) throw new Error("Missing required arg: bigqueryClient");
  if (matchKeyType !== "id") throw new Error("This version expects matchKeyType='id' for speed + batching.");

  const multiMap = typeof property === "object" && property !== null;
  if (!multiMap && !column_name) {
    throw new Error("Missing required arg: column_name (when property is a string)");
  }

  // SQL from file if needed
  const query = sql.trim().endsWith(".sql")
    ? fs.readFileSync(path.resolve(sql), "utf8")
    : sql;

  // checkpoint set
  const processed = loadCheckpoint(checkpointPath);

  const clean = (obj) => {
    const entries = Object.entries(obj).filter(([, v]) => {
      if (v === undefined || v === null) return false;
      if (!dropBlanks) return true;
      return String(v).trim() !== "";
    });
    return Object.fromEntries(entries);
  };

  // Start BQ job
  const [job] = await bigqueryClient.createQueryJob({ query });

  let pageToken = null;
  let totalFetched = 0;
  let totalAttempted = 0;
  let totalUpdated = 0;
  let totalSkipped = 0;
  const errors = [];

  console.log("🚀 Starting BigQuery pagination...");
  console.log("📌 checkpointPath:", checkpointPath);
  console.log("📄 outputCsvPath:", outputCsvPath || "(none)");

  // Process pages
  while (true) {
    const { rows, nextPageToken } = await fetchBigQueryPage(job, pageToken, pageSize);
    pageToken = nextPageToken;
    totalFetched += rows.length;

    if (rows.length === 0) break;

    // Build updates for this page
    const batchUpdates = [];

    for (const r of rows) {
      const idRaw = r?.[matching_id];
      const id = idRaw === undefined || idRaw === null ? "" : String(idRaw).trim();
      if (!id) continue;

      // skip duplicates across whole run / reruns
      if (skipAlreadyProcessed && processed.has(id)) {
        totalSkipped += 1;
        continue;
      }

      let props;
      if (multiMap) {
        const mapped = {};
        for (const [hsProp, bqCol] of Object.entries(property)) mapped[hsProp] = r?.[bqCol];
        props = clean(mapped);
      } else {
        props = clean({ [property]: r?.[column_name] });
      }

      if (Object.keys(props).length === 0) {
        totalSkipped += 1;
        processed.add(id); // mark as processed so we don't loop forever on blanks
        continue;
      }

      batchUpdates.push({ id, properties: props });
    }

    // Process in waves of updateBatchSize
    for (let i = 0; i < batchUpdates.length; i += updateBatchSize) {
      const wave = batchUpdates.slice(i, i + updateBatchSize);
      if (wave.length === 0) continue;

      // dry run just logs
      if (!apply) {
        totalAttempted += wave.length;
        for (const w of wave) {
          if (outputCsvPath) {
            writeCsvAppend(
              outputCsvPath,
              [{
                matchVal: w.id,
                status: "DRY_RUN",
                properties_json: JSON.stringify(w.properties),
                error: "",
              }],
              ["matchVal", "status", "properties_json", "error"]
            );
          }
          processed.add(w.id);
        }
        saveCheckpoint(checkpointPath, processed);
        continue;
      }

      // apply updates with concurrency
      const waveCsvRows = [];

      await runWithConcurrency(wave, updateConcurrency, async (w) => {
        totalAttempted += 1;
        try {
          await updateSingleByObjectType(hubspotClient, object, w.id, w.properties);
          totalUpdated += 1;

          waveCsvRows.push({
            matchVal: w.id,
            status: "UPDATED",
            properties_json: JSON.stringify(w.properties),
            error: "",
          });

          processed.add(w.id);
        } catch (e) {
          const errText = e?.response?.body
            ? JSON.stringify(e.response.body)
            : (e?.message || String(e));

          errors.push({ matchVal: w.id, error: errText, properties: w.properties });

          waveCsvRows.push({
            matchVal: w.id,
            status: "ERROR",
            properties_json: JSON.stringify(w.properties),
            error: errText,
          });

          // still mark as processed so we don't hammer the same failing record forever
          processed.add(w.id);
        }
      });

      // flush CSV once per wave
      if (outputCsvPath && waveCsvRows.length) {
        writeCsvAppend(outputCsvPath, waveCsvRows, ["matchVal", "status", "properties_json", "error"]);
      }

      // checkpoint flush once per wave
      saveCheckpoint(checkpointPath, processed);

      console.log(
        `✅ Wave done: attempted=${totalAttempted}, updated=${totalUpdated}, skipped=${totalSkipped}, errors=${errors.length}, fetched=${totalFetched}`
      );
    }

    if (!pageToken) break; // no more pages
  }

  return {
    fetched: totalFetched,
    attempted: totalAttempted,
    updated: totalUpdated,
    skipped: totalSkipped,
    errors,
    checkpointPath,
    outputCsvPath: outputCsvPath || undefined,
    note: apply ? "Executed updates." : "Dry run only.",
  };
}

module.exports = { syncHubSpotPropertiesFromBigQuery };
