// export-contact-properties.js
// Exports HubSpot CONTACT properties to CSV + JSON for later import mapping.

require("dotenv").config();
const fs = require("fs");
const path = require("path");
const hubspot = require("@hubspot/api-client");
const { createObjectCsvWriter } = require("csv-writer");

const HUBSPOT_TOKEN = process.env.HUBSPOT_PERSONAL_ACCESS_KEY;
const HUBSPOT_ACCOUNT_ID = process.env.HUBSPOT_ACCOUNT_ID;

if (!HUBSPOT_TOKEN) {
  console.error("❌ HUBSPOT_PERSONAL_ACCESS_KEY is missing in .env");
  process.exit(1);
}

console.log(`🔑 Using HubSpot account: ${HUBSPOT_ACCOUNT_ID || "(not set)"}`);

const hubspotClient = new hubspot.Client({ accessToken: HUBSPOT_TOKEN });

function ensureDataDir() {
  const dataDir = path.join(__dirname, "data");
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  return dataDir;
}

async function main() {
  console.log("🚀 Fetching HubSpot CONTACT properties...");

  // Returns an array of Property objects
  const res = await hubspotClient.crm.properties.coreApi.getAll("contacts");
  const props = Array.isArray(res) ? res : (res?.results || []);

  console.log(`✅ Found ${props.length} contact properties`);

  const dataDir = ensureDataDir();

  // Write JSON (full fidelity)
  const jsonPath = path.join(dataDir, "contact_properties.json");
  fs.writeFileSync(jsonPath, JSON.stringify(props, null, 2), "utf8");
  console.log(`📁 Wrote JSON -> ${jsonPath}`);

  // Flatten for CSV
  const rows = props.map((p) => {
    const options = Array.isArray(p.options) ? p.options : [];
    const optionValues = options.map((o) => o.value).filter(Boolean).join("|");
    const optionLabels = options.map((o) => o.label).filter(Boolean).join("|");

    return {
      name: p.name || "",
      label: p.label || "",
      description: p.description || "",
      groupName: p.groupName || "",
      type: p.type || "",
      fieldType: p.fieldType || "",
      formField: p.formField === true ? "true" : "false",
      hidden: p.hidden === true ? "true" : "false",
      readOnlyValue: p.readOnlyValue === true ? "true" : "false",
      calculated: p.calculated === true ? "true" : "false",
      externalOptions: p.externalOptions === true ? "true" : "false",
      displayOrder: (p.displayOrder ?? "").toString(),
      hasUniqueValue: p.hasUniqueValue === true ? "true" : "false",
      // For enumerations (dropdown, checkbox, radio)
      option_values: optionValues,
      option_labels: optionLabels,
      // If present
      referencedObjectType: p.referencedObjectType || "",
    };
  });

  const csvPath = path.join(dataDir, "contact_properties.csv");
  const csvWriter = createObjectCsvWriter({
    path: csvPath,
    header: [
      { id: "name", title: "name" },
      { id: "label", title: "label" },
      { id: "description", title: "description" },
      { id: "groupName", title: "groupName" },
      { id: "type", title: "type" },
      { id: "fieldType", title: "fieldType" },
      { id: "formField", title: "formField" },
      { id: "hidden", title: "hidden" },
      { id: "readOnlyValue", title: "readOnlyValue" },
      { id: "calculated", title: "calculated" },
      { id: "externalOptions", title: "externalOptions" },
      { id: "displayOrder", title: "displayOrder" },
      { id: "hasUniqueValue", title: "hasUniqueValue" },
      { id: "option_values", title: "option_values" },
      { id: "option_labels", title: "option_labels" },
      { id: "referencedObjectType", title: "referencedObjectType" },
    ],
  });

  await csvWriter.writeRecords(rows);
  console.log(`📁 Wrote CSV  -> ${csvPath}`);

  console.log("\n✅ Done. Send me contact_properties.csv (or JSON) and I’ll map your import columns to real HubSpot property names.");
}

main().catch((err) => {
  const body = err?.response?.body;
  console.error("❌ Error exporting contact properties");
  if (body) console.error(JSON.stringify(body, null, 2));
  else console.error(err);
  process.exit(1);
});
