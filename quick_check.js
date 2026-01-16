// missing_id_proof_test.js
require("dotenv").config();

const hubspot = require("@hubspot/api-client");

const HUBSPOT_TOKEN = process.env.HUBSPOT_PERSONAL_ACCESS_KEY;
if (!HUBSPOT_TOKEN) {
  console.error("❌ Missing HUBSPOT_PERSONAL_ACCESS_KEY in .env");
  process.exit(1);
}

const hubspotClient = new hubspot.Client({ accessToken: HUBSPOT_TOKEN });

// Put the IDs that show up in missing_ids here
const IDS_TO_TEST = [
  "3376176",
  "176740489630",
  "25382375467",
  "25393861038",
];

// Helper: try batch read exactly like validateMatches does
async function batchReadContacts(ids) {
  const res = await hubspotClient.apiRequest({
    method: "POST",
    path: "/crm/v3/objects/contacts/batch/read",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: {
      inputs: ids.map((id) => ({ id: String(id) })),
      properties: ["email", "firstname", "lastname"],
    },
  });

  // apiRequest may return parsed JSON on .body or raw object
  return res?.body || res;
}

async function checkOneId(id) {
  console.log("\n==============================");
  console.log("🔎 Testing ID:", id);

  // 1) Check v3 GET by ID
  try {
    const c = await hubspotClient.crm.contacts.basicApi.getById(String(id), ["email", "firstname"]);
    console.log("✅ v3 getById FOUND");
    console.log("   returned id:", c?.id);
    console.log("   email:", c?.properties?.email || "");
  } catch (e) {
    const status = e?.code || e?.response?.statusCode || e?.response?.status;
    console.log("❌ v3 getById FAILED", status ? `(status ${status})` : "");
    if (e?.response?.body) console.log("   body:", JSON.stringify(e.response.body, null, 2));
    else console.log("   msg:", e?.message || String(e));
  }

  // 2) Check batch/read endpoint with only this ID (mirrors validateMatches)
  try {
    const b = await batchReadContacts([String(id)]);
    const results = b?.results || [];
    console.log("📦 batch/read results length:", results.length);
    if (results[0]) {
      console.log("✅ batch/read FOUND");
      console.log("   returned id:", results[0]?.id);
      console.log("   email:", results[0]?.properties?.email || "");
    } else {
      console.log("❌ batch/read DID NOT FIND this id");
      if (b?.errors?.length) console.log("   errors:", JSON.stringify(b.errors, null, 2));
      if (b?._rawText) console.log("   raw:", b._rawText);
    }
  } catch (e) {
    console.log("❌ batch/read FAILED");
    if (e?.response?.body) console.log("   body:", JSON.stringify(e.response.body, null, 2));
    else console.log("   msg:", e?.message || String(e));
  }
}

async function main() {
  console.log("🔑 Using token from .env");
  console.log("🧪 IDs to test:", IDS_TO_TEST.join(", "));

  for (const id of IDS_TO_TEST) {
    await checkOneId(id);
  }

  console.log("\n✅ Done.");
}

main().catch((err) => {
  console.error("\n❌ missing_id_proof_test.js failed");
  console.error(err?.response?.body ? JSON.stringify(err.response.body, null, 2) : err);
  process.exit(1);
});
