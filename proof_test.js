// proof_test.js
require("dotenv").config();

const hubspot = require("@hubspot/api-client");

const HUBSPOT_TOKEN = process.env.HUBSPOT_PERSONAL_ACCESS_KEY;
if (!HUBSPOT_TOKEN) {
  console.error("❌ Missing HUBSPOT_PERSONAL_ACCESS_KEY in .env");
  process.exit(1);
}

const hubspotClient = new hubspot.Client({ accessToken: HUBSPOT_TOKEN });

// Put a real HubSpot Contact ID here:
const CONTACT_ID = "68270134575";

// Pick a property that definitely exists + is writable.
// firstname is safe. You can also use a custom text property if you want.
const TEST_PROPERTY = "firstname";
const TEST_VALUE = `Test_${Date.now()}`;

async function main() {
  console.log("🔎 1) Checking token by calling /oauth/v1/access-tokens/{token} ...");

  // This endpoint returns info about the token and proves auth works.
  // Note: hubspotClient.apiRequest returns a stream sometimes; but for small JSON it’s usually fine.
  const tokenInfoRes = await hubspotClient.apiRequest({
    method: "GET",
    path: `/oauth/v1/access-tokens/${HUBSPOT_TOKEN}`,
  });

  const tokenInfo = tokenInfoRes?.body || tokenInfoRes;
  console.log("✅ Token is valid. Hub ID:", tokenInfo?.hub_id || tokenInfo?.hubId || "(unknown)");

  console.log(`\n🔎 2) Reading contact by ID: ${CONTACT_ID}`);
  const before = await hubspotClient.crm.contacts.basicApi.getById(CONTACT_ID, [TEST_PROPERTY, "email"]);
  console.log("✅ Contact exists. Email:", before?.properties?.email || "(no email)");
  console.log(`   Before ${TEST_PROPERTY}:`, before?.properties?.[TEST_PROPERTY]);

  console.log(`\n✍️  3) Updating contact ${TEST_PROPERTY} -> ${TEST_VALUE}`);
  await hubspotClient.crm.contacts.basicApi.update(CONTACT_ID, {
    properties: { [TEST_PROPERTY]: TEST_VALUE },
  });
  console.log("✅ Update call succeeded");

  console.log(`\n🔎 4) Re-reading contact to confirm update`);
  const after = await hubspotClient.crm.contacts.basicApi.getById(CONTACT_ID, [TEST_PROPERTY, "email"]);
  console.log(`✅ After ${TEST_PROPERTY}:`, after?.properties?.[TEST_PROPERTY]);

  if (after?.properties?.[TEST_PROPERTY] === TEST_VALUE) {
    console.log("\n🎉 PROOF PASSED: HubSpot connection works and contact updates succeed.");
  } else {
    console.log("\n⚠️ PROOF PARTIAL: Request succeeded but value didn't match. Check property permissions/history.");
  }
}

main().catch((err) => {
  console.error("\n❌ proof_test.js failed");
  console.error(err?.response?.body ? JSON.stringify(err.response.body, null, 2) : err);
  process.exit(1);
});
