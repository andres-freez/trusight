// scrape-imports-ui.js
//
// Scrapes HubSpot CRM Imports UI into CSV:
//  data/ui_imports_export.csv
//
// ⚠️ This uses browser automation. You log in with your credentials.
//     For 2FA, you may need to complete it manually in the browser window.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');
const { createObjectCsvWriter } = require('csv-writer');

const HUBSPOT_EMAIL = process.env.HUBSPOT_LOGIN_EMAIL;
const HUBSPOT_PASSWORD = process.env.HUBSPOT_LOGIN_PASSWORD;
const HUBSPOT_PORTAL_ID = process.env.HUBSPOT_PORTAL_ID; // optional, for direct URL

if (!HUBSPOT_EMAIL || !HUBSPOT_PASSWORD) {
  console.error('❌ HUBSPOT_LOGIN_EMAIL and HUBSPOT_LOGIN_PASSWORD must be set in .env');
  process.exit(1);
}

async function loginAndGoToImports(page) {
  console.log('🌐 Opening HubSpot login page...');

  await page.goto('https://app.hubspot.com/login', {
    waitUntil: 'networkidle2',
  });

  // Fill login form
  await page.type('input[name="username"]', HUBSPOT_EMAIL, { delay: 50 });
  await page.type('input[name="password"]', HUBSPOT_PASSWORD, { delay: 50 });

  await Promise.all([
    page.click('button[type="submit"]'),
    page.waitForNavigation({ waitUntil: 'networkidle2' }),
  ]);

  console.log('✅ Logged in (assuming no 2FA prompts).');

  // If portal id is known, you can go directly to imports
  // The URL may vary; adjust if needed based on your portal.
  let importsUrl;
  if (HUBSPOT_PORTAL_ID) {
    importsUrl = `https://app.hubspot.com/import/${HUBSPOT_PORTAL_ID}`;
  } else {
    // fallback: generic imports root (you may need to pick portal manually first time)
    importsUrl = 'https://app.hubspot.com/import';
  }

  console.log(`➡️ Navigating to imports page: ${importsUrl}`);
  await page.goto(importsUrl, { waitUntil: 'networkidle2' });

  // Wait for the imports table to appear. This selector may need tweaking
  await page.waitForTimeout(5000);
}

async function scrapeImports(page) {
  console.log('🔍 Scraping imports table rows...');

  // Adjust selectors if HubSpot changes their UI.
  // We'll try to be generic: find table rows inside a main grid.
  const rows = await page.$$eval('table tbody tr', (trs) => {
    return trs.map((tr) => {
      const cells = Array.from(tr.querySelectorAll('td')).map((td) =>
        td.innerText.trim()
      );

      // Try to map columns by position:
      // 0: Import name
      // 1: Type (Objects)
      // 2: Status
      // 3: Created by
      // 4: Date/Time
      return {
        importName: cells[0] || '',
        type: cells[1] || '',
        status: cells[2] || '',
        createdBy: cells[3] || '',
        dateTime: cells[4] || '',
        raw_row: cells.join(' | '),
      };
    });
  });

  console.log(`✅ Scraped ${rows.length} rows from the UI.`);
  return rows;
}

async function main() {
  const browser = await puppeteer.launch({
    headless: false, // set to true once you know login works
    defaultViewport: { width: 1280, height: 800 },
  });

  try {
    const page = await browser.newPage();
    await loginAndGoToImports(page);

    // Give time to fully render table (and you to complete 2FA if needed)
    await page.waitForTimeout(5000);

    const imports = await scrapeImports(page);

    if (!fs.existsSync('data')) {
      fs.mkdirSync('data');
    }

    const outPath = path.join('data', 'ui_imports_export.csv');

    const csvWriter = createObjectCsvWriter({
      path: outPath,
      header: [
        { id: 'importName', title: 'importName' },
        { id: 'type', title: 'type' },
        { id: 'status', title: 'status' },
        { id: 'createdBy', title: 'createdBy' },
        { id: 'dateTime', title: 'dateTime' },
        { id: 'raw_row', title: 'raw_row' },
      ],
    });

    await csvWriter.writeRecords(imports);
    console.log(`📁 Wrote ${imports.length} rows to ${outPath}`);
  } catch (err) {
    console.error('❌ Error scraping imports UI:', err);
  } finally {
    // comment this out if you want to inspect the browser afterwards
    await browser.close();
  }
}

main();
