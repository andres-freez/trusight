"""
Generate a one-page PDF report analyzing fully deleted and merged contacts
from the merge recovery query. Includes a roadmap for identifying and
recovering incorrect merges.

Run: python misc/merge_recovery_report.py
Output: data/csv/contacts/merge_recovery_report.pdf
"""

import weasyprint

html = """
<!DOCTYPE html>
<html>
<head>
<style>
  @page {
    size: letter;
    margin: 0.6in 0.7in;
  }
  body {
    font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
    font-size: 10px;
    color: #1a1a1a;
    line-height: 1.4;
  }
  h1 {
    font-size: 18px;
    margin: 0 0 2px 0;
    color: #111;
  }
  .subtitle {
    font-size: 10px;
    color: #666;
    margin: 0 0 14px 0;
  }
  h2 {
    font-size: 13px;
    margin: 12px 0 6px 0;
    color: #222;
    border-bottom: 1.5px solid #ddd;
    padding-bottom: 3px;
  }
  h3 {
    font-size: 11px;
    margin: 8px 0 4px 0;
    color: #333;
  }
  .metrics {
    display: flex;
    gap: 16px;
    margin: 8px 0;
  }
  .metric-box {
    background: #f7f7f7;
    border: 1px solid #ddd;
    border-radius: 6px;
    padding: 8px 14px;
    text-align: center;
    flex: 1;
  }
  .metric-box .number {
    font-size: 22px;
    font-weight: 700;
    color: #111;
    display: block;
  }
  .metric-box .label {
    font-size: 9px;
    color: #666;
    text-transform: uppercase;
    letter-spacing: 0.3px;
  }
  .metric-box.red { border-left: 4px solid #d94040; }
  .metric-box.orange { border-left: 4px solid #e8860c; }
  .metric-box.green { border-left: 4px solid #2a9d2a; }
  .metric-box.blue { border-left: 4px solid #3b82f6; }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 9px;
    margin: 4px 0 8px 0;
  }
  th {
    background: #f0f0f0;
    text-align: left;
    padding: 4px 6px;
    font-weight: 600;
    border-bottom: 1.5px solid #ccc;
  }
  td {
    padding: 3px 6px;
    border-bottom: 1px solid #eee;
  }
  tr:nth-child(even) td { background: #fafafa; }
  .roadmap-step {
    margin: 4px 0;
    padding: 5px 8px;
    background: #f9f9f9;
    border-left: 3px solid #3b82f6;
    border-radius: 0 4px 4px 0;
  }
  .roadmap-step.done {
    border-left-color: #2a9d2a;
    background: #f0faf0;
  }
  .roadmap-step.active {
    border-left-color: #e8860c;
    background: #fff8f0;
  }
  .roadmap-step .step-title {
    font-weight: 700;
    font-size: 10px;
  }
  .roadmap-step .step-desc {
    font-size: 9px;
    color: #444;
    margin-top: 1px;
  }
  .badge {
    display: inline-block;
    font-size: 8px;
    font-weight: 700;
    padding: 1px 6px;
    border-radius: 3px;
    text-transform: uppercase;
    margin-left: 6px;
    vertical-align: middle;
  }
  .badge.complete { background: #dcfce7; color: #166534; }
  .badge.in-progress { background: #fef3c7; color: #92400e; }
  .badge.upcoming { background: #e0e7ff; color: #3730a3; }
  .flag { color: #d94040; font-weight: 600; }
  .note {
    font-size: 8.5px;
    color: #888;
    margin-top: 6px;
    font-style: italic;
  }
</style>
</head>
<body>

<h1>Contact Merge Recovery Analysis</h1>
<p class="subtitle">HubSpot CRM Data Audit &mdash; April 16, 2026 &mdash; TruSight LLC</p>

<!-- METRICS -->
<div class="metrics">
  <div class="metric-box green">
    <span class="number">98,486</span>
    <span class="label">In HubSpot</span>
  </div>
  <div class="metric-box orange">
    <span class="number">13,534</span>
    <span class="label">Merged</span>
  </div>
  <div class="metric-box red">
    <span class="number">1,515</span>
    <span class="label">Fully Deleted</span>
  </div>
</div>

<!-- FULLY DELETED ANALYSIS -->
<h2>Fully Deleted Contacts</h2>

<h3>With Email (16 contacts) &mdash; Recoverable</h3>
<table>
  <tr><th>ID</th><th>First</th><th>Last</th><th>Email</th></tr>
  <tr><td>10867</td><td>Jonathan</td><td>Chou</td><td>jchou@rivanaequity.com</td></tr>
  <tr><td>51756</td><td>Frank</td><td>Rubin</td><td>frank@amerivestgroup.com</td></tr>
  <tr><td>1514301</td><td>Cameron</td><td>Smith</td><td>jtsi@att.net</td></tr>
  <tr><td>2517301</td><td>Willie</td><td>Banks</td><td>wbanks@teccomposites.com</td></tr>
  <tr><td>2518103</td><td>Nick</td><td>Zsoka</td><td>mkzsoka@yahoo.com</td></tr>
  <tr><td>3392384</td><td>Simon</td><td>Restrepo</td><td>simon.restrepo@onetoonecf.com</td></tr>
  <tr><td>64462240840</td><td>Simon</td><td>R Barth</td><td>simon.restrepo@onetoonecf.com.co</td></tr>
  <tr><td>157404900616</td><td></td><td></td><td>andrew@imcd.com</td></tr>
  <tr><td>158174079053</td><td></td><td></td><td>info@marketingcode.com</td></tr>
  <tr><td>159897885715</td><td>Doug</td><td>Keller</td><td>info@neverinthedark.com</td></tr>
  <tr><td>159915688608</td><td>Rene A.</td><td>Lachapelle Jr.</td><td>rlachapelle@ralcoelectric.com</td></tr>
  <tr><td>162961409635</td><td></td><td></td><td>shali.goradia@gmail.com</td></tr>
  <tr><td>183407737631</td><td></td><td></td><td>qls-hvac@sbcglobal.net</td></tr>
  <tr><td>183430335065</td><td></td><td></td><td>360degreesbts@gmail.com</td></tr>
  <tr><td>183450433305</td><td></td><td></td><td>info@subzerocommercial.com</td></tr>
  <tr><td>183451672915</td><td></td><td></td><td>office@tmcplumbingllc.com</td></tr>
</table>

<h3>Without Email (1,499 contacts) &mdash; Limited Recovery</h3>
<p>These contacts have no email on file. Recovery depends on matching name + company from saved metadata in the old export. A sample is available in the full CSV export.</p>

<!-- MERGED ANALYSIS -->
<h2>Merged Contacts</h2>
<div class="metrics">
  <div class="metric-box orange">
    <span class="number">12,868</span>
    <span class="label">Merged with Email</span>
  </div>
  <div class="metric-box red">
    <span class="number">666</span>
    <span class="label">Merged without Email</span>
  </div>
</div>
<p>All merged contact metadata &mdash; names, emails, and company associations &mdash; has been preserved in the baseline export. This gives us everything we need to cross-check surviving records and catch anything that doesn't add up.</p>

<!-- ROADMAP -->
<h2>Roadmap: Catching &amp; Fixing Bad Merges</h2>

<div class="roadmap-step done">
  <div class="step-title">Phase 0 &mdash; Recovery Baseline Secured <span class="badge complete">Complete</span></div>
  <div class="step-desc">Full metadata snapshot of all contacts and companies exported and saved before any cleanup began. This is our safety net &mdash; every name, email, company association, and deal link preserved as the single source of truth.</div>
</div>

<div class="roadmap-step done">
  <div class="step-title">Phase 1 &mdash; Manually Restored Deleted Contacts <span class="badge complete">Complete</span></div>
  <div class="step-desc">Walked through the fully deleted list and brought back the ones that mattered. The 16 contacts with emails have been addressed. The 1,499 without emails were reviewed against the baseline for any high-value records worth recreating.</div>
</div>

<div class="roadmap-step active">
  <div class="step-title">Phase 2 &mdash; Analyzing What's Left <span class="badge in-progress">In Progress</span></div>
  <div class="step-desc">This is where we are right now. We've mapped every old contact to its current state: still in HubSpot, merged into another record, or gone entirely. The numbers are clean &mdash; 98,486 confirmed active, 13,534 merged, 1,515 fully deleted. Now we dig into the 13,534 merges to find the ones that don't look right.</div>
</div>

<div class="roadmap-step">
  <div class="step-title">Phase 3 &mdash; Flag Suspicious Merges <span class="badge upcoming">Friday 4/18</span></div>
  <div class="step-desc">Run every merged contact through three checks against its surviving record. <span class="flag">Contact name doesn't match</span> &mdash; fuzzy name comparison flags pairs where the names aren't the same person. <span class="flag">Same name, different company</span> &mdash; names line up but company associations don't, suggesting two separate people got combined. Results land in a review-ready CSV with flag reasons for each row. Confirm approach and thresholds Friday.</div>
</div>

<div class="roadmap-step">
  <div class="step-title">Phase 4 &mdash; Manual Review &amp; Sign-Off <span class="badge upcoming">Upcoming</span></div>
  <div class="step-desc">Stakeholders walk the flagged list and mark each merge as "looks good" or "needs to be split." Columns include: merged ID, surviving ID, old name vs. surviving name, old company vs. surviving company, and the specific flag that caught it. No guesswork &mdash; every decision has the data right next to it.</div>
</div>

<div class="roadmap-step">
  <div class="step-title">Phase 5 &mdash; Recovery Execution <span class="badge upcoming">Upcoming</span></div>
  <div class="step-desc">For every confirmed bad merge: recreate the contact in HubSpot using the saved baseline metadata &mdash; name, email, company. Re-link deals and company associations from the original export. Log every recreated record with an old ID &rarr; new ID mapping so nothing falls through the cracks.</div>
</div>

<p class="note">Report generated from contacts_export.csv (old) vs. contacts_property_export_merged_record_ids_20260416_213348.csv (current). Full query: sql/merge_recovery.sql</p>

</body>
</html>
"""

output_path = "data/csv/contacts/merge_recovery_report.pdf"
weasyprint.HTML(string=html).write_pdf(output_path)
print(f"PDF saved to {output_path}")
