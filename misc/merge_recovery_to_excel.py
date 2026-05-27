"""
Build an Excel workbook from the merge recovery artifacts.

Tab 1 — Contact Recovery: enriched contact CSV (status + company + closed won deals)
Tab 2 — Contacts with Closed Won: contact-level aggregate, filtered to contacts with deals
Tab 3 — Company Recovery: enriched company CSV (status + closed won deals)
Tab 4 — Companies with Closed Won: company-level aggregate, filtered to companies with deals

Run: .venv/bin/python misc/merge_recovery_to_excel.py
Output: data/csv/contacts/merge_recovery_workbook.xlsx
"""

import pandas as pd

CONTACTS_ENRICHED_CSV  = "data/csv/contacts/merge_recovery_enriched.csv"
COMPANIES_ENRICHED_CSV = "data/csv/companies/merge_recovery_companies.csv"
OUTPUT = "data/csv/contacts/merge_recovery_workbook.xlsx"

contacts           = pd.read_csv(CONTACTS_ENRICHED_CSV)
companies_enriched = pd.read_csv(COMPANIES_ENRICHED_CSV)

contacts_with_deals = (
    contacts[contacts["closed_won_deals_count"] > 0]
    .sort_values("closed_won_deals_amount", ascending=False)
)

companies_with_deals = (
    companies_enriched[companies_enriched["closed_won_deals_count"] > 0]
    .sort_values("closed_won_deals_amount", ascending=False)
)

with pd.ExcelWriter(OUTPUT, engine="openpyxl") as writer:
    contacts.to_excel(writer, sheet_name="Contact Recovery", index=False)
    contacts_with_deals.to_excel(writer, sheet_name="Contacts with Closed Won", index=False)
    companies_enriched.to_excel(writer, sheet_name="Company Recovery", index=False)
    companies_with_deals.to_excel(writer, sheet_name="Companies with Closed Won", index=False)

print(f"Workbook saved to {OUTPUT}")
print(f"  Contact Recovery:          {len(contacts):>7,} rows")
print(f"  Contacts with Closed Won:  {len(contacts_with_deals):>7,} rows")
print(f"  Company Recovery:          {len(companies_enriched):>7,} rows")
print(f"  Companies with Closed Won: {len(companies_with_deals):>7,} rows")
