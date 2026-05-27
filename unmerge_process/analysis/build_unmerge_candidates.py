"""Joins the export CSVs into one row per (primary, secondary) merge pair,
applies separability classification, and writes output/unmerge_candidates.csv.

Single responsibility: produce the decision-support fact table. Pure
CSV-to-CSV; no HubSpot API calls.

For each row in merged_contacts.csv, this script expands merged_vids into
one (primary_vid, secondary_vid) pair per secondary, then enriches each pair
with:
    - secondary_email_recovered: best-guess email for the secondary, sourced
      from primary additional_emails or from property_history pre-merge values
    - engagements_attributable_to_secondary: engagements whose
      from_email/to_emails reference the recovered secondary email
    - engagements_ambiguous: engagements that match neither primary nor any
      identifiable secondary
    - separability_class + reason: imported from classify_separability and
      applied per pair using thresholds from config/settings.yaml

CSV columns (output/unmerge_candidates.csv):
    primary_vid
    secondary_vid
    secondary_email_recovered
    engagements_attributable_to_secondary
    engagements_ambiguous
    separability_class               -- CONFIDENT | PROBABLE | NOT_SEPARABLE
    reason                           -- short text explanation

Reads from output/merged_contacts.csv, output/engagements_for_merged.csv,
output/property_history.csv. Imports classification rules from
analysis/classify_separability.py.
Idempotent: overwrites the CSV on each run.
"""


def main() -> None:
    raise NotImplementedError("Scaffolding only. Awaiting approval before adding analysis logic.")


if __name__ == "__main__":
    main()
