"""Pulls merge events from the HubSpot audit log and writes one row per
merge event to output/audit_log_merges.csv.

Single responsibility: when available, capture who performed each merge and
when, so candidate review can attribute merges to their actor. The audit
log API is **Enterprise only** -- if the portal does not have access, this
script must skip gracefully (log a warning, write the CSV with the header
row only, exit 0).

CSV columns (output/audit_log_merges.csv):
    merge_timestamp
    primary_vid
    secondary_vid
    performed_by_user
    performed_by_email

Reuses auth and retry helpers from hubspot_export.py. Cutoff date for the
audit window is sourced from config/settings.yaml::merge_cutoff_date.
Idempotent: overwrites the CSV on each run.
"""


def main() -> None:
    raise NotImplementedError("Scaffolding only. Awaiting approval before adding pull logic.")


if __name__ == "__main__":
    main()
