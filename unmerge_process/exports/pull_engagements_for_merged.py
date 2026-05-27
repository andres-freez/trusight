"""Pulls every engagement (email, call, meeting, note, task) attached to any
primary_vid in output/merged_contacts.csv and writes one row per engagement
to output/engagements_for_merged.csv.

Single responsibility: pull the engagement timeline for the merged-primary
set. No classification of which engagements belong to whom -- that is the
analysis layer's job.

CSV columns (output/engagements_for_merged.csv):
    engagement_id
    engagement_type           -- email | call | meeting | note | task
    primary_vid               -- current owning contact
    timestamp
    from_email                -- where applicable
    to_emails                 -- semicolon-separated, where applicable
    subject_or_title
    created_before_merge      -- bool, computed against merge_cutoff_date

Reads input from output/merged_contacts.csv. Reuses auth, session, retry,
and pagination helpers from hubspot_export.py. Engagement types pulled and
batch sizes are read from config/settings.yaml.
Idempotent: overwrites the CSV on each run.
"""


def main() -> None:
    raise NotImplementedError("Scaffolding only. Awaiting approval before adding pull logic.")


if __name__ == "__main__":
    main()
