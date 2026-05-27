"""Pulls every primary contact whose hs_merged_object_ids is non-empty
and writes one row per primary to output/merged_contacts.csv.

Single responsibility: produce the seed list of merged primaries that all
downstream pulls and analyses key off. No transformation beyond column
selection and rename.

CSV columns (output/merged_contacts.csv):
    primary_vid
    primary_email
    primary_name
    merged_vids            -- semicolon-separated, parsed from hs_merged_object_ids
    merged_count
    additional_emails      -- semicolon-separated, from hs_additional_emails
    last_modified_date
    created_date

Reuses auth, session, and retry helpers from hubspot_export.py.
Idempotent: overwrites the CSV on each run.
"""


def main() -> None:
    raise NotImplementedError("Scaffolding only. Awaiting approval before adding pull logic.")


if __name__ == "__main__":
    main()
