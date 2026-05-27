"""Pulls property-change history for each primary_vid in
output/merged_contacts.csv, restricted to identity-bearing properties, and
writes one row per change to output/property_history.csv.

Single responsibility: capture how identity fields evolved on each primary,
including pre-merge values that may be the only way to recover a secondary
contact's email or name. No interpretation -- just the raw change events.

CSV columns (output/property_history.csv):
    primary_vid
    property_name        -- one of identity_properties from settings.yaml
    value
    timestamp
    source_type          -- MERGE | FORM | IMPORT | CRM_UI | API | etc.
    source_id

Reads input from output/merged_contacts.csv. Properties to track come from
config/settings.yaml::identity_properties. Reuses propertiesWithHistory
patterns from hubspot_export.py.
Idempotent: overwrites the CSV on each run.
"""


def main() -> None:
    raise NotImplementedError("Scaffolding only. Awaiting approval before adding pull logic.")


if __name__ == "__main__":
    main()
