"""[PHASE 2 -- SCAFFOLD ONLY] Reassign engagements from primary to recreated secondary.

Single responsibility: read output/engagements_for_merged.csv and
output/recreated_contacts.csv. For each engagement whose from_email or
to_emails reference the recovered secondary email, update the engagement's
contact association from primary_vid to the new vid.

Reads:  output/engagements_for_merged.csv, output/recreated_contacts.csv
Writes: output/reassigned_engagements.csv
        Columns: engagement_id, from_primary_vid, to_new_vid, status, error_message

Idempotent: skip engagements whose association already points at the new vid.
This file is a stub. Phase 2 work begins only after the candidate list has
been reviewed.
"""


def main() -> None:
    raise NotImplementedError("Phase 2 -- pending candidate review")


if __name__ == "__main__":
    main()
