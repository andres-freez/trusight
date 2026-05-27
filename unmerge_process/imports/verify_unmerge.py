"""[PHASE 2 -- SCAFFOLD ONLY] Post-import verification report.

Single responsibility: read every Phase 2 output CSV plus the original
candidate list. For each CONFIDENT pair, confirm that:
    - a new contact exists (recreated_contacts.csv)
    - attributable engagements moved (reassigned_engagements.csv)
    - relevant associations moved (split_associations.csv)
Produce a verification CSV showing what moved, what stayed, what failed.

Reads:  output/unmerge_candidates.csv, output/recreated_contacts.csv,
        output/reassigned_engagements.csv, output/split_associations.csv
Writes: output/verification_report.csv
        Columns: primary_vid, secondary_vid, new_vid, contact_recreated,
                 engagements_moved, associations_moved, overall_status, notes

This file is a stub. Phase 2 work begins only after Phase 2 imports have run.
"""


def main() -> None:
    raise NotImplementedError("Phase 2 -- pending candidate review")


if __name__ == "__main__":
    main()
