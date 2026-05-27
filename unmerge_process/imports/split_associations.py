"""[PHASE 2 -- SCAFFOLD ONLY] Move associations that should follow the secondary.

Single responsibility: read output/associations.csv and
output/recreated_contacts.csv. For company / deal / ticket associations
that should follow the recreated secondary contact (decision rule TBD --
likely owner + timestamp + company-domain match), detach from primary_vid
and attach to the new vid.

Reads:  output/associations.csv, output/recreated_contacts.csv
Writes: output/split_associations.csv
        Columns: associated_object_type, associated_object_id,
                 from_primary_vid, to_new_vid, status, error_message

Idempotent: skip if the association already exists on the new vid.
This file is a stub. Phase 2 work begins only after the candidate list has
been reviewed and the split-decision rule has been finalized.
"""


def main() -> None:
    raise NotImplementedError("Phase 2 -- pending candidate review")


if __name__ == "__main__":
    main()
