-- Contact Email Summary
-- Compares the original contacts export against the current merged-record export
-- to surface total emails, unique emails, and emails lost during merges.
--
-- The current export has both v3 property columns (email, hs_additional_emails)
-- and v1 identity columns (identity_primary_email, identity_additional_emails).
-- We use the identity columns as the source of truth (matches HubSpot UI).
--
-- Sources:
--   original: data/contacts_export.csv (full export with all emails)
--   current:  data/csv/contacts/contacts_property_export_merged_record_ids.csv (post-merge state)

-- 1) High-level counts
WITH current_all_emails AS (
    SELECT TRIM(UNNEST(
        string_split(
            CASE
                WHEN identity_additional_emails IS NOT NULL AND identity_additional_emails != ''
                THEN identity_primary_email || ';' || identity_additional_emails
                ELSE identity_primary_email
            END,
            ';'
        )
    )) AS email
    FROM read_csv_auto('data/csv/contacts/contacts_property_export_merged_record_ids.csv')
    WHERE identity_primary_email IS NOT NULL AND identity_primary_email != ''
),
current_unique AS (
    SELECT DISTINCT email FROM current_all_emails WHERE email != ''
),
original_emails AS (
    SELECT DISTINCT email
    FROM read_csv_auto('data/contacts_export.csv')
    WHERE email IS NOT NULL AND email != ''
)

SELECT
    (SELECT COUNT(*) FROM original_emails)   AS unique_emails_original,
    (SELECT COUNT(*) FROM current_unique)     AS unique_emails_current,
    (SELECT COUNT(*)
       FROM original_emails o
      WHERE o.email NOT IN (SELECT email FROM current_unique)
    ) AS emails_lost;


-- 2) List of lost emails (in original, not in current identity primary + additional)
WITH current_all_emails AS (
    SELECT TRIM(UNNEST(
        string_split(
            CASE
                WHEN identity_additional_emails IS NOT NULL AND identity_additional_emails != ''
                THEN identity_primary_email || ';' || identity_additional_emails
                ELSE identity_primary_email
            END,
            ';'
        )
    )) AS email
    FROM read_csv_auto('data/csv/contacts/contacts_property_export_merged_record_ids.csv')
    WHERE identity_primary_email IS NOT NULL AND identity_primary_email != ''
),
current_unique AS (
    SELECT DISTINCT email FROM current_all_emails WHERE email != ''
)

SELECT
    o.*
FROM read_csv_auto('data/contacts_export.csv') o
WHERE o.email IS NOT NULL
  AND o.email != ''
  AND o.email NOT IN (SELECT email FROM current_unique)
ORDER BY o.email;
