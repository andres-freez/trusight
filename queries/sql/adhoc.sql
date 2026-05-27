-- Lost emails: fuzzy name match against current contacts
-- Finds emails from the original export that are no longer in the current export
-- (not as primary, not as additional), then matches them by firstname + lastname
-- against current contacts to see if they still exist under a different email.

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
    FROM read_csv_auto('data/csv/contacts/contacts_property_export_merged_record_ids.csv', all_varchar=true)
    WHERE identity_primary_email IS NOT NULL AND identity_primary_email != ''
),
current_unique AS (
    SELECT DISTINCT email FROM current_all_emails WHERE email != ''
),
lost AS (
    SELECT
        id,
        email       AS lost_email,
        firstname,
        lastname
    FROM read_csv_auto('data/contacts_export.csv', all_varchar=true)
    WHERE email IS NOT NULL
      AND email != ''
      AND email NOT IN (SELECT email FROM current_unique)
)

SELECT
    l.id              AS lost_contact_id,
    l.lost_email,
    l.firstname       AS lost_firstname,
    l.lastname        AS lost_lastname,
    c.contact_id      AS matched_contact_id,
    c.identity_primary_email      AS matched_email,
    c.identity_additional_emails  AS matched_additional_emails,
    c.firstname       AS matched_firstname,
    c.lastname        AS matched_lastname
FROM lost l
JOIN read_csv_auto('data/csv/contacts/contacts_property_export_merged_record_ids.csv', all_varchar=true) c
    ON LOWER(TRIM(l.firstname)) = LOWER(TRIM(c.firstname))
   AND LOWER(TRIM(l.lastname))  = LOWER(TRIM(c.lastname))
ORDER BY l.lastname, l.firstname, l.lost_email;
