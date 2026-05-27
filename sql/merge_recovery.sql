-- merge_recovery.sql
-- Compare contacts_export.csv (old) against the recent contact property export
-- to classify each old contact as: merged | in hubspot | fully deleted
--
-- Run with: duckdb < sql/merge_recovery.sql

-- Step 1: Build a distinct set of all emails currently in HubSpot
--         (union of email, hs_additional_emails, identity_primary_email, identity_additional_emails)

WITH new_export AS (
    SELECT * FROM read_csv_auto(
        'data/csv/contacts/contacts_property_export_merged_record_ids_20260416_213348.csv',
        types={'hs_merged_object_ids': 'VARCHAR'}
    )
),

old_export AS (
    SELECT * FROM read_csv_auto('data/contacts_export.csv')
),

all_current_emails AS (
    SELECT DISTINCT email AS email
    FROM new_export
    WHERE email IS NOT NULL AND email != ''

    UNION

    SELECT DISTINCT TRIM(value) AS email
    FROM new_export,
         unnest(string_split(hs_additional_emails, ';')) AS t(value)
    WHERE hs_additional_emails IS NOT NULL AND hs_additional_emails != ''

    UNION

    SELECT DISTINCT identity_primary_email AS email
    FROM new_export
    WHERE identity_primary_email IS NOT NULL AND identity_primary_email != ''

    UNION

    SELECT DISTINCT TRIM(value) AS email
    FROM new_export,
         unnest(string_split(identity_additional_emails, ';')) AS t(value)
    WHERE identity_additional_emails IS NOT NULL AND identity_additional_emails != ''
),

-- Step 2: Explode merged object IDs so we can look up old IDs that were merged

merged_ids AS (
    SELECT
        contact_id AS surviving_contact_id,
        CAST(TRIM(value) AS BIGINT) AS merged_id
    FROM new_export,
         unnest(string_split(hs_merged_object_ids, ';')) AS t(value)
    WHERE hs_merged_object_ids IS NOT NULL AND hs_merged_object_ids != ''
),

-- Step 3: Check each old contact against the three conditions

classified AS (
    SELECT
        o.id,
        o.firstname,
        o.lastname,
        o.email,
        CASE
            WHEN n.contact_id IS NOT NULL
                THEN 'in hubspot'
            WHEN m.merged_id IS NOT NULL
                THEN 'merged'
            WHEN ace.email IS NOT NULL
                THEN 'in hubspot'
            ELSE 'fully deleted'
        END AS status,
        m.surviving_contact_id AS merged_into_contact_id
    FROM old_export o
    LEFT JOIN new_export n
        ON o.id = n.contact_id
    LEFT JOIN merged_ids m
        ON o.id = m.merged_id
    LEFT JOIN all_current_emails ace
        ON LOWER(TRIM(o.email)) = LOWER(TRIM(ace.email))
)

SELECT
    id,
    firstname,
    lastname,
    email,
    status,
    merged_into_contact_id
FROM classified
ORDER BY
    CASE status
        WHEN 'fully deleted' THEN 1
        WHEN 'merged'        THEN 2
        WHEN 'in hubspot'    THEN 3
    END,
    id;
