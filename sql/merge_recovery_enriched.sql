-- merge_recovery_enriched.sql
-- Extends merge_recovery with closed won deal totals + company name/domain per contact.
--
-- Closed won = deals where closedate IS NOT NULL
-- Contact → deal link uses the new associated_contacts_ids column in the deal export
-- Company info pulled from contact's associated_companies_ids (new export) or company_ids (old export fallback)
--
-- Run: duckdb < sql/merge_recovery_enriched.sql > data/csv/contacts/merge_recovery_enriched.csv

COPY (
WITH new_export AS (
    SELECT * FROM read_csv_auto(
        'data/csv/contacts/contacts_property_export_merged_record_ids_20260416_213348.csv',
        types={'hs_merged_object_ids': 'VARCHAR'}
    )
),

old_export AS (
    SELECT * FROM read_csv_auto('data/contacts_export.csv')
),

deals AS (
    SELECT * FROM read_csv_auto('data/csv/deals/deals_property_export_20260421_081424.csv')
),

companies AS (
    SELECT
        "Record ID" AS company_id,
        "Company Name" AS company_name,
        "Domain" AS domain
    FROM read_csv_auto('data/companies.csv')
),

-- Build email lookup set for classification
all_current_emails AS (
    SELECT DISTINCT email FROM new_export WHERE email IS NOT NULL AND email != ''
    UNION
    SELECT DISTINCT TRIM(value) FROM new_export,
         unnest(string_split(hs_additional_emails, ';')) AS t(value)
    WHERE hs_additional_emails IS NOT NULL AND hs_additional_emails != ''
    UNION
    SELECT DISTINCT identity_primary_email FROM new_export
    WHERE identity_primary_email IS NOT NULL AND identity_primary_email != ''
    UNION
    SELECT DISTINCT TRIM(value) FROM new_export,
         unnest(string_split(identity_additional_emails, ';')) AS t(value)
    WHERE identity_additional_emails IS NOT NULL AND identity_additional_emails != ''
),

merged_ids AS (
    SELECT contact_id AS surviving_contact_id,
           CAST(TRIM(value) AS BIGINT) AS merged_id
    FROM new_export, unnest(string_split(hs_merged_object_ids, ';')) AS t(value)
    WHERE hs_merged_object_ids IS NOT NULL AND hs_merged_object_ids != ''
),

-- Closed won deals exploded by contact
deals_by_contact AS (
    SELECT
        CAST(TRIM(value) AS BIGINT) AS contact_id,
        deal_id,
        amount
    FROM deals, unnest(string_split(associated_contacts_ids, ';')) AS t(value)
    WHERE closedate IS NOT NULL
      AND associated_contacts_ids IS NOT NULL
      AND associated_contacts_ids != ''
),

deal_totals AS (
    SELECT
        contact_id,
        COUNT(DISTINCT deal_id) AS closed_won_deals_count,
        SUM(amount) AS closed_won_deals_amount
    FROM deals_by_contact
    GROUP BY contact_id
),

-- First company id per contact: prefer new export, fall back to old export
contact_company AS (
    SELECT
        o.id AS contact_id,
        COALESCE(
            TRY_CAST(NULLIF(TRIM(split_part(n.associated_companies_ids, ';', 1)), '') AS BIGINT),
            TRY_CAST(NULLIF(TRIM(split_part(o.company_ids, '|', 1)), '') AS BIGINT)
        ) AS company_id
    FROM old_export o
    LEFT JOIN new_export n ON o.id = n.contact_id
),

classified AS (
    SELECT
        o.id,
        o.firstname,
        o.lastname,
        o.email,
        CASE
            WHEN n.contact_id IS NOT NULL THEN 'in hubspot'
            WHEN m.merged_id IS NOT NULL THEN 'merged'
            WHEN ace.email IS NOT NULL THEN 'in hubspot'
            ELSE 'fully deleted'
        END AS status,
        m.surviving_contact_id AS merged_into_contact_id
    FROM old_export o
    LEFT JOIN new_export n ON o.id = n.contact_id
    LEFT JOIN merged_ids m ON o.id = m.merged_id
    LEFT JOIN all_current_emails ace
        ON LOWER(TRIM(o.email)) = LOWER(TRIM(ace.email))
)

SELECT
    c.id,
    c.firstname,
    c.lastname,
    c.email,
    c.status,
    c.merged_into_contact_id,
    cc.company_id,
    co.company_name,
    co.domain,
    COALESCE(dt.closed_won_deals_count, 0) AS closed_won_deals_count,
    COALESCE(dt.closed_won_deals_amount, 0) AS closed_won_deals_amount
FROM classified c
LEFT JOIN contact_company cc ON c.id = cc.contact_id
LEFT JOIN companies co       ON cc.company_id = co.company_id
LEFT JOIN deal_totals dt     ON c.id = dt.contact_id
ORDER BY
    CASE c.status
        WHEN 'fully deleted' THEN 1
        WHEN 'merged'        THEN 2
        WHEN 'in hubspot'    THEN 3
    END,
    c.id
) TO 'data/csv/contacts/merge_recovery_enriched.csv' (HEADER, DELIMITER ',');
