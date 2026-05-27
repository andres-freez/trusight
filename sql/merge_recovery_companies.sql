-- merge_recovery_companies.sql
-- Companies version of the merge recovery analysis.
--
-- Baseline:      data/companies.csv               (pre-merge snapshot — still has both primaries and duplicates)
-- Merge history: data/merge_log_companies.csv     (merges that executed successfully; errors ignored)
-- Deal source:   data/csv/deals/deals_property_export_20260421_081424.csv
--
-- Status logic for each baseline company:
--   - merged      : Record ID appears as duplicate_record_id in merge log with status='merged'
--   - in hubspot  : otherwise (still present post-merge)
--
-- Run: duckdb < sql/merge_recovery_companies.sql
-- Output: data/csv/companies/merge_recovery_companies.csv

COPY (
WITH baseline AS (
    SELECT
        "Record ID"    AS company_id,
        "Company Name" AS company_name,
        "Domain"       AS domain,
        "Create Date"  AS create_date
    FROM read_csv_auto('data/companies.csv')
),

merged_log AS (
    SELECT
        duplicate_record_id AS merged_id,
        primary_record_id   AS surviving_company_id
    FROM read_csv_auto('data/merge_log_companies.csv')
    WHERE status = 'merged'
),

deals AS (
    SELECT * FROM read_csv_auto('data/csv/deals/deals_property_export_20260421_081424.csv')
),

deals_by_company AS (
    SELECT
        TRY_CAST(TRIM(value) AS BIGINT) AS company_id,
        deal_id,
        amount
    FROM deals, unnest(string_split(associated_companies_ids, ';')) AS t(value)
    WHERE closedate IS NOT NULL
      AND associated_companies_ids IS NOT NULL
      AND associated_companies_ids != ''
),

deal_totals AS (
    SELECT
        company_id,
        COUNT(DISTINCT deal_id) AS closed_won_deals_count,
        SUM(amount)             AS closed_won_deals_amount
    FROM deals_by_company
    WHERE company_id IS NOT NULL
    GROUP BY company_id
),

classified AS (
    SELECT
        b.company_id,
        b.company_name,
        b.domain,
        b.create_date,
        CASE
            WHEN m.merged_id IS NOT NULL THEN 'merged'
            ELSE 'in hubspot'
        END AS status,
        m.surviving_company_id AS merged_into_company_id
    FROM baseline b
    LEFT JOIN merged_log m ON b.company_id = m.merged_id
)

SELECT
    c.company_id,
    c.company_name,
    c.domain,
    c.create_date,
    c.status,
    c.merged_into_company_id,
    COALESCE(dt.closed_won_deals_count, 0)  AS closed_won_deals_count,
    COALESCE(dt.closed_won_deals_amount, 0) AS closed_won_deals_amount
FROM classified c
LEFT JOIN deal_totals dt ON c.company_id = dt.company_id
ORDER BY
    CASE c.status
        WHEN 'merged'     THEN 1
        WHEN 'in hubspot' THEN 2
    END,
    c.company_id
) TO 'data/csv/companies/merge_recovery_companies.csv' (HEADER, DELIMITER ',');
