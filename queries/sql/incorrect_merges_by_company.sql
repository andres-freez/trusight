-- Incorrect Merges by Company Mismatch
-- Step-by-step breakdown to inspect the structure at each stage.
--
-- Sources:
--   original: data/contacts_export.csv (pre-merge, pipe-delimited company_ids)
--   current:  data/csv/contacts/contacts_property_export_merged_record_ids.csv
--             (post-merge, semicolon-delimited associated_companies_ids & hs_merged_object_ids)
--
-- Note: read_csv_auto with all_varchar=true to prevent DuckDB from
--       casting semicolon-delimited ID columns as BIGINT.


-- Step 1: All contacts that absorbed merges — one row per winner-loser pair
SELECT
    contact_id                  AS winner_id,
    firstname                   AS winner_firstname,
    lastname                    AS winner_lastname,
    identity_primary_email      AS winner_email,
    associated_companies_ids    AS winner_company_ids,
    TRIM(UNNEST(string_split(hs_merged_object_ids, ';'))) AS loser_id
FROM read_csv_auto('data/csv/contacts/contacts_property_export_merged_record_ids.csv', all_varchar=true)
WHERE hs_merged_object_ids IS NOT NULL
  AND hs_merged_object_ids != '';


-- Step 2: Look up each loser in the original export to see their company associations
WITH merge_pairs AS (
    SELECT
        contact_id                  AS winner_id,
        firstname                   AS winner_firstname,
        lastname                    AS winner_lastname,
        identity_primary_email      AS winner_email,
        associated_companies_ids    AS winner_company_ids,
        TRIM(UNNEST(string_split(hs_merged_object_ids, ';'))) AS loser_id
    FROM read_csv_auto('data/csv/contacts/contacts_property_export_merged_record_ids.csv', all_varchar=true)
    WHERE hs_merged_object_ids IS NOT NULL
      AND hs_merged_object_ids != ''
)
SELECT
    mp.winner_id,
    mp.winner_firstname,
    mp.winner_lastname,
    mp.winner_email,
    mp.winner_company_ids,
    mp.loser_id,
    o.email             AS loser_email,
    o.firstname         AS loser_firstname,
    o.lastname          AS loser_lastname,
    o.company_ids       AS loser_company_ids
FROM merge_pairs mp
LEFT JOIN read_csv_auto('data/contacts_export.csv', all_varchar=true) o
    ON o.id = mp.loser_id
WHERE mp.loser_id != ''
ORDER BY mp.winner_id, mp.loser_id;


-- Step 3: Flag mismatches — loser companies NOT in winner's company set
WITH merge_pairs AS (
    SELECT
        contact_id                  AS winner_id,
        firstname                   AS winner_firstname,
        lastname                    AS winner_lastname,
        identity_primary_email      AS winner_email,
        associated_companies_ids    AS winner_company_ids_raw,
        TRIM(UNNEST(string_split(hs_merged_object_ids, ';'))) AS loser_id
    FROM read_csv_auto('data/csv/contacts/contacts_property_export_merged_record_ids.csv', all_varchar=true)
    WHERE hs_merged_object_ids IS NOT NULL
      AND hs_merged_object_ids != ''
),
winner_company_set AS (
    SELECT DISTINCT
        winner_id,
        TRIM(UNNEST(string_split(winner_company_ids_raw, ';'))) AS company_id
    FROM merge_pairs
    WHERE winner_company_ids_raw IS NOT NULL AND winner_company_ids_raw != ''
),
losers AS (
    SELECT
        id               AS loser_id,
        email            AS loser_email,
        firstname        AS loser_firstname,
        lastname         AS loser_lastname,
        company_ids      AS loser_company_ids_raw
    FROM read_csv_auto('data/contacts_export.csv', all_varchar=true)
),
loser_company_set AS (
    SELECT DISTINCT
        loser_id,
        TRIM(UNNEST(string_split(loser_company_ids_raw, '|'))) AS company_id
    FROM losers
    WHERE loser_company_ids_raw IS NOT NULL AND loser_company_ids_raw != ''
)
SELECT
    mp.winner_id,
    mp.winner_firstname,
    mp.winner_lastname,
    mp.winner_email,
    mp.loser_id,
    l.loser_email,
    l.loser_firstname,
    l.loser_lastname,
    lc.company_id AS mismatched_company_id
FROM merge_pairs mp
JOIN losers l ON l.loser_id = mp.loser_id
JOIN loser_company_set lc ON lc.loser_id = mp.loser_id
WHERE mp.loser_id != ''
  AND lc.company_id NOT IN (
      SELECT wc.company_id
      FROM winner_company_set wc
      WHERE wc.winner_id = mp.winner_id
  )
ORDER BY mp.winner_id, mp.loser_id;
