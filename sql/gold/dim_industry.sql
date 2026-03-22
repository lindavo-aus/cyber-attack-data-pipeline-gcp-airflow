-- Layer: Gold
-- Type: Dimension
-- Description: Industry dimension table with primary and secondary industry classification

CREATE OR REPLACE TABLE `cyberproject-488907.cyber_gold.dim_industry` AS
SELECT
  ROW_NUMBER() OVER (
    ORDER BY industry_primary, industry_secondary
  ) AS industry_key,
  industry_primary,
  industry_secondary
FROM (
  SELECT DISTINCT
    industry_primary,
    industry_secondary
  FROM `cyberproject-488907.cyber_silver.incidents_master_clean`
  WHERE industry_primary IS NOT NULL
     OR industry_secondary IS NOT NULL
);