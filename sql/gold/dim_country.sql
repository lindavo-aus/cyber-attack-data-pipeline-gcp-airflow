-- Layer: Gold
-- Type: Dimension
-- Description: Country dimension table based on company headquarters country

CREATE OR REPLACE TABLE `cyberproject-488907.cyber_gold.dim_country` AS
SELECT
  ROW_NUMBER() OVER (ORDER BY country_hq) AS country_key,
  country_hq
FROM (
  SELECT DISTINCT country_hq
  FROM `cyberproject-488907.cyber_silver.incidents_master_clean`
  WHERE country_hq IS NOT NULL
);