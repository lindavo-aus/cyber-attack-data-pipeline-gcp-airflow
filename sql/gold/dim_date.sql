-- Layer: Gold
-- Type: Dimension
-- Description: Date dimension table generated from incident, discovery, and disclosure dates

CREATE OR REPLACE TABLE `cyberproject-488907.cyber_gold.dim_date` AS
WITH all_dates AS (
  SELECT incident_date AS dt
  FROM `cyberproject-488907.cyber_silver.incidents_master_clean`
  WHERE incident_date IS NOT NULL

  UNION DISTINCT

  SELECT discovery_date AS dt
  FROM `cyberproject-488907.cyber_silver.incidents_master_clean`
  WHERE discovery_date IS NOT NULL

  UNION DISTINCT

  SELECT disclosure_date AS dt
  FROM `cyberproject-488907.cyber_silver.incidents_master_clean`
  WHERE disclosure_date IS NOT NULL
)
SELECT
  CAST(FORMAT_DATE('%Y%m%d', dt) AS INT64) AS date_key,
  dt AS full_date,
  EXTRACT(DAY FROM dt) AS day_of_month,
  EXTRACT(MONTH FROM dt) AS month_num,
  FORMAT_DATE('%B', dt) AS month_name,
  EXTRACT(QUARTER FROM dt) AS quarter_num,
  EXTRACT(YEAR FROM dt) AS year_num,
  EXTRACT(ISOWEEK FROM dt) AS week_of_year,
  EXTRACT(DAYOFWEEK FROM dt) AS day_of_week_num,
  FORMAT_DATE('%A', dt) AS day_name,
  EXTRACT(DAYOFWEEK FROM dt) IN (1, 7) AS is_weekend
FROM all_dates;