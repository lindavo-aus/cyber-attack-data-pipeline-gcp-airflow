-- Layer: Gold
-- Type: Dimension
-- Description: Company dimension table with company identity and size/revenue bands

CREATE OR REPLACE TABLE `cyberproject-488907.cyber_gold.dim_company` AS
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      company_name,
      stock_ticker,
      is_public_company,
      revenue_band,
      employee_size_band
  ) AS company_key,

  company_name,
  stock_ticker,
  is_public_company,
  revenue_band,
  employee_size_band

FROM (
  SELECT DISTINCT
    company_name,
    stock_ticker,
    is_public_company,

    CASE
      WHEN company_revenue_usd IS NULL THEN 'Unknown'
      WHEN company_revenue_usd < 10000000 THEN '< 10M'
      WHEN company_revenue_usd < 100000000 THEN '10M - <100M'
      WHEN company_revenue_usd < 1000000000 THEN '100M - <1B'
      ELSE '1B+'
    END AS revenue_band,

    CASE
      WHEN employee_count IS NULL THEN 'Unknown'
      WHEN employee_count < 100 THEN '< 100'
      WHEN employee_count < 1000 THEN '100 - 999'
      WHEN employee_count < 10000 THEN '1,000 - 9,999'
      ELSE '10,000+'
    END AS employee_size_band

  FROM `cyberproject-488907.cyber_silver.incidents_master_clean`
  WHERE company_name IS NOT NULL
);