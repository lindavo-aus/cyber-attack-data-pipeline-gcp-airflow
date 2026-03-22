-- Layer: Gold
-- Type: Fact
-- Description: Central fact table at incident grain, combining company, financial, operational, and market impact metrics

CREATE OR REPLACE TABLE `cyberproject-488907.cyber_gold.fact_cyber_incident` AS
SELECT
  i.incident_id,

  CAST(FORMAT_DATE('%Y%m%d', i.incident_date) AS INT64) AS incident_date_key,
  CAST(FORMAT_DATE('%Y%m%d', i.discovery_date) AS INT64) AS discovery_date_key,
  CAST(FORMAT_DATE('%Y%m%d', i.disclosure_date) AS INT64) AS disclosure_date_key,

  dc.company_key,
  di.industry_key,
  dco.country_key,
  da.attack_key,
  dsq.source_quality_key,

  i.company_revenue_usd,
  i.employee_count,
  i.data_compromised_records,
  i.downtime_hours,

  f.direct_loss_usd,
  f.ransom_demanded_usd,
  f.ransom_paid_usd,
  f.recovery_cost_usd,
  f.legal_fees_usd,
  f.regulatory_fine_usd,
  f.insurance_payout_usd,
  f.total_loss_usd,
  f.total_loss_lower_bound,
  f.total_loss_upper_bound,
  f.inflation_adjusted_usd,

  m.price_7d_before,
  m.price_disclosure_day,
  m.price_1d_after,
  m.price_7d_after,
  m.price_30d_after,
  m.abnormal_return_1d,
  m.abnormal_return_7d,
  m.abnormal_return_30d,
  m.car_neg1_to_pos1,
  m.car_0_to_7,
  m.car_0_to_30,
  m.car_0_to_90,
  m.days_to_price_recovery,
  m.pct_change_disclosure_to_1d,

  DATE_DIFF(i.discovery_date, i.incident_date, DAY) AS days_to_discovery,
  DATE_DIFF(i.disclosure_date, i.incident_date, DAY) AS days_to_disclosure,

  i.is_public_company,

  f.incident_id IS NOT NULL AS has_financial_impact,
  m.incident_id IS NOT NULL AS has_market_impact,

  (f.ransom_demanded_usd IS NOT NULL OR f.ransom_paid_usd IS NOT NULL) AS is_ransom_case,
  i.data_compromised_records IS NOT NULL AS is_data_breach,
  i.downtime_hours IS NOT NULL AND i.downtime_hours > 0 AS is_operational_disruption,

  CASE
    WHEN f.total_loss_usd IS NULL THEN 'Unknown'
    WHEN f.total_loss_usd < 10000000 THEN 'Low'
    WHEN f.total_loss_usd < 50000000 THEN 'Medium'
    ELSE 'High'
  END AS loss_severity_band,

  CASE
    WHEN i.data_compromised_records IS NULL THEN 'Unknown'
    WHEN i.data_compromised_records < 100000 THEN 'Low'
    WHEN i.data_compromised_records < 1000000 THEN 'Medium'
    ELSE 'High'
  END AS record_breach_band,

  CASE
    WHEN
      COALESCE(f.total_loss_usd, 0) >= 50000000
      OR COALESCE(i.data_compromised_records, 0) >= 1000000
      OR COALESCE(i.downtime_hours, 0) >= 72
    THEN 'Critical'

    WHEN
      COALESCE(f.total_loss_usd, 0) >= 10000000
      OR COALESCE(i.data_compromised_records, 0) >= 100000
      OR COALESCE(i.downtime_hours, 0) >= 24
    THEN 'High'

    WHEN
      f.total_loss_usd IS NOT NULL
      OR i.data_compromised_records IS NOT NULL
      OR i.downtime_hours IS NOT NULL
    THEN 'Medium'

    ELSE 'Low'
  END AS severity_band,

  CURRENT_TIMESTAMP() AS gold_loaded_at

FROM `cyberproject-488907.cyber_silver.incidents_master_clean` i
LEFT JOIN `cyberproject-488907.cyber_silver.financial_impact_clean` f
  ON i.incident_id = f.incident_id
LEFT JOIN `cyberproject-488907.cyber_silver.market_impact_clean` m
  ON i.incident_id = m.incident_id

LEFT JOIN `cyberproject-488907.cyber_gold.dim_company` dc
  ON i.company_name = dc.company_name
 AND IFNULL(i.stock_ticker, '') = IFNULL(dc.stock_ticker, '')
 AND IFNULL(i.is_public_company, FALSE) = IFNULL(dc.is_public_company, FALSE)
 AND (
      CASE
        WHEN i.company_revenue_usd IS NULL THEN 'Unknown'
        WHEN i.company_revenue_usd < 10000000 THEN '< 10M'
        WHEN i.company_revenue_usd < 100000000 THEN '10M - <100M'
        WHEN i.company_revenue_usd < 1000000000 THEN '100M - <1B'
        ELSE '1B+'
      END
    ) = dc.revenue_band
 AND (
      CASE
        WHEN i.employee_count IS NULL THEN 'Unknown'
        WHEN i.employee_count < 100 THEN '< 100'
        WHEN i.employee_count < 1000 THEN '100 - 999'
        WHEN i.employee_count < 10000 THEN '1,000 - 9,999'
        ELSE '10,000+'
      END
    ) = dc.employee_size_band

LEFT JOIN `cyberproject-488907.cyber_gold.dim_industry` di
  ON IFNULL(i.industry_primary, '') = IFNULL(di.industry_primary, '')
 AND IFNULL(i.industry_secondary, '') = IFNULL(di.industry_secondary, '')

LEFT JOIN `cyberproject-488907.cyber_gold.dim_country` dco
  ON i.country_hq = dco.country_hq

LEFT JOIN `cyberproject-488907.cyber_gold.dim_attack` da
  ON IFNULL(i.attack_vector_primary, '') = IFNULL(da.attack_vector_primary, '')
 AND IFNULL(i.attack_vector_secondary, '') = IFNULL(da.attack_vector_secondary, '')
 AND IFNULL(i.attack_chain, '') = IFNULL(da.attack_chain, '')
 AND IFNULL(i.attributed_group, '') = IFNULL(da.attributed_group, '')
 AND IFNULL(i.attribution_confidence, '') = IFNULL(da.attribution_confidence, '')

LEFT JOIN `cyberproject-488907.cyber_gold.dim_source_quality` dsq
  ON IFNULL(i.data_source_primary, '') = IFNULL(dsq.data_source_primary, '')
 AND IFNULL(i.data_source_secondary, '') = IFNULL(dsq.data_source_secondary, '')
 AND IFNULL(i.data_source_type, '') = IFNULL(dsq.data_source_type, '')
 AND IFNULL(CAST(i.confidence_tier AS STRING), '') = IFNULL(CAST(dsq.confidence_tier AS STRING), '')
 AND IFNULL(CAST(i.quality_score AS STRING), '') = IFNULL(CAST(dsq.quality_score AS STRING), '')
 AND IFNULL(i.quality_grade, '') = IFNULL(dsq.quality_grade, '')
 AND IFNULL(CAST(i.review_flag AS STRING), '') = IFNULL(CAST(dsq.review_flag AS STRING), '');