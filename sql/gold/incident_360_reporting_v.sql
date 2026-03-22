-- Layer: Gold
-- Type: Reporting View
-- Description: Flattened reporting view for BI consumption, joining the fact table with all relevant dimensions

CREATE OR REPLACE VIEW `cyberproject-488907.cyber_gold.incident_360_reporting_v` AS
SELECT
  -- core identifiers
  f.incident_id,
  c.company_name,
  c.stock_ticker,
  co.country_hq,
  ind.industry_primary,
  ind.industry_secondary,
  a.attack_vector_primary,
  a.attack_vector_secondary,
  a.attributed_group,
  a.attribution_confidence,

  -- date fields
  d_inc.full_date AS incident_date,
  d_inc.year_num AS incident_year,
  d_inc.quarter_num AS incident_quarter,
  d_inc.month_num AS incident_month,
  d_inc.month_name AS incident_month_name,
  d_inc.week_of_year AS incident_week_of_year,
  d_inc.day_name AS incident_day_name,

  d_dis.full_date AS discovery_date,
  d_dcl.full_date AS disclosure_date,

  -- business descriptors
  c.is_public_company AS company_is_public_company,
  c.revenue_band,
  c.employee_size_band,
  a.attack_chain,
  sq.data_source_type,
  sq.confidence_tier,
  sq.quality_score,
  sq.quality_grade,
  sq.review_flag,

  -- key KPIs / flags
  f.severity_band,
  f.loss_severity_band,
  f.record_breach_band,
  f.has_financial_impact,
  f.has_market_impact,
  f.is_ransom_case,
  f.is_data_breach,
  f.is_operational_disruption,
  f.days_to_discovery,
  f.days_to_disclosure,

  -- operational / company measures
  f.company_revenue_usd,
  f.employee_count,
  f.data_compromised_records,
  f.downtime_hours,

  -- financial measures
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

  -- market measures
  f.price_7d_before,
  f.price_disclosure_day,
  f.price_1d_after,
  f.price_7d_after,
  f.price_30d_after,
  f.abnormal_return_1d,
  f.abnormal_return_7d,
  f.abnormal_return_30d,
  f.car_neg1_to_pos1,
  f.car_0_to_7,
  f.car_0_to_30,
  f.car_0_to_90,
  f.days_to_price_recovery,
  f.pct_change_disclosure_to_1d,

  -- optional technical keys at the end
  f.incident_date_key,
  f.discovery_date_key,
  f.disclosure_date_key,
  f.company_key,
  f.industry_key,
  f.country_key,
  f.attack_key,
  f.source_quality_key,
  f.gold_loaded_at

FROM `cyberproject-488907.cyber_gold.fact_cyber_incident` f
LEFT JOIN `cyberproject-488907.cyber_gold.dim_date` d_inc
  ON f.incident_date_key = d_inc.date_key
LEFT JOIN `cyberproject-488907.cyber_gold.dim_date` d_dis
  ON f.discovery_date_key = d_dis.date_key
LEFT JOIN `cyberproject-488907.cyber_gold.dim_date` d_dcl
  ON f.disclosure_date_key = d_dcl.date_key
LEFT JOIN `cyberproject-488907.cyber_gold.dim_company` c
  ON f.company_key = c.company_key
LEFT JOIN `cyberproject-488907.cyber_gold.dim_country` co
  ON f.country_key = co.country_key
LEFT JOIN `cyberproject-488907.cyber_gold.dim_industry` ind
  ON f.industry_key = ind.industry_key
LEFT JOIN `cyberproject-488907.cyber_gold.dim_attack` a
  ON f.attack_key = a.attack_key
LEFT JOIN `cyberproject-488907.cyber_gold.dim_source_quality` sq
  ON f.source_quality_key = sq.source_quality_key;