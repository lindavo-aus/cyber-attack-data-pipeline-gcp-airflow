-- Layer: Silver
-- Description: Clean and standardize financial impact data from the Bronze layer
-- Source: cyber_bronze.financial_impact

CREATE OR REPLACE TABLE `cyberproject-488907.cyber_silver.financial_impact_clean` AS
WITH src AS (
  SELECT *
  FROM `cyberproject-488907.cyber_bronze.financial_impact`
)
SELECT
  NULLIF(TRIM(CAST(incident_id AS STRING)), '') AS incident_id,

  SAFE_CAST(direct_loss_usd AS NUMERIC) AS direct_loss_usd,
  LOWER(NULLIF(TRIM(CAST(direct_loss_method AS STRING)), '')) AS direct_loss_method,

  SAFE_CAST(ransom_demanded_usd AS NUMERIC) AS ransom_demanded_usd,
  SAFE_CAST(ransom_paid_usd AS NUMERIC) AS ransom_paid_usd,
  NULLIF(TRIM(CAST(ransom_source AS STRING)), '') AS ransom_source,

  SAFE_CAST(recovery_cost_usd AS NUMERIC) AS recovery_cost_usd,
  SAFE_CAST(legal_fees_usd AS NUMERIC) AS legal_fees_usd,
  SAFE_CAST(regulatory_fine_usd AS NUMERIC) AS regulatory_fine_usd,
  SAFE_CAST(insurance_payout_usd AS NUMERIC) AS insurance_payout_usd,

  SAFE_CAST(total_loss_usd AS NUMERIC) AS total_loss_usd,
  LOWER(NULLIF(TRIM(CAST(total_loss_method AS STRING)), '')) AS total_loss_method,
  SAFE_CAST(total_loss_lower_bound AS NUMERIC) AS total_loss_lower_bound,
  SAFE_CAST(total_loss_upper_bound AS NUMERIC) AS total_loss_upper_bound,
  SAFE_CAST(inflation_adjusted_usd AS NUMERIC) AS inflation_adjusted_usd,

  NULLIF(TRIM(CAST(cpi_index_used AS STRING)), '') AS cpi_index_used,
  NULLIF(TRIM(CAST(notes AS STRING)), '') AS notes,
  SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
  SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at,

  CASE
    WHEN SAFE_CAST(total_loss_lower_bound AS NUMERIC) IS NOT NULL
     AND SAFE_CAST(total_loss_upper_bound AS NUMERIC) IS NOT NULL
     AND SAFE_CAST(total_loss_lower_bound AS NUMERIC) <= SAFE_CAST(total_loss_upper_bound AS NUMERIC)
    THEN TRUE
    ELSE FALSE
  END AS is_loss_range_valid,

  CASE
    WHEN SAFE_CAST(ransom_paid_usd AS NUMERIC) IS NOT NULL
     AND SAFE_CAST(ransom_demanded_usd AS NUMERIC) IS NOT NULL
     AND SAFE_CAST(ransom_paid_usd AS NUMERIC) > SAFE_CAST(ransom_demanded_usd AS NUMERIC)
    THEN TRUE
    ELSE FALSE
  END AS is_ransom_paid_gt_demanded,

  CURRENT_TIMESTAMP() AS silver_loaded_at
FROM src;