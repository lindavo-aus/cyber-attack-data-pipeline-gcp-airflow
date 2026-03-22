def transform_market_impact():
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{SILVER_DATASET}.market_impact_clean` AS
    WITH src AS (
      SELECT *
      FROM `{PROJECT_ID}.{BRONZE_DATASET}.market_impact`
    )
    SELECT
      NULLIF(TRIM(CAST(incident_id AS STRING)), '') AS incident_id,
      UPPER(NULLIF(TRIM(CAST(stock_ticker AS STRING)), '')) AS stock_ticker,

      SAFE_CAST(price_7d_before AS FLOAT64) AS price_7d_before,
      SAFE_CAST(price_disclosure_day AS FLOAT64) AS price_disclosure_day,
      SAFE_CAST(price_1d_after AS FLOAT64) AS price_1d_after,
      SAFE_CAST(price_7d_after AS FLOAT64) AS price_7d_after,
      SAFE_CAST(price_30d_after AS FLOAT64) AS price_30d_after,

      SAFE_CAST(volume_avg_30d_baseline AS INT64) AS volume_avg_30d_baseline,
      SAFE_CAST(volume_disclosure_day AS INT64) AS volume_disclosure_day,

      NULLIF(TRIM(CAST(sector_index AS STRING)), '') AS sector_index,
      SAFE_CAST(sector_return_same_period AS FLOAT64) AS sector_return_same_period,
      SAFE_CAST(abnormal_return_1d AS FLOAT64) AS abnormal_return_1d,
      SAFE_CAST(abnormal_return_7d AS FLOAT64) AS abnormal_return_7d,
      SAFE_CAST(abnormal_return_30d AS FLOAT64) AS abnormal_return_30d,
      SAFE_CAST(car_neg1_to_pos1 AS FLOAT64) AS car_neg1_to_pos1,
      SAFE_CAST(car_0_to_7 AS FLOAT64) AS car_0_to_7,
      SAFE_CAST(car_0_to_30 AS FLOAT64) AS car_0_to_30,
      SAFE_CAST(car_0_to_90 AS FLOAT64) AS car_0_to_90,
      SAFE_CAST(t_statistic_1d AS FLOAT64) AS t_statistic_1d,
      SAFE_CAST(p_value_1d AS FLOAT64) AS p_value_1d,
      SAFE_CAST(t_statistic_30d AS FLOAT64) AS t_statistic_30d,
      SAFE_CAST(p_value_30d AS FLOAT64) AS p_value_30d,

      SAFE_CAST(earnings_announcement_within_7d AS BOOL) AS earnings_announcement_within_7d,
      SAFE_CAST(market_cap_at_disclosure AS NUMERIC) AS market_cap_at_disclosure,
      SAFE_CAST(volume_ratio_disclosure AS FLOAT64) AS volume_ratio_disclosure,
      SAFE_CAST(pre_incident_volatility_30d AS FLOAT64) AS pre_incident_volatility_30d,
      SAFE_CAST(post_incident_volatility_30d AS FLOAT64) AS post_incident_volatility_30d,
      SAFE_CAST(days_to_price_recovery AS INT64) AS days_to_price_recovery,

      NULLIF(TRIM(CAST(notes AS STRING)), '') AS notes,
      SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
      SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at,

      CASE
        WHEN SAFE_CAST(price_disclosure_day AS FLOAT64) IS NOT NULL
         AND SAFE_CAST(price_1d_after AS FLOAT64) IS NOT NULL
         AND SAFE_CAST(price_disclosure_day AS FLOAT64) != 0
        THEN ROUND(
          (SAFE_CAST(price_1d_after AS FLOAT64) - SAFE_CAST(price_disclosure_day AS FLOAT64))
          / SAFE_CAST(price_disclosure_day AS FLOAT64),
          6
        )
        ELSE NULL
      END AS pct_change_disclosure_to_1d,

      CURRENT_TIMESTAMP() AS silver_loaded_at
    FROM src;
    """
    run_query(query)