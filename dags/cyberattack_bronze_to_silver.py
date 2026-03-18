from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

BRONZE_DATASET = "cyber_bronze"
SILVER_DATASET = "cyber_silver"
BQ_LOCATION = "australia-southeast2"


def run_query(query: str):
    client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
    job = client.query(query, location=BQ_LOCATION)
    job.result()


def create_silver_dataset():
    query = f"""
    CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{SILVER_DATASET}`
    OPTIONS (
      location = '{BQ_LOCATION}'
    );
    """
    run_query(query)


def transform_incidents_master():
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{SILVER_DATASET}.incidents_master_clean` AS
    WITH src AS (
      SELECT *
      FROM `{PROJECT_ID}.{BRONZE_DATASET}.incidents_master`
    )
    SELECT
      NULLIF(TRIM(CAST(incident_id AS STRING)), '') AS incident_id,
      NULLIF(TRIM(CAST(company_name AS STRING)), '') AS company_name,
      SAFE_CAST(company_revenue_usd AS NUMERIC) AS company_revenue_usd,
      UPPER(NULLIF(TRIM(CAST(country_hq AS STRING)), '')) AS country_hq,
      NULLIF(TRIM(CAST(industry_primary AS STRING)), '') AS industry_primary,
      NULLIF(TRIM(CAST(industry_secondary AS STRING)), '') AS industry_secondary,
      SAFE_CAST(employee_count AS INT64) AS employee_count,
      SAFE_CAST(is_public_company AS BOOL) AS is_public_company,
      UPPER(NULLIF(TRIM(CAST(stock_ticker AS STRING)), '')) AS stock_ticker,

      DATE(incident_date) AS incident_date,
      SAFE_CAST(incident_date_estimated AS BOOL) AS incident_date_estimated,
      DATE(discovery_date) AS discovery_date,
      DATE(disclosure_date) AS disclosure_date,

      LOWER(NULLIF(TRIM(CAST(attack_vector_primary AS STRING)), '')) AS attack_vector_primary,
      LOWER(NULLIF(TRIM(CAST(attack_vector_secondary AS STRING)), '')) AS attack_vector_secondary,

      NULLIF(
        TRIM(
          REGEXP_REPLACE(
            REPLACE(
              REPLACE(CAST(attack_chain AS STRING), 'â†’', ' -> '),
              '→', ' -> '
            ),
            r'\\s+',
            ' '
          )
        ),
        ''
      ) AS attack_chain,

      NULLIF(TRIM(CAST(attributed_group AS STRING)), '') AS attributed_group,
      LOWER(NULLIF(TRIM(CAST(attribution_confidence AS STRING)), '')) AS attribution_confidence,
      SAFE_CAST(data_compromised_records AS INT64) AS data_compromised_records,
      LOWER(NULLIF(TRIM(CAST(data_type AS STRING)), '')) AS data_type,
      NULLIF(TRIM(CAST(systems_affected AS STRING)), '') AS systems_affected,
      SAFE_CAST(downtime_hours AS FLOAT64) AS downtime_hours,
      NULLIF(TRIM(CAST(data_source_primary AS STRING)), '') AS data_source_primary,
      NULLIF(TRIM(CAST(data_source_secondary AS STRING)), '') AS data_source_secondary,
      LOWER(NULLIF(TRIM(CAST(data_source_type AS STRING)), '')) AS data_source_type,
      SAFE_CAST(confidence_tier AS INT64) AS confidence_tier,
      SAFE_CAST(quality_score AS FLOAT64) AS quality_score,
      INITCAP(LOWER(NULLIF(TRIM(CAST(quality_grade AS STRING)), ''))) AS quality_grade,
      NULLIF(TRIM(CAST(review_flag AS STRING)), '') AS review_flag,
      NULLIF(TRIM(CAST(notes AS STRING)), '') AS notes,
      SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
      SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at,

      CURRENT_TIMESTAMP() AS silver_loaded_at
    FROM src;
    """
    run_query(query)


def transform_financial_impact():
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{SILVER_DATASET}.financial_impact_clean` AS
    WITH src AS (
      SELECT *
      FROM `{PROJECT_ID}.{BRONZE_DATASET}.financial_impact`
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
    """
    run_query(query)


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


with DAG(
    dag_id="cyberattack_bronze_to_silver",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cyber", "silver", "bigquery", "australia-southeast2"],
) as dag:

    create_silver_dataset_task = PythonOperator(
        task_id="create_silver_dataset",
        python_callable=create_silver_dataset,
    )

    transform_incidents_master_task = PythonOperator(
        task_id="transform_incidents_master",
        python_callable=transform_incidents_master,
    )

    transform_financial_impact_task = PythonOperator(
        task_id="transform_financial_impact",
        python_callable=transform_financial_impact,
    )

    transform_market_impact_task = PythonOperator(
        task_id="transform_market_impact",
        python_callable=transform_market_impact,
    )

    create_silver_dataset_task >> [
        transform_incidents_master_task,
        transform_financial_impact_task,
        transform_market_impact_task,
    ]
