from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from google.cloud import storage, bigquery
import os
from io import BytesIO

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_BUCKET")
BQ_DATASET = os.environ.get("BQ_DATASET")


def read_csv_from_gcs(file_name: str) -> pd.DataFrame:
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    data = blob.download_as_bytes()
    df = pd.read_csv(BytesIO(data))
    return df


def load_df_to_bq(df: pd.DataFrame, table_name: str):
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

    job = bq_client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )
    )
    job.result()
    print(f"Loaded {table_name} to BigQuery")


def load_incidents():
    df = read_csv_from_gcs("incidents_master_02.csv")

    # fix datetime columns if present
    date_cols = ["incident_date", "discovery_date", "disclosure_date", "created_at", "updated_at"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # fix encoding issue if present
    if "attack_chain" in df.columns:
        df["attack_chain"] = (
            df["attack_chain"]
            .astype(str)
            .str.encode("latin1", errors="ignore")
            .str.decode("utf-8", errors="ignore")
        )

    load_df_to_bq(df, "incidents_master")


def load_financial():
    df = read_csv_from_gcs("financial_impact_02.csv")

    for col in ["created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    load_df_to_bq(df, "financial_impact")


def load_market():
    df = read_csv_from_gcs("market_impact_02.csv")

    for col in ["created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    load_df_to_bq(df, "market_impact")


default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="cyberattack_gcs_to_bigquery",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["gcs", "bigquery", "cyber"],
) as dag:

    task_incidents = PythonOperator(
        task_id="load_incidents_master",
        python_callable=load_incidents,
    )

    task_financial = PythonOperator(
        task_id="load_financial_impact",
        python_callable=load_financial,
    )

    task_market = PythonOperator(
        task_id="load_market_impact",
        python_callable=load_market,
    )

    [task_incidents, task_financial, task_market]
