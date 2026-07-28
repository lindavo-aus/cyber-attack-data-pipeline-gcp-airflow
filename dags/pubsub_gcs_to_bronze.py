from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import json
import uuid

from google.cloud import pubsub_v1
from google.cloud import bigquery


PROJECT_ID = "cyberproject-488907"
BQ_LOCATION = "australia-southeast2"
BUCKET_NAME = "cyberbucket-01"

SUBSCRIPTION_ID = "cyber-raw-file-uploaded-sub"

BRONZE_DATASET = "cyber_bronze"
INGESTION_LOG_TABLE = "ingestion_log"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 0,
}


def pull_pubsub_message(**context):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": 1,
        },
        timeout=30,
    )

    if not response.received_messages:
        raise AirflowSkipException("No Pub/Sub messages found. Skipping this run.")

    received_message = response.received_messages[0]
    message_data = received_message.message.data.decode("utf-8")
    message_json = json.loads(message_data)

    file_name = message_json.get("name")

    if not file_name:
        raise ValueError("No file name found in Pub/Sub message.")

    context["ti"].xcom_push(key="file_name", value=file_name)
    context["ti"].xcom_push(key="ack_id", value=received_message.ack_id)


def write_ingestion_log(
    file_name,
    target_table,
    rows_loaded,
    status,
    error_message,
    context,
):
    client = bigquery.Client(project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{BRONZE_DATASET}.{INGESTION_LOG_TABLE}"

    row = [{
        "log_id": str(uuid.uuid4()),
        "file_name": file_name,
        "bucket_name": BUCKET_NAME,
        "target_table": target_table,
        "rows_loaded": rows_loaded,
        "status": status,
        "error_message": error_message,
        "dag_id": context["dag"].dag_id,
        "dag_run_id": context["run_id"],
        "task_id": context["task"].task_id,
        "loaded_at": datetime.utcnow().isoformat(),
    }]

    errors = client.insert_rows_json(table_id, row)

    if errors:
        raise RuntimeError(f"Failed to insert ingestion log: {errors}")


def load_gcs_file_to_bronze(**context):
    ti = context["ti"]

    file_name = ti.xcom_pull(task_ids="pull_pubsub_message", key="file_name")
    ack_id = ti.xcom_pull(task_ids="pull_pubsub_message", key="ack_id")

    if "incidents_master" in file_name:
        target_table = "incidents_master"
    elif "financial_impact" in file_name:
        target_table = "financial_impact"
    elif "market_impact" in file_name:
        target_table = "market_impact"
    else:
        raise ValueError(f"Unknown file type: {file_name}")

    client = bigquery.Client(project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{BRONZE_DATASET}.{target_table}"
    uri = f"gs://{BUCKET_NAME}/{file_name}"

    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        load_job = client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config,
            location=BQ_LOCATION,
        )

        load_job.result()
        rows_loaded = load_job.output_rows

        write_ingestion_log(
            file_name=file_name,
            target_table=table_id,
            rows_loaded=rows_loaded,
            status="SUCCESS",
            error_message=None,
            context=context,
        )

        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

        subscriber.acknowledge(
            request={
                "subscription": subscription_path,
                "ack_ids": [ack_id],
            }
        )

        print(f"Loaded {uri} into {table_id}. Rows loaded: {rows_loaded}")

    except Exception as e:
        write_ingestion_log(
            file_name=file_name,
            target_table=table_id,
            rows_loaded=0,
            status="FAILED",
            error_message=str(e)[:1000],
            context=context,
        )
        raise


with DAG(
    dag_id="pubsub_gcs_to_bronze",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
    tags=["pubsub", "gcs", "bronze", "incremental"],
) as dag:

    pull_message = PythonOperator(
        task_id="pull_pubsub_message",
        python_callable=pull_pubsub_message,
        provide_context=True,
    )

    load_to_bronze = PythonOperator(
        task_id="load_gcs_file_to_bronze",
        python_callable=load_gcs_file_to_bronze,
        provide_context=True,
    )

    pull_message >> load_to_bronze
