from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import timedelta, datetime

# Define default arguments for the DAG
yesterday = datetime.combine(datetime.now() - timedelta(1), datetime.min.time())

default_args = {
    'start_date': yesterday,  # DAG execution start date
    'email_on_failure': False,  # Disable email alerts on failure
    'email_on_retry': False,  # Disable email alerts on retry
    'retries': 1,  # Number of retry attempts for failed tasks
    'retry_delay': timedelta(minutes=1)  # Delay between retry attempts
}

# DAG Definition
with DAG(
    dag_id="gcs_to_bq_dag",  # DAG name
    default_args=default_args,  # Default settings like retries, start date
    schedule=timedelta(days=1),  # Runs daily
    catchup=False  # Prevents backfilling for missed runs
) as dag:

    start_operator = EmptyOperator(
        task_id="start_empty_operator",  # Defines a no-op task as a starting point
        dag=dag  # Associates the task with the DAG
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq_load",  # Task ID for loading data from GCS to BigQuery
        bucket="skkholiya_upload_data",  # Name of the GCS bucket
        source_objects=["csv/trading_data.csv"],  # Path to the source file(s) in GCS (NO leading `/`)
        destination_project_dataset_table="chrome-horizon-448017-g5.airflow_dumps.gs_csv_to_bq",  # Destination BigQuery table
        autodetect=True,  # Automatically detect schema
        write_disposition="WRITE_TRUNCATE",  # Overwrites existing data in the table
        skip_leading_rows=1,  # Skip the first row (header)
        dag=dag  # Associates the task with the DAG
    )

    bigquery_query_task = BigQueryInsertJobOperator(
        task_id="query_bq",  # Task ID for executing a BigQuery SQL query
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `chrome-horizon-448017-g5.airflow_dumps.airflow_bq_insert` AS
                    SELECT 
                        `Symbol`, 
                        `BuyorSell`, 
                        SUM(`QuantityTraded`) AS total_count
                    FROM `chrome-horizon-448017-g5.airflow_dumps.gs_csv_to_bq`
                    GROUP BY `Symbol`, `BuyorSell`
                """,
                "useLegacySql": False  # Ensure Standard SQL is used
            }
        },
        location="us-central1",  # Set the BigQuery dataset location
        project_id="chrome-horizon-448017-g5",  # Google Cloud project ID
        dag=dag  # Associates the task with the DAG
    )

    end = EmptyOperator(
        task_id="end",  # Defines an empty task as a stopping point
        dag=dag  # Associates the task with the DAG
    )

    # Define task dependencies
    start_operator >> gcs_to_bq >> bigquery_query_task >> end
