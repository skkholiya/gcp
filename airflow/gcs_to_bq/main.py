from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import timedelta, datetime

yesterday = datetime.combine(datetime.now()-timedelta(1),datetime.min.time())

default_args = {
    'start_date': yesterday,  # DAG execution start date
    'email_on_failure': False,  # Disable email alerts on failure
    'email_on_retry': False,  # Disable email alerts on retry
    'retries': 1,  # Number of retry attempts for failed tasks
    'retry_delay': timedelta(minutes=5)  # Delay between retry attempts
}

#Dag Defination
with DAG(dag_id="gcs_to_bq_dag",  # DAG name
    default_args=default_args,  # Default settings like retries, start date
    schedule=timedelta(days=1),  # Runs daily
    catchup=False  # Prevents backfilling for missed runs
    ) as dag:

    
    start_operator = EmptyOperator(  
    task_id="start_empty_operator",  # Defines a no-op task as a starting point  
    dag=dag  # Associates the task with the DAG  
    )
    
     
