from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago #it will run the job each time the days_ago mention in the parameter
from airflow.utils.trigger_rule import TriggerRule #Want to delete job even it's fail
from airflow.exceptions import AirflowFailException
from datetime import timedelta
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor

from airflow.exceptions import AirflowSkipException

#Configuration
PROJECT_ID = "chrome-horizon-448017-g5"
REGION = "us-central1"
CLUSTER_NAME = "dataproc-event-cluster"
BUCKET_NAME = "skkholiya_upload_data"
FILE_PATH = "csv/incoming_data/"
PYSPARK_JOB_URI = f"gs://{BUCKET_NAME}/csv/spark_job/group_country.py"

default_args = {
    "owner":"airflow",
    "start_date": days_ago(1),
    "retries":2,
    "retry_delay": timedelta(minutes=5) #controls the delay between retry attempts for a task
}

dag = DAG(
    "dataproc_event_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

#Task 1: wait for a new file in GCS
wait_for_file = GCSObjectExistenceSensor(
    task_id="wait_for_gcs_file",
    bucket = BUCKET_NAME,
    object = FILE_PATH,
    poke_interval = 30, #sec
    timeout=600, #Timeout after 10 min
    mode = "poke", #Data required within mins
    dag=dag,
)

# Task 2: Create Dataproc cluster with e2 machine
create_cluster = DataprocCreateClusterOperator(
    task_id = "create_cluster",
    project_id = PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    cluster_config = {
        "gce_cluster_config": {
            "zone_uri": "us-central1-a",
            "internal_ip_only": True,  # No external IPs
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "e2-standard-2",
            "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 100},
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n2-standard-4",
            "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 30},
        },
        "software_config": {"image_version": "2.2-debian12"},
    },
    dag=dag,
)

#Task 3: Submit PySpark job with timeout and retry
submit_job = DataprocSubmitJobOperator(
    task_id = "submit_pyspark_job",
    project_id = PROJECT_ID,
    region=REGION,
    job={
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": PYSPARK_JOB_URI},
    },
    execution_timeout=timedelta(minutes=30),
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)

# Check if cluster exists before deleting
def check_cluster_exists():
    from google.cloud import dataproc_v1
    client = dataproc_v1.ClusterControllerClient()
    try:
        client.get_cluster(
            project_id="chrome-horizon-448017-g5",
            region="us-central1",
            cluster_name="dataproc-event-cluster"
        )
        return True
    except Exception:
        raise AirflowSkipException("Cluster does not exist. Skipping deletion.")

check_cluster_before_delete = PythonOperator(
    task_id="check_cluster_before_delete",
    python_callable=check_cluster_exists,
    dag=dag,
)

# Task 4: Delete Cluster to save costs
delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule=TriggerRule.ALL_DONE,  # Runs even if job fails
    dag=dag,
)

# DAG Workflow
wait_for_file >> create_cluster >> submit_job >> check_cluster_before_delete >> delete_cluster
