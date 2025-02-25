import functions_framework
from google.cloud import dataproc_v1
import json
# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def trigger_dataproc(event, context):

  """Triggered by a new file upload to GCS."""
  # Configure your settings
  PROJECT_ID = "chrome-horizon-448017-g5"
  REGION = "us-central1"
  CLUSTER_NAME = "cluster-b4b2"
  BUCKET_NAME = "skkholiya_upload_data"  # Replace with your GCS bucket name
  file_name = event['name']
  print(f"File {file_name} uploaded. Submitting Dataproc job...")

  # Initialize Dataproc client
  client = dataproc_v1.JobControllerClient()

  # Configure the PySpark job
  job = {
      "placement": {"cluster_name": CLUSTER_NAME},
      "pyspark_job": {"main_python_file_uri": f"gs://{BUCKET_NAME}/csv/spark_job/group_country.py"},
  }

  # Submit the job
  response = client.submit_job(
      request={"project_id": PROJECT_ID, "region": REGION, "job": job}
  )
  
  print(f"Dataproc job submitted: {response.reference.job_id}")

