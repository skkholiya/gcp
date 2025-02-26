import functions_framework
from google.cloud import bigquery, storage
import pandas as pd
import logging
import json
import re
import os

# Initialize clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# GCP details
PROJECT_ID = "chrome-horizon-448017-g5"
DATASET_ID = "cf_dump"
TABLE_ID = "aq-dump"
BUCKET_NAME = "cloud_functions_kholiya"

# Set up logging
logging.basicConfig(level=logging.INFO)

def gcs_to_bigquery():
    """Processes all new files in GCS, transforms data, and loads it into BigQuery."""
    
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()

    for blob in blobs:
        file_name = blob.name
        logging.info(f"Processing file: {file_name}")

        temp_file = f"/tmp/{os.path.basename(file_name)}"
        blob.download_to_filename(temp_file)

        # Transform the data
        transformed_data = transform_data(temp_file)

        # Load transformed data into BigQuery
        load_to_bigquery(transformed_data, TABLE_ID)

        # Delete file after processing (optional)
        blob.delete()
        logging.info(f"File {file_name} deleted from GCS after processing.")

def transform_data(file_path):
    """Reads, cleans, and transforms data for BigQuery."""

    #Read CSV with correct delimiter
    df = pd.read_csv(file_path, sep=";", decimal=",", engine="python")

    #Rename columns: Replace invalid characters
    df.columns = [re.sub(r"[^\w]", "_", col).strip("_") for col in df.columns]

    #Drop empty columns
    df = df.loc[:, ~df.columns.str.contains("Unnamed")]

    #Convert numeric columns (fix decimal issue)
    for col in df.columns:
        if df[col].dtype == object:  # Check for string columns
            df[col] = df[col].str.replace(",", ".")  # Replace `,` with `.`
            df[col] = pd.to_numeric(df[col], errors="ignore")  # Convert numbers

    #Add processing timestamp
    df["processed_timestamp"] = pd.Timestamp.now()

    logging.info(f"Transformed data:\n{df.head()}")
    return df


def load_to_bigquery(df, table_id):
    """Loads transformed data into BigQuery."""
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    table_ref = bigquery_client.dataset(DATASET_ID).table(table_id)
    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    
    job.result()  # Wait for completion
    logging.info(f"Data loaded into {table_id}")

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    """Cloud Function triggered by GCS events."""
    data = cloud_event.data
    logging.info(f"Received event data:\n{json.dumps(data, indent=2)}") 

    # Extract event details
    event_id = cloud_event["id"]
    event_type = cloud_event["type"]
    bucket = data["bucket"]
    file_name = data["name"]

    logging.info(f"Event ID: {event_id}")
    logging.info(f"Event type: {event_type}")
    logging.info(f"Bucket: {bucket}")
    logging.info(f"File: {file_name}")

    # Call function to process files
    gcs_to_bigquery()
