import functions_framework
import os
from google.cloud import storage
from google.cloud import pubsub_v1
import json
import logging


#--------------deploy cmd-------
# gcloud functions deploy check_and_transform_json \
#   --runtime python310 \
#   --trigger-http \
#   --entry-point check_and_transform_json \
#   --set-env-vars GCP_PROJEC,BUCKET_NAME=cloud_functions_kholiya,FILE_NAME=ipl_player_stats_nested_v5.json,PUBSUB_TOPIC_ID=json-file-processed \
#   --timeout=60s \
#   --memory=256MB \

# ------------------ CONFIGURATION ------------------

# Fetch environment variables securely
PROJECT_ID = 'chrome-horizon-448017-g5'  # Google Cloud project ID
BUCKET_NAME = "cloud_functions_kholiya"  # GCS bucket to check
FILE_NAME = "ipl_player_stats_nested_v5.json"  # File name to check
PUBSUB_TOPIC_ID = "file-missing-alerts"  # Pub/Sub topic for alerts

# Initialize google client
storage = storage.Client()
pub_sub = pubsub_v1.PublisherClient()

# Set Up logging
logger = logging.getLogger("gcs-file-checker")
logger.setLevel(logging.INFO)

#-------- Main Function --------

@functions_framework.http
def check_and_transform_json(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
    Returns:
        HTTPS status codes.
    """
    topic_path = pub_sub.create_topic(PROJECT_ID,PUBSUB_TOPIC_ID)
    logger.info(f"Checking for file '{FILE_NAME}' in bucket '{BUCKET_NAME}'.")

    try:
        #accessing bucket
        bucket = storage.bucket(BUCKET_NAME)
        #get file inside bucket
        blob = bucket_info.blob(FILE_NAME)
        
        if not blob.exists():
            alert_message = {
                "project":PROJECT_ID,
                "bucket": BUCKET_NAME,
                "file_name": FILE_NAME,
                "alert": "File not found!!!!!!",
                "reason": f"File is not available in the {BUCKET_NAME}"
            }
        return ("File processed and transformed data published successfully.", 200)
    except Exception as e:
        logger.error(f"⚠️ Error processing JSON file: {str(e)}", exc_info=True)
        return (f"Internal server error: {str(e)}", 500)
