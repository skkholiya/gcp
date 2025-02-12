from google.cloud import pubsub_v1
from datetime import datetime as dt
import time
import logging
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_id = "chrome-horizon-448017-g5"
topic_name = "generate-1-100-nums"

client = pubsub_v1.PublisherClient()
#Added project_id and topic_name to the publisher
publisher_topic = client.topic_path(project_id,topic_name)
for num in range(1,100):
    str_obj = f"{num},{dt.now()}".encode("utf-8")
    logger.info(f"str_obj{str_obj}")
    time.sleep(1)
    #Publishing data to the specified topic
    future = client.publish(
        data=str_obj,
        topic=topic_name
    )
logger.info(f"Data is successfully loaded to the topic:{topic_name}")
