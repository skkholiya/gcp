rom google.cloud import pubsub_v1
import time

# Config project and topic
project_id = "chrome-horizon-448017-g5"
topic_id = "topic-from-cli"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id,topic_id)

for num in range(1,10):
        data_str = f"Message number {num}"
        # Data must be a bytestring
        data = data_str.encode("utf-8")
        print(data)
        # When we publish a message, the client returns a future.
        # Set some sleep time between messages
        time.sleep(1)
        future = publisher.publish(topic_path, data)
        print(future.result())

print(f"Published message to {topic_path}.")
