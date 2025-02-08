from google.cloud import pubsub_v1

project_id = "chrome-horizon-448017-g5"
subscription_id = "topic-from-cli-sub"

#Number of seconds the subscriber should listen for messages
timeout = 5.0
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message)->None:
        print(f"received {message}")
        message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path,callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

#Wrapping subscriber in a 'with' block to automatically call close() when doen.
with subscriber:
        try:
                streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
                streaming_pull_future.cancel() #Trigger is shutdown
                streaming_pull_future.result() #Block unti the shutdown is complete
