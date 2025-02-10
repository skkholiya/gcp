import apache_beam as beam
import logging
import json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromPubSub,WriteToBigQuery
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProcessPubSubMessages(beam.DoFn):
    """Parses and processes Pub/Sub messages."""
    def process(self, element):
        try:
            message = json.loads(element.decode('utf-8'))
            logger.info(f" Received message: {message}")
            yield message
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

def run(input_subscription,output_path,window_size=1.0,num_shards=5,pipeline_args=None):
    project_id = "chrome-horizon-448017-g5"
    subscription_name = f"projects/{project_id}/subscriptions/{input_subscription}"
    pipeline_options = PipelineOptions(
        pipeline_args=pipeline_args,
        streaming=True,
        save_main_session=True,
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> ReadFromPubSub(subscription=subscription_name)
            | "Process Messages" >> beam.ParDo(ProcessPubSubMessages())
            | "Load to bq" >> beam.io.WriteToBigQuery(
                "static_realtime_data",
                project=project_id,
                dataset= "topic_to_bq",
                schema= "timestamp:STRING,data:STRING",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help="The cloud pub/sub topic to read from"
    )
    parser.add_argument(
        "--window_size",
        type= float,
        default= 1.0,
        help="output file window in minute"
    )
    parser.add_argument(
        "--output_path",
        help="GCS file path to store the elements"
    )
    parser.add_argument(
        "--num_shards",
        type= int,
        default =5,
        help="no of worker nodes required to process the window elements"
    )
    known_args,pipeline_args = parser.parse_known_args()
    run(
        known_args.input_subscription,
        known_args.output_path,
        known_args.window_size,
        known_args.num_shards,
        pipeline_args
    )
