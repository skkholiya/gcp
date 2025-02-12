import argparse
import apache_beam as beam
import logging
from datetime import datetime
from apache_beam import ParDo, DoFn, Map, Pipeline, WindowInto, CombineGlobally
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AddWindowedtlsFn(DoFn):
    def process(self, element, window=DoFn.WindowParam):
        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()
        output = f"{element}  [{window_start} - {window_end}]"
        return [output]  # Ensure output is iterable

def run(input_topic, output_path, pipeline_args=None):
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    pipeline_options.view_as(StandardOptions).streaming = True

    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=input_topic)
            | "Convert to string" >> Map(lambda s: s.decode("utf-8"))
            | "Print Pub/Sub Messages" >> Map(lambda msg: logger.info("-------message %s", msg))
            | "Convert to dict" >> Map(lambda topic_data: {"num": topic_data.split(",")[0], "timestamp": datetime.strptime(topic_data.split(",")[1], "%Y-%m-%d %H:%M:%S.%f")})
            | "Events with timestamp" >> Map(lambda dic: beam.window.TimestampedValue(int(dic["num"]), dic["timestamp"].timestamp()))
            | "Converts to window" >> WindowInto(FixedWindows(5))  # Use FixedWindows(300) for 5 minutes
            | "No of events per window" >> CombineGlobally(beam.combiners.CountCombineFn()).without_defaults()
            | "Final Result with windowInto" >> ParDo(AddWindowedtlsFn())
            | "Convert to dict for BQ" >> Map(lambda data: {"word_count": data})
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                "chrome-horizon-448017-g5:topic_to_bq.stream_data_100_nums",
                schema="word_count:STRING",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The cloud Pub/Sub topic to read from. Format: projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUBSCRIPTION"
    )
    parser.add_argument(
        "--output_path",
        help="Path of the output GCS file including the prefix.",
    )
    
    known_args, pipeline_args = parser.parse_known_args()
    run(
        known_args.input_topic,
        known_args.output_path
        )
