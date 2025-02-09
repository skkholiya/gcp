import argparse
import apache_beam as beam
import logging #log service
import random
from datetime import datetime
from apache_beam import ParDo,DoFn,GroupByKey, io,\
    Pipeline,PTransform,WindowInto,WithKeys
#Pipeline: object manage the DAG of PValue(node,edges) and PTransform(computing)
#ParDo(Parallel Processing Transform): applies a used-denfined function(DoFn) to distribute element-wise processing across workers
#DoFn: Process logic and produce one or more transformed objects.
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows #Assign each element to one time interval


class GroupMessagesByFixedWindows(PTransform):
    ''' A composite transform that group pub/sub messages based on publish time
    and output list of tuples.
        Each containing a message and its publish time.
    '''

    def __init__(self,window_size,num_shards=5):
        self.window_size = (window_size*60)
        self.num_shards = num_shards
    def expand(self,pcoll):
        return (
            pcoll
            # Bind window to each element using element publish time
            |"Window into fixed intervals" >> WindowInto(FixedWindows(self.window_size))
            |"Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            #Assign a random key to each windowed element based on the number of shards
            |"Add key" >> WithKeys(lambda _:random.randint(0,self.num_shards -1))
            #Group windowed elements by key.
            |"Group by key" >> GroupByKey()
        )
class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Process each windwed element by extracting the message body and its
        publish time into a tuple.
        """
        yield(
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S. %f"
            ),
        )
# Parsing method to prepare data to writeable to bigquery(dict format)
def parse_method(string_input):
    row = {"batch_num":string_input[0],
            "message":string_input[1][0][0],
            "send_time":string_input[1][0][1]}
    return row

def run(input_topic, output_path,window_size=1.0,num_shards=5, pipeline_args=None):
    # Set save_main_session:True, to access globally imported module in DoFns.
    pipeline_options = PipelineOptions(
        pipeline_args,streaming=True, save_main_session=True
    )
    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            # Reading data from PubSub topic
            | "Read from Pub/Sub" >> io.ReadFromPubSub(topic=input_topic).with_output_types(bytes)
            | "Print Pub/Sub Messages" >> beam.Map(lambda msg: print(f"Received: {msg}"))
            | "Window Into" >> GroupMessagesByFixedWindows(window_size,num_shards)
            | "Tuple to BigQuery Row" >> beam.Map(lambda row: parse_method(row))
            | "Write to Bigquery" >> beam.io.Write(
                beam.io.WriteToBigQuery(
                    "stream_data",
                    dataset = "topic_to_bq",
                    project = "chrome-horizon-448017-g5",
                    schema = 'batch_num:STRING,message:STRING,send_time:TIMESTAMP',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        )
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help = "The cloud Pub/Sub topic to read from."
        '"projects//topics".',
    )
    parser.add_argument(
        "--window_size",
        type=float,
        default = 1.0,
        help = "output file's window size in minutes"
    )
    parser.add_argument(
        "--output_path",
        help = "Path of the output GCS file including the prefix.",
    )
    parser.add_argument(
        "--num_shards",
        type=int,
        default=5,
        help= "Number of shards to use when writing windowed elements to GCS."
    )
    known_args, pipeline_args = parser.parse_known_args()
    run(
        known_args.input_topic,
        known_args.output_path,
        known_args.window_size,
        known_args.num_shards,
        pipeline_args,
    )
