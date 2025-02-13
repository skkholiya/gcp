import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchPipelineOptions(PipelineOptions):
    """Custom Pipeline Options using argparse arguments."""
    
    @classmethod
    def from_args(cls):
        """Parses arguments using argparse and returns PipelineOptions."""
        parser = argparse.ArgumentParser()

        # Define command-line arguments
        parser.add_argument("--project", required=True, help="GCP Project ID")
        parser.add_argument("--runner", default="DirectRunner", help="Beam Runner (DataflowRunner, DirectRunner, etc.)")
        parser.add_argument("--region", default="us-central1", help="GCP Region")
        parser.add_argument("--input", required=True, help="GCS input path")
        parser.add_argument("--output", required=True, help="GCS output path")
        parser.add_argument("--job_name", default="argparse-pipeline-job", help="Name for the Dataflow job")
     
        # Parse known arguments
        known_args, pipeline_args = parser.parse_known_args()

        # Construct PipelineOptions using parsed arguments
        options = cls(pipeline_args)
        
        # Set streaming option if provided
        #options.view_as(StandardOptions).streaming = known_args.streaming

        return known_args, options

class CSVToJson(beam.DoFn):
    def process(self, element, headers):
        # Split CSV line into fields
        fields = element.split(',')
        # Convert to dictionary
        record = dict(zip(headers, fields))
        # Convert to JSON string (optional)
        yield json.dumps(record)

#Find the first employee to join in each department and count the number of employees in each department.
def run():
    known_args, pipeline_options = BatchPipelineOptions.from_args()
    csv_headers = ["emp_id","name","depart","join_date"]
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
            | "Convert to JSON" >> beam.ParDo(CSVToJson(), headers=csv_headers)
            | "Write JSON" >> beam.Map(lambda msg:logger.info("--msg--:",msg))
            )

if __name__ == "__main__":
    run()
