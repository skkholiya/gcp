'''
Run in CLI:

-> python -m venv  venv

-> pip install apache-beam[gcp]

-> source venv/bin/activate

-> python main.py 
   --region asia-south2
   --runner DirectRunner
   --input_path gs://skkholiya_upload_data/csv/trading_data.csv 
   --project chrome-horizon-448017-g5 
   --temp_location=gs://skkholiya_upload_data/temp

'''

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.sql import SqlTransform
import logging
import argparse

#Set up logging
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
        parser.add_argument("--runner", help="Beam Runner (DataflowRunner, DirectRunner, etc.)")
        parser.add_argument("--region",  help="Region to create a dataflow job")
        parser.add_argument("--input_path", required=True, help="GCS input path")
        parser.add_argument("--temp_location", required=True, help="GCS temp location path")
     
        # Parse known arguments
        known_args, pipeline_args = parser.parse_known_args()

        # Construct PipelineOptions using parsed arguments
        options = cls(pipeline_args)
        
        # Set streaming option if provided
        #options.view_as(StandardOptions).streaming = known_args.streaming

        return known_args, options

def run():
    known_args, pipeline_options = BatchPipelineOptions.from_args()
    #Companies buy or sell orders if Buy then QuantityTraded else -QuantityTraded
    sql_query = '''with cte as(
                select Symbol,
                case when ByeOrSell="Buy" then cast(QuantityTraded as INT64)
                else -cast(replace(QuantityTraded,",","") as INT64) end as QuantityTraded
                from PCOLLECTION
                )
                select Symbol, sum(QuantityTraded) as QuantityTraded from cte group by Symbol'''
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            |"Read data from source:" >> beam.io.ReadFromText(known_args.input_path,skip_header_lines=1)
            # Convert collection of strings to beam sql rows
            |"beam_to_sql_rows" >> beam.Map(lambda string: beam.Row(Symbol=str(string.split(",")[0]),
                                   ByeOrSell = str(string.split(",")[1]),
                                   QuantityTraded = str(string.split(",")[2])))
            #zeta-sql is compatible with bigQuery
            |"Transform the data using beam SQL(SqlTranform)" >> SqlTransform(sql_query,dialect="zetasql")
            #Convert to dict, to load the data to bigquery
            |"sql_rows to dict" >> beam.Map(lambda row: row._asdict())
            |"load data to bigquery">>beam.io.WriteToBigQuery(
                table = f"{known_args.project}.dataflow_batch_jobs.beam_sql_dump",
                schema = "Symbol:STRING,QuantityTraded:INT64",
                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
                write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND,
                custom_gcs_temp_location=known_args.temp_location
            )
        )

if __name__ == "__main__":
    run()