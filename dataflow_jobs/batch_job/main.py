'''
Run in CLI:

-> python -m venv  venv

-> source venv/bin/activate

-> python main.py \
    --runner DirectRunner \
    --project chrome-horizon-448017-g5 \
    --region asia-south2 \
    --input_path gs://skkholiya_upload_data/csv/employee_data_1000_records.csv \
    --output gs://skkholiya_upload_data/dataflow_job/batch/output \
    --temp_location gs://skkholiya_upload_data/temp/ \
    --job_name my-dataflow-batch-job
'''

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging
import datetime
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
        parser.add_argument("--runner", help="Beam Runner (DataflowRunner, DirectRunner, etc.)")
        parser.add_argument("--region",  help="Region to create a dataflow job")
        parser.add_argument("--input_path", required=True, help="GCS input path")
        parser.add_argument("--output_path", required=True, help="GCS input path")
        parser.add_argument("--temp_location", required=True, help="GCS temp location path")
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
        fields = element.split(',')
        record = dict(zip(headers, fields))
        yield record

def format_joined_data(element):
    department, data = element
    counts = data['counts']
    first_employees = data['first_employees']

    count = counts[0] if counts else 0
    first_employee = first_employees[0] if first_employees else {}

    try:
        join_date = datetime.datetime.strptime(first_employee.get('join_date', ''), "%Y-%m-%d") if first_employee.get('join_date') else None  # Parse date or None
    except ValueError:
        join_date = None  # Handle invalid date format
        logger.warning(f"Invalid date format: {first_employee.get('join_date')}")
    #While loading data to gcs other wise use join_date
    join_date_string = join_date.isoformat() if join_date else None # Convert datetime to string or None
    return {
        "depart": department,
        "emp_count_depart": count,
        "emp_id": first_employee.get('emp_id', ''),
        "join_date": join_date_string  # Store datetime or None

    }
#Find the first employee to join in each department and count the number of employees in each department.
def run():
    known_args, pipeline_options = BatchPipelineOptions.from_args()

    csv_headers = ["emp_id","name","depart","join_date"]
    with beam.Pipeline(options=pipeline_options) as pipeline:
        group_by_department = (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(known_args.input_path, skip_header_lines=1)
            | "Convert to JSON" >> beam.ParDo(CSVToJson(), headers=csv_headers)
            | "Key by Department" >> beam.Map(lambda x: (x['depart'], x))  
        )

        department_count = (
            group_by_department
            | "Count Employees" >> beam.combiners.Count.PerKey()
        )

        deprt_first_employee = (
            group_by_department
            | "First Employee" >> beam.CombinePerKey(min, key=lambda x: datetime.datetime.strptime(x['join_date'], "%Y-%m-%d") if isinstance(x['join_date'], str) else x['join_date']) #Handle String and Datetime
        )

        joined_data = (
            {'counts': department_count,'first_employees': deprt_first_employee}
            | "CoGroupByKey" >> beam.CoGroupByKey()
        )
        # display joined data
        # | "extract(id,depart,count)" >> beam.Map(lambda msg: [msg[0],*msg[1]["counts"],*[i['emp_id'] for i in msg[1]["first_employees"]]
        # ,*[i['join_date'] for i in msg[1]["first_employees"]]])
        final_data = (
            joined_data
            | "Format Data" >> beam.Map(format_joined_data)
        )

        # final_data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
        #     table=f"{known_args.project}:dataflow_batch_jobs.emp_depart_count",
        #     schema="depart:STRING,emp_count_depart:INTEGER,emp_id:STRING,join_date:TIMESTAMP", 
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #     custom_gcs_temp_location=known_args.temp_location
        # )

        #write to gcs
        final_data | "write to gcs" >> beam.Map(json.dumps) | beam.io.WriteToText(
            known_args.output_path, 
            file_name_suffix=".json"
        )
if __name__ == "__main__":
    run()
