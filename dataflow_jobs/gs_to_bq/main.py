
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import logging
from apache_beam.io.filesystems import FileSystems
from apache_beam.transforms.sql import SqlTransform
import ast
# Define Apache Beam pipeline options
pipeline_options = PipelineOptions(
    [
        "--project=chrome-horizon-448017-g5",
        "--region=us-central1",
        "--runner=DirectRunner",
        "--temp_location=gs://load_data_to_colossus/temp",
    ]
)
# Optimized Data Transformation Function
class CustomJsonParse(beam.DoFn):
    def process(self, element): 
        try:
            for nested_data in element["performance"]:
                yield {
                    "runs": nested_data["batting"]["runs"],
                    "age": element["age"],
                    "player_name": element["player_name"],
                    "overs": nested_data["bowling"]["overs"],
                    "wickets": nested_data["bowling"]["wickets"],
                    "catches": nested_data["fielding"]["catches"],
                    "team": element["team"],
                    "match_no": nested_data["match_no"],
                    "previous3innings": element["previous_3_batting_avg"]
                }
        except Exception as e:
            logging.warning(f"Bad record: {element}")
        


class ParseJSONFile(beam.DoFn):
    """Reads and parses the entire JSON file from GCS."""
    
    def process(self, file_path):
        # Read entire file content
        with FileSystems.open(file_path) as f:
            file_content = f.read().decode("utf-8")
        
        # Convert JSON string to Python object (list of dictionaries)
        data = json.loads(file_content)
        
        # Emit each record in the JSON array
        for record in data:
            yield record

def customFilter(element):
    return all(item['overs'] != 0 for item in element)

qry = '''
select playername, max(previous3innings) as best_previous3innings,
sum(runs) as total_run, sum(wickets) as total_wickets
from PCOLLECTION 
group by 1
'''
def str_to_list(dct):
    dct.update({"best_previous3innings":ast.literal_eval(dct["best_previous3innings"])})
    return dct

# Define Apache Beam Pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:
    transformd_data = (
        pipeline
        | "Create PCollection" >> beam.Create(["gs://skkholiya_upload_data/data/ipl_player_stats.json"])
        | "Read and Parse JSON" >> beam.ParDo(ParseJSONFile())  # Read full JSON file from GCS
        | "Parse JSON" >> beam.ParDo(CustomJsonParse())
        | "Filter records, where player not bowled" >> beam.Filter(lambda item: item['overs'] != 0)
        | "beam to row" >> beam.Map(lambda data:beam.Row(
            playername = str(data['player_name']),
            opposition_team = str(data['team']),
            runs = int(data['runs']),
            overs = int(data['overs']),
            wickets = int(data['wickets']),
            catches = int(data['catches']),
            match_no = str(data['match_no']),
            age = int(data['age']),
            previous3innings = str(data['previous3innings'])
        ))
        | "Player runs and wickets">> SqlTransform(qry,"zetasql")
        | "Convert bq readable dict" >> beam.Map(lambda x: x._asdict())
        | "Convert best_previous3innings to dict" >> beam.Map(lambda data: str_to_list(data))
    )
    write_to_bq = (
        transformd_data | "write to bq">> beam.io.Write(
            beam.io.WriteToBigQuery(
                # "ipl_dump",
                # project="chrome-horizon-448017-g5",
                # dataset = f"dataflow_batch_jobs",
                #or
                table= "chrome-horizon-448017-g5:dataflow_batch_jobs.ipl_dump",
                schema= "SCHEMA_AUTODETECT",
                create_disposition= beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
    )