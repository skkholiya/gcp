import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.direct.direct_runner import DirectRunner
import re

options = PipelineOptions(["--runner=DirectRunner"])


news_org_side_input = ["Business Today","The Hindu","NDTV"]

regex_hour = "^[0-9]+ (hours|hour) ago$"

class TransfromNews(beam.DoFn):
    def process(self,element,channel_side_input):
        result = [content.strip() for content in element if content.strip() not in channel_side_input]
        hour_check = [i for i in result if re.match(regex_hour,i) is None]
        yield hour_check

with beam.Pipeline(options=options) as pipeline:
    data = (
        pipeline | beam.Create(
            ['''NDTV
             Almost Irrelevant PM Modi Slams UN, International Organisations On Podcast
             2 hours ago
             
            By Manjiri Chitre

            Business Today
            ‘He who is afraid of…’: Congress criticises PM Modi’s podcast with Lex Fridman, calls it hypocrisy
            1 hour ago

            The Hindu
            False narrative created over 2002 Gujarat riots, courts found us innocent: PM
            12 hours ago'''
            ]
        )
    )
    transfrom = (
        data | "Split data to list" >> beam.Map(lambda data: data.split("\n")) 
        | "news channel ParDO" >> beam.ParDo(TransfromNews(),news_org_side_input)
        | "flatten the list" >> beam.FlatMap(lambda x:x)
        | "filter hours data" >> beam.Filter(lambda data: data!="")
    )
    print_data = transfrom | beam.Map(print)