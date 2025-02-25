#!/usr/bin/env python
# coding: utf-8

# In[19]:


import apache_beam as beam
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner

query = '''SELECT distinct rental_id,start_date FROM 
`bigquery-public-data.london_bicycles.cycle_hire`
where DATE(start_date) = '2017-01-01'
order by start_date LIMIT 100'''
pipeline_options = PipelineOptions(
    project = "chrome-horizon-448017-g5",
    region = "asia-south2",
    temp_location = "gs://skkholiya_upload_data/temp"
)

pipeline = beam.Pipeline(InteractiveRunner(),pipeline_options)
read_from_bq = pipeline|"Read london bicycles data from bq" >> beam.io.ReadFromBigQuery(query=query,use_standard_sql=True)


# In[34]:


#Create a map of rental_id and start_date
map_data = (read_from_bq | "Map data">> beam.Map(lambda data:{"rental_id":data['rental_id'],"start_date":data['start_date']})
           |"Create timestamp" >> beam.Map(lambda data :beam.window.TimestampedValue(data,data['start_date'].timestamp()))
           )
#ib.show(map_data)


# In[40]:


@beam.ptransform_fn
def ProcessWindowElements(pcol):
    class PrintCountInfo(beam.DoFn):
        def process(self, element, window=beam.DoFn.WindowParam):
            yield element
    return pcol | "count per window count" >> beam.combiners.Count.Globally().without_defaults() | "print count info" >> beam.ParDo(PrintCountInfo())

#fixed_window = map_data | "convert to fixed window" >>beam.WindowInto(beam.window.FixedWindows(60*5)) | "print window info" >> ProcessWindowElements()
#side_window = map_data | "convert to side window" >> beam.WindowInto(beam.window.SlidingWindows(300,150)) | "print side info" >> ProcessWindowElements
session_window = map_data | "convert to session window" >> beam.WindowInto(beam.window.Sessions(30*60)) | "print session info" >> ProcessWindowElements()
ib.show(session_window)


# In[ ]:




