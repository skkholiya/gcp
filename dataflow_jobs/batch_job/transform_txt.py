#!/usr/bin/env python
# coding: utf-8

# In[43]:


import re
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText,WriteToText
from apache_beam.runners import DataflowRunner
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib
import google.auth

#Create a ParDo which transform txt file to words
# In[33]:


class ConvertToWords(beam.DoFn):
    def process(self,element):
        try:
            data = element.split(" ")
            return data
        except Exception as e:
            logging.error("Not able to process the data:",e)   


# In[55]:


pipeline = beam.Pipeline(InteractiveRunner())

read_data = pipeline | "read data from gcs" >> ReadFromText("gs://skkholiya_upload_data/data/text_story_epic.txt")

words = read_data | "extract words form file" >> beam.ParDo(ConvertToWords())

filter_data = words | "extract only selected words" >> beam.Filter(lambda word: word in ["Malakar’s","Eldoria","Kael","Celestial","Seraphina"])

group_by_key = filter_data | "Each key count">> beam.combiners.Count.PerElement()



# Set pipeline option to run the job in Dataflow

# In[57]:


option = pipeline_options.PipelineOptions(flags=[])

#Set pipeline option to current auth login user
_,option.view_as(GoogleCloudOptions).project = google.auth.default()
option.view_as(GoogleCloudOptions).region = "us-central1"
#output_path to load the data
output_path = "gs://skkholiya_upload_data/dataflow_job/batch"
option.view_as(GoogleCloudOptions).temp_location = f"{output_path}/temp"


# load the data into GCS bucket

# In[58]:


dump_data = group_by_key | "load data to gcs" >> beam.io.WriteToText(f"{output_path}/output/dump.txt")

DataflowRunner().run_pipeline(pipeline,option)

ib.show_graph(pipeline)


# In[ ]:




