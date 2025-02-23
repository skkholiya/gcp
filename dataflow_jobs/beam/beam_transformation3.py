#!/usr/bin/env python
# coding: utf-8

# **RegEx

# In[47]:


import apache_beam as beam
import time
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
regex_matches = '([a-d][a-d]|\d+)'
pipeline = beam.Pipeline(InteractiveRunner())
pcol_txt = pipeline | "Create Pipeline" >> beam.Create([
    'aa sold qty is 10',
    'bb sold qty is 9',
    'cc sold qty is 22',
    'dd sold qty is 58',
    'qty sold 35 of bb'
])
#ib.show(pcol_txt)


# In[22]:


matches = pcol_txt | "Matches" >> beam.Regex.matches(regex_matches)
all_matches = pcol_txt | "All Matches" >> beam.Regex.all_matches(regex_matches)
matches_kv = pcol_txt | "Matches values and return kv" >> beam.Regex.matches_kv(regex_matches,keyGroup=0)
find = pcol_txt | "return first value search in whole string" >> beam.Regex.find(regex_matches)
find_all = pcol_txt | "search in whole str return all matches" >> beam.Regex.find_all(regex_matches)
find_kv = pcol_txt | "search in whole str return all matches in kv" >> beam.Regex.find_kv(regex_matches,keyGroup=0)
replace_all = pcol_txt | "replace all data by a given pattern" >>  beam.Regex.replace_all(r'\s',",")
replace_first = pcol_txt | "replace first value with specified value" >>  beam.Regex.replace_first(r'\s',",")
split_data = pcol_txt | "Split data with given seperator" >>  beam.Regex.split(" ")
#ib.show(split_data)


# **withTimestamp

# In[38]:


event_time_feed = pipeline |"Timestamp by Event time" >> beam.Create([
    {"name":"Strawberry","season":1585699200},
    {"name":"Carrot","season":1590969600}
])
logical_clock_feed = pipeline |"Timestamp by logical clock" >> beam.Create([
    {"name":"Strawberry","event_id":1},
    {"name":"Carrot","event_id":4}
])
process_feed = pipeline |"imeStamp by Processing time">> beam.Create([
    {"name":"Strawberry"},
    {"name":"Carrot"}
])


# In[42]:


class TimestampEventTime(beam.DoFn):
    def process(self, plant, timestamp=beam.DoFn.TimestampParam):
        yield '{}-{}'.format(timestamp.to_utc_datetime(),plant['name'])

class TimestampLogicalClock(beam.DoFn):
    def process(self, plant, timestamp = beam.DoFn.TimestampParam):
        event_id = int(timestamp.micros/1e6) #equivelent to seconds
        yield f"{event_id}-{plant['name']}"

class ProcessingTime(beam.DoFn):
    def process(self, plant, timestamp = beam.DoFn.TimestampParam):
        yield f"{timestamp.to_utc_datetime()}-{plant['name']}"


# In[48]:


with_ts_event = event_time_feed | "with timestamp event" >> beam.Map(lambda event: beam.window.TimestampedValue(event,event['season']))
get_timestamp_event = with_ts_event |"Get TimeStamp event" >> beam.ParDo(TimestampEventTime())

with_ts_logical = logical_clock_feed | "with timestamp logical" >> beam.Map(lambda event: beam.window.TimestampedValue(event,event['event_id']))
get_timestamp_logical = with_ts_logical |"Get TimeStamp logical" >> beam.ParDo(TimestampLogicalClock())

with_ts_processing = process_feed | "with timestamp processing" >> beam.Map(lambda event: beam.window.TimestampedValue(event,time.time()))
get_timestamp_processing = with_ts_processing |"Get TimeStamp processing" >> beam.ParDo(ProcessingTime())

ib.show(get_timestamp_processing)
