#!/usr/bin/env python
# coding: utf-8

# In[51]:


import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib


# In[52]:


with beam.Pipeline(InteractiveRunner()) as pipeline:
    read_data = pipeline | "Gardining Farms" >> beam.Create(["Onion, Strawberry,Carrot,Potato,Eggplant,Tomato"])
    #flatten_data = pipeline | "Collection of list data" >> beam.Create([[2,3],[2,1]])
    
    #Map: for one to one mapping
    map_transform = read_data | "Map the data" >> beam.Map(lambda data: len(data))
    #FlatMap: for one to many mapping(take one element as input and produce multiple element as op)
    flat_map_transform = read_data | "Flat map the data" >> beam.FlatMap(lambda dta: dta.split(","))
    ib.show(read_data)
    ib.show(flat_map_transform)
    


# In[54]:


#Pardo is for user defined transformation
class SplitData(beam.DoFn):
    def __init__(self, delimiter=","):
        self.delimiter = delimiter
    def process(self, data):
        for word in data.split(self.delimiter):
            yield (word,len(word))

p = beam.Pipeline(InteractiveRunner())
read_farm_data = p | "Gardining Farms" >> beam.Create(["Onion, Strawberry,Carrot,Potato,Eggplant,Tomato"])
custom_defined_function = read_farm_data | beam.ParDo(SplitData(","))
ib.show(custom_defined_function)


# In[5]:


ib.show_graph(p)


# In[55]:


#Filter: filter out the records based on some conditions
read_input = p | "List of elements" >> beam.Create([1,33,2,44,3,22,8,32,99,5,6,45,65])
filter_data = read_input | "filtering the even numbers" >> beam.Filter(lambda data: data%2==0)
ib.show(filter_data)


# In[57]:


#Flatten: used to union the two pcollection together
countries1 = [
    {"name": "United States", "population": 331002651},
    {"name": "India", "population": 1393409038},
    {"name": "China", "population": 1444216107},
    {"name": "Brazil", "population": 213993437},
    {"name": "Russia", "population": 145912025}
]

countries2 = [
    {"name": "Japan", "population": 126476461},
    {"name": "Germany", "population": 83783942},
    {"name": "United Kingdom", "population": 67886011},
    {"name": "France", "population": 65273511},
    {"name": "Italy", "population": 60461826}
]

country1_pcol = p | "Read countries1" >> beam.Create(countries1)
country2_pcol = p | "Read countries2" >> beam.Create(countries2)

union_pcol = (country1_pcol, country2_pcol) | beam.Flatten()

ib.show(union_pcol)


# In[58]:


#Partition: branching pipeline. syntax: Partition(fun, partition_num)
def age_group(age):
    if age<20:
        return 0
    elif age>21 and age<41:
        return 1
    elif age>41 and age<56:
        return 2
    else:
        return 3
    

read_feed = p | beam.Create(["banana", "kiwi","orange","strawberry","graps","avacado"])
par1,par2 = read_feed | beam.Partition(lambda data,partition: len(data)%2,2)

teen,young,middle,old = read_input | "Partition using custom function">> beam.Partition(lambda data,partition:age_group(data),4)
ib.show(teen,young,middle,old)


# In[62]:


#GroupByKey: Groupping elements based on the key
elements = [("aa",1),("bb",3),("cc",11),("bb",10),("aa",9),("cc",7),("aa",6),("bb",22),("aa",2)]
data_read = p | "read data from in-memory" >> beam.Create(elements)
groupping_data = data_read | "groupping keys data" >> beam.GroupByKey(lambda x:x[0])

#CoGroupByKey: Joining pcollection based on same key
job = [
    ("Alice", "Software Engineer"),
    ("Bob", "Doctor"),
    ("Charlie", "Teacher"),
    ("David", "Architect"),
    ("Emma", "Graphic Designer")
]

hobbies = [
    ("Alice", "Reading"),
    ("Bob", "Cycling"),
    ("Charlie", "Painting"),
    ("David", "Photography"),
    ("Emma", "Gardening")
]

job_pcol = p | "Create job pcollection" >> beam.Create(job)
hobbies_pcol = p | "Hobbies pcollection" >> beam.Create(hobbies)

joining_data = (job_pcol,hobbies_pcol) | "joining same key data" >> beam.CoGroupByKey()

ib.show(joining_data)






# In[ ]:



