#!/usr/bin/env python
# coding: utf-8

# In[4]:


import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib


# In[37]:


with beam.Pipeline(InteractiveRunner()) as pipeline:
    create_species_name = pipeline | "Species and name" >> beam.Create([
        ("Simba", "Lion"),
        ("Dory", "Fish"),
        ("Baloo", "Bear"),
        ("Timon", "Meerkat"),
        ("Pumbaa", "Warthog"),
        ("Bambi", "Deer"),
        ("Dumbo", "Elephant"),
        ("Mickey", "Mouse"),
        ("Scooby", "Dog"),
        ("Garfield", "Cat")
    ])
    city_state_us = pipeline | "US city and state" >> beam.Create([
        ("NY", "New York"),
        ("CA", "Los Angeles"),
        ("IL", "Chicago"),
        ("TX", "Houston"),
        ("AZ", "Phoenix"),
        ("PA", "Philadelphia"),
        ("TX", "San Antonio"),
        ("CA", "San Diego"),
        ("TX", "Dallas"),
        ("CA", "San Jose")
    ])
    pcol3 = pipeline | "letters and rnak" >> beam.Create([
        ("abc",1),
        ("pqr",4),
        ("xyz",2),
        ("abc",8),
        ("xyz",7)
    ])
    fruits = pipeline | "fruits" >> beam.Create([
        ("mango","pineapple","guava","himalayan berry","mango","guava","apple")
    ])
    nested_values = pipeline | "List of int list" >> beam.Create([
        [1,2,3],
        [5,6,7],
        [11,4,3]
    ])


# In[17]:


#Convert a pcol to string using: ToString
df_to_kv_string = create_species_name | "To kv String" >> beam.ToString.Kvs()
df_to_element_string = city_state_us | "To element String" >> beam.ToString.Element()
df_to_iterable_string = pcol3 | "To Itreable String" >> beam.ToString.Iterables()
#ib.show(df_to_kv_string,df_to_element_string,df_to_iterable_string)


# In[22]:


#Keys element wise operation keys and values, KEY VALUE SWAP
keys = pcol3 | "keys from dataframe" >> beam.Keys()
values = pcol3 | "values from dataframe" >> beam.Values()
kvswap = pcol3 | "swap the key with value and value with key" >> beam.KvSwap()
#ib.show(values)


# In[35]:


#Distinct: for selecting non-duplicate element from a pcol
distinct_fruit_records = fruits | "remove duplicate" >> beam.Distinct()
#ib.show(distinct_fruit_records)


# In[36]:


#CombineGlobally: Combine all elements based on some logic
def custom_combiner(elements:list) -> list:
    final_list = []
    for list_obj in elements:
        final_list += list_obj
    return final_list

combine_data = nested_values | "Reduce list of int list to list" >> beam.CombineGlobally(custom_combiner)
#ib.show(combine_data)


# In[43]:


#CombinePerKey: combine elements based on key
sum_per_key = pcol3 | "Sum values based on key" >> beam.CombinePerKey(sum)
#ib.show(combine_per_key)


# In[55]:


#Count: for counting no of elements in each aggregation
#Mean: transformers for computing the arithimatic mean of the elements in a collection
count_globally = pcol3 | "Count elements globally" >> beam.combiners.Count.Globally()
count_element = pcol3 | "Count per element" >> beam.combiners.Count.PerElement()
count_per_key = pcol3 | "Count per key elements" >> beam.combiners.Count.PerKey()
#Mean operations
mean_globally = pcol3 |"Extract Values" >> beam.Values() | "Mean of the elements globally" >> beam.combiners.Mean.Globally()
mean_perKey = pcol3 | "Mean of the elements perKey" >> beam.combiners.Mean.PerKey()

ib.show(mean_globally,mean_perKey)

