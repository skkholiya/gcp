#!/usr/bin/env python
# coding: utf-8

# In[32]:


import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib

def custom_side_inputs(element, min_len, max_len):
    for word in element.split():
        if len(word)>min_len and len(word)<max_len:
            yield (word,len(word))
class SearchElement(beam.DoFn):
    def process(self, element, side_input):
        for word in element.split(" "):
            if word in side_input:
                yield (word,1)

pipeline = beam.Pipeline(InteractiveRunner())
ai_txt_data = (
    pipeline | "AI text" >>
    beam.Create([
        'Artificial Intelligence (AI) is transforming the way we interact with technology, revolutionizing industries ranging from healthcare and finance to transportation and entertainment.\
        By leveraging advanced algorithms, machine learning, and deep learning, AI enables computers to perform tasks that typically require human intelligence, such as problem-solving, decision-making, and language understanding.\
        From voice assistants and recommendation systems to autonomous vehicles and medical diagnostics, AI enhances efficiency, accuracy, and personalization. While AI offers immense benefits,\
        it also raises ethical concerns regarding Intelligence data privacy, bias, and job displacement. As the field continues to evolve, responsible AI development and regulation will be crucial in ensuring its positive impact on society.'
    ]))
#pass side inputs using user defined function
word_len = ai_txt_data | "Group word and len" >> beam.FlatMap(custom_side_inputs,2,10)

#pass side inputs using pardo
word_len_pardo = ai_txt_data | "Group word and len pardo" >> beam.ParDo(SearchElement(),["AI","intelligence","algorithm"])

#Count the side input keys data
count_per_key = word_len_pardo | "count per key" >> beam.combiners.Count.PerKey()

#ib.show(count_per_key)
    


# In[33]:


#Composite ptransform: combine nested ptransfromation to a single composite ptransfrom
class CompositeTransformation(beam.PTransform):
    def expand(self, pcol):
        return (
            pcol
            | "Split the data" >> beam.FlatMap(lambda data: data.split())
            | "Remove special chars" >> beam.Regex.matches(r"[^.|,|(|)]+")
            | "Element length should be >5" >> beam.Filter(lambda word: len(word)>5)
            | "Map to tuple" >> beam.Map(lambda data:(data,1))
        )
sanitize_data = ai_txt_data | "extract words frequency from txt">> CompositeTransformation()
count_perKey = sanitize_data | beam.combiners.Count.PerKey()
ib.show(count_perKey)

