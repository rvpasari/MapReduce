''' This problem uses MapReduce to 
condense two different columns into 
one in a large data file. Problem as 
part of assignment given in UW course.
'''
# coding: utf-8

# In[15]:

import MapReduce
import sys
import json

mr = MapReduce.MapReduce()

def mapper(record):
    key = record[0]
    order_id = record[1]
    values = [] 
    if key == 'line_item':
        values = record[2:18]
    if key == 'order': 
        values = record[2:11]   
    for attribute in values: 
        mr.emit_intermediate(order_id, attribute)
        
def reducer(key, value): 
    mr.emit((key, value))
    

inputdata = open(sys.argv[0])
mr.execute(inputdata,mapper,reducer)


# In[ ]:




# In[ ]:




# In[ ]:



