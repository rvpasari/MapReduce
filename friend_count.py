''' We implement a mapreduce to calculate 
the number of friends a user has. Data obtained 
from the friends.json file, problem given as part 
of assignment from a UW course. 
'''
# coding: utf-8


import MapReduce
import sys
import json

mr = MapReduce.MapReduce()

def mapper(record):
    friend1 = record[0]
    friend2 = record[1] 
    #print (friend1, friend2)
    mr.emit_intermediate(friend1, 1)
    mr.emit_intermediate(friend2, 1)
        
def reducer(key, values): 
    total = 0 
    for value in values: 
        total += value
    mr.emit((key, total))

inputdata = open('friends.json')
mr.execute(inputdata,mapper,reducer)



