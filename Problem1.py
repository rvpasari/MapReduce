import MapReduce 
import sys
import json

mr = MapReduce.MapReduce()

def mapper(document_list): # list is as [doc_id, text]
	key = document_list[0]	
	value = document_list[1]
	words = values.split()
	for word in words:
		mr.emit_intermediate(word,key)

def reducer(word, documents): 
	doc_list = [] 
	return(word, documents)

inputdata = open(sys.argv[0])
books = list(json.loads(json.dumps(inputdata)))
mr.execute(books,mapper,reducer)
