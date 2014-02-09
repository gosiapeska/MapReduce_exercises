import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    person = record[0]
    friend = record[1]
    
    mr.emit_intermediate(person + friend, [1, person, friend])
    mr.emit_intermediate(friend + person, [0, friend, person])
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
	total = 0
	print "*****************************"
	for v in list_of_values:
		total += v[0]
	if total == 0:
		mr.emit((list_of_values[0][1], list_of_values[0][2]))
		mr.emit((list_of_values[0][2], list_of_values[0][1]))
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
