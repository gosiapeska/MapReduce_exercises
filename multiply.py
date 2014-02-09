import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	value = { }
	if record[0] == 'a':
		key = record[1]
		value[record[2]] = record[3]
		for a in range(5):	
			mr.emit_intermediate((key,a), value)
	else:
		key = record[2]
		value[record[1]] = record[3]
		for a in range(5):	
			mr.emit_intermediate((a, key), value)
def reducer(key, list_of_values):
    #append string
	values = {}
	total = 0 
	for v in list_of_values:
		k = v.keys()[0]
		if k in values:
			values[k].append(v[k]) 
		else:
			values[k] = [v[k]]
	for u in values.values():
		if len(u) > 1:
			total += u[0] * u[1]
	mr.emit((key[0], key[1], total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
