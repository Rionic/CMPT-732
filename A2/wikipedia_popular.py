from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def convert_to_int(line):
    hour, language, title, requests, numBytes = line.split()
    return (hour, language, title, int(requests), numBytes)

def filter_pages(tup):
    return not (tup[2].startswith("Special:") or tup[2] == "Main_Page" or tup[1] != "en")

def create_key_value_pairs(tup):
    return (tup[0], (tup[3], tup[2]))

def max_val(x, y):
    return x if (x[0] > y[0]) else y

def tab_separated(tup):
    return "%s\t%s" % (tup[0], tup[1])

sortedPages = sc.textFile(inputs).map(convert_to_int).filter(filter_pages) \
              .map(create_key_value_pairs).reduceByKey(max_val).sortBy(lambda x: x[0])
              
sortedPages.map(tab_separated).saveAsTextFile(output)

print(sortedPages.take(10))
