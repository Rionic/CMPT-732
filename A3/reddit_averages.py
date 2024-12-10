from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

inputs = sys.argv[1]
output = sys.argv[2]

#{"subreddit": "helloworld", "score": 1}
#{"subreddit": "helloworld", "score": 2}

def main(inputs, output):
    subreddits = sc.textFile(inputs)
    jsonSubreddits = subreddits.map(rdd_to_json)
    tupleSubreddits = jsonSubreddits.map(json_to_tuple)
    combinedSubreddits = tupleSubreddits.reduceByKey(add_pairs)
    averageSubreddits = combinedSubreddits.map(average)
    jsonAverage = averageSubreddits.map(tuple_to_json)
    jsonOutput = jsonAverage.saveAsTextFile(output)

def tuple_to_json(line):
    return json.dumps({"subreddit": line[0], "average_score": line[1]})

def average(line):
    return (line[0], line[1][1]/line[1][0])

def add_pairs(x, y):
    return  (x[0] + y[0], x[1] + y[1])

def json_to_tuple(line):
    return (line['subreddit'], (1, line['score']))

def rdd_to_json(line):
    return json.loads(line)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)