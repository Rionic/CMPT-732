from pyspark import SparkConf, SparkContext
import sys, json
assert sys.version_info >= (3, 5)

def main(inputs, output):
    redditData = sc.textFile(inputs)
    jsonSubreddits = redditData.map(rdd_to_json)
    tupleSubreddits = jsonSubreddits.map(json_to_tuple)
    eSubreddits = tupleSubreddits.filter(contains_e)
    eSubreddits.cache()
    positiveSubreddits = eSubreddits.filter(is_positive).map(json.dumps).saveAsTextFile(output + '/positive')
    negativeSubreddits = eSubreddits.filter(is_negative).map(json.dumps).saveAsTextFile(output + '/negative')

def rdd_to_json(line):
    return json.loads(line)

def json_to_tuple(line):
    return (line['subreddit'], line['score'], line['author'])

def contains_e(line):
    return 'e' in line[0]

def is_positive(line):
    return line[1] > 0

def is_negative(line):
    return line[1] <= 0

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)