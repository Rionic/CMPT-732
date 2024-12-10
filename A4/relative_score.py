from pyspark import SparkConf, SparkContext
import sys, json
assert sys.version_info >= (3, 5)

def main(inputs, output):
    # This is a config for my local so spark can read directories
    # sc._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    
    subreddits = sc.textFile(inputs)
    jsonSubreddits = subreddits.map(json.loads)
    jsonSubreddits.cache()
    tupleSubreddits = jsonSubreddits.map(json_to_tuple)
    combinedSubreddits = tupleSubreddits.reduceByKey(add_pairs)
    averageSubreddits = combinedSubreddits.map(average)
    positiveSubreddits = averageSubreddits.filter(lambda x: x[1] > 0)

    commentBySub = jsonSubreddits.map(lambda c: (c['subreddit'], c))
    joinedSubreddits = positiveSubreddits.join(commentBySub)
    relativeScores = joinedSubreddits.map(relative_score)
    sortedScores = relativeScores.sortBy(lambda x: x[0], ascending=False)
    sortedScores.saveAsTextFile(output)

def relative_score(line):
    subreddit, (avg, comment) = line
    return (comment['score']/avg, comment['author'])

def tuple_to_json(line):
    return json.dumps({"subreddit": line[0], "average_score": line[1]})

def average(line):
    subreddit, (count, total_score) = line
    avg_score = total_score / count
    return (subreddit, avg_score)

def add_pairs(x, y):
    count_x, total_x = x  
    count_y, total_y = y
    return  (count_x + count_y, total_x + total_y)

def json_to_tuple(line):
    return (line['subreddit'], (1, line['score']))

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)