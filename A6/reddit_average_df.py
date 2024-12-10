import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs, output):
    comments = spark.read.json(inputs, schema=reddit_schema)
    averages = comments.groupBy('subreddit').agg(functions.avg('score').alias('avg_score'))
    averages.write.csv(output, mode='overwrite')
    averages.explain()

if __name__ == '__main__':
    spark = SparkSession.builder.appName('reddit average df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    inputs = sys.argv[1]
    output = sys.argv[2]
    reddit_schema = types.StructType([
        types.StructField('subreddit', types.StringType()),
        types.StructField('score', types.IntegerType()),
    ])
    main(inputs, output)