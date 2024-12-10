import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from kafka import KafkaConsumer

from pyspark.sql import SparkSession, functions as F, types

def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()
        
    values = messages.select(
        F.split(messages['value'].cast('string'), ' ').alias('coords')
    )
    values = values.select(
        F.col('coords').getItem(0).cast('float').alias('x'),
        F.col('coords').getItem(1).cast('float').alias('y')
    )
    values = values.withColumn('xy', F.col('x') * F.col('y')) \
    .withColumn('x^2', F.col('x')**2) \
    .withColumn('1', F.lit(1))
    values = values.groupBy().agg(
        F.sum('x').alias('sum_x'),
        F.sum('y').alias('sum_y'),
        F.sum('xy').alias('sum_xy'),
        F.sum('x^2').alias('sum_x^2'),
        F.sum('1').alias('n')
    )
    values = values.withColumn('1/n', 1 / F.col('n'))
    values = values.withColumn('beta', (F.col('sum_xy') - F.col('1/n') * F.col('sum_x') * F.col('sum_y')) / 
                               (F.col('sum_x^2') - F.col('1/n') * F.col('sum_x')**2))
    values = values.withColumn('alpha', F.col('sum_y') / F.col('n') - F.col('beta') * F.col('sum_x') / F.col('n'))
    values = values.select('alpha', 'beta')
    query = values.writeStream.outputMode('complete').format('console').start()

    query.awaitTermination(600)

if __name__ == '__main__':
    topic = sys.argv[1]
    spark = SparkSession.builder.appName('read stream').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(topic)