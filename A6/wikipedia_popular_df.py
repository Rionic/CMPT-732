import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs, output):
    pages = spark.read.csv(inputs, sep=" ", schema=wikipedia_schema).withColumn('filename', functions.input_file_name())
    extract_datetime_udf = functions.udf(extract_datetime, types.StringType())
    pages = pages.withColumn('hour', extract_datetime_udf(pages['filename']))
    pages = pages.filter((pages['language'] == 'en') & (pages['title'] != 'Main_Page') \
                          & (~pages['title'].startswith('Special:')))
    
    pages = pages.cache()
    pages = pages.withColumnRenamed('requests', 'views')
    most_viewed = pages.groupBy(pages['hour']).agg(functions.max('views'))
    most_viewed = most_viewed.withColumnRenamed('hour', 'hour2')

    joined_max = pages.join(most_viewed.hint('broadcast'), (pages['views'] == most_viewed['max(views)']) &
                             (pages['hour'] == most_viewed['hour2'])).select(
                                 pages['hour'], pages['title'], pages['views'])
    joined_max = joined_max.orderBy(functions.asc('hour'), functions.asc('title'))
    joined_max.write.json(output, mode='overwrite')

def extract_datetime(path):
    split_ext = os.path.splitext(path)[0]
    split_path = split_ext[-15:-4]
    return split_path
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    inputs = sys.argv[1]
    output = sys.argv[2]
    wikipedia_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('requests', types.LongType()),
    ])
    main(inputs, output)