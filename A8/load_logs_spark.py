from cassandra.cluster import Cluster
    
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import expr, to_timestamp
import sys, re, math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def main(input, keyspace, table):
    nasa_rdd = spark.sparkContext.textFile(input)
    split_logs = nasa_rdd.map(split_data).filter(lambda x: x is not None)
    nasa_df = spark.createDataFrame(split_logs, schema=nasa_schema)
    nasa_df = nasa_df.withColumn("datetime", to_timestamp("datetime", "dd/MMM/yyyy:HH:mm:ss")) \
                    .withColumn("id", expr("uuid()"))
    nasa_df = nasa_df.repartition(80)
    nasa_df.write.format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace).mode('append').save()   
         
def split_data(log):
    match = line_re.search(log)

    if match:
        host = match.group(1)
        datetime = match.group(2)
        path = match.group(3)
        bytes = int(match.group(4))
        return (host, datetime, path, bytes)

if __name__ == '__main__':
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder \
        .appName("Load logs spark") \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    
    input = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    
    nasa_schema = types.StructType([
        types.StructField('host', types.StringType()),
        types.StructField('datetime', types.StringType()),
        types.StructField('path', types.StringType()),
        types.StructField('bytes', types.IntegerType())
    ])
    main(input, keyspace, table)
    