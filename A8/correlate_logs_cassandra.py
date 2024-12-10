from pyspark.sql import SparkSession, functions as F
import sys, re, math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def main(input, keyspace, table):
    logs = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace).load()
    logs.cache()
    requests_bytes = logs.groupBy('host').agg(F.count('host').alias('x'), F.sum('bytes').alias('y'))
    
    pre_sums = requests_bytes.withColumn('n', F.lit('1')) \
        .withColumn('x^2', requests_bytes['x']**2) \
        .withColumn('y^2', requests_bytes['y']**2) \
        .withColumn('xy', requests_bytes['y']*requests_bytes['x']) \

    sums = pre_sums.agg(
        F.sum('x').alias('x'),
        F.sum('y').alias('y'),
        F.sum('n').alias('n'),
        F.sum('x^2').alias('x^2'),
        F.sum('y^2').alias('y^2'),
        F.sum('xy').alias('xy')
    )
    sums = sums.collect()[0]
    n, x, y, x2, y2, xy = sums['n'], sums['x'], sums['y'], sums['x^2'], sums['y^2'], sums['xy']
    r = (n*xy - x*y)/(math.sqrt(n*x2 - x**2) * math.sqrt(n*y2 - y**2))
    print('r = ', r)
    print('r^2 = ', r**2)
        
def calculate_r(sums):
    n, x, x2, y, y2, xy = sums[0], sums[1], sums[2], sums[3], sums[4], sums[5]
    return ((n*xy - x*y)/(math.sqrt(n*x2 - x**2) * math.sqrt(n*y2 - y**2)))
    
if __name__ == '__main__':
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder \
        .appName("Load logs spark") \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    keyspace = sys.argv[1]
    table = sys.argv[2]
    
    main(input, keyspace, table)
