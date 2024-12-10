from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(graph, output, source, destination):
    rdd = sc.textFile(graph + '/links-simple-sorted.txt')
    edges_rdd = rdd.map(constuct_edges)
    edges = spark.createDataFrame(edges_rdd, ['node', 'neighbours'])
    edges.cache()
    known_paths = sc.parallelize([(source, 0, 0)])
    known_paths = spark.createDataFrame(known_paths, ['node', 'source', 'distance'])

    for i in range(6):
        known_paths = union_neighbour_path(known_paths, edges)
        known_paths.write.csv(output + '/iter-' + str(i))
        stop = known_paths.filter(known_paths['node'] == destination)
        if stop.count() > 0:
            break

    path = [destination]
    node = destination
    
    while node != source:
        row = known_paths.where(F.col('node') == node).select(F.col('source')).first()
        if row:
            node = row['source']
            path.append(node)
        else:
            print('No path found')
            break
        
    path.reverse()
    path_rdd = sc.parallelize(path)
    path_rdd.saveAsTextFile(output + '/path')
        
        
def union_neighbour_path(known_paths, edges):
    joined_neighbours = known_paths.join(edges, 'node').select(
        F.col('neighbours'),
        F.col('node').alias('source'),
        F.col('distance')
    )
    new_paths = joined_neighbours.withColumn('node', F.explode('neighbours')).select(
        F.col('node'),
        F.col('source'),
        (F.col('distance') + 1).alias('distance')
    )
    union_paths = known_paths.union(new_paths)
    min_paths = union_paths.withColumn('min_distance', F.min('distance').over(Window.partitionBy('node')))
    min_paths = min_paths.filter(min_paths['distance']  == min_paths['min_distance']).drop('min_distance')
    min_paths = min_paths.dropDuplicates()
    return min_paths


def constuct_edges(line):
    nodes = line.split()
    return (int(nodes[0][:-1]), [int(node) for node in nodes[1:]])


if __name__ == '__main__':
    conf = SparkConf().setAppName('shortest path')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sc.setLogLevel("WARN")
    graph = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    main(graph, output, int(source), int(destination))