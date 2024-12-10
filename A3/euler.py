from pyspark import SparkConf, SparkContext
import sys, random
assert sys.version_info >= (3, 5)

def main(inputs):
    samples = int(inputs)
    rddSamples = sc.parallelize(range(samples), 80)
    sample_Vs = rddSamples.mapPartitions(compute_sample)
    total_iterations = sample_Vs.sum()
    print(total_iterations/samples)

def compute_sample(partition):
    random.seed()
    total_iterations = 0

    for i in partition:
        iterations = 0
        eSum = 0.0
        while eSum < 1:
            eSum += random.random()
            iterations += 1
        total_iterations += iterations
    yield total_iterations

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'
    inputs = sys.argv[1]
    main(inputs)