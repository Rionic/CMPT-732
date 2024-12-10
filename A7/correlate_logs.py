from pyspark import SparkConf, SparkContext
import sys, re, math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def main(inputs):
    logs = sc.textFile(inputs)
    host_bytes_logs = logs.map(get_host_bytes)
    host_bytes_logs.cache()
    host_bytes_logs = host_bytes_logs.filter(lambda x: x is not None)
    bytes_count = host_bytes_logs.map(count_value)
    
    bytes_per_host = bytes_count.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    sums = bytes_per_host.map(calculate_inner).reduce(calculate_sum)
    r = calculate_r(sums)
    print('r = ', r)
    print('r^2 = ', r**2)
    
def calculate_r(sums):
    n, x, x2, y, y2, xy = sums[0], sums[1], sums[2], sums[3], sums[4], sums[5]
    return ((n*xy - x*y)/(math.sqrt(n*x2 - x**2) * math.sqrt(n*y2 - y**2)))
    
def calculate_sum(x, y):
    return (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4], x[5] + y[5])
    
def calculate_inner(xy):
    x = xy[1][0]
    y = xy[1][1]
    return (1, x, x**2, y, y**2, x*y)
    
def count_value(log):
    return (log[0], (1, log[1]))

def get_host_bytes(log):
    match = line_re.search(log)

    if match:
        host = match.group(1)
        bytes = int(match.group(4))
        return (host, bytes)
    
if __name__ == '__main__':
    conf = SparkConf().setAppName('correlate logs')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    main(inputs)