import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs, output):
    weather = spark.read.csv(inputs, schema=temp_schema)
    weather = weather.filter(weather['qflag'].isNull())

    tmax = weather.filter(weather['observation'] == 'TMAX').select('station', 'date', (weather['value']).alias('tmax'))
    tmin = weather.filter(weather['observation'] == 'TMIN').select('station', 'date', (weather['value']).alias('tmin'))

    temp_ranges = tmax.join(tmin, on=['station', 'date'])
    temp_ranges = temp_ranges.withColumn('range', (temp_ranges['tmax'] - temp_ranges['tmin'])/10)
    max_day_range = temp_ranges.groupBy('date').agg(functions.max('range')).withColumnRenamed('date', 'max_date').cache()

    max_station = temp_ranges.join(max_day_range, (temp_ranges['date'] == max_day_range['max_date']) &
                                     (temp_ranges['range'] == max_day_range['max(range)']))
    max_station = max_station.select('date', 'station', 'range').orderBy('date', 'station')
    max_station.write.csv(output, mode='overwrite', header=True)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    inputs = sys.argv[1]
    output = sys.argv[2]
    temp_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])
    main(inputs, output)