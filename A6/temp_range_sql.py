import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs, output):
    weather = spark.read.csv(inputs, schema=temp_schema)
    weather.createOrReplaceTempView("weather")
    spark.sql("""
              CREATE OR REPLACE TEMP VIEW valid_weather AS
              SELECT station, date, observation, value
              FROM weather
              WHERE qflag is NULL
                """)
    
    spark.sql("""
              CREATE OR REPLACE TEMP VIEW tmax AS
              SELECT station, date, value AS t_max
              FROM valid_weather
              WHERE observation = 'TMAX'
                """)
    
    spark.sql("""
              CREATE OR REPLACE TEMP VIEW tmin AS
              SELECT station, date, value AS t_min
              FROM valid_weather
              WHERE observation = 'TMIN'
                """)
    spark.sql("""
              CREATE OR REPLACE TEMP VIEW temp_range AS
              SELECT tmax.station, tmax.date, (t_max - t_min)/10 AS temp_range_col
              FROM tmax
              JOIN tmin on tmax.station = tmin.station AND tmax.date = tmin.date
                """)
    spark.sql("""
              CREATE OR REPLACE TEMP VIEW max_day_range AS
              SELECT date, MAX(temp_range_col) AS max_range
              FROM temp_range
              GROUP BY date
              """)
    
    max_station = spark.sql("""
              SELECT temp_range.date, temp_range.station, max_day_range.max_range 
              FROM temp_range
              JOIN max_day_range
              ON temp_range.date = max_day_range.date AND temp_range.temp_range_col = max_day_range.max_range
              ORDER BY temp_range.date, temp_range.station
              """)
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