import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from weather_test import tmax_schema
import datetime


def test_model(model_file):
    data = [
        ('SFU', datetime.date(2024, 11, 22), 49.2771, -122.9146, 330.0, 12.0),
        ('SFU', datetime.date(2024, 11, 23), 49.2771, -122.9146, 330.0, 0.0)
    ]

    test_tmax = spark.createDataFrame(data, schema=tmax_schema)

    model = PipelineModel.load(model_file)
    
    prediction = model.transform(test_tmax)
    prediction = prediction.select('prediction').collect()[0][0]
    print('Predicted tmax tomorrow:', prediction)



if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)
