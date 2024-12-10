import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

from weather_test import tmax_schema

def main(inputs, model_file):
    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    
    day_of_year_query = 'SELECT *, dayofyear(date) AS day_of_year FROM __THIS__'
    sql_trans = SQLTransformer(statement=day_of_year_query)
    yesterday_temp_query = '''
        SELECT today.*, yesterday.tmax AS yesterday_tmax
        FROM __THIS__ as today
        INNER JOIN __THIS__ as yesterday
        ON date_sub(today.date, 1) = yesterday.date
        AND today.station = yesterday.station
        '''
    sql_trans_yesterday = SQLTransformer(statement=yesterday_temp_query)
        
    weather_assembler = VectorAssembler(
        inputCols=['latitude', 'longitude', 'elevation', 'day_of_year', 'yesterday_tmax'],
        outputCol='features'
        )
    gbt_regressor = GBTRegressor(featuresCol='features', labelCol='tmax')
    pipeline = Pipeline(stages=[sql_trans, sql_trans_yesterday, weather_assembler, gbt_regressor])
    model = pipeline.fit(train)
    
    predictions = model.transform(validation)
    
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    
    print('r2 =', r2)
    print('rmse =', rmse)
    
    model.write().overwrite().save(model_file)

    
if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    main(inputs, model_file)
