import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    
    # TODO: create a pipeline to predict RGB colours -> word
    rgb_assembler = VectorAssembler(inputCols=['R', 'G', 'B'], outputCol='rgb')
    word_indexer = StringIndexer(inputCol='word', outputCol='label')
    logistic_classifier = LogisticRegression(featuresCol='rgb', labelCol='label')

    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, logistic_classifier])
    rgb_model = rgb_pipeline.fit(train)

    # TODO: create an evaluator and score the validation data
    predictions = rgb_model.transform(validation)
    evaluator = MulticlassClassificationEvaluator(
        labelCol='label',
        predictionCol='prediction',
        metricName='accuracy'
    )
    
    accuracy = evaluator.evaluate(predictions)
    
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('Validation score for RGB model: %g' % (accuracy, ))
    
    # TODO: create a pipeline RGB colours -> LAB colours -> word; train and evaluate.
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    sql_trans = SQLTransformer(statement=rgb_to_lab_query)
    
    lab_assembler = VectorAssembler(inputCols=['labL', 'labA', 'labB'], outputCol='lab')
    word_indexer = StringIndexer(inputCol='word', outputCol='label')
    logistic_classifier = LogisticRegression(featuresCol='lab', labelCol='label')
    
    pipeline = Pipeline(stages=[sql_trans, lab_assembler, word_indexer, logistic_classifier])
    model = pipeline.fit(train)
    predictions = model.transform(validation)
    
    evaluator = MulticlassClassificationEvaluator(
        labelCol='label',
        predictionCol='prediction',
        metricName='accuracy'
    )
    accuracy = evaluator.evaluate(predictions)
    
    plot_predictions(model, 'LAB', labelCol='word')
    print('Validation score for LAB model:', accuracy)
    

    
if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
