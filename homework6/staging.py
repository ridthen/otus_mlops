import os
import sys
import logging

sys.path.append(
    "/usr/lib/spark/python"
)

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as f
import re
from pyspark.sql.functions import col, asc, desc, isnan, when, trim

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import FloatType, DoubleType, IntegerType, StringType, TimestampType

from pyspark.sql.functions import udf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.pipeline import Pipeline

import mlflow
from mlflow.tracking import MlflowClient
from datetime import datetime

from scipy.stats  import norm, ttest_ind
from scipy import stats
import numpy as np

from pyspark.mllib.evaluation import BinaryClassificationMetrics, MulticlassMetrics

from pyspark.ml.tuning import CrossValidator, CrossValidatorModel, TrainValidationSplit, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

spark = (
    SparkSession.builder.appName('validate')
    # .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4')
    # .config("spark.executor.memory", "128g")
    # .config("spark.driver.memory", "128g")
    # .config("spark.executor.cores", "40")
    # .config("spark.executor.instances", "48")
    # .config("spark.default.parallelism", "48")
    .getOrCreate()
)
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.access.key", "key")
hadoop_conf.set("fs.s3a.secret.key", "secret")
hadoop_conf.set("fs.s3a.endpoint", "storage.yandexcloud.net")
hadoop_conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4')

# расчет метрик по предсказаниям модели
def evaluateModel(predictions):
    predictionAndTarget = predictions.select([predictions["tx_fraud"].cast(DoubleType()), predictions["prediction"].cast(DoubleType()).alias('label')])
    metrics_binary = BinaryClassificationMetrics(predictionAndTarget.rdd.map(tuple))
    metrics_multi = MulticlassMetrics(predictionAndTarget.rdd.map(tuple))
    return {'acc': metrics_multi.accuracy,
            'f1': metrics_multi.fMeasure(1.0),
            'precision': metrics_multi.precision(1.0),
            'recall': metrics_multi.recall(1.0),
            'auc': metrics_binary.areaUnderROC,
            'arp': metrics_binary.areaUnderPR
           }

# интервальные оценки выбранной метрики
def getScores(predictions):
    bootstrap_iterations = 100
    predictionAndTarget = predictions.select([predictions["tx_fraud"].cast(DoubleType()), predictions["prediction"].cast(DoubleType()).alias('label')])
    scores = []
    for i in range(bootstrap_iterations):
        sample = predictionAndTarget.sample(True, 0.01)
        metrics_binary = BinaryClassificationMetrics(sample.rdd.map(tuple))
        scores.append(metrics_binary.areaUnderPR)
    return scores

# построение гистограммы сравнения интервальной оценки на текущей модели и кандидате
def makeFig(scores, scores_candidate):
    # Create a figure and axis for the plot
    fig, axes = plt.subplots(1, 1, figsize=(8, 5))
    
    # Plot histograms for each dataset
    sns.histplot(data=pd.DataFrame(scores, columns=['APR']), x="APR", ax=axes, color='blue', label=calc_confidence_interval(scores), alpha=0.3)
    sns.histplot(data=pd.DataFrame(scores_candidate, columns=['APR']), x="APR", ax=axes, color='orange', label=calc_confidence_interval(scores_candidate), alpha=0.3)
    
    # Set plot labels and titles
    axes.set_xlabel("Area under Precision-Recall")
    axes.set_ylabel('Count')
    axes.set_title(f'{"Area under Precision-Recall"}')
    
    # Add legend
    axes.legend()
    
    # Adjust the layout
    plt.tight_layout()
    
    # Show the plot
    plt.savefig("/tmp/apr.png")
    return fig

# t-тест базовой модели и кандидата
def isValid(scores, scores_candidate, alpha = 0.01):
    pvalue = ttest_ind(scores, scores_candidate).pvalue
    
    print(f"p-value (APR): {pvalue:g}\t-\t", end='')
    if pvalue < alpha:
        print("Reject null hypothesis.")
    else:
        print("Accept null hypothesis.")
    return pvalue < alpha

# расчет доверительного варианта для выбранной метрики
def calc_confidence_interval(scores):
    scores_arr = np.asarray(scores)
    mean = scores_arr.mean()
    std_error = scores_arr.std() / np.sqrt(len(scores_arr))
    confidence_level = 0.95
    confidence_interval = stats.t.interval(confidence_level, len(scores)-1, mean, std_error)
    return (f'Confidence interval for APR: {confidence_interval[0]:.4f} <- {scores_arr.mean():.4f} -> {confidence_interval[1]:.4f}')

df = spark.read.parquet("s3a://mlops-homework-rvsmsjima/dataset-cleaned.parquet")

# загрузка и кодирование фич
reload_data = False
if reload_data:
    # возьмем только 1% данных для сокращения времени отладки и обучения
    #subsets = df.randomSplit([0.5, 0.95])
    subset = df.sample(0.01)
    
    df_1 = subset.filter(subset["tx_fraud"] == 1)
    df_0 = subset.filter(subset["tx_fraud"] == 0)
    
    df_1count = df_1.count()
    df_0count = df_0.count()
    
    df1_oversampled = df_1 \
    .withColumn("dummy",
            f.explode(
                f.array(*[f.lit(x)
                          for x in range(int(df_0count / df_1count))]))) \
    .drop("dummy")
    
    data = df_0.unionAll(df1_oversampled)
    
    data_indexer = StringIndexer(inputCols=["tx_fraud_scenario"], outputCols=["tx_fraud_scenario_index"])
    data_encoder = OneHotEncoder(inputCols=["tx_fraud_scenario_index"], outputCols=["tx_fraud_scenario_encoded"])
    
    numeric_cols = ["tx_amount", "tx_time_seconds", "tx_time_days"]
    cat_cols = ["tx_fraud_scenario_encoded"]
    featureColumns = numeric_cols + cat_cols
    
    assembler = VectorAssembler() \
    .setInputCols(featureColumns) \
    .setOutputCol("features") \
    .setHandleInvalid("skip")
    
    scaler = MinMaxScaler() \
    .setInputCol("features") \
    .setOutputCol("scaledFeatures")
    
    scaled = Pipeline(stages=[data_indexer,
                              data_encoder,
                              assembler,
                              scaler,
                             ]).fit(data).transform(data)
    
    tt = scaled.randomSplit([0.8, 0.2])
    training = tt[0]
    test = tt[1]

# загрузка предобработанных данных для ускорения
training = spark.read.parquet("s3a://mlops-homework-rvsmsjima/training.parquet").cache()
test = spark.read.parquet("s3a://mlops-homework-rvsmsjima/test.parquet").cache()

os.environ["AWS_ACCESS_KEY_ID"] = "key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "secret"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
os.environ["AWS_DEFAULT_REGION"] = "ru-central1"

mlflow.set_tracking_uri("http://mlflow.fraudnames.ru:5000")

experiment = mlflow.set_experiment("baseline")
experiment_id = experiment.experiment_id

run_name = 'baseline model for data subset ' + str(datetime.now())

# baseline модель
with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
    setRegParam = 0.2
    setElasticNetParam = 0.8
    
    lr = (
    LogisticRegression()
        .setMaxIter(1)
        .setRegParam(setRegParam)
        .setElasticNetParam(setElasticNetParam)
        .setFamily("binomial")
        .setFeaturesCol("scaledFeatures")
        .setLabelCol("tx_fraud")
    )
    lrModel = lr.fit(training)
    baseline_run_id = mlflow.active_run().info.run_id
    mlflow.log_param('optimal_regParam', setRegParam)
    mlflow.log_param('optimal_elasticNetParam', setElasticNetParam)
    trainingSummary = lrModel.summary
    accuracy_trainingSummary = trainingSummary.accuracy
    areaUnderROC_trainingSummary = trainingSummary.areaUnderROC
    mlflow.log_metric("accuracy_trainingSummary", accuracy_trainingSummary)
    mlflow.log_metric("areaUnderROC_trainingSummary", areaUnderROC_trainingSummary)
    predictions_baseline = lrModel.transform(test)
    metrics_baseline = evaluateModel(predictions_baseline)
    for metric in metrics_baseline.keys():
        mlflow.log_metric(metric, metrics_baseline[metric])
    
    mlflow.spark.log_model(lrModel, "lrModel")
mlflow.end_run()

experiment = mlflow.set_experiment("staging")
experiment_id = experiment.experiment_id
run_name = 'staging model for data subset ' + str(datetime.now())


# staging модель
with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
    staging_run_id = mlflow.active_run().info.run_id
    evaluator = BinaryClassificationEvaluator(labelCol="tx_fraud", metricName="areaUnderPR")
    grid = (
    ParamGridBuilder()
        .addGrid(lr.regParam, [0.05, 0.1, 0.5])
        .addGrid(lr.fitIntercept, [False, True])
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
        .build()
    )

    tvs = (
    TrainValidationSplit()
        .setEstimator(lr)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(grid)
        .setCollectSubModels(True)
        .setParallelism(2)
    )
    
    tvsModel = tvs.fit(training)
    param_map = tvsModel.bestModel.extractParamMap()
    for param in param_map:
        mlflow.log_param(param.name, param_map[param])
    
    predictions_staging = tvsModel.transform(test)
    metrics_staging = evaluateModel(predictions_staging)
    for metric in metrics_staging.keys():
        mlflow.log_metric(metric, metrics_staging[metric])

    mlflow.spark.log_model(tvsModel, "tvsModel")

    scores = getScores(predictions_baseline)
    scores_candidate = getScores(predictions_staging)

    mlflow.log_figure(makeFig(scores, scores_candidate), "plots/apr.png")
    # логируем метрику валидации модели
    mlflow.log_metric("isValid", isValid(scores, scores_candidate))

# если новая модель валидна, регистрируем модель в mlflow как bestModel
if (isValid(scores, scores_candidate)):
    result = mlflow.register_model('runs:/'+staging_run_id+'/tvsModel', "bestModel")
    print(result)

client = MlflowClient()
client.transition_model_version_stage(
    name="bestModel", version=result.version, stage="Production"
)

spark.stop()
