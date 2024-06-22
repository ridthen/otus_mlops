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

spark = SparkSession.builder.appName('train').getOrCreate()
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.access.key", "key")
hadoop_conf.set("fs.s3a.secret.key", "secret")
hadoop_conf.set("fs.s3a.endpoint", "storage.yandexcloud.net")


df = spark.read.parquet("s3a://mlops-homework-rvsmsjima/dataset-cleaned.parquet")

# возьмем только 1% данных для сокращения времени отладки и обучения
subsets = df.randomSplit([0.01, 0.95])
subset = subsets[0]

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

os.environ["AWS_ACCESS_KEY_ID"] = "YCAJEsMkTeWfF5SgBtFgpXcNq"
os.environ["AWS_SECRET_ACCESS_KEY"] = "YCP5L2m54HTfALwi4b3huQJdRuq2qo_ZHJ_kUv44"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
os.environ["AWS_DEFAULT_REGION"] = "ru-central1"

mlflow.set_tracking_uri("http://51.250.64.3:5000")

experiment = mlflow.set_experiment("baseline")
experiment_id = experiment.experiment_id

run_name = 'My run name' + 'TEST_LogReg' + str(datetime.now())

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

    run_id = mlflow.active_run().info.run_id

    mlflow.log_param('optimal_regParam', setRegParam)
    mlflow.log_param('optimal_elasticNetParam', setElasticNetParam)

    trainingSummary = lrModel.summary
    accuracy_trainingSummary = trainingSummary.accuracy
    areaUnderROC_trainingSummary = trainingSummary.areaUnderROC

    mlflow.log_metric("accuracy_trainingSummary", accuracy_trainingSummary)
    mlflow.log_metric("areaUnderROC_trainingSummary", areaUnderROC_trainingSummary)

    predicted = lrModel.transform(test)

    tp = predicted.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 1)).count()
    tn = predicted.filter((f.col("tx_fraud") == 0) & (f.col("prediction") == 0)).count()
    fp = predicted.filter((f.col("tx_fraud") == 0) & (f.col("prediction") == 1)).count()
    fn = predicted.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 0)).count()

    accuracy = (tp + tn) / (tp + tn + fp + fn)
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)
    F1 = (2 * recall * precision) / (recall + precision)

    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("F1", F1)

    mlflow.spark.log_model(lrModel, "lrModel")

spark.stop()
