import sys
sys.path.append(
    "/usr/lib/spark/python"
)
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import isnan, when, count, col

spark = SparkSession.builder.appName('g1').getOrCreate()
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.access.key", "YCAJEQ6hoybux3TZsjsbpURg4")
hadoop_conf.set("fs.s3a.secret.key", "YCPhC9mxk9Or5WIqcbyvUMpogn2UbWyHlRedILTb")
hadoop_conf.set("fs.s3a.endpoint", "storage.yandexcloud.net")

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("tx_datetime", TimestampType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("terminal_id", IntegerType(), True),
    StructField("tx_amount", DoubleType(), True),
    StructField("tx_time_seconds", IntegerType(), True),
    StructField("tx_time_days", IntegerType(), True),
    StructField("tx_fraud", IntegerType(), True),
    StructField("tx_fraud_scenario", IntegerType(), True)
])

df1 = spark.read.csv("s3://mlops-homework-rvsmsjim/",
                    header=False,
                    pathGlobFilter="*.txt",
                    schema=schema, comment='#',
                    mode='DROPMALFORMED')

df2 =(
df1
    .where((year(df1.tx_datetime) >= 2018) | (year(df1.tx_datetime) <= 2024))
    .where((month(df1.tx_datetime) >= 1) | (month(df1.tx_datetime) <= 12))
    .where((dayofmonth(df1.tx_datetime) >= 1) | (dayofmonth(df1.tx_datetime) <= 31))
    .where(df1.customer_id >= 0)
    .where(df1.terminal_id > 0)
    .where(df1.tx_amount >= 0)
    .where(df1.tx_time_seconds >= 0)
    .where((df1.tx_fraud == 0) | (df1.tx_fraud == 1))
    .where((df1.tx_fraud_scenario == 0) | (df1.tx_fraud_scenario == 1) | (df1.tx_fraud_scenario == 2) | (df1.tx_fraud_scenario == 3))
    .dropDuplicates(['transaction_id'])
)

df2.write.parquet("s3a://mlops-homework-rvsmsjima/dataset-cleaned.parquet", mode="overwrite")

spark.stop()
