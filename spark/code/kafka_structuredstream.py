from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.dataframe import DataFrame

sc = SparkContext(appName="PythonStructuredStreamsKafka")
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

kafkaServer="broker:9092"
topic = "tap"

# Streaming Query

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafkaServer) \
  .option("subscribe", topic) \
  .load()

df=df.selectExpr("CAST(timestamp AS STRING)","CAST(value AS STRING)") \

df.writeStream \
  .format('console')\
  .option('truncate', 'false')\
  .start()\
  .awaitTermination()