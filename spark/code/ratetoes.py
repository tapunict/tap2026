from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
import sys

# main
elastic_index= sys.argv[1] 

# Create a Spark Session 

sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200") \
                        .set("es.index.auto.create", "true")

spark = SparkSession.builder.appName("mastapon").config(conf=sparkConf).getOrCreate()
# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR") 

# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR") 

# Create DataFrame reading from Rate Source
df = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load()

df = df.withColumn("iso_timestamp", F.date_format(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

df.writeStream \
   .option("checkpointLocation", "/tmp/") \
   .format("es") \
   .start(elastic_index) \
   .awaitTermination()

