from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
from pyspark.sql.functions import rint
from pyspark.sql.functions import expr

# Create a Spark Session 

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR")

# Create DataFrame reading from Rate Source
impressions = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 1000) \
    .load()

clicks = spark \
   .readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .load()

impressions = impressions.withColumnRenamed("timestamp","impsTimestamp")
impressions = impressions.withColumn("impsUserId",rint(rand()*10))
impressions = impressions.withColumn("impsAdId",rint(rand()*3))

clicks = clicks.withColumnRenamed("timestamp","clicksTimestamp")
clicks = clicks.withColumn("clickUserId",rint(rand()*10))
clicks = clicks.withColumn("clickAdId",rint(rand()*3))

# Apply watermarks on event-time columns
impressionsWithWatermark = impressions.withWatermark("impsTimestamp", "30 minutes")
clicksWithWatermark = clicks.withWatermark("clicksTimestamp", "30 minutes")

# Join with event-time constraints
serving=impressionsWithWatermark.join(
  clicksWithWatermark,
  expr("""
    impsUserId = clickUserId AND
    impsAdId = clickAdId AND
    clicksTimestamp > impsTimestamp + interval 10 second
    """)
)

query = serving \
    .writeStream \
    .format("console") \
    .option("truncate",False) \
    .start()

query.awaitTermination()

