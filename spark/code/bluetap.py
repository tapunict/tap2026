from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json, date_format
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.functions import col


kafkaServer="broker:9092"
topic = "bluetap"
modelPath="/opt/tap/models/sentitap"

elastic_index="taptweet"

sparkConf = SparkConf().set("es.nodes", "elasticsearch").set("es.port", "9200") 
# Uncomment .set("es.nodes", "elasticsearch").set("es.port", "9200") 

spark = SparkSession.builder.appName("bluetap").config(conf=sparkConf).getOrCreate()
# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR") 

print("Loading model")
sentitapModel = PipelineModel.load(modelPath)
print("Done")

# Define Training Set Structure
tweetKafka = tp.StructType([
    tp.StructField(name= 'uri', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'cid', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'author',       dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'text',       dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'created_at',       dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'hashtag',       dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'langs',       dataType= tp.StringType(),  nullable= True)
])



print("Reading stream from kafka...")
# Read the stream from kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .load()


# Cast the message received from kafka with the provided schema
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", tweetKafka).alias("data")) \
    .select("data.cid","data.created_at", "data.langs", col("data.text").alias("text"))



# Apply the machine learning model and select only the interesting columns
# df1 = df.filter(df["langs"] == 'en') # TODO contains
df=sentitapModel.transform(df)
# Some MLLib fields seems to be not serializable
df = df.select("cid", "created_at", "langs", "text", "prediction")
# df = df.withColumn("iso_timestamp", date_format(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
# Add other flow not en directly
# df.writeStream \
#    .format("console") \
#    .option("truncate",False) \
#    .start() \
#    .awaitTermination()

# Add other flow not en directly
df.writeStream \
   .option("checkpointLocation", "/tmp/") \
   .format("es") \
   .start(elastic_index) \
   .awaitTermination()