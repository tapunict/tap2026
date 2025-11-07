from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json
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
topic = "mastapon"
modelPath="/opt/tap/models/sentitap"

elastic_index="taptweet"

sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200")

spark = SparkSession.builder.appName("mastapon").config(conf=sparkConf).getOrCreate()
# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR") 

print("Loading model")
sentitapModel = PipelineModel.load(modelPath)
print("Done")

# Define Training Set Structure
tweetKafka = tp.StructType([
    tp.StructField(name= 'id', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'created_at', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'content',       dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'language',       dataType= tp.StringType(),  nullable= True)
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
    .select("data.id","data.created_at", "data.language", col("data.content").alias("text"))

# Apply the machine learning model and select only the interesting columns
df1 = df.filter(df["language"] == 'en')
df1=sentitapModel.transform(df1)
#.select("id", "created_at", "text", "prediction")
df1 = df1.select("id", "created_at", "text", "language","prediction")

# Add other flow not en directly
#df.writeStream \
#    .format("console") \
#    .option("truncate",False) \
#    .start() \
#    .awaitTermination()

df1.writeStream \
   .option("checkpointLocation", "/tmp/") \
   .format("es") \
   .start(elastic_index) \
   .awaitTermination()