# Import Spark NLP
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp as sparknlp
import os

# Suppress Ivy logs
os.environ['IVY_LOG_LEVEL'] = 'error'

# Start SparkSession with Spark NLP
# start() functions has 3 parameters: gpu, apple_silicon, and memory
# sparknlp.start(gpu=True) will start the session with GPU support
# sparknlp.start(apple_silicon=True) will start the session with macOS M1 & M2 support
# sparknlp.start(memory="16G") to change the default driver memory in SparkSession
print("Starting Spark session...")
spark = sparknlp.start(memory="16G", gpu=True)

# Set Spark log level to ERROR to reduce verbosity
spark.sparkContext.setLogLevel("ERROR")
print("Spark session started successfully!")

# Reference https://sparknlp.org/2025/05/22/twitter_roberta_base_sentiment_latest_en.html

documentAssembler = DocumentAssembler() \
    .setInputCol('text') \
    .setOutputCol('document')
    
tokenizer = Tokenizer() \
    .setInputCols(['document']) \
    .setOutputCol('token')

sequenceClassifier  = RoBertaForSequenceClassification.pretrained("twitter_roberta_base_sentiment_latest","en") \
     .setInputCols(["document","token"]) \
     .setOutputCol("class")

pipeline = Pipeline().setStages([documentAssembler, tokenizer, sequenceClassifier])
data = spark.createDataFrame([["I hate spark-nlp"]]).toDF("text")
pipelineModel = pipeline.fit(data)
pipelineDF = pipelineModel.transform(data)
pipelineDF.show(truncate=False)
print("\nStopping Spark session...")
spark.stop()
print("Done!")
