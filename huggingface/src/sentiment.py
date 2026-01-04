from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
from transformers import pipeline

# --- Setup del modello (Pattern Inizializzazione Lazy) ---
# Usiamo una variabile globale per caricare il modello UNA SOLA VOLTA per worker (executor),
# e non ricaricarlo per ogni batch di dati.
sentiment_pipeline = None

def get_pipeline():
    global sentiment_pipeline
    if sentiment_pipeline is None:
        # Carica il modello (se hai fatto il pre-download nel Dockerfile, è istantaneo)
        # device=-1 forza l'uso della CPU
        sentiment_pipeline = pipeline(
            "sentiment-analysis", 
            model="distilbert-base-uncased-finetuned-sst-2-english",
            device=-1 
        )
    return sentiment_pipeline

# --- Definizione della Pandas UDF ---
@pandas_udf(StringType())
def hf_sentiment_analysis(text_series: pd.Series) -> pd.Series:
    # Ottieni il modello (già caricato in memoria su questo worker)
    pipe = get_pipeline()
    
    # Hugging Face accetta liste di stringhe. 
    # Convertiamo la serie Pandas in lista per l'inferenza
    input_list = text_series.tolist()
    
    # Eseguiamo l'inferenza (batch processing)
    # L'output è una lista di dict: [{'label': 'POSITIVE', 'score': 0.99}, ...]
    predictions = pipe(input_list, batch_size=8, truncation=True)
    
    # Estraiamo solo la label (o formatta come vuoi, es. "POSITIVE (0.99)")
    labels = [p['label'] for p in predictions]
    
    return pd.Series(labels)

sparkConf = SparkConf()

spark = SparkSession.builder.appName("Sentiment HG").config(conf=sparkConf).getOrCreate()
# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR") 

lines = spark.createDataFrame([("I hate Spark ",)], ["value"])

# Applichiamo la UDF
processed_stream = lines.withColumn("sentiment_hf", hf_sentiment_analysis("value"))

processed_stream.show(truncate=False)