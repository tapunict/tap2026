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

# Download a pre-trained pipeline (using a lighter pipeline)
print("\nDownloading pre-trained pipeline...")
pipeline = PretrainedPipeline('recognize_entities_dl', lang='en')
print("Pipeline loaded successfully!")

# Your testing dataset
text = """
The Mona Lisa is a 16th century oil painting created by Leonardo.
It's held at the Louvre in Paris.
"""

# Annotate your testing dataset
print("\nAnnotating text...")
result = pipeline.annotate(text)
print("Annotation completed!")


for key in list(result.keys()):
    print("=" * 50)
    print(key)
    for item in result[key]:
        print(f" - {item}")

# Stop Spark session
print("\nStopping Spark session...")
spark.stop()
print("Done!")
