from pyspark.sql import SparkSession

logFile = "/tmp/dataset/3amazon.txt"  # Should be some file on your system
spark = SparkSession.builder.appName("Amazon").getOrCreate()

# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR") 
logData = spark.read.text(logFile)

books = logData.filter(logData.value.contains('ASIN')).count()
numTot = logData.count()
print("Num books %s" % (books))
print("Tot number %d" % (numTot))
spark.stop()