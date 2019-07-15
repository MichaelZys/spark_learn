from pyspark.sql import SparkSession

DE_File = "/usr/local/spark/README.md"
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(DE_File).cache()

numA = logData.filter(logData.value.contains('a')).count()
numB = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i"  % (numA, numB))

spark.stop()