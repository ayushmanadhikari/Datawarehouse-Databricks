from pyspark.sql import SparkSession

## creating spark session
spark = SparkSession.builder.appName('Spark1').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

##
if spark is not None:
    print("Spark Session Initialized successfully!!")
else:
    print("Spark Session could not be initialized!")