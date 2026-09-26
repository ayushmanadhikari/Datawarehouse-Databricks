from pyspark.sql import SparkSession

## creating spark session
spark = SparkSession.builder.config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0")\
.appName('Spark1').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

##
if spark is not None:
    print("Spark Session Initialized successfully!!")
else:
    print("Spark Session could not be initialized!")