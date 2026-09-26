from pyspark.sql import SparkSession

## creating spark session
spark = SparkSession.builder.config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0")\
.config('spark.log.level', "ERROR")\
.config("spark.driver.memory", "2g") \
.config("spark.executor.memory", "2g") \
.appName('Spark1')\
.getOrCreate()


## config spark driver and executor memory to 2gb instead of 1(default) for faster write operation
spark.sparkContext.setLogLevel("ERROR")

##
if spark is not None:
    print("Spark Session Initialized successfully!!")
else:
    print("Spark Session could not be initialized!")