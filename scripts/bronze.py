from spark_session import spark
spark.sparkContext.setLogLevel("ERROR")
from pyspark.sql import functions as F

## reading the fraudTrain.csv into a dataframe
file_path = 'file:///Users/ayusman/thisWorks/DE/DW-DB/data/raw_data/fraudTrain.csv'
df = spark.read.csv(file_path, inferSchema=True, header=True)


if df:
    print("file sucessfully imported and stored in the dataframe!")
else:
    print("file could not be imported!")


## appending the source filename name and ingestion timestamp into the dataframe
bronze_df = df.withColumn('source_file', F.input_file_name())\
    .withColumn('ingestion_ts', F.current_timestamp())

## saving this dataframe in the bronze layer database
## we can directly write from pyspark data frame to the mysql database using the pyspark provided jdbc connector
jdbc_url = 'jdbc:mysql://localhost:3306/bronze'
connect_properties = {
    'user': 'root',
    'password': 'macintosh',
    'driver': 'com.mysql.cj.jdbc.Driver',
    'rewriteBatchedStatements': 'true',
    'batchsize': '7000'
}

print("Writing to mysql server bronze layer database...")

## repartitining to 4 for write operation
bronze_df = bronze_df.repartition(4)
print(bronze_df.rdd.getNumPartitions())

## writing to mysql using this created connection property and jdbc_url
try:
    bronze_df.write.jdbc(url=jdbc_url, mode='overwrite', table='b_transaction', properties=connect_properties)
    print("successfully stored in the bronze table 'b_transaction'")
except Exception as e:
    print(f"could not write to the database. Error: {e}")

## ps: writing this dataframe data to mysql database is consuming significant time, we will focus on optimizing in the next iteration