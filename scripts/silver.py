# importing the spark session 
from spark_session import spark
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, DatetimeType, DoubleType, StringType

spark.sparkContext.setLogLevel("ERROR")

# setting up connection parameters and configurations
connect_properties_read = {
    'user': 'root',
    'password': 'macintosh',
    'driver': 'com.mysql.cj.jdbc.Driver'
}

url_read = 'jdbc:mysql://localhost:3306/bronze'

## reading the data from bronze layer database into a dataframe
try:
    print("reading from b_transaction...")
    bronze_df = spark.read.jdbc(url=url_read, properties=connect_properties_read, table='b_transaction')
    print("data extracted to dataframe: 'bronze_df'")
except Exception as e:
    print(f"could not load into dataframe. Error {e}")


## calculates the distance using merch lat, long and customer lat,long
def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2)
    dist = F.round(2 * r * F.asin(F.sqrt(a)), 2)
    return dist


## dropping null values in columns: merch, cat, first, last, trans_dt, trans_num
print(f"Rows count before dropping nulls: {bronze_df.count()}")
bronze_df = bronze_df.dropna(how='any', subset=['merchant', 'category', 'first', 'last', 'trans_date_trans_time', 'trans_num'])
print(f"Rows count after dropping nulls: {bronze_df.count()}")


##  silver layer table definition with type casting, dropping some old columns and adding some new columns
print("performing transformation...")
df_silver = bronze_df\
    .withColumnRenamed('trans_ts', 'trans_date_trans_time')\
    .withColumn('card_last4', F.substring(col('cc_num').cast('string'), -4, 4))\
    .withColumn('merchant', F.trim(col('merchant')))\
    .withColumn('category_id', F.trim(col('category')))\
    .withColumnRenamed('amount', 'amt')\
    .withColumn('fname', F.trim(col('first')))\
    .withColumn('lname', F.trim(col('last')))\
    .withColumn('gender', F.upper(F.trim(col('gender'))))\
    .withColumn('street', F.trim(col('street')))\
    .withColumn('city', F.trim(col('city')))\
    .withColumn('state', F.trim(col('state')))\
    .withColumn('zip', F.lpad(F.trim(col('zip')), 5, "0"))\
    .withColumn('cust_lat', col('lat'))\
    .withColumn('cust_long', col('long'))\
    .withColumn('cust_job', F.trim(col('job')))\
    .withColumnRenamed('cust_dob', 'dob')\
    .withColumn('dist', haversine_km(col('cust_lat'), col('cust_long'), col('merch_lat'), col('merch_long')))\
    .withColumn('s_processing_ts', F.current_timestamp())\
    .drop('_c0', 'cc_num', 'amt', 'lat', 'long', 'city_pop', 'unix_time')
print("transformation completed!!!")


## repartitining to 8 for write operation
print("partitioning the data frame...")
df_silver = df_silver.repartition(4)
print(f"number of partitions: {df_silver.rdd.getNumPartitions()}")
print("partitioning completed!!!")

connect_properties_write = {
    'user': 'root',
    'password': 'macintosh',
    'driver': 'com.mysql.cj.jdbc.Driver',
    'rewriteBatchedStatements': 'true',
    'batchSize': '3000'
}

write_url = 'jdbc:mysql://localhost:3306/silver'
## saving this processed dataframe into the silver mysql server
try:
    print("writing to silver transactions table... ")
    df_silver.write.jdbc(url=write_url, properties=connect_properties_write, table='s_transaction', mode='overwrite')
    print(f"write successfull! Rows added: {df_silver.count()}")
except Exception as e:
    print("writing to silver transaction table failed. Error: {e}")


## performing transformations on the dataframe
## 1. removing null values from PK and unique columns
## 2. Data type enforcement:
    ## a. date format
    ## b. string, int formats
## 3. trimming string data 
## 4. tracking source to target mapping

## removing 

