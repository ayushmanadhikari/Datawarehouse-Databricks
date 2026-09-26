from spark_session import spark
## import mysql.connector

## reading the fraudTrain.csv into a dataframe
file_path = 'file:///Users/ayusman/thisWorks/DE/DW-DB/data/raw_data/fraudTrain.csv'
df = spark.read.csv(file_path, inferSchema=True, header=True)


if df:
    print("file sucessfully imported and stored in the dataframe!")
else:
    print("file could not be imported!")


## saving this dataframe in the bronze layer database

## 1. establishing database connection 
'''connector = mysql.connector.connect(host="localhost",
    user="root",
    password="macintosh",
    database = 'bronze')

cursor = connector.cursor()
cursor.execute('show databases;')


for row in cursor.fetchall():
    print(row) '''

## we can directly write from pyspark data frame to the mysql database using the pyspark provided jdbc connector
jdbc_url = 'jdbc:mysql://localhost:3306/bronze'

connect_properties = {
    'user': 'root',
    'password': 'macintosh',
    'driver': 'com.mysql.cj.jdbc.Driver'
}

## writing to mysql using this created connection property and jdbc_url
test_write = df.write.jdbc(url=jdbc_url, mode='overwrite', table='b_transaction', properties=connect_properties)


if test_write:
    print("successfully stored in the bronze table 'b_transaction'")
else:
    print("could not write to the database!")