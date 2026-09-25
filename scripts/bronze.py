#from spark_session import spark
import mysql.connector

## reading the fraudTrain.csv into a dataframe
'''file_path = 'file:///Users/ayusman/thisWorks/DE/DW-DB/data/raw_data/fraudTrain.csv'
df = spark.read.csv(file_path, inferSchema=True, header=True)


if df:
    print("file sucessfully imported and stored in the dataframe!")
else:
    print("file could not be imported!")

'''
## saving this dataframe in the bronze layer database

## 1. establishing database connection 
connector = mysql.connector.connect(host="localhost",
    user="root",
    password="macintosh",
    database = 'bronze')

cursor = connector.cursor()
cursor.execute('show databases;')


for row in cursor.fetchall():
    print(row)