from pyspark.sql import functions as F

from spark_session import spark

## reading silver layer transactions table and loading into a dataframe
read_url = 'jdbc:mysql://localhost:3306/silver'

conn_properties = {
    'user': 'root',
    'password': 'macintosh',
    'driver': 'com.mysql.cj.jdbc.Driver'
}


silver_df = spark.read.jdbc(url=read_url, properties=conn_properties, table='s_transaction')
print(f"{silver_df.count()} number of rows imported from silver transactions!")


## sets connection properties and url
def write_conn_prop_url(tbl_name):
    write_conn_prop = {
        'user': 'root',
        'password': 'macintosh',
        'driver': 'jdbc.mysql.cj.jdbc.Driver',
        'rewriteBatchedStatements': 'true',
        'batchSize': '3000'
    }
    write_url = f'jdbc:mysql://localhost:3306/{tbl_name}'
    return (write_conn_prop, write_url)


## saves the dataframe
def save_df(dataframe, tbl_name):
    write_conn_prop, write_url = write_conn_prop_url(tbl_name)
    # repartition for faster write
    dataframe = dataframe.repartition(4)
    try:
        print(f"writing into gold layer's {tbl_name} table.")
        dataframe.write.jdbc(url=write_url, properties=write_conn_prop, table=tbl_name)
        print(f"write sucessfull! {dataframe.count()} number of rows added!")
    except Exception as e:
        pass


## populates category dimension
def dim_category_populate():
    cat_df = silver_df.select(F.col('category_id'))
    save_df(cat_df, 'dim_category')

def dim_cust_populate():
    pass

def dim_merch_populate():
    pass

def dim_date_populate():
    pass


def main():
    pass


if __name__ == '__main__':
    main()



