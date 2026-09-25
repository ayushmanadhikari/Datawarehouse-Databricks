-- CREATING 3 SEPARATE DATABASES FOR EACH LAYER OF MEDALLION ARCHI
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;


-- using database gold for dimesnsions and fact table 
USE gold;


-- CREATING TABLES INSIDE gold layer (4 dimensions and 1 fact)
CREATE TABLE IF NOT EXISTS dim_category (
    cat_id INT AUTO_INCREMENT PRIMARY KEY ,
    cat_name VARCHAR(50)
)


CREATE TABLE IF NOT EXISTS dim_merchant (
    merch_key BIGINT AUTO_INCREMENT PRIMARY KEY ,
    merch_name varchar(200),
    merch_lat varchar(50),
    merch_long varchar(50),
    is_fraud bool default TRUE
)


CREATE TABLE IF NOT EXISTS dim_date (
    date_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_full TIMESTAMP,
    date_year int,
    date_month int,
    day_of_month int,
    day_of_week int,
    month_name varchar(15),
    week_name varchar(15),
    is_weekend bool default FALSE
)


CREATE TABLE IF NOT EXISTS dim_customer (
    cust_key BIGINT AUTO_INCREMENT PRIMARY KEY , 
    cust_id varchar(64),
    card_lastfour varchar(4),
    cust_fname varchar(20),
    cust_lname varchar(20), 
    cust_dob date, 
    cust_gender varchar(10),
    cust_lat varchar(50),
    cust_long varchar(50),
    cust_distance int, 
    cust_street varchar(20),
    cust_city varchar(20),
    cust_state varchar(20), -- test comment
    cust_zip varchar(15),
    cust_job varchar(20),
    is_active bool default TRUE
)

-- FACT TABLE TRANSACTIONS
CREATE TABLE IF NOT EXISTS fact_transaction (
    fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    trans_num varchar(50),
    trans_dt_ts TIMESTAMP, 
    cc_num varchar(30),
    amount BIGINT,
    cust_key BIGINT,
    date_key BIGINT, 
    merch_key BIGINT,
    cat_id INT,
    CONSTRAINT unq_trans_key UNIQUE (trans_num),
    CONSTRAINT f_key_cust FOREIGN KEY (cust_key) REFERENCES dim_customer(cust_key),
    CONSTRAINT f_key_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_merchant_key FOREIGN KEY (merch_key) REFERENCES dim_merchant(merch_key),
    CONSTRAINT fk_category FOREIGN KEY (cat_id) REFERENCES dim_category(cat_id)
);
