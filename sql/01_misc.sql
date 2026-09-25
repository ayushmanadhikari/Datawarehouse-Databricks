create database if not exists test;

use test;

create table if not exists emp(
    emp_id BIGINT AUTO_INCREMENT primary key,
    emp_name varchar(20),
    emp_phone varchar(20)
);


--- working with date and time functions 
select NOW();
select CURDATE();
select curtime();
select time(now());

-- using extract 
select EXTRACT(day from now());
select EXTRACT(hour from now());


-- date_Add
select date_add(now(), INTERVAL 5 hour);

-- date diff(number of days)
select datediff(now(), date_add(now(), interval -1 month ));

-- date_format() to modify the formatting of date
select DATE_FORMAT(now(), '%W, %M %d, %Y');