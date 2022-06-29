DROP TABLE IF EXISTS staging.customer_research;
DROP TABLE IF EXISTS staging.user_orders_log;
DROP TABLE IF EXISTS staging.user_activity_log;
DROP TABLE IF EXISTS staging.dq_checks_results;

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.customer_research (
id SERIAL PRIMARY KEY,
date_time TIMESTAMP,
category_id INT,
geo_id INT,
sales_qty INT,
sales_amt NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS staging.user_activity_log (
action_id BIGINT,
customer_id BIGINT,
date_time TIMESTAMP,
id SERIAL PRIMARY KEY,
quantity BIGINT
);

CREATE TABLE IF NOT EXISTS staging.user_orders_log (
city_id INT,
city_name VARCHAR(100),
customer_id BIGINT,
date_time TIMESTAMP,
first_name VARCHAR(100),
id SERIAL PRIMARY KEY,
item_id INT,
item_name VARCHAR(100),
last_name VARCHAR(100),
quantity BIGINT,
payment_amount NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS staging.dq_checks_results (
	table_name varchar(255) NULL,
	dq_check_name varchar(255) NULL,
	datetime timestamp NULL,
	dq_check_result numeric(8, 2) NULL
);
