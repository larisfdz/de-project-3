DROP TABLE IF EXISTS mart.d_calendar CASCADE;
DROP TABLE IF EXISTS mart.d_item CASCADE;
DROP TABLE IF EXISTS mart.f_activity;
DROP TABLE IF EXISTS mart.f_daily_sales;
DROP TABLE IF EXISTS mart.d_customer;
DROP TABLE IF EXISTS mart.d_city;
DROP TABLE IF EXISTS  mart.f_customer_retention;

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.d_calendar(
   date_time        TIMESTAMP         PRIMARY KEY,
   day_num          SMALLINT,
   month_num        SMALLINT,
   month_name       VARCHAR(8),
   year_num         SMALLINT
);
CREATE INDEX d_calendar1  ON mart.d_calendar (year_num);

CREATE TABLE IF NOT EXISTS mart.d_city (
	id serial4 NOT NULL,
	city_id int4 NULL,
	city_name varchar(50) NULL,
	CONSTRAINT d_city_city_id_key UNIQUE (city_id),
	CONSTRAINT d_city_pkey PRIMARY KEY (id)
);
CREATE INDEX d_city1 ON mart.d_city USING btree (city_id);

CREATE TABLE IF NOT EXISTS mart.d_customer (
	id serial4 NOT NULL,
	customer_id int4 NOT NULL,
	first_name varchar(15) NULL,
	last_name varchar(15) NULL,
	city_id int4 NULL,
	CONSTRAINT d_customer_customer_id_key UNIQUE (customer_id),
	CONSTRAINT d_customer_pkey PRIMARY KEY (id)
);
CREATE INDEX d_cust1 ON mart.d_customer USING btree (customer_id);

CREATE TABLE IF NOT EXISTS mart.d_item (
   id serial4 NOT NULL,
   item_id int4 NOT NULL,
   item_name varchar(50) NULL,
   category_name varchar(50), --выделила продукты / не продукты
   CONSTRAINT d_item_item_id_key UNIQUE (item_id),
   CONSTRAINT d_item_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX d_item1 ON mart.d_item USING btree (item_id);


CREATE TABLE IF NOT EXISTS mart.f_activity(
   activity_id      INT NOT NULL,
   date_time          TIMESTAMP NOT NULL,
   click_number     INT,
   PRIMARY KEY (activity_id, date_time),
   FOREIGN KEY (date_time) REFERENCES mart.d_calendar(date_time) ON UPDATE CASCADE
);


CREATE INDEX f_activity1  ON mart.f_activity (date_time);
CREATE INDEX f_activity2  ON mart.f_activity (activity_id);


CREATE TABLE IF NOT EXISTS mart.f_daily_sales(
   date_time          TIMESTAMP NOT NULL,
   item_id          INT NOT NULL,
   customer_id      INT NOT NULL,
   price            decimal(10,2), --цена из прайс листа
   quantity         BIGINT,
   payment_amount   DECIMAL(10,2),
   PRIMARY KEY (date_time, item_id, customer_id),
   FOREIGN KEY (date_time) REFERENCES mart.d_calendar(date_time) ON UPDATE CASCADE,
   FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id) ON UPDATE CASCADE,
   FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id) ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
id SERIAL PRIMARY KEY,
period_name VARCHAR(20),
period_id VARCHAR(20),
category_name VARCHAR(100),
cohort VARCHAR(20),
new_customers_count INT,
returning_customers_count INT,
refunded_customers_count INT,
new_customers_revenue NUMERIC(14,2),
returning_customers_revenue NUMERIC (14,2),
refunded_customers_sum NUMERIC(14,2),
customers_refunded INT
);