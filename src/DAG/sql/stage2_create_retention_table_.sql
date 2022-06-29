--DROP VIEW mart.f_customer_retention;
CREATE TABLE  mart.f_customer_retention (
id SERIAL PRIMARY KEY,
period_name VARCHAR(20),
period_id VARCHAR(20),
category_name VARCHAR(100),
new_customers_count INT,
returning_customers_count INT,
refunded_customers_count INT,
new_customers_revenue NUMERIC(14,2),
returning_customers_revenue NUMERIC (14,2),
refunded_customers_sum NUMERIC(14,2),
customers_refunded INT
);