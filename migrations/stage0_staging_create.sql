
DROP TABLE IF EXISTS staging.customer_research;
DROP TABLE IF EXISTS staging.user_order_log;
-DROP TABLE IF EXISTS staging.user_activity_log;

CREATE TABLE staging.customer_research (
category_id INT,
date_id TIMESTAMP,
geo_id INT,
id SERIAL PRIMARY KEY,
sales_amt NUMERIC(14,2),
sales_qty INT
);

CREATE TABLE staging.user_activity_log (
action_id BIGINT,
customer_id BIGINT,
date_time TIMESTAMP,
id SERIAL PRIMARY KEY,
quantity BIGINT
);

CREATE TABLE staging.user_orders_log (
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

/*
COPY staging.customer_research (
date_id,
category_id,
geo_id,
sales_qty,
sales_amt
)
FROM 'C:\Users\Администратор\YandexDisk\2022_DE\sprint_4\2. Анализ вводных по задаче\7. Использование файлов и подключение к БД\Задание 1\staging\customer_research.csv' 
DELIMITER ',' CSV HEADER;

COPY staging.user_activity_log (id, date_time, action_id, customer_id, quantity)
FROM '/lessons/2. Анализ вводных по задаче/7. Использование файлов и подключение к БД/Задание 1/staging/user_activity_log.csv'
DELIMITER ',' CSV HEADER;

COPY staging.user_order_log (
id,
date_time,
city_id,
city_name,
customer_id,
first_name,
last_name,
item_id,
item_name,
quantity,
payment_amount)
FROM '/lessons/2. Анализ вводных по задаче/7. Использование файлов и подключение к БД/Задание 1/staging/user_order_log.csv'
DELIMITER ',' CSV HEADER;
*/
