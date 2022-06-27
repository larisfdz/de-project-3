DROP TABLE IF EXISTS mart.d_calendar CASCADE;
CREATE TABLE mart.d_calendar(
   date_id          TIMESTAMP         PRIMARY KEY,
   day_num          SMALLINT,
   month_num        SMALLINT,
   month_name       VARCHAR(8),
   year_num         SMALLINT
);
CREATE INDEX d_calendar1  ON mart.d_calendar (year_num);

DROP TABLE IF EXISTS mart.d_item CASCADE;
CREATE TABLE mart.d_item (
   id serial4 NOT NULL,
   item_id int4 NOT NULL,
   item_name varchar(50) NULL,
   category_name varchar(50), --выделила продукты / не продукты
   CONSTRAINT d_item_item_id_key UNIQUE (item_id),
   CONSTRAINT d_item_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX d_item1 ON mart.d_item USING btree (item_id);

DROP TABLE IF EXISTS mart.f_activity;
CREATE TABLE mart.f_activity(
   activity_id      INT NOT NULL,
   date_id          TIMESTAMP NOT NULL,
   click_number     INT,
   PRIMARY KEY (activity_id, date_id),
   FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id) ON UPDATE CASCADE
);

CREATE INDEX f_activity1  ON mart.f_activity (date_id);
CREATE INDEX f_activity2  ON mart.f_activity (activity_id);

DROP TABLE IF EXISTS mart.f_daily_sales;
CREATE TABLE mart.f_daily_sales(
   date_id          TIMESTAMP NOT NULL,
   item_id          INT NOT NULL,
   customer_id      INT NOT NULL,
   price            decimal(10,2), --цена из прайс листа
   quantity         BIGINT,
   payment_amount   DECIMAL(10,2),
   PRIMARY KEY (date_id, item_id, customer_id),
   FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id) ON UPDATE CASCADE,
   FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id) ON UPDATE CASCADE,
   FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id) ON UPDATE CASCADE
);