with new_data as (
    select item_id, item_name, category_name
    from (
        select item_id, item_name,
        CASE WHEN UPPER(item_name) SIMILAR TO '(%ТОМАТЫ%|%ХЛЕБ%|%МОЛОКО%|%ЯБЛОКО%|%САХАР%)' THEN 'Продукты'
            ELSE 'Другие товары' END AS category_name,
            row_number() over (partition by item_id order by date_time desc) rn
        from staging.user_orders_log
        )t
    where rn = 1
)
update mart.d_item
set item_name = new_data.item_name,
    category_name = new_data.category_name
from new_data
where mart.d_item.item_id = new_data.item_id
    and mart.d_item.item_name != new_data.item_name;

insert into mart.d_item (item_id, item_name, category_name)
select 
distinct item_id, last_value(item_name) over(partition by item_id order by date_time desc) as name,
 CASE WHEN UPPER(last_value(item_name) over(partition by item_id order by date_time desc)) SIMILAR TO '(%ТОМАТЫ%|%ХЛЕБ%|%МОЛОКО%|%ЯБЛОКО%|%САХАР%)' THEN 'Продукты'
            ELSE 'Другие товары' END AS category_name
from staging.user_orders_log
where item_id not in (select distinct item_id from mart.d_item)
;

insert into mart.d_customer (customer_id, first_name, last_name)
SELECT 
DISTINCT customer_id, first_name, last_name 
FROM staging.user_orders_log
where customer_id not in (select distinct customer_id from mart.d_customer)
;

INSERT INTO mart.d_calendar (date_id, day_num, month_num, month_name, year_num)
SELECT DISTINCT date_time,
EXTRACT (DAY FROM date_time),
EXTRACT (MONTH FROM date_time),
TO_CHAR(date_time, 'month'),
EXTRACT (YEAR FROM date_time)
FROM staging.user_orders_log st
WHERE st.date_time NOT IN (select distinct date_id from mart.d_calendar)
;

insert into mart.d_city (city_id, city_name)
SELECT DISTINCT city_id, city_name
FROM staging.user_orders_log
where city_id not in (select distinct city_id from mart.d_city)
;