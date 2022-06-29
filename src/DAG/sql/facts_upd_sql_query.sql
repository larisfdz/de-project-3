delete from mart.f_daily_sales
where date_time in (select distinct date_time::date from staging.user_orders_log);
insert into mart.f_daily_sales
(date_time,
item_id,
customer_id,
price,
quantity,
payment_amount, 
status)
select distinct date_time::date as date_time, 
item_id, 
customer_id, 
CASE WHEN status = 'refunded' AND payment_amount > 0 
THEN payment_amount/quantity * -1 
ELSE payment_amount/quantity END AS price,
quantity, 
CASE WHEN status = 'refunded' AND payment_amount > 0 THEN payment_amount*-1
    ELSE payment_amount END AS amount,
status
from staging.user_orders_log