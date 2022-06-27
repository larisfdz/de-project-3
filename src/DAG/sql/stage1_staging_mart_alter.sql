ALTER TABLE staging.user_orders_log ADD COLUMN status TEXT;
UPDATE staging.user_orders_log SET status = (SELECT 'shipped');
ALTER TABLE mart.f_daily_sales ADD COLUMN status TEXT;