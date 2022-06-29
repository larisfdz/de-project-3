DELETE FROM {{ params.table }} 
WHERE date_time = make_date(left({{ yesterday_ds }}::text,4), substring({{ yesterday_ds }}::text,6,1), right({{ yesterday_ds }}::text,2));