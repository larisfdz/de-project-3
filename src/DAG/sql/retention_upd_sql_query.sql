DELETE
FROM
	mart.f_customer_retention
WHERE
	period_id IN (
	SELECT
		DISTINCT date_part('year'::TEXT, date_time)::TEXT || date_part('week'::TEXT, date_time)::TEXT
	FROM
		mart.f_daily_sales);

INSERT
	INTO
	mart.f_customer_retention (
period_name,
	period_id,
	category_name,
	cohort,
	new_customers_count,
	returning_customers_count,
	refunded_customers_count,
	new_customers_revenue,
	returning_customers_revenue,
	refunded_customers_sum,
	customers_refunded)

WITH cust AS (
		--getting the first buy week
SELECT customer_id,
			date_time,
			ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date_time ASC) AS first_buy,
			EXTRACT(YEAR FROM date_time) :: TEXT || EXTRACT(week FROM date_time) :: TEXT AS period_id
		FROM
			mart.f_daily_sales
		WHERE status != 'refunded'
		),
totals AS (
--определяем тип покупателя новый / вернувшийся и когорту
SELECT
	DISTINCT date_part('year'::TEXT, fds.date_time)::TEXT || date_part('week'::TEXT, fds.date_time)::TEXT AS period_id,
		fds.date_time,
		fds.customer_id,
		di.category_name,
		fds.item_id,
		fds.payment_amount,
		fds.status,
		fb.cohort,
	CASE
		WHEN fb.cohort = (date_part('year'::TEXT, fds.date_time)::TEXT || date_part('week'::TEXT, fds.date_time)::TEXT)
		 THEN 'new'
		ELSE 'returning'
	END AS customer_type
FROM
	mart.f_daily_sales fds
LEFT JOIN mart.d_item di ON
		fds.item_id = di.item_id	
LEFT JOIN 
(SELECT customer_id,
		period_id AS cohort
		FROM cust
		WHERE cust.first_buy = 1
) AS fb
ON fds.customer_id = fb.customer_id
),
grouped AS (
	SELECT
		totals.period_id,
		totals.customer_type,
		totals.status,
		totals.category_name,
		totals.cohort,
		count(DISTINCT totals.customer_id) AS n_customers,
		sum(totals.payment_amount) AS payment_amount,
		count(*) AS n_transactions
	FROM
		totals
	GROUP BY
		totals.period_id,
		totals.customer_type,
		totals.status,
		totals.category_name, 
		totals.cohort
        ),
	almost AS (
	SELECT
		'weekly'::TEXT AS period_name,
		grouped.period_id,
		grouped.category_name,
		grouped.cohort,
		CASE
			WHEN grouped.status = 'shipped'::TEXT
				AND grouped.customer_type = 'new'::TEXT THEN grouped.n_customers
				ELSE 0::bigint
			END AS new_customers_count,
			CASE
				WHEN grouped.status = 'shipped'::TEXT
				AND grouped.customer_type = 'returning'::TEXT THEN grouped.n_customers
				ELSE 0::bigint
			END AS returning_customers_count,
			CASE
				WHEN grouped.status = 'refunded'::TEXT
				AND grouped.customer_type = 'returning'::TEXT THEN grouped.n_customers
				ELSE 0::bigint
			END +
                CASE
				WHEN grouped.status = 'refunded'::TEXT
				AND grouped.customer_type = 'new'::TEXT THEN grouped.n_customers
				ELSE 0::bigint
			END AS refunded_customers_count,
			CASE
				WHEN grouped.status = 'shipped'::TEXT
				AND grouped.customer_type = 'new'::TEXT THEN grouped.payment_amount
				ELSE 0::NUMERIC
			END AS new_customers_revenue,
			CASE
				WHEN grouped.status = 'shipped'::TEXT
				AND grouped.customer_type = 'returning'::TEXT THEN grouped.payment_amount
				ELSE 0::NUMERIC
			END AS returning_customers_revenue,
			CASE
				WHEN grouped.status = 'refunded'::TEXT
				AND grouped.customer_type = 'returning'::TEXT THEN grouped.payment_amount
				ELSE 0::NUMERIC
			END +
                CASE
				WHEN grouped.status = 'refunded'::TEXT
				AND grouped.customer_type = 'new'::TEXT THEN grouped.payment_amount
				ELSE 0::NUMERIC
			END AS refunded_customers_sum,
			CASE
				WHEN grouped.status = 'refunded'::TEXT
				AND grouped.customer_type = 'returning'::TEXT THEN grouped.n_transactions
				ELSE 0::bigint
			END +
                CASE
				WHEN grouped.status = 'refunded'::TEXT
				AND grouped.customer_type = 'new'::TEXT THEN grouped.n_transactions
				ELSE 0::bigint
			END AS customers_refunded
		FROM
			grouped
        )
 SELECT
	almost.period_name,
	almost.period_id,
	almost.category_name,
	almost.cohort,
	sum(almost.new_customers_count) AS new_customers_count,
	sum(almost.returning_customers_count) AS returning_customers_count,
	sum(almost.refunded_customers_count) AS refunded_customers_count,
	sum(almost.new_customers_revenue) AS new_customers_revenue,
	sum(almost.returning_customers_revenue) AS returning_customers_revenue,
	sum(almost.refunded_customers_sum) AS refunded_customers_sum,
	sum(almost.customers_refunded) AS customers_refunded
FROM
	almost
GROUP BY
	almost.period_name,
	almost.period_id,
	almost.category_name,
	almost.cohort;