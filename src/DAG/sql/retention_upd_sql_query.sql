DELETE
FROM
	mart.f_customer_retention
WHERE
	period_id IN (
	SELECT
		DISTINCT date_part('year'::TEXT, date_id)::TEXT || date_part('week'::TEXT, date_id)::TEXT
	FROM
		mart.f_daily_sales);

INSERT
	INTO
	mart.f_customer_retention (
period_name,
	period_id,
	category_name,
	new_customers_count,
	returning_customers_count,
	refunded_customers_count,
	new_customers_revenue,
	returning_customers_revenue,
	refunded_customers_sum,
	customers_refunded)

WITH ct AS (
	SELECT
		a.period_id,
		a.customer_id,
		CASE
			WHEN a.n_buys = 1 THEN 'new'::TEXT
			ELSE 'returning'::TEXT
		END AS customer_type
	FROM
		(
		SELECT
			DISTINCT date_part('year'::TEXT, fds.date_id)::TEXT || date_part('week'::TEXT, fds.date_id)::TEXT AS period_id,
			fds.customer_id,
			count(*) AS n_buys
		FROM
			mart.f_daily_sales fds
		WHERE
			fds.status = 'shipped'::TEXT
		GROUP BY
			(date_part('year'::TEXT, fds.date_id)::TEXT || date_part('week'::TEXT, fds.date_id)::TEXT),
			fds.customer_id) a
        ),
	totals AS (
	SELECT
		DISTINCT date_part('year'::TEXT, fds.date_id)::TEXT || date_part('week'::TEXT, fds.date_id)::TEXT AS period_id,
		fds.date_id,
		fds.customer_id,
		ct.customer_type,
		di.category_name,
		fds.item_id,
		fds.payment_amount,
		fds.status
	FROM
		mart.f_daily_sales fds
	LEFT JOIN mart.d_item di ON
		fds.item_id = di.item_id
	LEFT JOIN ct ON
		fds.customer_id = ct.customer_id
		AND (date_part('year'::TEXT, fds.date_id)::TEXT || date_part('week'::TEXT, fds.date_id)::TEXT) = ct.period_id
        ),
	grouped AS (
	SELECT
		totals.period_id,
		totals.customer_type,
		totals.status,
		totals.category_name,
		count(DISTINCT totals.customer_id) AS n_customers,
		sum(totals.payment_amount) AS payment_amount,
		count(*) AS n_transactions
	FROM
		totals
	GROUP BY
		totals.period_id,
		totals.customer_type,
		totals.status,
		totals.category_name
        ),
	almost AS (
	SELECT
		'weekly'::TEXT AS period_name,
		grouped.period_id,
		grouped.category_name,
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
	almost.category_name;
