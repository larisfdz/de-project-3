CREATE VIEW mart.f_customer_retention AS
(WITH ct AS
--определяем тип клиента на каждой неделе (1 покупка - new, больше 1 - returned)
(
SELECT
	a.period_id,
	a.customer_id, 
	CASE
		WHEN a.n_buys = 1 THEN 'new'
		ELSE 'returning'
	END AS customer_type
FROM
	(
	SELECT
		DISTINCT EXTRACT(YEAR
	FROM
		fds.date_id) :: TEXT || 
	EXTRACT(week
	FROM
		fds.date_id) :: TEXT AS period_id,
		fds.customer_id,
		COUNT(*) AS n_buys
	FROM
		mart.f_daily_sales fds
	WHERE
		status = 'shipped'
	GROUP BY
		EXTRACT(YEAR
	FROM
		date_id) :: TEXT || 
	EXTRACT(week
	FROM
		date_id) :: TEXT,
		customer_id
	) AS a
	),
--добавляем название категории товаров, тип клиента, указание недели к основной таблице фактов
totals AS (
SELECT
	DISTINCT EXTRACT(YEAR
FROM
	date_id) :: TEXT || EXTRACT(week
FROM
	date_id) :: TEXT AS period_id,
	fds.date_id,
	fds.customer_id,
	ct.customer_type,
	di.category_name,
	fds.item_id,
	--CASE WHEN fds.status = 'shipped' THEN COALESCE(payment_amount,0) END AS revenue,
	--CASE WHEN fds.status = 'refunded' THEN COALESCE(payment_amount,0) END AS refunded
	fds.payment_amount,
	fds.status
FROM
	mart.f_daily_sales fds
LEFT JOIN mart.d_item di ON
	fds.item_id = di.item_id
LEFT JOIN ct ON
	fds.customer_id = ct.customer_id
	AND
(EXTRACT(YEAR
FROM
	fds.date_id) :: TEXT || EXTRACT(week
FROM
	fds.date_id) :: TEXT) = ct.period_id
),
--группируем и суммируем значения
grouped AS (
SELECT
	period_id,
	customer_type,
	status,
	category_name,
	COUNT(DISTINCT customer_id) AS n_customers,
	SUM(payment_amount) AS payment_amount,
	COUNT(*) AS n_transactions
FROM
	totals
GROUP BY
	period_id,
	customer_type,
	status,
	category_name
),
almost AS (
SELECT
	'weekly' AS period_name,
	period_id,
	category_name,
	CASE
		WHEN status = 'shipped'
			AND customer_type = 'new' THEN n_customers
			ELSE 0
		END AS new_customers_count,
		CASE
			WHEN status = 'shipped'
			AND customer_type = 'returning' THEN n_customers
			ELSE 0
		END AS returning_customers_count,
		CASE
			WHEN status = 'refunded'
			AND customer_type = 'returning' THEN n_customers
			ELSE 0
		END +
CASE
			WHEN status = 'refunded'
			AND customer_type = 'new' THEN n_customers
			ELSE 0
		END AS refunded_customers_count,
		CASE
			WHEN status = 'shipped'
			AND customer_type = 'new' THEN payment_amount
			ELSE 0
		END AS new_customers_revenue,
		CASE
			WHEN status = 'shipped'
			AND customer_type = 'returning' THEN payment_amount
			ELSE 0
		END AS returning_customers_revenue,
		CASE
			WHEN status = 'refunded'
			AND customer_type = 'returning' THEN payment_amount
			ELSE 0
		END +
CASE
			WHEN status = 'refunded'
			AND customer_type = 'new' THEN payment_amount
			ELSE 0
		END AS refunded_customers_sum,
		CASE
			WHEN status = 'refunded'
			AND customer_type = 'returning' THEN n_transactions
			ELSE 0
		END +
CASE
			WHEN status = 'refunded'
			AND customer_type = 'new' THEN n_transactions
			ELSE 0
		END AS customers_refunded
	FROM
		grouped
)
--из строк в столбцы, как указано в задании
SELECT
	period_name,
	period_id,
	category_name,
	SUM(new_customers_count) AS new_customers_count,
	SUM(returning_customers_count) AS returning_customers_count,
	SUM(refunded_customers_count) AS refunded_customers_count,
	SUM(new_customers_revenue) AS new_customers_revenue,
	SUM(returning_customers_revenue) AS returning_customers_revenue,
	SUM(refunded_customers_sum) refunded_customers_sum, --доп.поле - сумма возврата по всем клиентам
	SUM(customers_refunded) customers_refunded
FROM
	almost
GROUP BY
	period_name,
	period_id,
	category_name
);