DROP TABLE IF EXISTS staging.dq_checks_results;
CREATE TABLE staging.dq_checks_results (
Table_name VARCHAR(255),
DQ_check_name VARCHAR(255),
Datetime TIMESTAMP,
DQ_check_result decimal(8,2));