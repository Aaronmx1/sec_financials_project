-- Truncate tables script to speed up re-loads
SET FOREIGN_KEY_CHECKS=0;
TRUNCATE TABLE dim_forms;
TRUNCATE TABLE dim_submissions;
TRUNCATE TABLE dim_accounts;
TRUNCATE TABLE staging_fact_data;
TRUNCATE TABLE fact_financial_reports;
SET FOREIGN_KEY_CHECKS=1;