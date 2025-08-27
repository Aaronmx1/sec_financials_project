-- Insert Fact Financial Report data
INSERT INTO fact_financial_reports (submission_id, account_id, form_id, start_date, end_date, filed_date, fiscal_year, amount)
SELECT
    ds.submission_id,
    da.account_id,
    df.form_id,
    staging.start_date,
    staging.end_date,
    staging.filed_date,
    staging.fiscal_year,
    staging.value
FROM
    staging_fact_data staging 
LEFT JOIN 
    dim_submissions ds ON staging.cik = ds.cik
LEFT JOIN
    dim_accounts da ON staging.account_name = da.account_name
LEFT JOIN
    dim_forms df ON staging.fiscal_period = df.fiscal_period