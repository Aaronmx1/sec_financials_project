-- Insert Form Name and Fiscal Period into Forms table
INSERT INTO dim_forms (form_name, fiscal_period)
SELECT DISTINCT
    form, 
    fiscal_period
FROM staging_fact_data;