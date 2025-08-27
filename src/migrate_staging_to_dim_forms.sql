-- Insert Form Name and Fiscal Period into Forms table
INSERT INTO dim_forms (fiscal_period)
SELECT DISTINCT 
    fiscal_period
FROM staging_fact_data;