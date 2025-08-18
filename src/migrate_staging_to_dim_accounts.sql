-- Insert Income Statement (IS) accounts and Balance Sheet (BS) accounts classification
INSERT INTO dim_accounts (account_name, description, financial_stmt_category)
SELECT DISTINCT
    account_name,
    description,
    CASE
        WHEN start_date IS NOT NULL THEN 'IS' -- If start_date exists, then it's an Income Statement item
        ELSE 'BS' -- Otherwise, it's a Balance Sheet item
    END AS financial_stmt_category
FROM
    staging_fact_data;