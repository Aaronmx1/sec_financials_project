-- Disable commits and foreign key checks to minimize import errors.
SET foreign_key_checks=0;
SET autocommit=0;

-- Drop existing tables to re-create them
DROP TABLE IF EXISTS fact_financial_reports;
DROP TABLE IF EXISTS dim_submissions;
DROP TABLE IF EXISTS dim_accounts;
DROP TABLE IF EXISTS dim_forms;
DROP TABLE IF EXISTS staging_fact_data;

/*-------------------------------------------
*               CREATE tables
*/-------------------------------------------
-- Create Submissions dimension table
CREATE TABLE dim_submissions (
    submission_id INT AUTO_INCREMENT PRIMARY KEY,
    entity_name VARCHAR(255) NOT NULL COMMENT 'Name of the registrant.',
    cik VARCHAR(10) NOT NULL COMMENT 'Central Index Key. A unique identifier for the registrant.',
    sic_description VARCHAR(255) NOT NULL,
    owner_organization VARCHAR(255) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    ein BIGINT NOT NULL
);

-- Create Accounts dimension table
CREATE TABLE dim_accounts (
    account_id INT AUTO_INCREMENT PRIMARY KEY,
    account_name VARCHAR(255) NOT NULL,
    description TEXT,
    financial_stmt_category VARCHAR(50)
);

-- Create Forms dimension table
CREATE TABLE dim_forms (
    form_id INT AUTO_INCREMENT PRIMARY KEY,
    fiscal_period VARCHAR(5) NOT NULL
);

-- Create Financial Reports fact table
CREATE TABLE fact_financial_reports (
    financial_report_id INT AUTO_INCREMENT PRIMARY KEY,
    submission_id INT NOT NULL,
    account_id INT NOT NULL,
    form_id INT NOT NULL,
    start_date DATE,
    end_date DATE NOT NULL,
    filed_date DATE NOT NULL,
    fiscal_year INT NOT NULL,
    amount BIGINT NOT NULL,
    FOREIGN KEY (submission_id) REFERENCES dim_submissions(submission_id),
    FOREIGN KEY (account_id) REFERENCES dim_accounts(account_id),
    FOREIGN KEY (form_id) REFERENCES dim_forms(form_id)
);

-- Create Staging Fact Data table
CREATE TABLE staging_fact_data (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    cik VARCHAR(10),
    entity_name VARCHAR(255),
    account_name VARCHAR(255),
    description TEXT,
    start_date DATE,
    end_date DATE,
    value BIGINT,
    accession_number VARCHAR(50),
    fiscal_year INT,
    fiscal_period VARCHAR(5),
    filed_date DATE,
    frame VARCHAR(10)
);

-- Re-enable commits and foreign key checks
COMMIT;
SET foreign_key_checks=1;