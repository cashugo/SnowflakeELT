CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB;
CREATE SCHEMA IF NOT EXISTS RAW_STAGING;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW_STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- I. External Stage Setup (Azure Blob Storage)
CREATE OR REPLACE STAGE azure_external_stage
    URL = 'azure://portfoliopracticerep67.blob.core.windows.net/practicefilesrep/'
    CREDENTIALS = (AZURE_SAS_TOKEN = '?sv=YOUR_AZURE_SAS_TOKEN_HERE');

-- II. File Formats
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE FILE FORMAT json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

-- III. Staging Tables (CSV & JSON)
CREATE TABLE IF NOT EXISTS stg_customers_csv (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    age INT,
    email STRING
);

CREATE TABLE IF NOT EXISTS stg_customers_json_raw (
    raw_payload VARIANT
);

-- IV. Master Table Setup
CREATE TABLE IF NOT EXISTS ANALYTICS.dim_customers (
    customer_id INT PRIMARY KEY,
    first_name STRING,
    last_name STRING,
    age INT,
    email STRING,
    postal_code STRING,
    loyalty_points INT,
    membership_status STRING,
    preferred_payment_method STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- V. Create Streams BEFORE Ingestion (Catches initial load delta)
CREATE OR REPLACE STREAM csv_stream ON TABLE stg_customers_csv;
CREATE OR REPLACE STREAM json_stream ON TABLE stg_customers_json_raw;

-- VI. Ingestion through Snowpipe
CREATE OR REPLACE PIPE csv_pipe
AS
    COPY INTO RAW_STAGING.stg_customers_csv (customer_id, first_name, last_name, age, email)
    FROM @azure_external_stage/
    PATTERN = 'CustomerData.*\.csv'
    FILE_FORMAT = (FORMAT_NAME = 'csv_format');

CREATE OR REPLACE PIPE json_pipe
AS
    COPY INTO RAW_STAGING.stg_customers_json_raw (raw_payload)
    FROM @azure_external_stage/
    PATTERN = '.*CustomerDataSuppInfo.*\.json'
    FILE_FORMAT = (FORMAT_NAME = 'json_format');

-- VII. Ingestion Trigger
ALTER PIPE csv_pipe REFRESH;
ALTER PIPE json_pipe REFRESH;    

-- VIII. Incremental Merging Task Definition
CREATE OR REPLACE TASK tsk_automerge_dim_customers
    WAREHOUSE = PORTFOLIO_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ECOMMERCE_DB.RAW_STAGING.csv_stream') 
      OR SYSTEM$STREAM_HAS_DATA('ECOMMERCE_DB.RAW_STAGING.json_stream')
AS
MERGE INTO ANALYTICS.dim_customers target
USING (
    SELECT 
        c.customer_id, c.first_name, c.last_name, c.age, c.email,
        COALESCE(j.postal_code, 'Unknown') AS postal_code,
        COALESCE(j.loyalty_points, 0) AS loyalty_points,
        COALESCE(j.membership_status, 'Non-Member') AS membership_status,
        COALESCE(j.preferred_payment_method, 'Unknown') AS preferred_payment_method
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS rn
        FROM RAW_STAGING.csv_stream
        WHERE METADATA$ACTION = 'INSERT'
    ) c
    LEFT JOIN (
        SELECT 
            raw_payload:customer_id::INT AS customer_id,
            raw_payload:postal_code::STRING AS postal_code,
            raw_payload:loyalty_points::INT AS loyalty_points,
            raw_payload:membership_status::STRING AS membership_status,
            raw_payload:preferred_payment_method::STRING AS preferred_payment_method,
            ROW_NUMBER() OVER (PARTITION BY raw_payload:customer_id::INT ORDER BY raw_payload:customer_id::INT) AS rn
        FROM RAW_STAGING.json_stream
        WHERE METADATA$ACTION = 'INSERT'
    ) j ON c.customer_id = j.customer_id AND j.rn = 1
    WHERE c.rn = 1
) src
ON target.customer_id = src.customer_id

WHEN MATCHED THEN UPDATE SET
    target.first_name = src.first_name,
    target.last_name = src.last_name,
    target.age = src.age,
    target.email = src.email,
    target.postal_code = src.postal_code,
    target.loyalty_points = src.loyalty_points,
    target.membership_status = src.membership_status,
    target.preferred_payment_method = src.preferred_payment_method,
    target.updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    customer_id, first_name, last_name, age, email,
    postal_code, loyalty_points, membership_status, preferred_payment_method, created_at, updated_at
) VALUES (
    src.customer_id, src.first_name, src.last_name, src.age, src.email,
    src.postal_code, src.loyalty_points, src.membership_status, src.preferred_payment_method, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- IX. Enable Task Execution
ALTER TASK tsk_automerge_dim_customers RESUME;

-- X. Quality Validation
SELECT customer_id, COUNT(*) FROM RAW_STAGING.stg_customers_csv
GROUP BY customer_id HAVING COUNT(*) > 1;

SELECT * FROM ANALYTICS.dim_customers;