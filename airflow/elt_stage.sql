CREATE OR REPLACE STORAGE INTEGRATION s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
STORAGE_AWS_ROLE_ARN = 'unique_IAM_role'
ENABLED = TRUE 
STORAGE_ALLOWED_LOCATIONS = ('s3://daily-transactions/orders/');

CREATE OR REPLACE STAGE dbt_db.staging.s3_orders_stage
STORAGE_INTEGRATION = s3_integration
URL = 's3://daily-transactions/orders/'
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

CREATE OR REPLACE TASK load_new_orders
WAREHOUSE = DBT_WH
SCHEDULE = '1 HOUR'
AS
COPY INTO dbt_db.staging.incremental_orders
FROM @dbt_db.staging.s3_orders_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
PATTERN = '.*\.csv'
ON_ERROR = 'CONTINUE';


ALTER TASK load_new_orders RESUME;


