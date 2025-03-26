from airflow import DAG 
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime 
import pandas as pd 
from faker import Faker 
import random 
from google.cloud import storage 
import io 

# configuration
PROJECT_ID = 'coherent-graph-411320'
BUCKET_NAME = 'arshavin_sales_transactions'
GCS_PATH = 'bulk_transactions.csv'
BIGQUERY_DATASET = 'transactions'
BIGQUERY_TABLE = 'raw_orders'
TRANSFORMED_AMOUNT_CATEGORY_TABLE = 'transformed_amount_sales_orders'
TRANSFORMED_STATUS_AMOUNT_TABLE = 'transformed_status_amount'
SOURCE_URI = f"gs://{BUCKET_NAME}/{GCS_PATH}"


# schema definition
schema_fields = [
    {"name":"order_id","type":"INTEGER","mode":"REQUIRED"},
    {"name":"customer_name","type":"STRING","mode":"NULLABLE"},
    {"name":"order_amount","type":"FLOAT","mode":"NULLABLE"},
    {"name":"order_date","type":"DATE","mode":"NULLABLE"},
    {"name":"product","type":"STRING","mode":"NULLABLE"},
    {"name":"status","type":"STRING","mode":"NULLABLE"},
    {"name":"transaction_channel","type":"STRING","mode":"NULLABLE"},
    {"name":"transaction_time","type":"TIMESTAMP","mode":"NULLABLE"}
]

def generate_and_upload_sales_data(bucket_name,gcs_path,num_orders=500):
    fake = Faker()
    data = {
        "order_id": [i for i in range(1,num_orders+1)],
        "customer_name": [fake.name() for _ in range(num_orders)],
        "order_amount": [round(random.uniform(10.0,1000.0),2) for _ in range(num_orders)],
        "order_date": [fake.date_between(start_date='-30d',end_date='today') for _ in range(num_orders)],
        "product": [random.choice(["SHIRT", 'SHOE', 'BELT', 'EYE-GLASSES','TROUSERS','BANGLES','GOLD-CHAINS']) for _ in range(num_orders)],
        "status": [random.choice(["CREATED", 'SHIPPED', 'DELIVERED', 'CANCELED']) for _ in range(num_orders)],
        "transaction_channel": [random.choice(["BANK_TRANSER", 'CREDIT_CARD','DEBIT_CARD','CASH']) for _ in range(num_orders)],
        "transaction_time": [datetime.now() for _ in range(num_orders)]  #Simulate CDC timestamp

    }
    df = pd.DataFrame(data)

    # Convert to CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f'Data uploaded to gs://{bucket_name}/{gcs_path}')

# Default arguments
default_args = {
    "start_date": datetime(2025, 3, 26),
    "catchup": False 
}

# DAG definition
with DAG(
    "sales_orders_to_bigquery_with_transformation",
    default_args=default_args,
    schedule_interval=None, #Trigger manually 
) as dag:
    
    start_task = EmptyOperator(task_id='start')

    generate_sales_data = PythonOperator(
        task_id='generate_sales_data',
        python_callable=generate_and_upload_sales_data,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "gcs_path": GCS_PATH,
            "num_orders": 50000 #Num of orders to generate
        },
        )

    load_to_bigquery = BigQueryInsertJobOperator(
        task_id  ='load_to_bigquery',
        configuration = {
            "load": {
                "sourceUrls": [SOURCE_URI],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": BIGQUERY_TABLE,},
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_APPEND",
                "skipLeadingRows": 1, # Skip CSV header row
                "schema": {"fields": schema_fields,
                           },
                    }
            },
    )

    with TaskGroup(group_id='transformation_tasks') as upstream_tasks:
    # categorize orders by status, amount, 
        transform_amount_category_query = f"""
            SELECT order_id,customer_name,order_amount,
                CASE WHEN order_amount < 100 THEN 'Small'
                    WHEN order_amount BETWEEN 100 AND 500 THEN 'Medium'
                    ELSE 'Large'
                END AS order_category,
                order_date,
                product,
                CURRENT_TIMESTAMP() AS batch_load_timestamp
            FROM '{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}'
        """

        # categorize orders by status, amount, 
        transform_status_amount_query = f"""
            SELECT status,
            COUNT(1) num_of_sales,
            SUM(order_amount) total_sales
            FROM '{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}'
            GROUP BY 1
        """

        transform_amount_category = BigQueryInsertJobOperator(
            task_id="transform_amount_category",
            configuration={
                "query": {
                    "query": transform_amount_category_query,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": TRANSFORMED_AMOUNT_CATEGORY_TABLE,
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "useLegacySql": False,
                }
            }
        )


        # transform_amount_category = BigQueryExecuteQueryOperator(
        #     task_id = "transform_amount_category",
        #     sql=transform_amount_category_query,
        #     destination_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
        #     write_destination="WRITE_TRUNCATE",
        #     use_legacy_sql=False,
        # )

        # transform_status_amount = BigQueryExecuteQueryOperator(
        #     task_id = "transform_status_amount",
        #     sql=transform_status_amount_query,
        #     destination_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
        #     write_destination="WRITE_TRUNCATE",
        #     use_legacy_sql=False,
        # )

        transform_status_amount = BigQueryInsertJobOperator(
        task_id="transform_status_amount",
        configuration={
                "query": {
                    "query": transform_status_amount_query,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": TRANSFORMED_STATUS_AMOUNT_TABLE,
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "useLegacySql": False,
                }
            }
        )

    end_task = EmptyOperator(task_id='end')

    # Task depencies
    start_task >> generate_sales_data >> load_to_bigquery >> [transform_amount_category,transform_status_amount] >> end_task